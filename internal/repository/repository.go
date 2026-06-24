package repository

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/tursom/turbk/internal/config"
	"github.com/zeebo/blake3"
)

type Repository struct {
	opts    Options
	index   *chunkIndex
	writer  *segmentWriter
	reader  segmentReader
	encoder *zstd.Encoder
	decoder *zstd.Decoder
	mu      sync.Mutex
}

type PutChunkResult struct {
	Ref     ChunkRef
	Existed bool
}

type putChunkRequest struct {
	index      int
	data       []byte
	hash       [32]byte
	hashString string
}

func Open(ctx context.Context, cfg config.Config) (*Repository, error) {
	segmentSize, err := parseSize(cfg.Repository.SegmentSize, defaultSegmentSize)
	if err != nil {
		return nil, err
	}
	chunkAvgSize, err := parseSize(cfg.Repository.ChunkAvgSize, defaultChunkAvgSize)
	if err != nil {
		return nil, err
	}
	opts := Options{
		StateDir:     cfg.Paths.StateDir,
		RepoDir:      cfg.Paths.RepoDir,
		SegmentSize:  segmentSize,
		ChunkAvgSize: int(chunkAvgSize),
	}
	return OpenWithOptions(ctx, opts)
}

func OpenWithOptions(_ context.Context, opts Options) (*Repository, error) {
	if opts.SegmentSize <= 0 {
		opts.SegmentSize = defaultSegmentSize
	}
	if opts.ChunkAvgSize <= 0 {
		opts.ChunkAvgSize = defaultChunkAvgSize
	}
	for _, dir := range []string{opts.StateDir, opts.RepoDir, filepath.Join(opts.StateDir, "manifests")} {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, fmt.Errorf("create repository dir %q: %w", dir, err)
		}
	}
	key, err := loadOrCreateMasterKey(opts.StateDir)
	if err != nil {
		return nil, err
	}
	aead, err := newAEAD(key)
	if err != nil {
		return nil, err
	}
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, fmt.Errorf("create zstd encoder: %w", err)
	}
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		encoder.Close()
		return nil, fmt.Errorf("create zstd decoder: %w", err)
	}
	index, err := openChunkIndex(opts.StateDir)
	if err != nil {
		encoder.Close()
		decoder.Close()
		return nil, err
	}
	writer, err := openSegmentWriter(opts.RepoDir, opts.SegmentSize, aead, encoder)
	if err != nil {
		_ = index.Close()
		encoder.Close()
		decoder.Close()
		return nil, err
	}
	return &Repository{
		opts:    opts,
		index:   index,
		writer:  writer,
		reader:  newSegmentReader(opts.RepoDir, aead, decoder),
		encoder: encoder,
		decoder: decoder,
	}, nil
}

func (r *Repository) Close() error {
	if r == nil {
		return nil
	}
	var err error
	if r.writer != nil {
		err = r.writer.Close()
	}
	if r.index != nil {
		if closeErr := r.index.Close(); err == nil {
			err = closeErr
		}
	}
	if r.encoder != nil {
		r.encoder.Close()
	}
	if r.decoder != nil {
		r.decoder.Close()
	}
	return err
}

func (r *Repository) PutChunk(ctx context.Context, data []byte) (ChunkRef, bool, error) {
	results, err := r.PutChunks(ctx, [][]byte{data})
	if err != nil {
		return ChunkRef{}, false, err
	}
	if len(results) != 1 {
		return ChunkRef{}, false, fmt.Errorf("put chunk returned %d results", len(results))
	}
	return results[0].Ref, results[0].Existed, nil
}

func (r *Repository) PutChunks(ctx context.Context, chunks [][]byte) ([]PutChunkResult, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	requests := make([]putChunkRequest, len(chunks))
	for i, data := range chunks {
		hash := blake3.Sum256(data)
		requests[i] = putChunkRequest{
			index:      i,
			data:       data,
			hash:       hash,
			hashString: hex.EncodeToString(hash[:]),
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}

	results := make([]PutChunkResult, len(chunks))
	pendingByHash := make(map[string]int)
	aliasesByPending := make(map[int][]int)
	pendingResultIndexes := make([]int, 0, len(chunks))
	pendingWrites := make([]segmentWriteChunk, 0, len(chunks))
	for _, request := range requests {
		if pendingIndex, ok := pendingByHash[request.hashString]; ok {
			aliasesByPending[pendingIndex] = append(aliasesByPending[pendingIndex], request.index)
			continue
		}
		if ref, ok, err := r.index.Get(request.hashString); err != nil {
			return nil, err
		} else if ok {
			results[request.index] = PutChunkResult{Ref: ref, Existed: true}
			continue
		}
		pendingByHash[request.hashString] = len(pendingWrites)
		pendingResultIndexes = append(pendingResultIndexes, request.index)
		pendingWrites = append(pendingWrites, segmentWriteChunk{Hash: request.hash, Data: request.data})
	}
	if len(pendingWrites) == 0 {
		return results, nil
	}
	refs, err := r.writer.WriteChunks(pendingWrites)
	if err != nil {
		return nil, err
	}
	if len(refs) != len(pendingWrites) {
		return nil, fmt.Errorf("put chunks wrote %d refs for %d chunks", len(refs), len(pendingWrites))
	}
	if err := r.index.PutBatch(refs); err != nil {
		return nil, err
	}
	for pendingIndex, ref := range refs {
		resultIndex := pendingResultIndexes[pendingIndex]
		results[resultIndex] = PutChunkResult{Ref: ref}
		for _, aliasIndex := range aliasesByPending[pendingIndex] {
			results[aliasIndex] = PutChunkResult{Ref: ref, Existed: true}
		}
	}
	return results, nil
}

func (r *Repository) RotateSegmentForMaintenance(_ context.Context) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.writer == nil {
		return 0, fmt.Errorf("segment writer is closed")
	}
	if r.writer.offset > 0 {
		if err := r.writer.rotate(); err != nil {
			return 0, err
		}
	}
	return r.writer.id, nil
}

func (r *Repository) RewriteChunkRef(ctx context.Context, oldRef ChunkRef) (ChunkRef, error) {
	if err := ctx.Err(); err != nil {
		return ChunkRef{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	data, err := r.reader.Read(oldRef)
	if err != nil {
		return ChunkRef{}, err
	}
	hash := blake3.Sum256(data)
	hashString := hex.EncodeToString(hash[:])
	if hashString != oldRef.Hash {
		return ChunkRef{}, fmt.Errorf("chunk %s rewrote as %s", oldRef.Hash, hashString)
	}
	newRef, err := r.writer.WriteChunk(hash, data)
	if err != nil {
		return ChunkRef{}, err
	}
	if err := r.index.Put(newRef); err != nil {
		return ChunkRef{}, err
	}
	return newRef, nil
}

func (r *Repository) DeleteUnreferencedChunks(_ context.Context, keep map[string]struct{}) (int64, error) {
	stats, err := r.DeleteUnreferencedChunkStats(context.Background(), keep)
	return stats.Count, err
}

func (r *Repository) DeleteUnreferencedChunkStats(_ context.Context, keep map[string]struct{}) (CleanupStats, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.index.deleteUnreferenced(keep, time.Time{})
}

func (r *Repository) DeleteUnreferencedChunksOlderThan(_ context.Context, keep map[string]struct{}, cutoff time.Time) (CleanupStats, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.index.DeleteUnreferencedOlderThan(keep, cutoff)
}

func (r *Repository) DeleteSegmentsExcept(_ context.Context, keep map[int64]struct{}) (int, int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.writer == nil {
		return 0, 0, fmt.Errorf("segment writer is closed")
	}
	if keep == nil {
		keep = make(map[int64]struct{})
	}
	keep[r.writer.id] = struct{}{}
	ids, err := listSegmentIDs(r.writer.dir)
	if err != nil {
		return 0, 0, err
	}
	var removed int
	var removedBytes int64
	for _, id := range ids {
		if _, ok := keep[id]; ok {
			continue
		}
		path := segmentPath(r.writer.dir, id)
		info, err := os.Stat(path)
		if err != nil {
			return removed, removedBytes, fmt.Errorf("stat segment %d before delete: %w", id, err)
		}
		if err := os.Remove(path); err != nil {
			return removed, removedBytes, fmt.Errorf("delete segment %d: %w", id, err)
		}
		removed++
		removedBytes += info.Size()
	}
	return removed, removedBytes, nil
}

func (r *Repository) HasChunk(_ context.Context, hash string) (bool, error) {
	_, ok, err := r.index.Get(hash)
	return ok, err
}

func (r *Repository) HasChunks(ctx context.Context, hashes []string) (map[string]struct{}, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	refs, err := r.index.GetBatch(hashes)
	if err != nil {
		return nil, err
	}
	exists := make(map[string]struct{}, len(refs))
	for hash := range refs {
		exists[hash] = struct{}{}
	}
	return exists, nil
}

func (r *Repository) GetChunkRef(_ context.Context, hash string) (ChunkRef, bool, error) {
	return r.index.Get(hash)
}

func (r *Repository) ReadChunk(_ context.Context, hash string) ([]byte, error) {
	ref, ok, err := r.index.Get(hash)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("chunk %s not found", hash)
	}
	return r.ReadChunkRef(context.Background(), ref)
}

func (r *Repository) ReadChunkRef(_ context.Context, ref ChunkRef) ([]byte, error) {
	return r.reader.Read(ref)
}

func (r *Repository) Stats() (Stats, error) {
	chunks, logicalBytes, compressedBytes, err := r.index.Stats()
	if err != nil {
		return Stats{}, err
	}
	segments, segmentBytes, err := segmentStats(r.opts.RepoDir)
	if err != nil {
		return Stats{}, err
	}
	manifestCount, err := countManifests(r.opts.StateDir)
	if err != nil {
		return Stats{}, err
	}
	return Stats{
		Segments:          segments,
		SegmentBytes:      segmentBytes,
		Chunks:            chunks,
		LogicalBytes:      logicalBytes,
		CompressedBytes:   compressedBytes,
		SegmentSize:       r.opts.SegmentSize,
		ChunkAvgSize:      r.opts.ChunkAvgSize,
		ManifestCount:     manifestCount,
		AppendOnlyRecords: true,
	}, nil
}

func (r *Repository) Chunker() Chunker {
	return NewChunker(r.opts.ChunkAvgSize)
}
