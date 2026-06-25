package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/tursom/turbk/internal/repository"
	"github.com/zeebo/blake3"
)

type chunkPipelineByteWindow struct {
	limit   int64
	used    int64
	maxUsed int64
	mu      sync.Mutex
}

func newChunkPipelineByteWindow(limit int64) *chunkPipelineByteWindow {
	if limit <= 0 {
		limit = defaultAgentMaxChunkPipelineBytes
	}
	return &chunkPipelineByteWindow{limit: limit}
}

func (w *chunkPipelineByteWindow) tryAcquire(n int64) bool {
	if n <= 0 {
		return true
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.used+n > w.limit {
		return false
	}
	w.used += n
	if w.used > w.maxUsed {
		w.maxUsed = w.used
	}
	return true
}

func (w *chunkPipelineByteWindow) release(n int64) {
	if n <= 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.used -= n
	if w.used < 0 {
		w.used = 0
	}
}

func (w *chunkPipelineByteWindow) max() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.maxUsed
}

type pipelineBatch struct {
	id           int64
	chunks       []*pendingBatchChunk
	requestBytes int64
	createdAt    time.Time
}

type pipelineResult struct {
	batch          *pipelineBatch
	checked        checkChunksResponse
	uploaded       uploadChunksResponse
	uploadedByHash map[string]uploadChunkResponse
	checkDuration  time.Duration
	uploadDuration time.Duration
	err            error
}

type agentChunkPipelineBatcher struct {
	client           *agentClient
	opts             backupRunOptions
	logger           *slog.Logger
	maxChunks        int
	maxRequestBytes  int64
	maxResponseBytes int64
	checkQueue       chan *pipelineBatch
	results          chan pipelineResult
	uploadSlots      chan struct{}
	done             chan struct{}
	stopOnce         sync.Once
	wg               sync.WaitGroup
	bytesWindow      *chunkPipelineByteWindow
	pending          []*pendingBatchChunk
	pendingByHash    map[string]*pendingBatchChunk
	pendingByEntry   map[*repository.FileEntry]int
	requestBytes     int64
	nextBatchID      int64
	err              error
}

func newAgentChunkPipelineBatcher(client *agentClient, opts backupRunOptions, logger *slog.Logger) *agentChunkPipelineBatcher {
	maxChunks := normalizedAgentChunkCheckBatchLimit(opts.MaxChunkCheckBatch)
	maxRequestBytes := opts.MaxChunkUploadBatchBytes
	if maxRequestBytes <= 0 {
		maxRequestBytes = defaultAgentChunkUploadBatchBytes
	}
	maxResponseBytes := opts.MaxChunkResponseBytes
	if maxResponseBytes <= 0 {
		maxResponseBytes = defaultAgentMaxChunkResponseBytes
	}
	checkInflight := opts.MaxChunkCheckInflight
	if checkInflight <= 0 {
		checkInflight = 1
	}
	uploadInflight := opts.MaxChunkUploadInflight
	if uploadInflight <= 0 {
		uploadInflight = 1
	}
	queueSize := checkInflight*2 + uploadInflight + 4
	b := &agentChunkPipelineBatcher{
		client:           client,
		opts:             opts,
		logger:           logger,
		maxChunks:        maxChunks,
		maxRequestBytes:  maxRequestBytes,
		maxResponseBytes: maxResponseBytes,
		checkQueue:       make(chan *pipelineBatch, queueSize),
		results:          make(chan pipelineResult, queueSize),
		uploadSlots:      make(chan struct{}, uploadInflight),
		done:             make(chan struct{}),
		bytesWindow:      newChunkPipelineByteWindow(opts.MaxChunkPipelineBytes),
		pendingByHash:    make(map[string]*pendingBatchChunk),
		pendingByEntry:   make(map[*repository.FileEntry]int),
	}
	for i := 0; i < checkInflight; i++ {
		b.wg.Add(1)
		go b.worker()
	}
	return b
}

func (b *agentChunkPipelineBatcher) Add(chunk []byte, entry *repository.FileEntry) (chunkBatchStats, error) {
	stats, err := b.drainResults()
	if err != nil {
		return stats, err
	}
	if b.err != nil {
		return stats, b.err
	}
	sum := blake3.Sum256(chunk)
	hash := hex.EncodeToString(sum[:])
	if b.opts.Catalog != nil {
		status, confirmedGeneration, ok, err := b.opts.Catalog.chunkStatus(hash)
		if err != nil {
			return stats, err
		}
		if ok && status == "confirmed" && confirmedGeneration >= b.opts.ChunkGeneration {
			entry.Chunks = append(entry.Chunks, repository.ChunkRef{Hash: hash, OriginalSize: int64(len(chunk))})
			stats.Reused++
			return stats, nil
		}
	}

	index := len(entry.Chunks)
	entry.Chunks = append(entry.Chunks, repository.ChunkRef{})
	waiter := chunkBatchWaiter{entry: entry, index: index}
	if pending, ok := b.pendingByHash[hash]; ok {
		pending.waiters = append(pending.waiters, waiter)
		b.pendingByEntry[entry]++
		return stats, nil
	}

	chunkRequestBytes := int64(32 + 8 + len(chunk))
	if 12+chunkRequestBytes > b.maxRequestBytes {
		return stats, fmt.Errorf("chunk %s size %d exceeds max batch request size %d", hash, len(chunk), b.maxRequestBytes)
	}
	if int64(len(chunk)) > b.bytesWindow.limit {
		return stats, fmt.Errorf("chunk %s size %d exceeds max pipeline bytes %d", hash, len(chunk), b.bytesWindow.limit)
	}
	if len(b.pending) > 0 && b.shouldFlushBeforeAdding(chunkRequestBytes) {
		flushed, err := b.submitPending()
		stats.Add(flushed)
		if err != nil {
			return stats, err
		}
	}
	waitStarted := time.Now()
	for !b.bytesWindow.tryAcquire(int64(len(chunk))) {
		if len(b.pending) > 0 {
			flushed, err := b.submitPending()
			stats.Add(flushed)
			if err != nil {
				return stats, err
			}
			continue
		}
		waited, err := b.waitApplyResult()
		stats.Add(waited)
		if err != nil {
			return stats, err
		}
	}
	if waited := time.Since(waitStarted); waited > 0 {
		stats.PipelineWait += waited
	}

	data := append([]byte(nil), chunk...)
	if b.requestBytes == 0 {
		b.requestBytes = 12
	}
	pending := &pendingBatchChunk{
		hash:         hash,
		data:         data,
		originalSize: int64(len(data)),
		waiters:      []chunkBatchWaiter{waiter},
	}
	b.pending = append(b.pending, pending)
	b.pendingByHash[hash] = pending
	b.pendingByEntry[entry]++
	b.requestBytes += chunkRequestBytes
	if len(b.pending) >= b.maxChunks ||
		b.requestBytes >= b.maxRequestBytes ||
		b.estimatedCheckResponseBytes(len(b.pending)) >= b.maxResponseBytes ||
		b.estimatedUploadResponseBytes(len(b.pending)) >= b.maxResponseBytes {
		flushed, err := b.submitPending()
		stats.Add(flushed)
		if err != nil {
			return stats, err
		}
	}
	stats.MaxPipelineBytes = b.bytesWindow.max()
	return stats, nil
}

func (b *agentChunkPipelineBatcher) Flush() (chunkBatchStats, error) {
	stats, err := b.submitPending()
	if err != nil {
		return stats, err
	}
	for {
		drained, err := b.drainResults()
		stats.Add(drained)
		if err != nil {
			return stats, err
		}
		if b.err != nil {
			return stats, b.err
		}
		if len(b.pending) == 0 && len(b.pendingByHash) == 0 {
			stats.MaxPipelineBytes = b.bytesWindow.max()
			return stats, nil
		}
		waited, err := b.waitApplyResult()
		stats.Add(waited)
		if err != nil {
			return stats, err
		}
	}
}

func (b *agentChunkPipelineBatcher) EntryPending(entry *repository.FileEntry) bool {
	_, _ = b.drainResults()
	return b.pendingByEntry[entry] > 0
}

func (b *agentChunkPipelineBatcher) Stop() {
	b.stopOnce.Do(func() {
		close(b.done)
	})
	b.wg.Wait()
}

func (b *agentChunkPipelineBatcher) submitPending() (chunkBatchStats, error) {
	var stats chunkBatchStats
	if len(b.pending) == 0 {
		stats.MaxPipelineBytes = b.bytesWindow.max()
		return stats, b.err
	}
	b.nextBatchID++
	batch := &pipelineBatch{
		id:           b.nextBatchID,
		chunks:       append([]*pendingBatchChunk(nil), b.pending...),
		requestBytes: b.requestBytes,
		createdAt:    time.Now(),
	}
	b.pending = b.pending[:0]
	b.requestBytes = 0
	for {
		if b.err != nil {
			return stats, b.err
		}
		select {
		case b.checkQueue <- batch:
			stats.MaxPipelineBytes = b.bytesWindow.max()
			return stats, nil
		case result := <-b.results:
			applied, err := b.applyResult(result)
			stats.Add(applied)
			if err != nil {
				return stats, err
			}
		case <-b.done:
			if b.err != nil {
				return stats, b.err
			}
			return stats, errors.New("chunk pipeline stopped")
		}
	}
}

func (b *agentChunkPipelineBatcher) drainResults() (chunkBatchStats, error) {
	var stats chunkBatchStats
	for {
		select {
		case result := <-b.results:
			applied, err := b.applyResult(result)
			stats.Add(applied)
			if err != nil {
				return stats, err
			}
		default:
			stats.MaxPipelineBytes = b.bytesWindow.max()
			return stats, b.err
		}
	}
}

func (b *agentChunkPipelineBatcher) waitApplyResult() (chunkBatchStats, error) {
	select {
	case result := <-b.results:
		return b.applyResult(result)
	case <-b.done:
		if b.err != nil {
			return chunkBatchStats{}, b.err
		}
		return chunkBatchStats{}, errors.New("chunk pipeline stopped")
	}
}

func (b *agentChunkPipelineBatcher) applyResult(result pipelineResult) (chunkBatchStats, error) {
	stats := chunkBatchStats{
		CheckDuration:    result.checkDuration,
		UploadDuration:   result.uploadDuration,
		MaxPipelineBytes: b.bytesWindow.max(),
	}
	if result.err != nil {
		b.err = result.err
		return stats, result.err
	}
	if result.batch == nil {
		return stats, nil
	}
	hashes := make([]string, 0, len(result.batch.chunks))
	byHash := make(map[string]*pendingBatchChunk, len(result.batch.chunks))
	for _, chunk := range result.batch.chunks {
		hashes = append(hashes, chunk.hash)
		byHash[chunk.hash] = chunk
	}
	stats.CheckRequests++
	stats.CheckResponseBytes += result.checked.ResponseBytes
	stats.BatchRetries += result.checked.RetryCount
	catalogUpdates := make([]agentChunkStatusUpdate, 0, len(result.batch.chunks))
	checkGeneration := result.checked.ChunkGeneration
	if checkGeneration == 0 {
		checkGeneration = b.opts.ChunkGeneration
	}
	for _, hash := range existingChunkHashes(hashes, result.checked, b.opts.CompactChunkCheckResponse) {
		chunk, ok := byHash[hash]
		if !ok {
			b.err = fmt.Errorf("server check returned unexpected chunk %s", hash)
			return stats, b.err
		}
		ref := repository.ChunkRef{Hash: hash, OriginalSize: chunk.originalSize}
		b.resolveChunk(chunk, ref)
		stats.Reused += int64(len(chunk.waiters))
		if b.opts.Catalog != nil {
			catalogUpdates = append(catalogUpdates, agentChunkStatusUpdate{
				Hash:         hash,
				OriginalSize: chunk.originalSize,
				Status:       "confirmed",
				Generation:   checkGeneration,
			})
		}
	}
	if len(result.uploadedByHash) > 0 {
		stats.UploadRequests++
		stats.UploadRequestBytes += result.uploaded.RequestBytes
		stats.UploadResponseBytes += result.uploaded.ResponseBytes
		stats.BatchRetries += result.uploaded.RetryCount
		stats.BatchSplits += result.uploaded.SplitCount
		uploadGeneration := result.uploaded.ChunkGeneration
		if uploadGeneration == 0 {
			uploadGeneration = b.opts.ChunkGeneration
		}
		for _, hash := range result.checked.Missing {
			chunk, ok := byHash[hash]
			if !ok {
				b.err = fmt.Errorf("server check returned unexpected missing chunk %s", hash)
				return stats, b.err
			}
			resp := result.uploadedByHash[hash]
			b.resolveChunk(chunk, resp.Ref)
			if resp.Uploaded {
				stats.Uploaded++
				if len(chunk.waiters) > 1 {
					stats.Reused += int64(len(chunk.waiters) - 1)
				}
			} else {
				stats.Reused += int64(len(chunk.waiters))
			}
			if b.opts.Catalog != nil {
				catalogUpdates = append(catalogUpdates, agentChunkStatusUpdate{
					Hash:         chunk.hash,
					OriginalSize: resp.Ref.OriginalSize,
					Status:       "confirmed",
					Generation:   uploadGeneration,
					Uploaded:     resp.Uploaded,
				})
			}
		}
	}
	if b.opts.Catalog != nil && len(catalogUpdates) > 0 {
		if err := b.opts.Catalog.markChunks(catalogUpdates); err != nil {
			b.err = err
			return stats, err
		}
	}
	stats.MaxPipelineBytes = b.bytesWindow.max()
	return stats, nil
}

func (b *agentChunkPipelineBatcher) resolveChunk(chunk *pendingBatchChunk, ref repository.ChunkRef) {
	if chunk == nil {
		return
	}
	for _, waiter := range chunk.waiters {
		waiter.entry.Chunks[waiter.index] = ref
		if count := b.pendingByEntry[waiter.entry]; count <= 1 {
			delete(b.pendingByEntry, waiter.entry)
		} else {
			b.pendingByEntry[waiter.entry] = count - 1
		}
	}
	delete(b.pendingByHash, chunk.hash)
	b.bytesWindow.release(chunk.originalSize)
}

func (b *agentChunkPipelineBatcher) worker() {
	defer b.wg.Done()
	for {
		select {
		case <-b.done:
			return
		case batch := <-b.checkQueue:
			if batch == nil {
				continue
			}
			b.publishResult(b.processBatch(batch))
		}
	}
}

func (b *agentChunkPipelineBatcher) processBatch(batch *pipelineBatch) pipelineResult {
	result := pipelineResult{batch: batch}
	hashes := make([]string, 0, len(batch.chunks))
	byHash := make(map[string]*pendingBatchChunk, len(batch.chunks))
	for _, chunk := range batch.chunks {
		hashes = append(hashes, chunk.hash)
		byHash[chunk.hash] = chunk
	}
	checkStarted := time.Now()
	checked, err := b.client.checkChunks(hashes, b.opts.RepositoryID, b.opts.ChunkGeneration, b.opts.CompactChunkCheckResponse, b.opts.chunkBatchRetryOptions(b.logger, batch.id, "check"))
	result.checkDuration = time.Since(checkStarted)
	if err != nil {
		result.err = err
		return result
	}
	if err := validateChunkCheckResponse(hashes, b.opts.RepositoryID, checked, b.opts.CompactChunkCheckResponse); err != nil {
		result.err = err
		return result
	}
	result.checked = checked
	if b.logger != nil {
		b.logger.Debug("agent chunk pipeline check",
			"batch_id", batch.id,
			"hashes", len(hashes),
			"request_bytes", estimateAgentChunkCheckRequestBytes(hashes),
			"response_bytes", checked.ResponseBytes,
			"duration", result.checkDuration.String(),
			"retries", checked.RetryCount,
			"compact_response", b.opts.CompactChunkCheckResponse,
		)
	}
	missingChunks := make([]*pendingBatchChunk, 0, len(checked.Missing))
	for _, hash := range checked.Missing {
		chunk, ok := byHash[hash]
		if !ok {
			result.err = fmt.Errorf("server check returned unexpected missing chunk %s", hash)
			return result
		}
		missingChunks = append(missingChunks, chunk)
	}
	if len(missingChunks) == 0 {
		return result
	}
	if err := b.acquireUploadSlot(); err != nil {
		result.err = err
		return result
	}
	uploadStarted := time.Now()
	uploaded, err := b.client.uploadChunksBatch(missingChunks, b.opts.CompactChunkUploadResponse, b.opts.chunkBatchRetryOptions(b.logger, batch.id, "upload"))
	result.uploadDuration = time.Since(uploadStarted)
	b.releaseUploadSlot()
	if err != nil {
		result.err = err
		return result
	}
	uploadedByHash, err := validateChunkUploadResponse(missingChunks, b.opts.RepositoryID, uploaded, b.opts.CompactChunkUploadResponse)
	if err != nil {
		result.err = err
		return result
	}
	result.uploaded = uploaded
	result.uploadedByHash = uploadedByHash
	if b.logger != nil {
		b.logger.Debug("agent chunk pipeline upload",
			"batch_id", batch.id,
			"chunks", len(missingChunks),
			"request_bytes", uploaded.RequestBytes,
			"response_bytes", uploaded.ResponseBytes,
			"duration", result.uploadDuration.String(),
			"retries", uploaded.RetryCount,
			"split_count", uploaded.SplitCount,
			"compact_response", b.opts.CompactChunkUploadResponse,
		)
	}
	return result
}

func (b *agentChunkPipelineBatcher) acquireUploadSlot() error {
	select {
	case b.uploadSlots <- struct{}{}:
		return nil
	case <-b.done:
		if b.err != nil {
			return b.err
		}
		return errors.New("chunk pipeline stopped")
	}
}

func (b *agentChunkPipelineBatcher) releaseUploadSlot() {
	select {
	case <-b.uploadSlots:
	default:
	}
}

func (b *agentChunkPipelineBatcher) publishResult(result pipelineResult) {
	select {
	case b.results <- result:
	case <-b.done:
	}
}

func (b *agentChunkPipelineBatcher) shouldFlushBeforeAdding(nextRequestBytes int64) bool {
	nextCount := len(b.pending) + 1
	return nextCount > b.maxChunks ||
		b.requestBytes+nextRequestBytes > b.maxRequestBytes ||
		b.estimatedCheckResponseBytes(nextCount) > b.maxResponseBytes ||
		b.estimatedUploadResponseBytes(nextCount) > b.maxResponseBytes
}

func (b *agentChunkPipelineBatcher) estimatedCheckResponseBytes(chunkCount int) int64 {
	return estimatedChunkBatchResponseBaseBytes + int64(chunkCount)*estimatedChunkCheckResponseBytesPerHash
}

func (b *agentChunkPipelineBatcher) estimatedUploadResponseBytes(chunkCount int) int64 {
	perChunk := estimatedFullChunkUploadResponseBytesPerChunk
	if b.opts.CompactChunkUploadResponse {
		perChunk = estimatedCompactChunkUploadResponseBytesPerChunk
	}
	return estimatedChunkBatchResponseBaseBytes + int64(chunkCount)*perChunk
}
