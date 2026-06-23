package repository

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tursom/turbk/internal/source"
)

func testRepository(t *testing.T, chunkAvg int, segmentSize int64) *Repository {
	t.Helper()
	root := t.TempDir()
	repo, err := OpenWithOptions(context.Background(), Options{
		StateDir:     filepath.Join(root, "state"),
		RepoDir:      filepath.Join(root, "repo"),
		ChunkAvgSize: chunkAvg,
		SegmentSize:  segmentSize,
	})
	if err != nil {
		t.Fatalf("OpenWithOptions() error = %v", err)
	}
	t.Cleanup(func() {
		if err := repo.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
	return repo
}

func TestChunkerDeterministicAndComplete(t *testing.T) {
	chunker := NewChunker(256)
	input := bytes.Repeat([]byte("abcdefghijklmnopqrstuvwxyz0123456789"), 100)

	var first [][]byte
	if err := chunker.Split(bytes.NewReader(input), func(chunk []byte) error {
		first = append(first, chunk)
		return nil
	}); err != nil {
		t.Fatalf("Split() error = %v", err)
	}
	var second [][]byte
	if err := chunker.Split(bytes.NewReader(input), func(chunk []byte) error {
		second = append(second, chunk)
		return nil
	}); err != nil {
		t.Fatalf("Split() error = %v", err)
	}
	if !bytes.Equal(bytes.Join(first, nil), input) {
		t.Fatal("chunks do not reconstruct original input")
	}
	if len(first) != len(second) {
		t.Fatalf("chunk count mismatch: %d != %d", len(first), len(second))
	}
	for i := range first {
		if !bytes.Equal(first[i], second[i]) {
			t.Fatalf("chunk %d differs between runs", i)
		}
	}
}

func TestPutChunkDeduplicatesAndReadsBack(t *testing.T) {
	repo := testRepository(t, 256, 4096)
	ctx := context.Background()
	data := bytes.Repeat([]byte("same-data-"), 200)

	ref1, existed, err := repo.PutChunk(ctx, data)
	if err != nil {
		t.Fatalf("PutChunk() error = %v", err)
	}
	if existed {
		t.Fatal("first PutChunk reported existing chunk")
	}
	statsAfterFirst, err := repo.Stats()
	if err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	ref2, existed, err := repo.PutChunk(ctx, data)
	if err != nil {
		t.Fatalf("second PutChunk() error = %v", err)
	}
	if !existed {
		t.Fatal("second PutChunk did not report existing chunk")
	}
	if ref1 != ref2 {
		t.Fatalf("dedup ref mismatch:\n%+v\n%+v", ref1, ref2)
	}
	statsAfterSecond, err := repo.Stats()
	if err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	if statsAfterSecond.Chunks != statsAfterFirst.Chunks || statsAfterSecond.SegmentBytes != statsAfterFirst.SegmentBytes {
		t.Fatalf("dedup changed storage stats: before=%+v after=%+v", statsAfterFirst, statsAfterSecond)
	}
	restored, err := repo.ReadChunk(ctx, ref1.Hash)
	if err != nil {
		t.Fatalf("ReadChunk() error = %v", err)
	}
	if !bytes.Equal(restored, data) {
		t.Fatal("restored chunk differs from original")
	}
}

func TestPutChunksDeduplicatesWithinBatchAndReadsBack(t *testing.T) {
	repo := testRepository(t, 256, 4096)
	ctx := context.Background()
	first := bytes.Repeat([]byte("batch-first-"), 200)
	second := bytes.Repeat([]byte("batch-second-"), 200)

	results, err := repo.PutChunks(ctx, [][]byte{first, second, first})
	if err != nil {
		t.Fatalf("PutChunks() error = %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("PutChunks() results = %d, want 3", len(results))
	}
	if results[0].Existed || results[1].Existed || !results[2].Existed {
		t.Fatalf("unexpected batch existence flags: %+v", results)
	}
	if results[0].Ref != results[2].Ref {
		t.Fatalf("duplicate chunk ref mismatch:\n%+v\n%+v", results[0].Ref, results[2].Ref)
	}
	statsAfterFirst, err := repo.Stats()
	if err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	if statsAfterFirst.Chunks != 2 {
		t.Fatalf("chunks after first PutChunks = %d, want 2", statsAfterFirst.Chunks)
	}
	for _, tc := range []struct {
		name string
		ref  ChunkRef
		want []byte
	}{
		{name: "first", ref: results[0].Ref, want: first},
		{name: "second", ref: results[1].Ref, want: second},
	} {
		got, err := repo.ReadChunk(ctx, tc.ref.Hash)
		if err != nil {
			t.Fatalf("ReadChunk(%s) error = %v", tc.name, err)
		}
		if !bytes.Equal(got, tc.want) {
			t.Fatalf("ReadChunk(%s) returned unexpected data", tc.name)
		}
	}

	duplicateResults, err := repo.PutChunks(ctx, [][]byte{first, second})
	if err != nil {
		t.Fatalf("duplicate PutChunks() error = %v", err)
	}
	if len(duplicateResults) != 2 || !duplicateResults[0].Existed || !duplicateResults[1].Existed {
		t.Fatalf("unexpected duplicate batch results: %+v", duplicateResults)
	}
	statsAfterSecond, err := repo.Stats()
	if err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	if statsAfterSecond.Chunks != statsAfterFirst.Chunks || statsAfterSecond.SegmentBytes != statsAfterFirst.SegmentBytes {
		t.Fatalf("duplicate PutChunks changed storage stats: first=%+v second=%+v", statsAfterFirst, statsAfterSecond)
	}
}

func TestReadChunkDetectsCorruptRecord(t *testing.T) {
	repo := testRepository(t, 256, 4096)
	ctx := context.Background()
	ref, _, err := repo.PutChunk(ctx, bytes.Repeat([]byte("verify-checksum-"), 100))
	if err != nil {
		t.Fatalf("PutChunk() error = %v", err)
	}
	path := segmentPath(filepath.Join(repo.opts.RepoDir, "segments"), ref.SegmentID)
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0xff}, ref.Offset+recordHeaderLen); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ReadChunk(ctx, ref.Hash); err == nil {
		t.Fatal("ReadChunk succeeded after segment record corruption")
	}
}

func TestSegmentWriterRotates(t *testing.T) {
	repo := testRepository(t, 256, 512)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		data := bytes.Repeat([]byte{byte('a' + i)}, 200)
		if _, _, err := repo.PutChunk(ctx, data); err != nil {
			t.Fatalf("PutChunk(%d) error = %v", i, err)
		}
	}
	stats, err := repo.Stats()
	if err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	if stats.Segments < 2 {
		t.Fatalf("segments = %d, want at least 2", stats.Segments)
	}
}

func TestClosedSegmentsAreNotRewrittenByNormalWrites(t *testing.T) {
	repo := testRepository(t, 256, 512)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		data := bytes.Repeat([]byte{byte('a' + i)}, 200)
		if _, _, err := repo.PutChunk(ctx, data); err != nil {
			t.Fatalf("PutChunk(%d) error = %v", i, err)
		}
	}
	closedSegmentSizes := make(map[int64]int64)
	segmentDir := filepath.Join(repo.opts.RepoDir, "segments")
	for id := int64(1); id < repo.writer.id; id++ {
		info, err := os.Stat(segmentPath(segmentDir, id))
		if err != nil {
			t.Fatal(err)
		}
		closedSegmentSizes[id] = info.Size()
	}
	if len(closedSegmentSizes) == 0 {
		t.Fatalf("writer id = %d, want at least one closed segment", repo.writer.id)
	}
	for i := 0; i < 3; i++ {
		data := bytes.Repeat([]byte{byte('z' - i)}, 200)
		if _, _, err := repo.PutChunk(ctx, data); err != nil {
			t.Fatalf("additional PutChunk(%d) error = %v", i, err)
		}
	}
	for id, size := range closedSegmentSizes {
		info, err := os.Stat(segmentPath(segmentDir, id))
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() != size {
			t.Fatalf("closed segment %d size changed from %d to %d", id, size, info.Size())
		}
	}
}

func TestRepositoryKeepsRandomWriteStateOutsideRepoDir(t *testing.T) {
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	repoDir := filepath.Join(root, "repo")
	repo, err := OpenWithOptions(context.Background(), Options{
		StateDir:     stateDir,
		RepoDir:      repoDir,
		ChunkAvgSize: 256,
		SegmentSize:  4096,
	})
	if err != nil {
		t.Fatalf("OpenWithOptions() error = %v", err)
	}
	defer repo.Close()
	if _, _, err := repo.PutChunk(context.Background(), bytes.Repeat([]byte("smr-friendly-"), 100)); err != nil {
		t.Fatal(err)
	}
	var repoFiles []string
	if err := filepath.WalkDir(repoDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(repoDir, path)
		if err != nil {
			return err
		}
		repoFiles = append(repoFiles, filepath.ToSlash(rel))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(repoFiles) != 1 || !strings.HasPrefix(repoFiles[0], "segments/") || !strings.HasSuffix(repoFiles[0], ".seg") {
		t.Fatalf("repo_dir contains non-segment files: %v", repoFiles)
	}
	for _, statePath := range []string{
		filepath.Join(stateDir, "chunk-index.pebble"),
		filepath.Join(stateDir, "manifests"),
		filepath.Join(stateDir, "keys", "repository.key"),
	} {
		if _, err := os.Stat(statePath); err != nil {
			t.Fatalf("expected state path %s: %v", statePath, err)
		}
	}
}

func TestBackupTreeManifestAndRestoreFile(t *testing.T) {
	repo := testRepository(t, 512, 32*1024)
	ctx := context.Background()
	source := t.TempDir()
	if err := os.MkdirAll(filepath.Join(source, "dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	repeated := bytes.Repeat([]byte("dedupe-this-block-"), 200)
	if err := os.WriteFile(filepath.Join(source, "dir", "alpha.txt"), repeated, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "dir", "beta.txt"), repeated, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("dir/alpha.txt", filepath.Join(source, "alpha-link")); err != nil {
		t.Fatal(err)
	}

	manifest, err := repo.BackupLocalTree(ctx, "snap1", "local", source)
	if err != nil {
		t.Fatalf("BackupLocalTree() error = %v", err)
	}
	if manifest.ID != "snap1" {
		t.Fatalf("manifest ID = %q", manifest.ID)
	}
	if _, ok := manifest.Find("dir/alpha.txt"); !ok {
		t.Fatal("alpha entry missing")
	}
	if link, ok := manifest.Find("alpha-link"); !ok || link.Type != EntryTypeSymlink || link.LinkTarget != "dir/alpha.txt" {
		t.Fatalf("symlink entry incorrect: %+v ok=%v", link, ok)
	}

	stats, err := repo.Stats()
	if err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	alpha, _ := manifest.Find("dir/alpha.txt")
	beta, _ := manifest.Find("dir/beta.txt")
	if int64(len(alpha.Chunks)+len(beta.Chunks)) <= stats.Chunks {
		t.Fatalf("expected duplicate file chunks to be deduplicated, refs=%d chunks=%d", len(alpha.Chunks)+len(beta.Chunks), stats.Chunks)
	}

	loaded, err := repo.ReadManifest("snap1")
	if err != nil {
		t.Fatalf("ReadManifest() error = %v", err)
	}
	if len(loaded.Entries) != len(manifest.Entries) {
		t.Fatalf("loaded manifest entries = %d, want %d", len(loaded.Entries), len(manifest.Entries))
	}
	target := filepath.Join(t.TempDir(), "restore", "alpha.txt")
	if err := repo.RestoreFile(ctx, "snap1", "dir/alpha.txt", target); err != nil {
		t.Fatalf("RestoreFile() error = %v", err)
	}
	restored, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(restored, repeated) {
		t.Fatal("restored file differs from source")
	}
}

func TestRepeatedBackupDoesNotAppendDuplicateChunks(t *testing.T) {
	repo := testRepository(t, 512, 32*1024)
	ctx := context.Background()
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "data.txt"), bytes.Repeat([]byte("stable"), 1000), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.BackupLocalTree(ctx, "snap-a", "local", source); err != nil {
		t.Fatalf("first BackupLocalTree() error = %v", err)
	}
	first, err := repo.Stats()
	if err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	if _, err := repo.BackupLocalTree(ctx, "snap-b", "local", source); err != nil {
		t.Fatalf("second BackupLocalTree() error = %v", err)
	}
	second, err := repo.Stats()
	if err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	if second.Chunks != first.Chunks || second.SegmentBytes != first.SegmentBytes {
		t.Fatalf("repeated backup appended duplicate chunks: first=%+v second=%+v", first, second)
	}
	if second.ManifestCount != 2 {
		t.Fatalf("manifest count = %d, want 2", second.ManifestCount)
	}
}

func TestBackupFromSourceConnector(t *testing.T) {
	repo := testRepository(t, 512, 32*1024)
	ctx := context.Background()
	sourceRoot := t.TempDir()
	data := bytes.Repeat([]byte("connector-data-"), 300)
	if err := os.MkdirAll(filepath.Join(sourceRoot, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourceRoot, "sub", "file.txt"), data, 0o644); err != nil {
		t.Fatal(err)
	}

	manifest, err := repo.BackupFromSource(ctx, "connector-snap", "local", sourceRoot, source.NewLocal())
	if err != nil {
		t.Fatalf("BackupFromSource() error = %v", err)
	}
	entry, ok := manifest.Find("sub/file.txt")
	if !ok || entry.Type != EntryTypeFile || len(entry.Chunks) == 0 {
		t.Fatalf("file entry missing or incomplete: %+v ok=%v", entry, ok)
	}
	target := filepath.Join(t.TempDir(), "file.txt")
	if err := repo.RestoreFile(ctx, "connector-snap", "sub/file.txt", target); err != nil {
		t.Fatalf("RestoreFile() error = %v", err)
	}
	restored, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(restored, data) {
		t.Fatal("restored connector backup differs from source")
	}
}

func TestBackupFromSourceIncrementalReusesUnchangedFileWithoutOpen(t *testing.T) {
	repo := testRepository(t, 512, 32*1024)
	ctx := context.Background()
	modTime := time.Unix(1710000000, 0).UTC()
	data := bytes.Repeat([]byte("unchanged-file-"), 300)
	entries := []source.Entry{
		{Path: "/srv", Type: source.EntryDir, Mode: os.ModeDir | 0o755, ModTime: modTime},
		{Path: "/srv/data.txt", Type: source.EntryFile, Size: int64(len(data)), Mode: 0o644, ModTime: modTime},
	}

	firstConnector := &countingConnector{
		entries: entries,
		data:    map[string][]byte{"/srv/data.txt": data},
	}
	first, err := repo.BackupFromSourceIncremental(ctx, "incremental-a", "sftp", "/srv", firstConnector, nil, nil)
	if err != nil {
		t.Fatalf("first BackupFromSourceIncremental() error = %v", err)
	}
	if firstConnector.openCount != 1 {
		t.Fatalf("first open count = %d, want 1", firstConnector.openCount)
	}
	firstStats, err := repo.Stats()
	if err != nil {
		t.Fatal(err)
	}

	secondConnector := &countingConnector{entries: entries}
	second, err := repo.BackupFromSourceIncremental(ctx, "incremental-b", "sftp", "/srv", secondConnector, first, nil)
	if err != nil {
		t.Fatalf("second BackupFromSourceIncremental() error = %v", err)
	}
	if secondConnector.openCount != 0 {
		t.Fatalf("second open count = %d, want 0 for unchanged file", secondConnector.openCount)
	}
	secondStats, err := repo.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if secondStats.Chunks != firstStats.Chunks || secondStats.SegmentBytes != firstStats.SegmentBytes {
		t.Fatalf("incremental reuse changed storage stats: first=%+v second=%+v", firstStats, secondStats)
	}
	firstEntry, _ := first.Find("data.txt")
	secondEntry, _ := second.Find("data.txt")
	if len(firstEntry.Chunks) == 0 || len(secondEntry.Chunks) != len(firstEntry.Chunks) {
		t.Fatalf("unexpected chunk refs: first=%+v second=%+v", firstEntry.Chunks, secondEntry.Chunks)
	}
	for i := range firstEntry.Chunks {
		if firstEntry.Chunks[i] != secondEntry.Chunks[i] {
			t.Fatalf("chunk ref %d differs: first=%+v second=%+v", i, firstEntry.Chunks[i], secondEntry.Chunks[i])
		}
	}
}

type countingConnector struct {
	entries   []source.Entry
	data      map[string][]byte
	openCount int
}

func (c *countingConnector) Walk(ctx context.Context, _ string, fn func(source.Entry) error) error {
	for _, entry := range c.entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(entry); err != nil {
			return err
		}
	}
	return nil
}

func (c *countingConnector) Open(_ context.Context, path string) (io.ReadCloser, error) {
	c.openCount++
	data, ok := c.data[path]
	if !ok {
		return nil, fmt.Errorf("unexpected open %q", path)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (c *countingConnector) Close() error {
	return nil
}
