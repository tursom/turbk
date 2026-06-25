package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/tursom/turbk/internal/fsfilter"
	"github.com/tursom/turbk/internal/repository"
	"github.com/tursom/turbk/internal/rootset"
)

type agentSkipReporter struct {
	logger *slog.Logger
	total  int64
	logged int64
}

func (r *agentSkipReporter) record(event fsfilter.SkipEvent) {
	r.total++
	if r.logger == nil || r.logged >= 20 {
		return
	}
	r.logged++
	r.logger.Warn("agent skipped path", "path", event.Path, "rel", event.Rel, "reason", event.Reason)
}

func agentPathErrorSkipEvent(root, scanPath string, err error) (fsfilter.SkipEvent, bool) {
	if err == nil {
		return fsfilter.SkipEvent{}, false
	}
	isRoot := filepath.Clean(scanPath) == filepath.Clean(root)
	reason := ""
	switch {
	case errors.Is(err, fs.ErrNotExist) || os.IsNotExist(err):
		if isRoot {
			return fsfilter.SkipEvent{}, false
		}
		reason = "path disappeared during scan"
	case errors.Is(err, fs.ErrPermission) || os.IsPermission(err):
		reason = "permission denied during scan"
	case isAgentPathIOError(err):
		reason = "filesystem IO error during scan"
	}
	if reason == "" {
		return fsfilter.SkipEvent{}, false
	}
	rel, relErr := filepath.Rel(root, scanPath)
	if relErr != nil {
		rel = scanPath
	}
	return fsfilter.SkipEvent{
		Path:   scanPath,
		Rel:    cleanManifestPath(rel),
		Reason: reason,
	}, true
}

func isAgentPathIOError(err error) bool {
	for _, target := range []error{
		syscall.EIO,
		syscall.ESTALE,
		syscall.ENXIO,
		syscall.ENODEV,
	} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

type agentFileCatalogBatcher struct {
	catalog      *agentCatalog
	logger       *slog.Logger
	pending      []agentFileCatalogUpdate
	pendingBytes int64
}

type pendingManifestFile struct {
	entry  *repository.FileEntry
	record catalogFileRecord
}

func newAgentFileCatalogBatcher(catalog *agentCatalog, logger *slog.Logger) *agentFileCatalogBatcher {
	return &agentFileCatalogBatcher{catalog: catalog, logger: logger}
}

func (b *agentFileCatalogBatcher) Add(record catalogFileRecord, chunks []catalogChunkRecord) {
	if b == nil || b.catalog == nil {
		return
	}
	update := agentFileCatalogUpdate{Record: record}
	if len(chunks) > 0 {
		update.Chunks = append([]catalogChunkRecord(nil), chunks...)
	}
	b.pending = append(b.pending, update)
	b.pendingBytes += estimateFileCatalogUpdateBytes(record, chunks)
	if len(b.pending) >= defaultAgentFileCatalogBatchFiles || b.pendingBytes >= defaultAgentFileCatalogBatchBytes {
		b.Flush()
	}
}

func (b *agentFileCatalogBatcher) Flush() {
	if b == nil || b.catalog == nil || len(b.pending) == 0 {
		return
	}
	updates := b.pending
	b.pending = nil
	b.pendingBytes = 0
	if err := b.catalog.replaceFiles(updates); err != nil && b.logger != nil {
		b.logger.Warn("agent catalog file batch update failed", "files", len(updates), "error", err)
	}
}

func estimateFileCatalogUpdateBytes(record catalogFileRecord, chunks []catalogChunkRecord) int64 {
	return int64(128 + len(record.RootID) + len(record.Path) + len(record.LinkTarget) + len(record.Fingerprint) + len(chunks)*40)
}

type backupScanner struct {
	client                *agentClient
	runID                 int64
	roots                 []string
	logger                *slog.Logger
	scanOptions           fsfilter.Options
	opts                  backupRunOptions
	manifest              *repository.SnapshotManifest
	progress              agentProgress
	lastProgress          time.Time
	chunker               repository.Chunker
	fileReadWorkers       int
	fileReadPipelineBytes int64
	skipReporter          *agentSkipReporter
	chunkUploader         agentChunkUploader
	packBatcher           *agentSmallFilePackBatcher
	fileCatalogBatcher    *agentFileCatalogBatcher
	batchStats            chunkBatchStats
	pendingFiles          []pendingManifestFile
	parallelJobs          []agentParallelFileJob
	fileReadWindow        *agentFileReadByteWindow
	multiRoot             bool
	entryPaths            map[string]struct{}
	packIDs               map[string]struct{}
}

func newBackupScanner(c *agentClient, runID int64, roots []string, logger *slog.Logger, scanOptions fsfilter.Options, opts backupRunOptions) *backupScanner {
	manifest := &repository.SnapshotManifest{
		CreatedAt:  time.Now().UTC(),
		SourceType: "agent",
	}
	if len(roots) == 1 {
		manifest.SourceRoot = roots[0]
	} else {
		manifest.SourceRoots = append([]string(nil), roots...)
	}
	fileReadWorkers := opts.FileReadWorkers
	if fileReadWorkers <= 0 {
		fileReadWorkers = 2
	}
	fileReadPipelineBytes := opts.FileReadPipelineBytes
	if fileReadPipelineBytes <= 0 {
		fileReadPipelineBytes = defaultAgentFileReadPipelineBytes
	}
	return &backupScanner{
		client:                c,
		runID:                 runID,
		roots:                 roots,
		logger:                logger,
		scanOptions:           scanOptions,
		opts:                  opts,
		manifest:              manifest,
		progress:              agentProgress{Phase: "scanning", Message: strings.Join(roots, ", ")},
		chunker:               repository.NewChunker(agentChunkAvgSize),
		fileReadWorkers:       fileReadWorkers,
		fileReadPipelineBytes: fileReadPipelineBytes,
		skipReporter:          &agentSkipReporter{logger: logger},
		pendingFiles:          make([]pendingManifestFile, 0),
		parallelJobs:          make([]agentParallelFileJob, 0),
		multiRoot:             len(roots) > 1,
		entryPaths:            make(map[string]struct{}),
		packIDs:               make(map[string]struct{}),
	}
}

func (c *agentClient) scanAndUpload(runID int64, roots []string, logger *slog.Logger, scanOptions fsfilter.Options, opts backupRunOptions) (*repository.SnapshotManifest, error) {
	return newBackupScanner(c, runID, roots, logger, scanOptions, opts).scan()
}

func (s *backupScanner) scan() (*repository.SnapshotManifest, error) {
	if err := s.client.sendProgress(s.runID, s.progress); err != nil {
		return nil, err
	}
	s.lastProgress = time.Now()
	s.chunkUploader = newAgentChunkUploader(s.client, s.opts, s.logger)
	defer s.chunkUploader.Stop()
	s.packBatcher = newAgentSmallFilePackBatcher(s.opts)
	s.fileCatalogBatcher = newAgentFileCatalogBatcher(s.opts.Catalog, s.logger)

	for _, root := range s.roots {
		if err := s.walkRoot(root); err != nil {
			return nil, err
		}
	}
	if err := s.processParallelJobs(); err != nil {
		return nil, err
	}
	if err := s.flushPackFiles(); err != nil {
		return nil, err
	}
	if err := s.flushPendingFiles(); err != nil {
		return nil, err
	}
	s.fileCatalogBatcher.Flush()
	s.progress.Phase = "manifest"
	s.progress.Message = "manifest ready"
	if err := s.sendProgress(true); err != nil {
		return nil, err
	}
	if s.opts.ScanParallelEnabled {
		sort.SliceStable(s.manifest.Entries, func(i, j int) bool {
			return s.manifest.Entries[i].Path < s.manifest.Entries[j].Path
		})
	}
	s.logComplete()
	return s.manifest, nil
}

func (s *backupScanner) walkRoot(root string) error {
	s.progress.Message = root
	if err := s.sendProgress(true); err != nil {
		return err
	}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		return s.walkPath(root, path, d, walkErr)
	})
	if err != nil {
		return err
	}
	if err := s.flushPackFiles(); err != nil {
		return err
	}
	return s.flushPendingFiles()
}

func (s *backupScanner) walkPath(root, path string, d fs.DirEntry, walkErr error) error {
	if walkErr != nil {
		if event, skip := agentPathErrorSkipEvent(root, path, walkErr); skip {
			s.skipReporter.record(event)
			s.progress.Message = "skipped " + event.Rel
			return s.sendProgress(false)
		}
		return walkErr
	}
	info, err := agentLstat(path)
	if err != nil {
		if event, skip := agentPathErrorSkipEvent(root, path, err); skip {
			s.skipReporter.record(event)
			s.progress.Message = "skipped " + event.Rel
			if progressErr := s.sendProgress(false); progressErr != nil {
				return progressErr
			}
			if d != nil && d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		return fmt.Errorf("stat %q: %w", path, err)
	}
	if event, skip := fsfilter.ShouldSkip(root, path, info, s.scanOptions); skip {
		s.skipReporter.record(event)
		s.progress.Message = "skipped " + event.Rel
		if err := s.sendProgress(false); err != nil {
			return err
		}
		if info.IsDir() {
			return filepath.SkipDir
		}
		return nil
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return fmt.Errorf("rel path %q: %w", path, err)
	}
	catalogPath := cleanManifestPath(rel)
	entry := repository.FileEntry{
		Path:    manifestEntryPath(root, catalogPath, s.multiRoot),
		Size:    info.Size(),
		Mode:    uint32(info.Mode()),
		ModTime: info.ModTime().UTC(),
	}
	entry.UID, entry.GID = fileOwner(info)
	record := catalogRecordFromFile(root, catalogPath, info)

	mode := info.Mode()
	switch {
	case mode.IsDir():
		return s.handleDir(entry, record)
	case mode&os.ModeSymlink != 0:
		return s.handleSymlink(root, path, entry, record)
	case mode.IsRegular():
		return s.handleRegularFile(root, catalogPath, path, info, entry, record)
	default:
		return nil
	}
}

func (s *backupScanner) handleDir(entry repository.FileEntry, record catalogFileRecord) error {
	entry.Type = repository.EntryTypeDir
	record.Type = string(repository.EntryTypeDir)
	s.fileCatalogBatcher.Add(record, nil)
	return s.appendManifestEntry(entry)
}

func (s *backupScanner) handleSymlink(root, path string, entry repository.FileEntry, record catalogFileRecord) error {
	entry.Type = repository.EntryTypeSymlink
	target, err := os.Readlink(path)
	if err != nil {
		if event, skip := agentPathErrorSkipEvent(root, path, err); skip {
			s.skipReporter.record(event)
			s.progress.Message = "skipped " + event.Rel
			return s.sendProgress(false)
		}
		return fmt.Errorf("read symlink %q: %w", path, err)
	}
	entry.LinkTarget = target
	record.Type = string(repository.EntryTypeSymlink)
	record.LinkTarget = target
	s.fileCatalogBatcher.Add(record, nil)
	return s.appendManifestEntry(entry)
}

func (s *backupScanner) handleRegularFile(root, catalogPath, path string, info fs.FileInfo, entry repository.FileEntry, record catalogFileRecord) error {
	entry.Type = repository.EntryTypeFile
	record.Type = string(repository.EntryTypeFile)
	if s.packBatcher.Eligible(info) {
		return s.handlePackFile(root, catalogPath, path, entry, record)
	}
	if s.opts.Catalog != nil {
		reused, err := s.client.tryReuseCatalogFile(s.opts.Catalog, record, &entry, s.opts)
		if err != nil {
			s.logger.Warn("agent catalog reuse failed; reading file", "path", entry.Path, "error", err)
		} else if reused {
			s.progress.ProcessedFiles++
			s.progress.ProcessedBytes += entry.Size
			s.progress.ReusedChunks += int64(len(entry.Chunks))
			s.progress.Message = entry.Path
			if err := s.sendProgress(true); err != nil {
				return err
			}
			return s.appendManifestEntry(entry)
		}
	}
	if s.opts.ScanParallelEnabled && entry.Size > 0 {
		return s.queueParallelFile(root, catalogPath, path, entry, record)
	}
	return s.uploadRegularFile(root, path, entry, record)
}

func (s *backupScanner) handlePackFile(root, catalogPath, path string, entry repository.FileEntry, record catalogFileRecord) error {
	if s.opts.Catalog != nil {
		reused, pack, err := s.client.tryReuseCatalogPackedFile(s.opts.Catalog, record, &entry, s.opts)
		if err != nil {
			s.logger.Warn("agent packed catalog reuse failed; reading file", "path", entry.Path, "error", err)
		} else if reused {
			s.appendPackManifest(pack)
			s.progress.ProcessedFiles++
			s.progress.ProcessedBytes += entry.Size
			s.progress.ReusedChunks += int64(len(pack.Chunks))
			s.progress.Message = entry.Path
			if err := s.sendProgress(true); err != nil {
				return err
			}
			return s.appendManifestEntry(entry)
		}
	}
	packFile, err := readPackFilePayload(root, catalogPath, path, record, entry)
	if err != nil {
		if event, skip := agentPathErrorSkipEvent(root, path, err); skip {
			s.skipReporter.record(event)
			s.progress.Message = "skipped " + event.Rel
			return s.sendProgress(false)
		}
		return err
	}
	s.progress.ProcessedFiles++
	s.progress.ProcessedBytes += entry.Size
	s.progress.Message = entry.Path
	if err := s.packBatcher.Add(pendingPackFile{entry: entry, record: record, file: packFile}, s.flushPackFiles); err != nil {
		return err
	}
	return s.sendProgress(true)
}

func (s *backupScanner) queueParallelFile(root, catalogPath, path string, entry repository.FileEntry, record catalogFileRecord) error {
	s.parallelJobs = append(s.parallelJobs, agentParallelFileJob{
		root:        root,
		catalogPath: catalogPath,
		filePath:    path,
		entry:       entry,
		record:      record,
	})
	s.progress.Message = entry.Path
	return s.sendProgress(false)
}

func (s *backupScanner) uploadRegularFile(root, path string, entry repository.FileEntry, record catalogFileRecord) error {
	file, err := agentOpen(path)
	if err != nil {
		if event, skip := agentPathErrorSkipEvent(root, path, err); skip {
			s.skipReporter.record(event)
			s.progress.Message = "skipped " + event.Rel
			return s.sendProgress(false)
		}
		return fmt.Errorf("open file %q: %w", path, err)
	}
	entryPtr := &entry
	if err := s.chunker.Split(file, func(chunk []byte) error {
		stats, err := s.chunkUploader.Add(chunk, entryPtr)
		if err != nil {
			_ = file.Close()
			return err
		}
		s.addChunkStats(stats)
		return s.sendProgress(false)
	}); err != nil {
		_ = file.Close()
		if event, skip := agentPathErrorSkipEvent(root, path, err); skip {
			s.skipReporter.record(event)
			s.progress.Message = "skipped " + event.Rel
			return s.sendProgress(false)
		}
		return fmt.Errorf("chunk file %q: %w", path, err)
	}
	if err := file.Close(); err != nil {
		if event, skip := agentPathErrorSkipEvent(root, path, err); skip {
			s.skipReporter.record(event)
			s.progress.Message = "skipped " + event.Rel
			return s.sendProgress(false)
		}
		return fmt.Errorf("close file %q: %w", path, err)
	}
	s.progress.ProcessedFiles++
	s.progress.ProcessedBytes += entry.Size
	s.progress.Message = entry.Path
	if err := s.queuePendingManifestFile(entryPtr, record); err != nil {
		return err
	}
	return s.sendProgress(true)
}

func (s *backupScanner) processParallelJobs() error {
	if len(s.parallelJobs) == 0 {
		return nil
	}
	s.progress.Message = "parallel file read"
	if err := s.sendProgress(true); err != nil {
		return err
	}
	results, window := runAgentParallelFileReaders(s.parallelJobs, s.fileReadWorkers, s.fileReadPipelineBytes)
	s.fileReadWindow = window
	for result := range results {
		if result.err != nil {
			releaseParallelFileChunks(window, result.chunks)
			if event, skip := agentPathErrorSkipEvent(result.job.root, result.job.filePath, result.err); skip {
				s.skipReporter.record(event)
				s.progress.Message = "skipped " + event.Rel
				if err := s.sendProgress(false); err != nil {
					return err
				}
				continue
			}
			return result.err
		}
		entry := result.job.entry
		entryPtr := &entry
		for i, chunk := range result.chunks {
			stats, err := s.chunkUploader.Add(chunk, entryPtr)
			window.release(int64(len(chunk)))
			result.chunks[i] = nil
			if err != nil {
				releaseParallelFileChunks(window, result.chunks[i+1:])
				return err
			}
			s.addChunkStats(stats)
			if err := s.sendProgress(false); err != nil {
				releaseParallelFileChunks(window, result.chunks[i+1:])
				return err
			}
		}
		s.progress.ProcessedFiles++
		s.progress.ProcessedBytes += entry.Size
		s.progress.Message = entry.Path
		if err := s.queuePendingManifestFile(entryPtr, result.job.record); err != nil {
			return err
		}
		if err := s.sendProgress(true); err != nil {
			return err
		}
	}
	return nil
}

func (s *backupScanner) flushPackFiles() error {
	if s.packBatcher == nil || len(s.packBatcher.pending) == 0 {
		return nil
	}
	pending := append([]pendingPackFile(nil), s.packBatcher.pending...)
	s.packBatcher.pending = s.packBatcher.pending[:0]
	s.packBatcher.pendingBytes = 0

	payloads := make([]repository.PackFilePayload, 0, len(pending))
	for _, file := range pending {
		payloads = append(payloads, file.file)
	}
	packData, indexes, err := repository.EncodePack(payloads)
	if err != nil {
		return err
	}
	packHash, err := repository.HashBytes(packData)
	if err != nil {
		return err
	}
	packID := fmt.Sprintf("pack-%d-%s", s.packBatcher.nextPackIndex, packHash[:16])
	s.packBatcher.nextPackIndex++

	packEntry := &repository.FileEntry{
		Path:    "__turbk_pack/" + packID,
		Type:    repository.EntryTypeFile,
		Size:    int64(len(packData)),
		Mode:    0o600,
		ModTime: time.Now().UTC(),
	}
	if err := s.chunker.Split(bytes.NewReader(packData), func(chunk []byte) error {
		stats, err := s.chunkUploader.Add(chunk, packEntry)
		if err != nil {
			return err
		}
		s.addChunkStats(stats)
		return nil
	}); err != nil {
		return fmt.Errorf("chunk small-file pack %s: %w", packID, err)
	}
	if err := s.flushPendingFiles(); err != nil {
		return err
	}
	if len(packEntry.Chunks) == 0 {
		return fmt.Errorf("small-file pack %s produced no chunks", packID)
	}
	s.appendPackManifest(repository.PackManifest{
		ID:     packID,
		Format: repository.PackFormatTBKPack1,
		Chunks: cloneChunkRefs(packEntry.Chunks),
	})
	for i, file := range pending {
		index := indexes[i]
		entry := file.entry
		record := file.record
		entry.Type = repository.EntryTypePackedFile
		entry.Chunks = nil
		entry.Pack = &repository.PackFileRef{
			ID:     packID,
			Offset: index.Offset,
			Length: index.Length,
		}
		record.Fingerprint = encodePackedFileFingerprint(packID, index.Offset, index.Length, packEntry.Chunks)
		s.fileCatalogBatcher.Add(record, nil)
		if err := s.appendManifestEntry(entry); err != nil {
			return err
		}
	}
	s.batchStats.PackCount++
	s.batchStats.PackedFiles += int64(len(pending))
	s.batchStats.PackedBytes += int64(len(packData))
	return nil
}

func (s *backupScanner) flushPendingFiles() error {
	stats, err := s.chunkUploader.Flush()
	if err != nil {
		return err
	}
	s.addChunkStats(stats)
	remaining := s.pendingFiles[:0]
	for _, pending := range s.pendingFiles {
		if s.chunkUploader.EntryPending(pending.entry) {
			remaining = append(remaining, pending)
			continue
		}
		if err := s.finalizeFileEntry(pending); err != nil {
			return err
		}
	}
	s.pendingFiles = remaining
	if stats.Uploaded != 0 || stats.Reused != 0 {
		return s.sendProgress(true)
	}
	return nil
}

func (s *backupScanner) queuePendingManifestFile(entry *repository.FileEntry, record catalogFileRecord) error {
	pending := pendingManifestFile{entry: entry, record: record}
	if s.chunkUploader.EntryPending(entry) {
		s.pendingFiles = append(s.pendingFiles, pending)
		return nil
	}
	return s.finalizeFileEntry(pending)
}

func (s *backupScanner) finalizeFileEntry(pending pendingManifestFile) error {
	s.fileCatalogBatcher.Add(pending.record, catalogChunksFromEntry(pending.entry))
	return s.appendManifestEntry(*pending.entry)
}

func (s *backupScanner) appendManifestEntry(entry repository.FileEntry) error {
	return appendManifestEntry(s.manifest, s.entryPaths, entry)
}

func (s *backupScanner) appendPackManifest(pack repository.PackManifest) {
	if _, ok := s.packIDs[pack.ID]; ok {
		return
	}
	s.packIDs[pack.ID] = struct{}{}
	s.manifest.Packs = append(s.manifest.Packs, pack)
}

func (s *backupScanner) addChunkStats(stats chunkBatchStats) {
	s.batchStats.Add(stats)
	s.progress.UploadedChunks += stats.Uploaded
	s.progress.ReusedChunks += stats.Reused
}

func (s *backupScanner) sendProgress(force bool) error {
	if !force && time.Since(s.lastProgress) < 500*time.Millisecond {
		return nil
	}
	s.lastProgress = time.Now()
	return s.client.sendProgress(s.runID, s.progress)
}

func (s *backupScanner) logComplete() {
	s.logger.Info("agent scan complete",
		"files", s.progress.ProcessedFiles,
		"uploaded_chunks", s.progress.UploadedChunks,
		"reused_chunks", s.progress.ReusedChunks,
		"skipped_paths", s.skipReporter.total,
		"chunk_check_requests", s.batchStats.CheckRequests,
		"chunk_upload_requests", s.batchStats.UploadRequests,
		"chunk_upload_request_bytes", s.batchStats.UploadRequestBytes,
		"chunk_check_response_bytes", s.batchStats.CheckResponseBytes,
		"chunk_upload_response_bytes", s.batchStats.UploadResponseBytes,
		"chunk_pipeline_enabled", s.opts.ChunkPipelineEnabled && s.opts.ChunkBatchUpload,
		"chunk_check_inflight", s.opts.MaxChunkCheckInflight,
		"chunk_upload_inflight", s.opts.MaxChunkUploadInflight,
		"chunk_pipeline_wait", s.batchStats.PipelineWait.String(),
		"chunk_check_duration", s.batchStats.CheckDuration.String(),
		"chunk_upload_duration", s.batchStats.UploadDuration.String(),
		"chunk_pipeline_max_bytes", s.batchStats.MaxPipelineBytes,
		"chunk_batch_retries", s.batchStats.BatchRetries,
		"chunk_batch_splits", s.batchStats.BatchSplits,
		"scan_parallel_enabled", s.opts.ScanParallelEnabled,
		"file_read_workers", s.fileReadWorkers,
		"file_read_jobs", len(s.parallelJobs),
		"file_read_pipeline_bytes", s.fileReadPipelineBytes,
		"file_read_pipeline_max_bytes", fileReadWindowMaxBytes(s.fileReadWindow),
		"manifest_bytes", estimateManifestBytes(s.manifest),
		"packed_files", s.batchStats.PackedFiles,
		"packed_bytes", s.batchStats.PackedBytes,
		"pack_count", s.batchStats.PackCount,
	)
}

func manifestEntryPath(root, catalogPath string, multiRoot bool) string {
	if !multiRoot {
		return catalogPath
	}
	if catalogPath == "." {
		return cleanManifestPath(rootset.ManifestPrefix(root))
	}
	return cleanManifestPath(filepath.Join(rootset.ManifestPrefix(root), catalogPath))
}

func appendManifestEntry(manifest *repository.SnapshotManifest, seen map[string]struct{}, entry repository.FileEntry) error {
	if _, ok := seen[entry.Path]; ok {
		return fmt.Errorf("manifest entry path %q is duplicated", entry.Path)
	}
	seen[entry.Path] = struct{}{}
	manifest.Entries = append(manifest.Entries, entry)
	return nil
}

func readPackFilePayload(root, catalogPath, filePath string, initial catalogFileRecord, entry repository.FileEntry) (repository.PackFilePayload, error) {
	file, err := agentOpen(filePath)
	if err != nil {
		return repository.PackFilePayload{}, fmt.Errorf("open file %q: %w", filePath, err)
	}
	limit := entry.Size + 1
	data, readErr := io.ReadAll(io.LimitReader(file, limit))
	stat, statErr := file.Stat()
	closeErr := file.Close()
	if readErr != nil {
		return repository.PackFilePayload{}, fmt.Errorf("read small file %q: %w", filePath, readErr)
	}
	if statErr != nil {
		return repository.PackFilePayload{}, fmt.Errorf("stat small file %q after read: %w", filePath, statErr)
	}
	if closeErr != nil {
		return repository.PackFilePayload{}, fmt.Errorf("close small file %q: %w", filePath, closeErr)
	}
	if int64(len(data)) != entry.Size {
		return repository.PackFilePayload{}, fmt.Errorf("small file %q changed while reading", filePath)
	}
	current := catalogRecordFromFile(root, catalogPath, stat)
	current.Type = initial.Type
	current.LinkTarget = initial.LinkTarget
	if !catalogFileMatches(current, initial) {
		return repository.PackFilePayload{}, fmt.Errorf("small file %q metadata changed while reading", filePath)
	}
	return repository.PackFilePayload{
		Path:    entry.Path,
		Mode:    entry.Mode,
		ModTime: entry.ModTime,
		Data:    data,
	}, nil
}

func cloneChunkRefs(chunks []repository.ChunkRef) []repository.ChunkRef {
	if len(chunks) == 0 {
		return nil
	}
	cloned := make([]repository.ChunkRef, len(chunks))
	copy(cloned, chunks)
	return cloned
}

func estimateManifestBytes(manifest *repository.SnapshotManifest) int64 {
	data, err := json.Marshal(manifest)
	if err != nil {
		return 0
	}
	return int64(len(data))
}

func (c *agentClient) tryReuseCatalogFile(catalog *agentCatalog, record catalogFileRecord, entry *repository.FileEntry, opts backupRunOptions) (bool, error) {
	existing, chunks, ok, err := catalog.fileRecordWithChunks(record.RootID, record.Path)
	if err != nil || !ok {
		return false, err
	}
	if !catalogFileMatches(existing, record) {
		return false, nil
	}
	if len(chunks) == 0 {
		return entry.Size == 0, nil
	}
	if ok, err := c.ensureCatalogChunksConfirmed(catalog, chunks, opts); err != nil || !ok {
		return false, err
	}
	entry.Chunks = entry.Chunks[:0]
	for _, chunk := range chunks {
		entry.Chunks = append(entry.Chunks, repository.ChunkRef{
			Hash:         chunk.Hash,
			OriginalSize: chunk.OriginalSize,
		})
	}
	return true, nil
}

func (c *agentClient) tryReuseCatalogPackedFile(catalog *agentCatalog, record catalogFileRecord, entry *repository.FileEntry, opts backupRunOptions) (bool, repository.PackManifest, error) {
	existing, _, ok, err := catalog.fileRecordWithChunks(record.RootID, record.Path)
	if err != nil || !ok {
		return false, repository.PackManifest{}, err
	}
	if !catalogFileMetadataMatches(existing, record) {
		return false, repository.PackManifest{}, nil
	}
	fingerprint, ok := decodePackedFileFingerprint(existing.Fingerprint)
	if !ok {
		return false, repository.PackManifest{}, nil
	}
	if fingerprint.Length != entry.Size {
		return false, repository.PackManifest{}, nil
	}
	if ok, err := c.ensureCatalogChunksConfirmed(catalog, fingerprint.Chunks, opts); err != nil || !ok {
		return false, repository.PackManifest{}, err
	}
	entry.Type = repository.EntryTypePackedFile
	entry.Chunks = nil
	entry.Pack = &repository.PackFileRef{
		ID:     fingerprint.PackID,
		Offset: fingerprint.Offset,
		Length: fingerprint.Length,
	}
	return true, repository.PackManifest{
		ID:     fingerprint.PackID,
		Format: repository.PackFormatTBKPack1,
		Chunks: packManifestChunksFromCatalog(fingerprint.Chunks),
	}, nil
}

func (c *agentClient) ensureCatalogChunksConfirmed(catalog *agentCatalog, chunks []catalogChunkRecord, opts backupRunOptions) (bool, error) {
	stale := make([]string, 0)
	originalSizes := make(map[string]int64, len(chunks))
	for _, chunk := range chunks {
		originalSizes[chunk.Hash] = chunk.OriginalSize
		status, generation, ok, err := catalog.chunkStatus(chunk.Hash)
		if err != nil {
			return false, err
		}
		if ok && status == "confirmed" && generation >= opts.ChunkGeneration {
			continue
		}
		stale = append(stale, chunk.Hash)
	}
	if len(stale) == 0 {
		return true, nil
	}
	checked, err := c.checkChunksBatched(stale, opts.RepositoryID, opts.ChunkGeneration, opts.MaxChunkCheckBatch, opts.CompactChunkCheckResponse, opts.chunkBatchRetryOptions(nil, 0, "check"))
	if err != nil {
		return false, err
	}
	if err := validateChunkCheckResponse(stale, opts.RepositoryID, checked, opts.CompactChunkCheckResponse); err != nil {
		return false, err
	}
	missing := make(map[string]struct{}, len(checked.Missing))
	updates := make([]agentChunkStatusUpdate, 0, len(checked.Exists)+len(checked.Missing))
	for _, hash := range existingChunkHashes(stale, checked, opts.CompactChunkCheckResponse) {
		updates = append(updates, agentChunkStatusUpdate{
			Hash:         hash,
			OriginalSize: originalSizes[hash],
			Status:       "confirmed",
			Generation:   checked.ChunkGeneration,
		})
	}
	for _, hash := range checked.Missing {
		missing[hash] = struct{}{}
		updates = append(updates, agentChunkStatusUpdate{
			Hash:         hash,
			OriginalSize: originalSizes[hash],
			Status:       "missing",
		})
	}
	if err := catalog.markChunks(updates); err != nil {
		return false, err
	}
	return len(missing) == 0, nil
}

func catalogRecordFromFile(root, rel string, info fs.FileInfo) catalogFileRecord {
	uid, gid := fileOwner(info)
	record := catalogFileRecord{
		RootID:  filepath.Clean(root),
		Path:    rel,
		Size:    info.Size(),
		Mode:    int64(info.Mode()),
		UID:     int64(uid),
		GID:     int64(gid),
		MTimeNS: info.ModTime().UnixNano(),
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		record.Dev = int64(stat.Dev)
		record.Inode = int64(stat.Ino)
	}
	return record
}

func fileOwner(info fs.FileInfo) (int, int) {
	if runtime.GOOS == "windows" {
		return 0, 0
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0
	}
	return int(stat.Uid), int(stat.Gid)
}

func cleanManifestPath(path string) string {
	path = filepath.ToSlash(filepath.Clean(path))
	path = strings.TrimPrefix(path, "/")
	if path == "." || path == "" {
		return "."
	}
	return path
}
