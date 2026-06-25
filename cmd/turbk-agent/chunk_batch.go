package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/tursom/turbk/internal/repository"
	"github.com/zeebo/blake3"
)

const agentChunkBatchContentType = "application/vnd.turbk.chunk-batch.v1"

const estimatedChunkCheckResponseBytesPerHash = int64(80)

const estimatedCompactChunkUploadResponseBytesPerChunk = int64(140)

const estimatedFullChunkUploadResponseBytesPerChunk = int64(360)

const estimatedChunkBatchResponseBaseBytes = int64(512)

type chunkBatchRetryOptions struct {
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	SplitOn413     bool
	Logger         *slog.Logger
	BatchID        int64
	Operation      string
}

func (opts backupRunOptions) chunkBatchRetryOptions(logger *slog.Logger, batchID int64, operation string) chunkBatchRetryOptions {
	retry := chunkBatchRetryOptions{
		MaxRetries:     opts.ChunkBatchMaxRetries,
		InitialBackoff: opts.ChunkBatchRetryInitialBackoff,
		MaxBackoff:     opts.ChunkBatchRetryMaxBackoff,
		SplitOn413:     opts.ChunkBatchSplitOn413,
		Logger:         logger,
		BatchID:        batchID,
		Operation:      operation,
	}
	return retry.normalized()
}

func (o chunkBatchRetryOptions) normalized() chunkBatchRetryOptions {
	if o.MaxRetries <= 0 {
		o.MaxRetries = defaultAgentChunkBatchMaxRetries
	}
	if o.InitialBackoff <= 0 {
		o.InitialBackoff = defaultAgentChunkBatchRetryInitialBackoff
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = defaultAgentChunkBatchRetryMaxBackoff
	}
	if o.MaxBackoff < o.InitialBackoff {
		o.MaxBackoff = o.InitialBackoff
	}
	return o
}

type chunkBatchStats struct {
	Uploaded            int64
	Reused              int64
	CheckRequests       int64
	UploadRequests      int64
	UploadRequestBytes  int64
	CheckResponseBytes  int64
	UploadResponseBytes int64
	CheckDuration       time.Duration
	UploadDuration      time.Duration
	PipelineWait        time.Duration
	MaxPipelineBytes    int64
	BatchRetries        int64
	BatchSplits         int64
	PackedFiles         int64
	PackedBytes         int64
	PackCount           int64
}

func (s *chunkBatchStats) Add(other chunkBatchStats) {
	s.Uploaded += other.Uploaded
	s.Reused += other.Reused
	s.CheckRequests += other.CheckRequests
	s.UploadRequests += other.UploadRequests
	s.UploadRequestBytes += other.UploadRequestBytes
	s.CheckResponseBytes += other.CheckResponseBytes
	s.UploadResponseBytes += other.UploadResponseBytes
	s.CheckDuration += other.CheckDuration
	s.UploadDuration += other.UploadDuration
	s.PipelineWait += other.PipelineWait
	if other.MaxPipelineBytes > s.MaxPipelineBytes {
		s.MaxPipelineBytes = other.MaxPipelineBytes
	}
	s.BatchRetries += other.BatchRetries
	s.BatchSplits += other.BatchSplits
	s.PackedFiles += other.PackedFiles
	s.PackedBytes += other.PackedBytes
	s.PackCount += other.PackCount
}

type chunkBatchWaiter struct {
	entry *repository.FileEntry
	index int
}

type pendingBatchChunk struct {
	hash         string
	data         []byte
	originalSize int64
	waiters      []chunkBatchWaiter
}

type agentChunkBatcher struct {
	client           *agentClient
	opts             backupRunOptions
	logger           *slog.Logger
	maxChunks        int
	maxRequestBytes  int64
	maxResponseBytes int64
	pending          []*pendingBatchChunk
	pendingByHash    map[string]*pendingBatchChunk
	pendingByEntry   map[*repository.FileEntry]int
	requestBytes     int64
	nextBatchID      int64
}

type agentChunkUploader interface {
	Add(chunk []byte, entry *repository.FileEntry) (chunkBatchStats, error)
	Flush() (chunkBatchStats, error)
	EntryPending(entry *repository.FileEntry) bool
	Stop()
}

func newAgentChunkUploader(client *agentClient, opts backupRunOptions, logger *slog.Logger) agentChunkUploader {
	if opts.ChunkPipelineEnabled && opts.ChunkBatchUpload {
		return newAgentChunkPipelineBatcher(client, opts, logger)
	}
	return newAgentChunkBatcher(client, opts, logger)
}

func newAgentChunkBatcher(client *agentClient, opts backupRunOptions, logger *slog.Logger) *agentChunkBatcher {
	maxChunks := normalizedAgentChunkCheckBatchLimit(opts.MaxChunkCheckBatch)
	maxRequestBytes := opts.MaxChunkUploadBatchBytes
	if maxRequestBytes <= 0 {
		maxRequestBytes = defaultAgentChunkUploadBatchBytes
	}
	maxResponseBytes := opts.MaxChunkResponseBytes
	if maxResponseBytes <= 0 {
		maxResponseBytes = defaultAgentMaxChunkResponseBytes
	}
	return &agentChunkBatcher{
		client:           client,
		opts:             opts,
		logger:           logger,
		maxChunks:        maxChunks,
		maxRequestBytes:  maxRequestBytes,
		maxResponseBytes: maxResponseBytes,
		pendingByHash:    make(map[string]*pendingBatchChunk),
		pendingByEntry:   make(map[*repository.FileEntry]int),
	}
}

func (b *agentChunkBatcher) Stop() {}

func normalizedAgentChunkCheckBatchLimit(maxChunks int) int {
	if maxChunks <= 0 {
		return 10000
	}
	return maxChunks
}

func (b *agentChunkBatcher) Add(chunk []byte, entry *repository.FileEntry) (chunkBatchStats, error) {
	if !b.opts.ChunkBatchUpload {
		ref, uploaded, err := b.client.ensureChunk(chunk, b.opts)
		if err != nil {
			return chunkBatchStats{}, err
		}
		entry.Chunks = append(entry.Chunks, ref)
		if uploaded {
			return chunkBatchStats{Uploaded: 1}, nil
		}
		return chunkBatchStats{Reused: 1}, nil
	}

	sum := blake3.Sum256(chunk)
	hash := hex.EncodeToString(sum[:])
	if b.opts.Catalog != nil {
		status, confirmedGeneration, ok, err := b.opts.Catalog.chunkStatus(hash)
		if err != nil {
			return chunkBatchStats{}, err
		}
		if ok && status == "confirmed" && confirmedGeneration >= b.opts.ChunkGeneration {
			entry.Chunks = append(entry.Chunks, repository.ChunkRef{Hash: hash, OriginalSize: int64(len(chunk))})
			return chunkBatchStats{Reused: 1}, nil
		}
	}

	index := len(entry.Chunks)
	entry.Chunks = append(entry.Chunks, repository.ChunkRef{})
	waiter := chunkBatchWaiter{entry: entry, index: index}
	if pending, ok := b.pendingByHash[hash]; ok {
		pending.waiters = append(pending.waiters, waiter)
		b.pendingByEntry[entry]++
		return chunkBatchStats{}, nil
	}

	data := append([]byte(nil), chunk...)
	chunkRequestBytes := int64(32 + 8 + len(data))
	if 12+chunkRequestBytes > b.maxRequestBytes {
		return chunkBatchStats{}, fmt.Errorf("chunk %s size %d exceeds max batch request size %d", hash, len(data), b.maxRequestBytes)
	}
	if len(b.pending) > 0 && b.shouldFlushBeforeAdding(chunkRequestBytes) {
		stats, err := b.Flush()
		if err != nil {
			return chunkBatchStats{}, err
		}
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
		return stats, nil
	}
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
	if len(b.pending) >= b.maxChunks || b.requestBytes >= b.maxRequestBytes {
		return b.Flush()
	}
	if b.estimatedCheckResponseBytes(len(b.pending)) >= b.maxResponseBytes || b.estimatedUploadResponseBytes(len(b.pending)) >= b.maxResponseBytes {
		return b.Flush()
	}
	return chunkBatchStats{}, nil
}

func (b *agentChunkBatcher) shouldFlushBeforeAdding(nextRequestBytes int64) bool {
	nextCount := len(b.pending) + 1
	return nextCount > b.maxChunks ||
		b.requestBytes+nextRequestBytes > b.maxRequestBytes ||
		b.estimatedCheckResponseBytes(nextCount) > b.maxResponseBytes ||
		b.estimatedUploadResponseBytes(nextCount) > b.maxResponseBytes
}

func (b *agentChunkBatcher) estimatedCheckResponseBytes(chunkCount int) int64 {
	return estimatedChunkBatchResponseBaseBytes + int64(chunkCount)*estimatedChunkCheckResponseBytesPerHash
}

func (b *agentChunkBatcher) estimatedUploadResponseBytes(chunkCount int) int64 {
	perChunk := estimatedFullChunkUploadResponseBytesPerChunk
	if b.opts.CompactChunkUploadResponse {
		perChunk = estimatedCompactChunkUploadResponseBytesPerChunk
	}
	return estimatedChunkBatchResponseBaseBytes + int64(chunkCount)*perChunk
}

func (b *agentChunkBatcher) EntryPending(entry *repository.FileEntry) bool {
	return b.pendingByEntry[entry] > 0
}

func (b *agentChunkBatcher) Flush() (chunkBatchStats, error) {
	if len(b.pending) == 0 {
		return chunkBatchStats{}, nil
	}
	b.nextBatchID++
	batchID := b.nextBatchID
	pending := append([]*pendingBatchChunk(nil), b.pending...)
	hashes := make([]string, 0, len(pending))
	byHash := make(map[string]*pendingBatchChunk, len(pending))
	for _, chunk := range pending {
		hashes = append(hashes, chunk.hash)
		byHash[chunk.hash] = chunk
	}
	checkStarted := time.Now()
	checked, err := b.client.checkChunks(hashes, b.opts.RepositoryID, b.opts.ChunkGeneration, b.opts.CompactChunkCheckResponse, b.opts.chunkBatchRetryOptions(b.logger, batchID, "check"))
	if err != nil {
		return chunkBatchStats{}, err
	}
	if err := validateChunkCheckResponse(hashes, b.opts.RepositoryID, checked, b.opts.CompactChunkCheckResponse); err != nil {
		return chunkBatchStats{}, err
	}
	stats := chunkBatchStats{}
	stats.CheckRequests++
	stats.CheckDuration += time.Since(checkStarted)
	stats.CheckResponseBytes += checked.ResponseBytes
	stats.BatchRetries += checked.RetryCount
	catalogUpdates := make([]agentChunkStatusUpdate, 0, len(pending))
	seen := make(map[string]struct{}, len(pending))
	for _, hash := range existingChunkHashes(hashes, checked, b.opts.CompactChunkCheckResponse) {
		chunk, ok := byHash[hash]
		if !ok {
			return chunkBatchStats{}, fmt.Errorf("server check returned unexpected chunk %s", hash)
		}
		seen[hash] = struct{}{}
		ref := repository.ChunkRef{Hash: hash, OriginalSize: chunk.originalSize}
		b.fillPendingChunk(chunk, ref)
		stats.Reused += int64(len(chunk.waiters))
		if b.opts.Catalog != nil {
			catalogUpdates = append(catalogUpdates, agentChunkStatusUpdate{
				Hash:         hash,
				OriginalSize: chunk.originalSize,
				Status:       "confirmed",
				Generation:   checked.ChunkGeneration,
			})
		}
	}
	missingChunks := make([]*pendingBatchChunk, 0, len(checked.Missing))
	for _, hash := range checked.Missing {
		chunk, ok := byHash[hash]
		if !ok {
			return chunkBatchStats{}, fmt.Errorf("server check returned unexpected missing chunk %s", hash)
		}
		seen[hash] = struct{}{}
		missingChunks = append(missingChunks, chunk)
	}
	for _, chunk := range pending {
		if _, ok := seen[chunk.hash]; !ok {
			return chunkBatchStats{}, fmt.Errorf("server check omitted chunk %s", chunk.hash)
		}
	}
	if len(missingChunks) > 0 {
		uploadStarted := time.Now()
		uploaded, err := b.client.uploadChunksBatch(missingChunks, b.opts.CompactChunkUploadResponse, b.opts.chunkBatchRetryOptions(b.logger, batchID, "upload"))
		if err != nil {
			return chunkBatchStats{}, err
		}
		stats.UploadRequests++
		stats.UploadDuration += time.Since(uploadStarted)
		stats.UploadRequestBytes += uploaded.RequestBytes
		stats.UploadResponseBytes += uploaded.ResponseBytes
		stats.BatchRetries += uploaded.RetryCount
		stats.BatchSplits += uploaded.SplitCount
		uploadedByHash, err := validateChunkUploadResponse(missingChunks, b.opts.RepositoryID, uploaded, b.opts.CompactChunkUploadResponse)
		if err != nil {
			return chunkBatchStats{}, err
		}
		generation := uploaded.ChunkGeneration
		if generation == 0 {
			generation = b.opts.ChunkGeneration
		}
		for _, pendingChunk := range missingChunks {
			resp := uploadedByHash[pendingChunk.hash]
			b.fillPendingChunk(pendingChunk, resp.Ref)
			if resp.Uploaded {
				stats.Uploaded++
				if len(pendingChunk.waiters) > 1 {
					stats.Reused += int64(len(pendingChunk.waiters) - 1)
				}
			} else {
				stats.Reused += int64(len(pendingChunk.waiters))
			}
			if b.opts.Catalog != nil {
				catalogUpdates = append(catalogUpdates, agentChunkStatusUpdate{
					Hash:         pendingChunk.hash,
					OriginalSize: resp.Ref.OriginalSize,
					Status:       "confirmed",
					Generation:   generation,
					Uploaded:     resp.Uploaded,
				})
			}
		}
	}
	if b.opts.Catalog != nil {
		if err := b.opts.Catalog.markChunks(catalogUpdates); err != nil {
			return chunkBatchStats{}, err
		}
	}
	for _, chunk := range pending {
		delete(b.pendingByHash, chunk.hash)
	}
	b.pending = b.pending[:0]
	b.requestBytes = 0
	return stats, nil
}

func (b *agentChunkBatcher) fillPendingChunk(chunk *pendingBatchChunk, ref repository.ChunkRef) {
	for _, waiter := range chunk.waiters {
		waiter.entry.Chunks[waiter.index] = ref
		if count := b.pendingByEntry[waiter.entry]; count <= 1 {
			delete(b.pendingByEntry, waiter.entry)
		} else {
			b.pendingByEntry[waiter.entry] = count - 1
		}
	}
}

func estimateAgentChunkCheckRequestBytes(hashes []string) int64 {
	total := int64(128)
	for _, hash := range hashes {
		total += int64(len(hash) + 4)
	}
	return total
}

func (c *agentClient) ensureChunk(chunk []byte, opts backupRunOptions) (repository.ChunkRef, bool, error) {
	sum := blake3.Sum256(chunk)
	hash := hex.EncodeToString(sum[:])
	if opts.Catalog != nil {
		status, confirmedGeneration, ok, err := opts.Catalog.chunkStatus(hash)
		if err != nil {
			return repository.ChunkRef{}, false, err
		}
		if ok && status == "confirmed" && confirmedGeneration >= opts.ChunkGeneration {
			return repository.ChunkRef{Hash: hash, OriginalSize: int64(len(chunk))}, false, nil
		}
	}
	var queried chunkResponse
	if _, err := c.doJSON(http.MethodGet, "/agent/v1/chunks/"+hash, nil, &queried); err != nil {
		return repository.ChunkRef{}, false, err
	}
	if queried.Exists {
		if queried.Ref.Hash == "" {
			return repository.ChunkRef{}, false, fmt.Errorf("server reported existing chunk %s without ref", hash)
		}
		if opts.Catalog != nil {
			_ = opts.Catalog.markChunk(hash, queried.Ref.OriginalSize, "confirmed", opts.ChunkGeneration, false)
		}
		return queried.Ref, false, nil
	}
	var uploaded chunkResponse
	status, err := c.doRaw(http.MethodPut, "/agent/v1/chunks/"+hash, bytes.NewReader(chunk), "application/octet-stream", &uploaded)
	if err != nil {
		return repository.ChunkRef{}, false, err
	}
	if status != http.StatusCreated && status != http.StatusOK {
		return repository.ChunkRef{}, false, fmt.Errorf("unexpected chunk upload status %d", status)
	}
	if uploaded.Ref.Hash == "" {
		return repository.ChunkRef{}, false, fmt.Errorf("server accepted chunk %s without ref", hash)
	}
	if opts.Catalog != nil {
		_ = opts.Catalog.markChunk(hash, uploaded.Ref.OriginalSize, "confirmed", opts.ChunkGeneration, status == http.StatusCreated && uploaded.Uploaded)
	}
	return uploaded.Ref, status == http.StatusCreated && uploaded.Uploaded, nil
}

func (c *agentClient) checkChunksBatched(hashes []string, repositoryID string, baseGeneration int64, maxChunks int, compactResponse bool, retry chunkBatchRetryOptions) (checkChunksResponse, error) {
	maxChunks = normalizedAgentChunkCheckBatchLimit(maxChunks)
	if len(hashes) <= maxChunks {
		return c.checkChunks(hashes, repositoryID, baseGeneration, compactResponse, retry)
	}
	combined := checkChunksResponse{RepositoryID: repositoryID}
	for start := 0; start < len(hashes); start += maxChunks {
		end := start + maxChunks
		if end > len(hashes) {
			end = len(hashes)
		}
		batch := hashes[start:end]
		checked, err := c.checkChunks(batch, repositoryID, baseGeneration, compactResponse, retry)
		if err != nil {
			return checkChunksResponse{}, err
		}
		if err := validateChunkCheckResponse(batch, repositoryID, checked, compactResponse); err != nil {
			return checkChunksResponse{}, err
		}
		if combined.RepositoryID == "" {
			combined.RepositoryID = checked.RepositoryID
		}
		if checked.ChunkGeneration > combined.ChunkGeneration {
			combined.ChunkGeneration = checked.ChunkGeneration
		}
		combined.Exists = append(combined.Exists, existingChunkHashes(batch, checked, compactResponse)...)
		combined.Missing = append(combined.Missing, checked.Missing...)
		combined.ResponseBytes += checked.ResponseBytes
		combined.RetryCount += checked.RetryCount
	}
	return combined, nil
}

func validateChunkCheckResponse(requested []string, repositoryID string, checked checkChunksResponse, allowCompact bool) error {
	if repositoryID != "" && checked.RepositoryID != "" && checked.RepositoryID != repositoryID {
		return fmt.Errorf("server check repository_id = %q, want %q", checked.RepositoryID, repositoryID)
	}
	requestedSet := make(map[string]struct{}, len(requested))
	for _, hash := range requested {
		hash = strings.TrimSpace(hash)
		if hash == "" {
			return fmt.Errorf("server check request contains empty chunk hash")
		}
		requestedSet[hash] = struct{}{}
	}
	seen := make(map[string]string, len(requestedSet))
	for _, hash := range checked.Exists {
		hash = strings.TrimSpace(hash)
		if _, ok := requestedSet[hash]; !ok {
			return fmt.Errorf("server check returned unexpected existing chunk %s", hash)
		}
		if previous, ok := seen[hash]; ok {
			if previous == "existing" {
				return fmt.Errorf("server check returned duplicate existing chunk %s", hash)
			}
			return fmt.Errorf("server check returned chunk %s as both %s and existing", hash, previous)
		}
		seen[hash] = "existing"
	}
	for _, hash := range checked.Missing {
		hash = strings.TrimSpace(hash)
		if _, ok := requestedSet[hash]; !ok {
			return fmt.Errorf("server check returned unexpected missing chunk %s", hash)
		}
		if previous, ok := seen[hash]; ok {
			if previous == "missing" {
				return fmt.Errorf("server check returned duplicate missing chunk %s", hash)
			}
			return fmt.Errorf("server check returned chunk %s as both %s and missing", hash, previous)
		}
		seen[hash] = "missing"
	}
	if !(allowCompact && len(checked.Exists) == 0) {
		for hash := range requestedSet {
			if _, ok := seen[hash]; !ok {
				return fmt.Errorf("server check omitted chunk %s", hash)
			}
		}
	}
	return nil
}

func existingChunkHashes(requested []string, checked checkChunksResponse, allowCompact bool) []string {
	if !(allowCompact && len(checked.Exists) == 0) {
		return checked.Exists
	}
	missing := make(map[string]struct{}, len(checked.Missing))
	for _, hash := range checked.Missing {
		missing[strings.TrimSpace(hash)] = struct{}{}
	}
	exists := make([]string, 0, len(requested)-len(missing))
	seen := make(map[string]struct{}, len(requested))
	for _, hash := range requested {
		hash = strings.TrimSpace(hash)
		if hash == "" {
			continue
		}
		if _, ok := seen[hash]; ok {
			continue
		}
		seen[hash] = struct{}{}
		if _, ok := missing[hash]; !ok {
			exists = append(exists, hash)
		}
	}
	return exists
}

func validateChunkUploadResponse(requested []*pendingBatchChunk, repositoryID string, uploaded uploadChunksResponse, allowCompact bool) (map[string]uploadChunkResponse, error) {
	if repositoryID != "" && uploaded.RepositoryID != "" && uploaded.RepositoryID != repositoryID {
		return nil, fmt.Errorf("server upload repository_id = %q, want %q", uploaded.RepositoryID, repositoryID)
	}
	requestedSet := make(map[string]int64, len(requested))
	for _, chunk := range requested {
		if chunk == nil || strings.TrimSpace(chunk.hash) == "" {
			return nil, fmt.Errorf("upload request contains empty chunk hash")
		}
		requestedSet[chunk.hash] = chunk.originalSize
	}
	uploadedByHash := make(map[string]uploadChunkResponse, len(uploaded.Chunks))
	for _, chunk := range uploaded.Chunks {
		hash := strings.TrimSpace(chunk.Hash)
		originalSize, ok := requestedSet[hash]
		if !ok {
			return nil, fmt.Errorf("server upload returned unexpected chunk %s", hash)
		}
		if _, ok := uploadedByHash[hash]; ok {
			return nil, fmt.Errorf("server upload returned duplicate chunk %s", hash)
		}
		if chunk.Exists != nil && !*chunk.Exists {
			return nil, fmt.Errorf("server upload did not confirm chunk %s", hash)
		}
		if chunk.Exists == nil && !allowCompact {
			return nil, fmt.Errorf("server upload did not confirm chunk %s", hash)
		}
		if chunk.Ref.Hash == "" {
			if !allowCompact {
				return nil, fmt.Errorf("server accepted chunk %s without ref", hash)
			}
			if chunk.OriginalSize <= 0 {
				return nil, fmt.Errorf("server accepted chunk %s without original_size", hash)
			}
			chunk.Ref = repository.ChunkRef{Hash: hash, OriginalSize: chunk.OriginalSize}
		} else if chunk.Ref.Hash != hash {
			return nil, fmt.Errorf("server returned ref %s for uploaded chunk %s", chunk.Ref.Hash, hash)
		}
		if chunk.OriginalSize != 0 && chunk.OriginalSize != originalSize {
			return nil, fmt.Errorf("server returned original_size %d for uploaded chunk %s, want %d", chunk.OriginalSize, hash, originalSize)
		}
		if chunk.Ref.OriginalSize != originalSize {
			return nil, fmt.Errorf("server returned ref original_size %d for uploaded chunk %s, want %d", chunk.Ref.OriginalSize, hash, originalSize)
		}
		uploadedByHash[hash] = chunk
	}
	for hash := range requestedSet {
		if _, ok := uploadedByHash[hash]; !ok {
			return nil, fmt.Errorf("server upload omitted chunk %s", hash)
		}
	}
	return uploadedByHash, nil
}

func (c *agentClient) checkChunks(hashes []string, repositoryID string, baseGeneration int64, compactResponse bool, retry chunkBatchRetryOptions) (checkChunksResponse, error) {
	retry = retry.normalized()
	requestValue := map[string]any{
		"repository_id":         repositoryID,
		"base_chunk_generation": baseGeneration,
		"hashes":                hashes,
		"compact_response":      compactResponse,
	}
	for attempt := 1; ; attempt++ {
		var resp checkChunksResponse
		_, err := c.doJSON(http.MethodPost, "/agent/v1/chunks/check", requestValue, &resp)
		if err == nil {
			resp.RetryCount = int64(attempt - 1)
			return resp, nil
		}
		if !retryableChunkBatchError(err) || attempt > retry.MaxRetries {
			return checkChunksResponse{}, err
		}
		backoff := chunkBatchRetryBackoff(retry, attempt, err)
		logChunkBatchRetry(retry, attempt, backoff, err)
		time.Sleep(backoff)
	}
}

func (c *agentClient) uploadChunksBatch(chunks []*pendingBatchChunk, compactResponse bool, retry chunkBatchRetryOptions) (uploadChunksResponse, error) {
	return c.uploadChunksBatchWithSplit(chunks, compactResponse, retry.normalized())
}

func (c *agentClient) uploadChunksBatchWithSplit(chunks []*pendingBatchChunk, compactResponse bool, retry chunkBatchRetryOptions) (uploadChunksResponse, error) {
	resp, err := c.uploadChunksBatchNoSplit(chunks, compactResponse, retry)
	if err == nil {
		return resp, nil
	}
	if retry.SplitOn413 && len(chunks) > 1 && chunkBatchHTTPStatus(err) == http.StatusRequestEntityTooLarge {
		logChunkBatchSplit(retry, len(chunks), err)
		mid := len(chunks) / 2
		left, leftErr := c.uploadChunksBatchWithSplit(chunks[:mid], compactResponse, retry)
		if leftErr != nil {
			return uploadChunksResponse{}, leftErr
		}
		right, rightErr := c.uploadChunksBatchWithSplit(chunks[mid:], compactResponse, retry)
		if rightErr != nil {
			return uploadChunksResponse{}, rightErr
		}
		return combineUploadChunksResponses(left, right)
	}
	return uploadChunksResponse{}, err
}

func (c *agentClient) uploadChunksBatchNoSplit(chunks []*pendingBatchChunk, compactResponse bool, retry chunkBatchRetryOptions) (uploadChunksResponse, error) {
	path := "/agent/v1/chunks/upload"
	if compactResponse {
		path += "?compact_response=1"
	}
	for attempt := 1; ; attempt++ {
		body, err := newAgentChunkBatchBody(chunks)
		if err != nil {
			return uploadChunksResponse{}, err
		}
		var resp uploadChunksResponse
		status, err := c.doRawAllowStatuses(http.MethodPost, path, body, agentChunkBatchContentType, &resp, http.StatusNotFound)
		if err == nil {
			if status == http.StatusNotFound {
				legacy, err := c.uploadChunksLegacy(chunks)
				if err != nil {
					return uploadChunksResponse{}, err
				}
				legacy.RetryCount = int64(attempt - 1)
				return legacy, nil
			}
			if status != http.StatusAccepted && status != http.StatusOK {
				return uploadChunksResponse{}, fmt.Errorf("unexpected chunk batch upload status %d", status)
			}
			resp.RequestBytes = body.Size()
			resp.RetryCount = int64(attempt - 1)
			return resp, nil
		}
		if chunkBatchHTTPStatus(err) == http.StatusRequestEntityTooLarge {
			return uploadChunksResponse{}, err
		}
		if !retryableChunkBatchError(err) || attempt > retry.MaxRetries {
			return uploadChunksResponse{}, err
		}
		backoff := chunkBatchRetryBackoff(retry, attempt, err)
		logChunkBatchRetry(retry, attempt, backoff, err)
		time.Sleep(backoff)
	}
}

func combineUploadChunksResponses(left, right uploadChunksResponse) (uploadChunksResponse, error) {
	combined := left
	if combined.Status == "" {
		combined.Status = right.Status
	}
	if combined.RepositoryID == "" {
		combined.RepositoryID = right.RepositoryID
	} else if right.RepositoryID != "" && combined.RepositoryID != right.RepositoryID {
		return uploadChunksResponse{}, fmt.Errorf("split chunk upload repository_id mismatch: %s != %s", combined.RepositoryID, right.RepositoryID)
	}
	if right.ChunkGeneration > combined.ChunkGeneration {
		combined.ChunkGeneration = right.ChunkGeneration
	}
	combined.Chunks = append(append([]uploadChunkResponse(nil), left.Chunks...), right.Chunks...)
	combined.RequestBytes = left.RequestBytes + right.RequestBytes
	combined.ResponseBytes = left.ResponseBytes + right.ResponseBytes
	combined.RetryCount = left.RetryCount + right.RetryCount
	combined.SplitCount = left.SplitCount + right.SplitCount + 1
	return combined, nil
}

func retryableChunkBatchError(err error) bool {
	var httpErr *agentHTTPError
	if errors.As(err, &httpErr) {
		return httpErr.StatusCode == http.StatusTooManyRequests || (httpErr.StatusCode >= 500 && httpErr.StatusCode <= 599)
	}
	var timeoutErr interface{ Timeout() bool }
	if errors.As(err, &timeoutErr) && timeoutErr.Timeout() {
		return true
	}
	var temporaryErr interface{ Temporary() bool }
	if errors.As(err, &temporaryErr) && temporaryErr.Temporary() {
		return true
	}
	return false
}

func chunkBatchHTTPStatus(err error) int {
	var httpErr *agentHTTPError
	if errors.As(err, &httpErr) {
		return httpErr.StatusCode
	}
	return 0
}

func chunkBatchRetryBackoff(retry chunkBatchRetryOptions, attempt int, err error) time.Duration {
	var httpErr *agentHTTPError
	if errors.As(err, &httpErr) && httpErr.RetryAfterSet {
		return httpErr.RetryAfter
	}
	backoff := retry.InitialBackoff
	for i := 1; i < attempt; i++ {
		if backoff >= retry.MaxBackoff/2 {
			return retry.MaxBackoff
		}
		backoff *= 2
	}
	if backoff > retry.MaxBackoff {
		return retry.MaxBackoff
	}
	return backoff
}

func logChunkBatchRetry(retry chunkBatchRetryOptions, attempt int, backoff time.Duration, err error) {
	if retry.Logger == nil {
		return
	}
	retry.Logger.Warn("agent chunk batch retry",
		"operation", retry.Operation,
		"batch_id", retry.BatchID,
		"attempt", attempt,
		"backoff", backoff.String(),
		"last_error", err,
	)
}

func logChunkBatchSplit(retry chunkBatchRetryOptions, chunks int, err error) {
	if retry.Logger == nil {
		return
	}
	retry.Logger.Warn("agent chunk batch split",
		"operation", retry.Operation,
		"batch_id", retry.BatchID,
		"chunks", chunks,
		"split_count", 1,
		"last_error", err,
	)
}

type agentChunkBatchBody struct {
	io.Reader
	size int64
}

func (b agentChunkBatchBody) Size() int64 {
	return b.size
}

func newAgentChunkBatchBody(chunks []*pendingBatchChunk) (agentChunkBatchBody, error) {
	readers := make([]io.Reader, 0, 2+len(chunks)*3)
	readers = append(readers, bytes.NewReader(agentChunkBatchMagic))
	var count [4]byte
	binary.BigEndian.PutUint32(count[:], uint32(len(chunks)))
	readers = append(readers, bytes.NewReader(count[:]))
	size := int64(len(agentChunkBatchMagic) + len(count))
	for _, chunk := range chunks {
		hashBytes, err := hex.DecodeString(chunk.hash)
		if err != nil || len(hashBytes) != 32 {
			return agentChunkBatchBody{}, fmt.Errorf("invalid chunk hash %q", chunk.hash)
		}
		length := make([]byte, 8)
		binary.BigEndian.PutUint64(length, uint64(len(chunk.data)))
		readers = append(readers, bytes.NewReader(hashBytes), bytes.NewReader(length), bytes.NewReader(chunk.data))
		size += int64(32 + len(length) + len(chunk.data))
	}
	return agentChunkBatchBody{Reader: io.MultiReader(readers...), size: size}, nil
}

func (c *agentClient) uploadChunksLegacy(chunks []*pendingBatchChunk) (uploadChunksResponse, error) {
	resp := uploadChunksResponse{Status: "accepted", Chunks: make([]uploadChunkResponse, 0, len(chunks))}
	for _, chunk := range chunks {
		uploaded, err := c.putSingleChunk(chunk.hash, chunk.data)
		if err != nil {
			return uploadChunksResponse{}, err
		}
		resp.Chunks = append(resp.Chunks, uploaded)
	}
	return resp, nil
}

func (c *agentClient) putSingleChunk(hash string, chunk []byte) (uploadChunkResponse, error) {
	var uploaded chunkResponse
	status, err := c.doRaw(http.MethodPut, "/agent/v1/chunks/"+hash, bytes.NewReader(chunk), "application/octet-stream", &uploaded)
	if err != nil {
		return uploadChunkResponse{}, err
	}
	if status != http.StatusCreated && status != http.StatusOK {
		return uploadChunkResponse{}, fmt.Errorf("unexpected chunk upload status %d", status)
	}
	if uploaded.Ref.Hash == "" {
		return uploadChunkResponse{}, fmt.Errorf("server accepted chunk %s without ref", hash)
	}
	return uploadChunkResponse{
		Hash:     hash,
		Exists:   boolPtr(uploaded.Exists),
		Uploaded: status == http.StatusCreated && uploaded.Uploaded,
		Ref:      uploaded.Ref,
	}, nil
}

func boolPtr(value bool) *bool {
	return &value
}
