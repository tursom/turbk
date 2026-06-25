package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tursom/turbk/internal/fsfilter"
	"github.com/tursom/turbk/internal/repository"
	"github.com/zeebo/blake3"
)

type agentThroughputBenchmarkOptions struct {
	Files              int
	FileSize           int64
	Modes              []string
	RTT                time.Duration
	HTTP500Uploads     int
	HTTP429Uploads     int
	HTTP413Threshold   int64
	MaxChunkCheckBatch int
}

type agentThroughputBenchmarkResult struct {
	Mode                  string  `json:"mode"`
	Files                 int     `json:"files"`
	FileSize              int64   `json:"file_size"`
	Bytes                 int64   `json:"bytes"`
	DurationSeconds       float64 `json:"duration_seconds"`
	ThroughputBytesPerSec float64 `json:"throughput_bytes_per_sec"`
	CheckRequests         int64   `json:"check_requests"`
	UploadRequests        int64   `json:"upload_requests"`
	UploadRequestBytes    int64   `json:"upload_request_bytes"`
	UploadResponseBytes   int64   `json:"upload_response_bytes"`
	Injected500           int64   `json:"injected_500"`
	Injected429           int64   `json:"injected_429"`
	Injected413           int64   `json:"injected_413"`
	ManifestEquivalent    bool    `json:"manifest_equivalent"`
	ManifestEntries       int     `json:"manifest_entries"`
	AllocBytes            uint64  `json:"alloc_bytes"`
	PeakHeapAllocBytes    uint64  `json:"peak_heap_alloc_bytes"`
}

type agentThroughputBenchmarkMemorySampler struct {
	done chan struct{}
	wg   sync.WaitGroup
	peak atomic.Uint64
}

type agentThroughputMockServer struct {
	rtt              time.Duration
	http500Uploads   int
	http429Uploads   int
	http413Threshold int64
	mu               sync.Mutex
	run              agentThroughputMockRun
}

type agentThroughputMockRun struct {
	remaining500        int
	remaining429        int
	checkRequests       int64
	uploadRequests      int64
	uploadRequestBytes  int64
	uploadResponseBytes int64
	injected500         int64
	injected429         int64
	injected413         int64
}

func newAgentThroughputBenchmarkOptions(files int, fileSizeValue, modesValue, rttValue string, http500Uploads, http429Uploads int, http413ThresholdValue string, maxChunkCheckBatch int) (agentThroughputBenchmarkOptions, error) {
	if files <= 0 {
		return agentThroughputBenchmarkOptions{}, errors.New("throughput-files must be positive")
	}
	fileSize, err := parseAgentByteSize(fileSizeValue)
	if err != nil {
		return agentThroughputBenchmarkOptions{}, fmt.Errorf("throughput-file-size: %w", err)
	}
	if fileSize < 0 {
		return agentThroughputBenchmarkOptions{}, errors.New("throughput-file-size must be non-negative")
	}
	rtt, err := time.ParseDuration(strings.TrimSpace(rttValue))
	if err != nil {
		return agentThroughputBenchmarkOptions{}, fmt.Errorf("throughput-rtt: %w", err)
	}
	if rtt < 0 {
		return agentThroughputBenchmarkOptions{}, errors.New("throughput-rtt must be non-negative")
	}
	var threshold int64
	if strings.TrimSpace(http413ThresholdValue) != "" {
		threshold, err = parseAgentByteSize(http413ThresholdValue)
		if err != nil {
			return agentThroughputBenchmarkOptions{}, fmt.Errorf("throughput-http-413-threshold: %w", err)
		}
		if threshold <= 0 {
			return agentThroughputBenchmarkOptions{}, errors.New("throughput-http-413-threshold must be positive")
		}
	}
	modes := splitBenchmarkModes(modesValue)
	if len(modes) == 0 {
		return agentThroughputBenchmarkOptions{}, errors.New("throughput-modes is required")
	}
	if maxChunkCheckBatch <= 0 {
		maxChunkCheckBatch = 1
	}
	return agentThroughputBenchmarkOptions{
		Files:              files,
		FileSize:           fileSize,
		Modes:              modes,
		RTT:                rtt,
		HTTP500Uploads:     http500Uploads,
		HTTP429Uploads:     http429Uploads,
		HTTP413Threshold:   threshold,
		MaxChunkCheckBatch: maxChunkCheckBatch,
	}, nil
}

func splitBenchmarkModes(value string) []string {
	parts := strings.Split(value, ",")
	modes := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		mode := strings.ToLower(strings.TrimSpace(part))
		if mode == "" {
			continue
		}
		if _, ok := seen[mode]; ok {
			continue
		}
		seen[mode] = struct{}{}
		modes = append(modes, mode)
	}
	return modes
}

func runAgentThroughputBenchmark(opts agentThroughputBenchmarkOptions, output io.Writer) error {
	root, err := os.MkdirTemp("", "turbk-agent-throughput-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(root)
	if err := generateBenchmarkFiles(root, opts.Files, opts.FileSize); err != nil {
		return err
	}
	mock := &agentThroughputMockServer{
		rtt:              opts.RTT,
		http500Uploads:   opts.HTTP500Uploads,
		http429Uploads:   opts.HTTP429Uploads,
		http413Threshold: opts.HTTP413Threshold,
	}
	server := httptest.NewServer(mock.handler())
	defer server.Close()
	client := newAgentClient(server.URL, "", "")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	results := make([]agentThroughputBenchmarkResult, 0, len(opts.Modes))
	var baseline string
	for _, mode := range opts.Modes {
		mock.reset()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		sampler := startAgentThroughputBenchmarkMemorySampler(10 * time.Millisecond)
		started := time.Now()
		manifest, err := client.scanAndUpload(1, []string{root}, logger, fsfilter.Options{}, benchmarkRunOptions(opts, mode))
		peakHeapAlloc := sampler.stop()
		if err != nil {
			return fmt.Errorf("mode %s: %w", mode, err)
		}
		duration := time.Since(started)
		runtime.ReadMemStats(&after)
		fingerprint := benchmarkManifestFingerprint(manifest)
		if baseline == "" {
			baseline = fingerprint
		}
		stats := mock.stats()
		totalBytes := int64(opts.Files) * opts.FileSize
		throughput := float64(0)
		if duration > 0 {
			throughput = float64(totalBytes) / duration.Seconds()
		}
		result := agentThroughputBenchmarkResult{
			Mode:                  mode,
			Files:                 opts.Files,
			FileSize:              opts.FileSize,
			Bytes:                 totalBytes,
			DurationSeconds:       duration.Seconds(),
			ThroughputBytesPerSec: throughput,
			CheckRequests:         stats.checkRequests,
			UploadRequests:        stats.uploadRequests,
			UploadRequestBytes:    stats.uploadRequestBytes,
			UploadResponseBytes:   stats.uploadResponseBytes,
			Injected500:           stats.injected500,
			Injected429:           stats.injected429,
			Injected413:           stats.injected413,
			ManifestEquivalent:    fingerprint == baseline,
			ManifestEntries:       len(manifest.Entries),
			AllocBytes:            after.TotalAlloc - before.TotalAlloc,
			PeakHeapAllocBytes:    peakHeapAlloc,
		}
		results = append(results, result)
	}
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	return encoder.Encode(results)
}

func startAgentThroughputBenchmarkMemorySampler(interval time.Duration) *agentThroughputBenchmarkMemorySampler {
	if interval <= 0 {
		interval = 10 * time.Millisecond
	}
	sampler := &agentThroughputBenchmarkMemorySampler{
		done: make(chan struct{}),
	}
	sample := func() {
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		atomicMaxUint64(&sampler.peak, stats.HeapAlloc)
	}
	sampler.wg.Add(1)
	go func() {
		defer sampler.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		sample()
		for {
			select {
			case <-ticker.C:
				sample()
			case <-sampler.done:
				sample()
				return
			}
		}
	}()
	return sampler
}

func (s *agentThroughputBenchmarkMemorySampler) stop() uint64 {
	close(s.done)
	s.wg.Wait()
	return s.peak.Load()
}

func atomicMaxUint64(value *atomic.Uint64, candidate uint64) {
	for {
		current := value.Load()
		if candidate <= current {
			return
		}
		if value.CompareAndSwap(current, candidate) {
			return
		}
	}
}

func benchmarkRunOptions(opts agentThroughputBenchmarkOptions, mode string) backupRunOptions {
	runOpts := backupRunOptions{
		RepositoryID:                  "benchmark-repo",
		ChunkGeneration:               1,
		MaxChunkCheckBatch:            opts.MaxChunkCheckBatch,
		MaxChunkUploadBatchBytes:      defaultAgentChunkUploadBatchBytes,
		MaxChunkResponseBytes:         defaultAgentMaxChunkResponseBytes,
		ChunkBatchMaxRetries:          defaultAgentChunkBatchMaxRetries,
		ChunkBatchRetryInitialBackoff: time.Millisecond,
		ChunkBatchRetryMaxBackoff:     10 * time.Millisecond,
		ChunkBatchSplitOn413:          true,
		ChunkBatchUpload:              true,
		CompactChunkCheckResponse:     true,
		CompactChunkUploadResponse:    true,
		MaxChunkCheckInflight:         1,
		MaxChunkUploadInflight:        1,
		MaxChunkPipelineBytes:         defaultAgentMaxChunkPipelineBytes,
		FileReadWorkers:               2,
		FileReadPipelineBytes:         defaultAgentFileReadPipelineBytes,
		MaxManifestRepairAttempts:     1,
	}
	switch mode {
	case "serial":
	case "pipeline", "write_queue":
		runOpts.ChunkPipelineEnabled = true
		runOpts.MaxChunkCheckInflight = 4
		runOpts.MaxChunkUploadInflight = 4
	case "parallel":
		runOpts.ChunkPipelineEnabled = true
		runOpts.MaxChunkCheckInflight = 4
		runOpts.MaxChunkUploadInflight = 4
		runOpts.ScanParallelEnabled = true
		runOpts.FileReadWorkers = 4
	default:
		runOpts.Trigger = mode
	}
	return runOpts
}

func generateBenchmarkFiles(root string, files int, size int64) error {
	for i := 0; i < files; i++ {
		data := make([]byte, size)
		for j := range data {
			data[j] = byte((i + j) % 251)
		}
		name := filepath.Join(root, fmt.Sprintf("file-%05d.bin", i))
		if err := os.WriteFile(name, data, 0o644); err != nil {
			return err
		}
	}
	return nil
}

func (m *agentThroughputMockServer) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.run = agentThroughputMockRun{
		remaining500: m.http500Uploads,
		remaining429: m.http429Uploads,
	}
}

func (m *agentThroughputMockServer) stats() agentThroughputMockRun {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.run
}

func (m *agentThroughputMockServer) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/progress"):
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "accepted"})
		case r.Method == http.MethodPost && r.URL.Path == "/agent/v1/chunks/check":
			time.Sleep(m.rtt)
			m.handleBenchmarkCheck(w, r)
		case r.Method == http.MethodPost && r.URL.Path == "/agent/v1/chunks/upload":
			time.Sleep(m.rtt)
			m.handleBenchmarkUpload(w, r)
		default:
			http.NotFound(w, r)
		}
	})
}

func (m *agentThroughputMockServer) handleBenchmarkCheck(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Hashes []string `json:"hashes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	m.mu.Lock()
	m.run.checkRequests++
	m.mu.Unlock()
	_ = json.NewEncoder(w).Encode(map[string]any{
		"repository_id":    "benchmark-repo",
		"chunk_generation": 1,
		"missing":          req.Hashes,
	})
}

func (m *agentThroughputMockServer) handleBenchmarkUpload(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	m.mu.Lock()
	m.run.uploadRequests++
	m.run.uploadRequestBytes += int64(len(body))
	if m.run.remaining500 > 0 {
		m.run.remaining500--
		m.run.injected500++
		m.mu.Unlock()
		http.Error(w, "injected 500", http.StatusInternalServerError)
		return
	}
	if m.run.remaining429 > 0 {
		m.run.remaining429--
		m.run.injected429++
		m.mu.Unlock()
		w.Header().Set("Retry-After", "0")
		http.Error(w, "injected 429", http.StatusTooManyRequests)
		return
	}
	if m.http413Threshold > 0 && int64(len(body)) > m.http413Threshold {
		m.run.injected413++
		m.mu.Unlock()
		http.Error(w, "injected 413", http.StatusRequestEntityTooLarge)
		return
	}
	m.mu.Unlock()

	chunks, err := decodeAgentChunkBatchData(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	respChunks := make([]map[string]any, 0, len(chunks))
	for _, chunk := range chunks {
		respChunks = append(respChunks, map[string]any{
			"hash":          chunk.hash,
			"uploaded":      true,
			"original_size": len(chunk.data),
		})
	}
	response := map[string]any{
		"status":           "accepted",
		"repository_id":    "benchmark-repo",
		"chunk_generation": 1,
		"chunks":           respChunks,
	}
	if data, err := json.Marshal(response); err == nil {
		m.mu.Lock()
		m.run.uploadResponseBytes += int64(len(data))
		m.mu.Unlock()
	}
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(response)
}

type decodedAgentChunk struct {
	hash string
	data []byte
}

func decodeAgentChunkBatchData(data []byte) ([]decodedAgentChunk, error) {
	if !bytes.HasPrefix(data, agentChunkBatchMagic) {
		return nil, errors.New("chunk batch magic mismatch")
	}
	offset := len(agentChunkBatchMagic)
	if len(data) < offset+4 {
		return nil, errors.New("chunk batch missing count")
	}
	count := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	chunks := make([]decodedAgentChunk, 0, count)
	for i := uint32(0); i < count; i++ {
		if len(data) < offset+32+8 {
			return nil, fmt.Errorf("chunk %d missing header", i)
		}
		hashBytes := data[offset : offset+32]
		offset += 32
		length := binary.BigEndian.Uint64(data[offset : offset+8])
		offset += 8
		if length > uint64(len(data)-offset) {
			return nil, fmt.Errorf("chunk %d length %d exceeds remaining body %d", i, length, len(data)-offset)
		}
		chunk := data[offset : offset+int(length)]
		offset += int(length)
		sum := blake3.Sum256(chunk)
		if !bytes.Equal(sum[:], hashBytes) {
			return nil, fmt.Errorf("chunk %d hash mismatch", i)
		}
		chunks = append(chunks, decodedAgentChunk{
			hash: hex.EncodeToString(hashBytes),
			data: chunk,
		})
	}
	if offset != len(data) {
		return nil, errors.New("chunk batch has trailing bytes")
	}
	return chunks, nil
}

func benchmarkManifestFingerprint(manifest *repository.SnapshotManifest) string {
	entries := make([]repository.FileEntry, len(manifest.Entries))
	copy(entries, manifest.Entries)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Path < entries[j].Path
	})
	type chunk struct {
		Hash         string `json:"hash"`
		OriginalSize int64  `json:"original_size"`
	}
	type entry struct {
		Path       string               `json:"path"`
		Type       repository.EntryType `json:"type"`
		Size       int64                `json:"size"`
		Mode       uint32               `json:"mode"`
		LinkTarget string               `json:"link_target,omitempty"`
		Chunks     []chunk              `json:"chunks,omitempty"`
	}
	fingerprint := make([]entry, 0, len(entries))
	for _, manifestEntry := range entries {
		next := entry{
			Path:       manifestEntry.Path,
			Type:       manifestEntry.Type,
			Size:       manifestEntry.Size,
			Mode:       manifestEntry.Mode,
			LinkTarget: manifestEntry.LinkTarget,
		}
		for _, ref := range manifestEntry.Chunks {
			next.Chunks = append(next.Chunks, chunk{Hash: ref.Hash, OriginalSize: ref.OriginalSize})
		}
		fingerprint = append(fingerprint, next)
	}
	data, err := json.Marshal(fingerprint)
	if err != nil {
		return ""
	}
	return string(data)
}
