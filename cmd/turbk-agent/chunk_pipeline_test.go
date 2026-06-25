package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/tursom/turbk/internal/fsfilter"
	"github.com/tursom/turbk/internal/repository"
)

func TestScanAndUploadPipelineImprovesHighLatencyBatchThroughput(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 4; i++ {
		if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("file-%d.txt", i)), []byte(fmt.Sprintf("unique pipeline payload %d", i)), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	const injectedRTT = 200 * time.Millisecond
	serialDuration := runHighLatencyPipelineScan(t, root, false, injectedRTT)
	pipelineDuration := runHighLatencyPipelineScan(t, root, true, injectedRTT)
	t.Logf("serial duration=%s pipeline duration=%s injected_rtt=%s", serialDuration, pipelineDuration, injectedRTT)
	if pipelineDuration >= serialDuration*3/4 {
		t.Fatalf("pipeline duration = %s, want clearly below serial duration %s", pipelineDuration, serialDuration)
	}
}

func runHighLatencyPipelineScan(t *testing.T, root string, pipeline bool, delay time.Duration) time.Duration {
	t.Helper()
	var mu sync.Mutex
	checkCalls := 0
	uploadCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/agent/v1/runs/1/progress":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "accepted"})
		case r.Method == http.MethodPost && r.URL.Path == "/agent/v1/chunks/check":
			time.Sleep(delay)
			var req struct {
				Hashes []string `json:"hashes"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode check request: %v", err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			mu.Lock()
			checkCalls++
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{
				"repository_id":    "repo-test",
				"chunk_generation": 7,
				"missing":          req.Hashes,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/agent/v1/chunks/upload":
			time.Sleep(delay)
			chunks := decodeAgentChunkBatchRequest(t, r.Body)
			mu.Lock()
			uploadCalls++
			mu.Unlock()
			respChunks := make([]map[string]any, 0, len(chunks))
			for _, chunk := range chunks {
				respChunks = append(respChunks, map[string]any{
					"hash":          chunk.hash,
					"uploaded":      true,
					"original_size": len(chunk.data),
				})
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status":           "accepted",
				"repository_id":    "repo-test",
				"chunk_generation": 7,
				"chunks":           respChunks,
			})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := newAgentClient(server.URL, "", "")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	opts := backupRunOptions{
		RepositoryID:               "repo-test",
		ChunkGeneration:            7,
		MaxChunkCheckBatch:         1,
		MaxChunkUploadBatchBytes:   defaultAgentChunkUploadBatchBytes,
		MaxChunkResponseBytes:      defaultAgentMaxChunkResponseBytes,
		ChunkBatchUpload:           true,
		CompactChunkCheckResponse:  true,
		CompactChunkUploadResponse: true,
		ChunkPipelineEnabled:       pipeline,
		MaxChunkCheckInflight:      4,
		MaxChunkUploadInflight:     4,
		MaxChunkPipelineBytes:      defaultAgentMaxChunkPipelineBytes,
	}
	started := time.Now()
	manifest, err := client.scanAndUpload(1, []string{root}, logger, fsfilter.Options{}, opts)
	if err != nil {
		t.Fatalf("scanAndUpload(pipeline=%v) error = %v", pipeline, err)
	}
	duration := time.Since(started)
	mu.Lock()
	defer mu.Unlock()
	if checkCalls != 4 || uploadCalls != 4 {
		t.Fatalf("pipeline=%v calls check=%d upload=%d, want 4/4", pipeline, checkCalls, uploadCalls)
	}
	if len(manifest.Entries) != 5 {
		t.Fatalf("pipeline=%v manifest entries = %d, want root plus four files", pipeline, len(manifest.Entries))
	}
	return duration
}

func TestScanAndUploadPipelineDeduplicatesPendingChunks(t *testing.T) {
	root := t.TempDir()
	payload := []byte("same chunk payload")
	if err := os.WriteFile(filepath.Join(root, "a.txt"), payload, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "b.txt"), payload, 0o644); err != nil {
		t.Fatal(err)
	}

	var checkCalls int
	var uploadCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/agent/v1/runs/1/progress":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "accepted"})
		case r.Method == http.MethodPost && r.URL.Path == "/agent/v1/chunks/check":
			checkCalls++
			var req struct {
				Hashes []string `json:"hashes"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode check request: %v", err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if len(req.Hashes) != 1 {
				t.Errorf("check hashes = %d, want deduplicated single hash", len(req.Hashes))
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"repository_id":    "repo-test",
				"chunk_generation": 7,
				"missing":          req.Hashes,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/agent/v1/chunks/upload":
			uploadCalls++
			chunks := decodeAgentChunkBatchRequest(t, r.Body)
			if len(chunks) != 1 {
				t.Errorf("upload chunks = %d, want deduplicated single chunk", len(chunks))
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status":           "accepted",
				"repository_id":    "repo-test",
				"chunk_generation": 7,
				"chunks": []map[string]any{{
					"hash":          chunks[0].hash,
					"uploaded":      true,
					"original_size": len(chunks[0].data),
				}},
			})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := newAgentClient(server.URL, "", "")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manifest, err := client.scanAndUpload(1, []string{root}, logger, fsfilter.Options{}, backupRunOptions{
		RepositoryID:               "repo-test",
		ChunkGeneration:            7,
		MaxChunkCheckBatch:         10000,
		MaxChunkUploadBatchBytes:   defaultAgentChunkUploadBatchBytes,
		MaxChunkResponseBytes:      defaultAgentMaxChunkResponseBytes,
		ChunkBatchUpload:           true,
		CompactChunkCheckResponse:  true,
		CompactChunkUploadResponse: true,
		ChunkPipelineEnabled:       true,
		MaxChunkCheckInflight:      2,
		MaxChunkUploadInflight:     2,
		MaxChunkPipelineBytes:      defaultAgentMaxChunkPipelineBytes,
	})
	if err != nil {
		t.Fatalf("scanAndUpload() error = %v", err)
	}
	if checkCalls != 1 || uploadCalls != 1 {
		t.Fatalf("pipeline calls check=%d upload=%d, want 1/1", checkCalls, uploadCalls)
	}
	a, ok := manifest.Find("a.txt")
	if !ok || len(a.Chunks) != 1 || a.Chunks[0].Hash == "" {
		t.Fatalf("a.txt manifest entry = %+v ok=%v", a, ok)
	}
	b, ok := manifest.Find("b.txt")
	if !ok || len(b.Chunks) != 1 || b.Chunks[0].Hash != a.Chunks[0].Hash {
		t.Fatalf("b.txt manifest entry = %+v ok=%v, want same chunk as a.txt %+v", b, ok, a.Chunks)
	}
}

func TestAgentChunkPipelineRespectsByteWindow(t *testing.T) {
	var mu sync.Mutex
	var uploadedHashes []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/agent/v1/chunks/check":
			var req struct {
				Hashes []string `json:"hashes"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode check request: %v", err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"repository_id":    "repo-test",
				"chunk_generation": 7,
				"missing":          req.Hashes,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/agent/v1/chunks/upload":
			chunks := decodeAgentChunkBatchRequest(t, r.Body)
			respChunks := make([]map[string]any, 0, len(chunks))
			mu.Lock()
			for _, chunk := range chunks {
				uploadedHashes = append(uploadedHashes, chunk.hash)
				respChunks = append(respChunks, map[string]any{
					"hash":          chunk.hash,
					"uploaded":      true,
					"original_size": len(chunk.data),
				})
			}
			mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status":           "accepted",
				"repository_id":    "repo-test",
				"chunk_generation": 7,
				"chunks":           respChunks,
			})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := newAgentClient(server.URL, "", "")
	batcher := newAgentChunkPipelineBatcher(client, backupRunOptions{
		RepositoryID:               "repo-test",
		ChunkGeneration:            7,
		MaxChunkCheckBatch:         10000,
		MaxChunkUploadBatchBytes:   defaultAgentChunkUploadBatchBytes,
		MaxChunkResponseBytes:      defaultAgentMaxChunkResponseBytes,
		ChunkBatchUpload:           true,
		CompactChunkCheckResponse:  true,
		CompactChunkUploadResponse: true,
		MaxChunkCheckInflight:      1,
		MaxChunkUploadInflight:     1,
		MaxChunkPipelineBytes:      2,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer batcher.Stop()

	entry := &repository.FileEntry{Path: "window.txt", Type: repository.EntryTypeFile}
	for _, chunk := range [][]byte{{'a'}, {'b'}, {'c'}} {
		if _, err := batcher.Add(chunk, entry); err != nil {
			t.Fatalf("Add(%q) error = %v", string(chunk), err)
		}
	}
	stats, err := batcher.Flush()
	if err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if stats.MaxPipelineBytes > 2 || batcher.bytesWindow.max() > 2 {
		t.Fatalf("max pipeline bytes stats=%d window=%d, want <= 2", stats.MaxPipelineBytes, batcher.bytesWindow.max())
	}
	if len(entry.Chunks) != 3 {
		t.Fatalf("entry chunks = %d, want 3", len(entry.Chunks))
	}
	mu.Lock()
	defer mu.Unlock()
	if len(uploadedHashes) != 3 {
		t.Fatalf("uploaded hashes = %d, want 3", len(uploadedHashes))
	}
}
