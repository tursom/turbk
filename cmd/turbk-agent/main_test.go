package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tursom/turbk/internal/fsfilter"
	"github.com/tursom/turbk/internal/repository"
	"github.com/tursom/turbk/internal/rootset"
	"github.com/zeebo/blake3"
)

func TestAgentClientUsesHTTPProxyFromEnvironment(t *testing.T) {
	const clientID = "agt_proxy"
	const clientSecret = "ags_proxy"
	seenProxyRequest := false
	proxyError := ""
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenProxyRequest = true
		if r.URL.Scheme != "http" || r.URL.Host != "turbk-agent-proxy.invalid" || r.URL.Path != "/agent/v1/heartbeat" {
			proxyError = "unexpected request URL: " + r.URL.String()
			http.Error(w, proxyError, http.StatusBadGateway)
			return
		}
		gotClientID, gotClientSecret, ok := r.BasicAuth()
		if !ok || gotClientID != clientID || gotClientSecret != clientSecret {
			proxyError = "unexpected basic auth credentials"
			http.Error(w, proxyError, http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "accepted"})
	}))
	defer proxy.Close()

	t.Setenv("HTTP_PROXY", proxy.URL)
	t.Setenv("HTTPS_PROXY", "")
	t.Setenv("NO_PROXY", "")
	t.Setenv("http_proxy", "")
	t.Setenv("https_proxy", "")
	t.Setenv("no_proxy", "")

	client := newAgentClient("http://turbk-agent-proxy.invalid", clientID, clientSecret)
	if err := client.sendHeartbeat(); err != nil {
		t.Fatal(err)
	}
	if proxyError != "" {
		t.Fatal(proxyError)
	}
	if !seenProxyRequest {
		t.Fatal("proxy did not receive the agent heartbeat request")
	}
}

func TestScanAndUploadMultiRootManifestPaths(t *testing.T) {
	sourceBase := t.TempDir()
	firstRoot := filepath.Join(sourceBase, "first")
	secondRoot := filepath.Join(sourceBase, "second")
	for _, root := range []string{firstRoot, secondRoot} {
		if err := os.MkdirAll(root, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "same.txt"), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	progressSeen := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/agent/v1/runs/1/progress" {
			progressSeen = true
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "accepted"})
			return
		}
		t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
	}))
	defer server.Close()

	roots, err := rootset.Normalize([]string{firstRoot, secondRoot})
	if err != nil {
		t.Fatal(err)
	}
	client := newAgentClient(server.URL, "", "")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manifest, err := client.scanAndUpload(1, roots, logger, fsfilter.Options{}, backupRunOptions{})
	if err != nil {
		t.Fatalf("scanAndUpload() error = %v", err)
	}
	if !progressSeen {
		t.Fatal("progress endpoint was not called")
	}
	if manifest.SourceRoot != "" {
		t.Fatalf("SourceRoot = %q, want empty for multi-root", manifest.SourceRoot)
	}
	if len(manifest.SourceRoots) != 2 || manifest.SourceRoots[0] != firstRoot || manifest.SourceRoots[1] != secondRoot {
		t.Fatalf("SourceRoots = %#v, want %#v", manifest.SourceRoots, roots)
	}
	firstFile := filepath.ToSlash(filepath.Join(rootset.ManifestPrefix(firstRoot), "same.txt"))
	secondFile := filepath.ToSlash(filepath.Join(rootset.ManifestPrefix(secondRoot), "same.txt"))
	if entry, ok := manifest.Find(firstFile); !ok || entry.Type != repository.EntryTypeFile {
		t.Fatalf("first file entry missing or wrong: %+v ok=%v", entry, ok)
	}
	if entry, ok := manifest.Find(secondFile); !ok || entry.Type != repository.EntryTypeFile {
		t.Fatalf("second file entry missing or wrong: %+v ok=%v", entry, ok)
	}
}

func TestScanAndUploadBatchesChunkCheckAndUpload(t *testing.T) {
	t.Setenv("TURBK_AGENT_CATALOG_BACKEND", "hybrid")
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("first batch file"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "b.txt"), []byte("second batch file"), 0o644); err != nil {
		t.Fatal(err)
	}

	var checkCalls int
	var uploadCalls int
	var checkedHashes []string
	var uploadedHashes []string
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
			checkedHashes = append(checkedHashes, req.Hashes...)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"repository_id":    "repo-test",
				"chunk_generation": 7,
				"exists":           []string{},
				"missing":          req.Hashes,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/agent/v1/chunks/upload":
			uploadCalls++
			chunks := decodeAgentChunkBatchRequest(t, r.Body)
			respChunks := make([]map[string]any, 0, len(chunks))
			for _, chunk := range chunks {
				uploadedHashes = append(uploadedHashes, chunk.hash)
				respChunks = append(respChunks, map[string]any{
					"hash":     chunk.hash,
					"exists":   true,
					"uploaded": true,
					"ref": repository.ChunkRef{
						Hash:         chunk.hash,
						OriginalSize: int64(len(chunk.data)),
					},
				})
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status":           "accepted",
				"repository_id":    "repo-test",
				"chunk_generation": 7,
				"chunks":           respChunks,
			})
		case strings.HasPrefix(r.URL.Path, "/agent/v1/chunks/"):
			t.Errorf("scan used legacy chunk endpoint: %s %s", r.Method, r.URL.Path)
			http.Error(w, "legacy chunk endpoint not allowed", http.StatusTeapot)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := newAgentClient(server.URL, "", "")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	stateDir := t.TempDir()
	catalog, err := openAgentCatalog(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if catalog != nil {
			_ = catalog.Close()
		}
	}()
	opts := backupRunOptions{
		Catalog:                  catalog,
		RepositoryID:             "repo-test",
		ChunkGeneration:          7,
		MaxChunkCheckBatch:       10000,
		MaxChunkUploadBatchBytes: defaultAgentChunkUploadBatchBytes,
		ChunkBatchUpload:         true,
	}
	manifest, err := client.scanAndUpload(1, []string{root}, logger, fsfilter.Options{}, opts)
	if err != nil {
		t.Fatalf("scanAndUpload() error = %v", err)
	}
	if checkCalls != 1 || uploadCalls != 1 {
		t.Fatalf("batch calls check=%d upload=%d, want 1/1", checkCalls, uploadCalls)
	}
	if len(checkedHashes) != 2 || len(uploadedHashes) != 2 {
		t.Fatalf("batched hashes check=%v upload=%v, want two chunks", checkedHashes, uploadedHashes)
	}
	for _, hash := range uploadedHashes {
		status, generation, ok, err := catalog.chunkStatus(hash)
		if err != nil {
			t.Fatal(err)
		}
		if !ok || status != "confirmed" || generation != 7 {
			t.Fatalf("catalog chunk %s = status=%q generation=%d ok=%v, want confirmed generation 7", hash, status, generation, ok)
		}
	}
	var sqliteChunkRows int
	if err := catalog.db.QueryRow(`SELECT COUNT(*) FROM server_chunks`).Scan(&sqliteChunkRows); err != nil {
		t.Fatal(err)
	}
	if sqliteChunkRows != 0 {
		t.Fatalf("sqlite server_chunks rows = %d, want 0 in hybrid mode", sqliteChunkRows)
	}
	var sqliteFileRows int
	if err := catalog.db.QueryRow(`SELECT COUNT(*) FROM files`).Scan(&sqliteFileRows); err != nil {
		t.Fatal(err)
	}
	if sqliteFileRows != 0 {
		t.Fatalf("sqlite files rows = %d, want 0 in hybrid mode", sqliteFileRows)
	}
	var sqliteFileChunkRows int
	if err := catalog.db.QueryRow(`SELECT COUNT(*) FROM file_chunks`).Scan(&sqliteFileChunkRows); err != nil {
		t.Fatal(err)
	}
	if sqliteFileChunkRows != 0 {
		t.Fatalf("sqlite file_chunks rows = %d, want 0 in hybrid mode", sqliteFileChunkRows)
	}
	if len(manifest.Entries) != 3 {
		t.Fatalf("manifest entries = %d, want root dir plus two files: %+v", len(manifest.Entries), manifest.Entries)
	}
	for _, name := range []string{"a.txt", "b.txt"} {
		entry, ok := manifest.Find(name)
		if !ok || len(entry.Chunks) != 1 || entry.Chunks[0].Hash == "" {
			t.Fatalf("manifest file %s missing uploaded chunk: %+v ok=%v", name, entry, ok)
		}
	}

	manifest, err = client.scanAndUpload(1, []string{root}, logger, fsfilter.Options{}, opts)
	if err != nil {
		t.Fatalf("second scanAndUpload() error = %v", err)
	}
	if checkCalls != 1 || uploadCalls != 1 {
		t.Fatalf("second scan used chunk network calls check=%d upload=%d, want still 1/1", checkCalls, uploadCalls)
	}
	for _, name := range []string{"a.txt", "b.txt"} {
		entry, ok := manifest.Find(name)
		if !ok || len(entry.Chunks) != 1 || entry.Chunks[0].Hash == "" {
			t.Fatalf("second manifest file %s missing reused chunk: %+v ok=%v", name, entry, ok)
		}
	}

	if err := catalog.Close(); err != nil {
		t.Fatal(err)
	}
	catalog = nil
	if err := os.RemoveAll(filepath.Join(stateDir, "catalog.pebble")); err != nil {
		t.Fatal(err)
	}
	catalog, err = openAgentCatalog(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	opts.Catalog = catalog
	manifest, err = client.scanAndUpload(1, []string{root}, logger, fsfilter.Options{}, opts)
	if err != nil {
		t.Fatalf("scan after deleting pebble catalog error = %v", err)
	}
	if checkCalls != 2 || uploadCalls != 2 {
		t.Fatalf("scan after deleting pebble catalog calls check=%d upload=%d, want 2/2", checkCalls, uploadCalls)
	}
	for _, name := range []string{"a.txt", "b.txt"} {
		entry, ok := manifest.Find(name)
		if !ok || len(entry.Chunks) != 1 || entry.Chunks[0].Hash == "" {
			t.Fatalf("rebuild manifest file %s missing chunk: %+v ok=%v", name, entry, ok)
		}
	}
}

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

func TestScanAndUploadPacksSmallFiles(t *testing.T) {
	t.Setenv("TURBK_AGENT_CATALOG_BACKEND", "hybrid")
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("first small file"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "b.txt"), []byte("second small file"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "empty.txt"), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	var uploadedPack []byte
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
			_ = json.NewEncoder(w).Encode(map[string]any{
				"repository_id":    "repo-test",
				"chunk_generation": 7,
				"missing":          req.Hashes,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/agent/v1/chunks/upload":
			uploadCalls++
			chunks := decodeAgentChunkBatchRequest(t, r.Body)
			if len(chunks) != 1 {
				t.Fatalf("uploaded pack chunks = %d, want 1", len(chunks))
			}
			uploadedPack = append([]byte(nil), chunks[0].data...)
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
	catalog, err := openAgentCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Close()
	opts := backupRunOptions{
		Catalog:                    catalog,
		RepositoryID:               "repo-test",
		ChunkGeneration:            7,
		MaxChunkCheckBatch:         10000,
		MaxChunkUploadBatchBytes:   defaultAgentChunkUploadBatchBytes,
		MaxChunkResponseBytes:      defaultAgentMaxChunkResponseBytes,
		ChunkBatchUpload:           true,
		CompactChunkCheckResponse:  true,
		CompactChunkUploadResponse: true,
		SmallFilePackEnabled:       true,
		SmallFilePackMaxFileSize:   64 * 1024,
		SmallFilePackTargetSize:    8 * 1024 * 1024,
	}
	manifest, err := client.scanAndUpload(1, []string{root}, logger, fsfilter.Options{}, opts)
	if err != nil {
		t.Fatalf("scanAndUpload() error = %v", err)
	}
	if checkCalls != 1 || uploadCalls != 1 {
		t.Fatalf("first scan calls check=%d upload=%d, want 1/1", checkCalls, uploadCalls)
	}
	if len(manifest.Packs) != 1 || len(manifest.Packs[0].Chunks) != 1 {
		t.Fatalf("manifest packs = %+v, want one pack with one chunk", manifest.Packs)
	}
	for _, name := range []string{"a.txt", "b.txt"} {
		entry, ok := manifest.Find(name)
		if !ok || entry.Type != repository.EntryTypePackedFile || entry.Pack == nil || len(entry.Chunks) != 0 {
			t.Fatalf("manifest file %s = %+v ok=%v, want packed_file", name, entry, ok)
		}
	}
	empty, ok := manifest.Find("empty.txt")
	if !ok || empty.Type != repository.EntryTypeFile || len(empty.Chunks) != 0 || empty.Pack != nil {
		t.Fatalf("empty file entry = %+v ok=%v, want regular empty file", empty, ok)
	}
	indexes, err := repository.DecodePackIndex(uploadedPack)
	if err != nil {
		t.Fatal(err)
	}
	if len(indexes) != 2 || indexes[0].Path != "a.txt" || indexes[1].Path != "b.txt" {
		t.Fatalf("pack indexes = %+v, want a.txt/b.txt", indexes)
	}

	manifest, err = client.scanAndUpload(1, []string{root}, logger, fsfilter.Options{}, opts)
	if err != nil {
		t.Fatalf("second scanAndUpload() error = %v", err)
	}
	if checkCalls != 1 || uploadCalls != 1 {
		t.Fatalf("second scan used chunk network calls check=%d upload=%d, want still 1/1", checkCalls, uploadCalls)
	}
	if len(manifest.Packs) != 1 || len(manifest.Packs[0].Chunks) != 1 {
		t.Fatalf("second manifest packs = %+v, want one reused pack", manifest.Packs)
	}
	for _, name := range []string{"a.txt", "b.txt"} {
		entry, ok := manifest.Find(name)
		if !ok || entry.Type != repository.EntryTypePackedFile || entry.Pack == nil {
			t.Fatalf("second manifest file %s = %+v ok=%v, want reused packed_file", name, entry, ok)
		}
	}
}

func TestLargeSmallFilePackPerformance(t *testing.T) {
	if os.Getenv("TURBK_RUN_LARGE_SMALL_FILE_TEST") != "1" {
		t.Skip("set TURBK_RUN_LARGE_SMALL_FILE_TEST=1 to run the 100k x 4KiB small-file pack acceptance test")
	}
	runSmallFilePackPerformance(t, 100000, 4*1024)
}

func TestLarge64KiBFilePackPerformance(t *testing.T) {
	if os.Getenv("TURBK_RUN_LARGE_64K_FILE_TEST") != "1" {
		t.Skip("set TURBK_RUN_LARGE_64K_FILE_TEST=1 to run the 10k x 64KiB small-file pack acceptance test")
	}
	runSmallFilePackPerformance(t, 10000, 64*1024)
}

func runSmallFilePackPerformance(t *testing.T, fileCount, fileSize int) {
	t.Helper()
	t.Setenv("TURBK_AGENT_CATALOG_BACKEND", "hybrid")

	root := t.TempDir()
	payload := bytes.Repeat([]byte("x"), fileSize)
	createStarted := time.Now()
	dirCount := 100
	for dirIndex := 0; dirIndex < dirCount; dirIndex++ {
		dir := filepath.Join(root, fmt.Sprintf("dir-%03d", dirIndex))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		for fileIndex := 0; fileIndex < fileCount/dirCount; fileIndex++ {
			path := filepath.Join(dir, fmt.Sprintf("file-%04d.bin", fileIndex))
			if err := os.WriteFile(path, payload, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Logf("created %d files of %d bytes in %s", fileCount, fileSize, time.Since(createStarted))

	var checkCalls int
	var uploadCalls int
	var checkedHashes int
	var uploadedChunks int
	var uploadedBytes int64
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
			checkedHashes += len(req.Hashes)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"repository_id":    "repo-test",
				"chunk_generation": 7,
				"missing":          req.Hashes,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/agent/v1/chunks/upload":
			uploadCalls++
			chunks := decodeAgentChunkBatchRequest(t, r.Body)
			respChunks := make([]map[string]any, 0, len(chunks))
			for _, chunk := range chunks {
				uploadedChunks++
				uploadedBytes += int64(len(chunk.data))
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
	catalog, err := openAgentCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Close()
	opts := backupRunOptions{
		Catalog:                    catalog,
		RepositoryID:               "repo-test",
		ChunkGeneration:            7,
		MaxChunkCheckBatch:         10000,
		MaxChunkUploadBatchBytes:   defaultAgentChunkUploadBatchBytes,
		MaxChunkResponseBytes:      defaultAgentMaxChunkResponseBytes,
		ChunkBatchUpload:           true,
		CompactChunkCheckResponse:  true,
		CompactChunkUploadResponse: true,
		SmallFilePackEnabled:       true,
		SmallFilePackMaxFileSize:   64 * 1024,
		SmallFilePackTargetSize:    8 * 1024 * 1024,
	}
	scanStarted := time.Now()
	manifest, err := client.scanAndUpload(1, []string{root}, logger, fsfilter.Options{}, opts)
	if err != nil {
		t.Fatalf("scanAndUpload() error = %v", err)
	}
	firstScanDuration := time.Since(scanStarted)
	var packedFiles int
	for _, entry := range manifest.Entries {
		if entry.Type == repository.EntryTypePackedFile {
			packedFiles++
		}
	}
	packChunks := 0
	uniquePackChunks := make(map[string]struct{})
	for _, pack := range manifest.Packs {
		packChunks += len(pack.Chunks)
		for _, chunk := range pack.Chunks {
			uniquePackChunks[chunk.Hash] = struct{}{}
		}
	}
	if packedFiles != fileCount {
		t.Fatalf("packed files = %d, want %d", packedFiles, fileCount)
	}
	if packChunks == 0 || packChunks >= fileCount/10 {
		t.Fatalf("pack chunks = %d, want much less than %d", packChunks, fileCount)
	}
	if uploadedChunks != len(uniquePackChunks) || checkedHashes != len(uniquePackChunks) {
		t.Fatalf("uploaded/check chunks = %d/%d, want unique pack chunks %d from %d pack refs", uploadedChunks, checkedHashes, len(uniquePackChunks), packChunks)
	}
	if uploadedBytes <= 0 {
		t.Fatalf("uploaded bytes = %d, want positive", uploadedBytes)
	}
	t.Logf("first scan: files=%d packs=%d pack_chunk_refs=%d unique_pack_chunks=%d check_calls=%d upload_calls=%d uploaded_bytes=%d duration=%s",
		fileCount, len(manifest.Packs), packChunks, len(uniquePackChunks), checkCalls, uploadCalls, uploadedBytes, firstScanDuration)

	checkCallsBeforeSecond := checkCalls
	uploadCallsBeforeSecond := uploadCalls
	scanStarted = time.Now()
	manifest, err = client.scanAndUpload(1, []string{root}, logger, fsfilter.Options{}, opts)
	if err != nil {
		t.Fatalf("second scanAndUpload() error = %v", err)
	}
	secondScanDuration := time.Since(scanStarted)
	if checkCalls != checkCallsBeforeSecond || uploadCalls != uploadCallsBeforeSecond {
		t.Fatalf("second scan used chunk network calls check=%d->%d upload=%d->%d",
			checkCallsBeforeSecond, checkCalls, uploadCallsBeforeSecond, uploadCalls)
	}
	if len(manifest.Packs) == 0 {
		t.Fatal("second scan did not reuse packed manifest entries")
	}
	t.Logf("second scan: packs=%d duration=%s", len(manifest.Packs), secondScanDuration)
}

func TestUploadChunksBatchAcceptsLargeJSONResponse(t *testing.T) {
	const chunkCount = 6000
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/agent/v1/chunks/upload" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		chunks := decodeAgentChunkBatchRequest(t, r.Body)
		if len(chunks) != chunkCount {
			t.Fatalf("uploaded chunks = %d, want %d", len(chunks), chunkCount)
		}
		respChunks := make([]uploadChunkResponse, 0, len(chunks))
		for i, chunk := range chunks {
			respChunks = append(respChunks, uploadChunkResponse{
				Hash:     chunk.hash,
				Exists:   boolPtr(true),
				Uploaded: true,
				Ref: repository.ChunkRef{
					Hash:           chunk.hash,
					SegmentID:      1,
					Offset:         int64(i),
					Length:         int64(len(chunk.data)),
					OriginalSize:   int64(len(chunk.data)),
					CompressedSize: int64(len(chunk.data)),
				},
			})
		}
		var response bytes.Buffer
		if err := json.NewEncoder(&response).Encode(uploadChunksResponse{
			Status:          "accepted",
			RepositoryID:    "repo-test",
			ChunkGeneration: 9,
			Chunks:          respChunks,
		}); err != nil {
			t.Fatal(err)
		}
		if response.Len() <= 1024*1024 {
			t.Fatalf("test response size = %d, want larger than legacy 1MiB response limit", response.Len())
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		if _, err := w.Write(response.Bytes()); err != nil {
			t.Fatal(err)
		}
	}))
	defer server.Close()

	chunks := make([]*pendingBatchChunk, 0, chunkCount)
	for i := 0; i < chunkCount; i++ {
		data := make([]byte, 16)
		binary.BigEndian.PutUint64(data[8:], uint64(i))
		sum := blake3.Sum256(data)
		chunks = append(chunks, &pendingBatchChunk{
			hash: hex.EncodeToString(sum[:]),
			data: data,
		})
	}

	client := newAgentClient(server.URL, "", "")
	uploaded, err := client.uploadChunksBatch(chunks, false)
	if err != nil {
		t.Fatalf("uploadChunksBatch() error = %v", err)
	}
	if len(uploaded.Chunks) != chunkCount {
		t.Fatalf("uploaded chunks = %d, want %d", len(uploaded.Chunks), chunkCount)
	}
}

func TestUploadChunksBatchRequestsCompactResponse(t *testing.T) {
	data := []byte("compact upload request")
	sum := blake3.Sum256(data)
	hash := hex.EncodeToString(sum[:])
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/agent/v1/chunks/upload" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if got := r.URL.Query().Get("compact_response"); got != "1" {
			t.Fatalf("compact_response query = %q, want 1", got)
		}
		chunks := decodeAgentChunkBatchRequest(t, r.Body)
		if len(chunks) != 1 || chunks[0].hash != hash {
			t.Fatalf("uploaded chunks = %+v, want hash %s", chunks, hash)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(uploadChunksResponse{
			Status:       "accepted",
			RepositoryID: "repo-test",
			Chunks: []uploadChunkResponse{{
				Hash:         hash,
				Uploaded:     true,
				OriginalSize: int64(len(data)),
			}},
		})
	}))
	defer server.Close()

	client := newAgentClient(server.URL, "", "")
	requested := []*pendingBatchChunk{{
		hash:         hash,
		data:         data,
		originalSize: int64(len(data)),
	}}
	uploaded, err := client.uploadChunksBatch(requested, true)
	if err != nil {
		t.Fatalf("uploadChunksBatch() error = %v", err)
	}
	uploadedByHash, err := validateChunkUploadResponse(requested, "repo-test", uploaded, true)
	if err != nil {
		t.Fatalf("validate compact upload response: %v", err)
	}
	if uploadedByHash[hash].Ref.Hash != hash || uploadedByHash[hash].Ref.OriginalSize != int64(len(data)) {
		t.Fatalf("compact upload ref = %+v", uploadedByHash[hash].Ref)
	}
	if uploaded.RequestBytes <= 0 || uploaded.ResponseBytes <= 0 {
		t.Fatalf("request/response bytes not recorded: %+v", uploaded)
	}
}

func TestPebbleChunkStatusKeyIsBinary(t *testing.T) {
	hash := testChunkHash("binary key")
	key, err := encodePebbleChunkStatusKey(hash)
	if err != nil {
		t.Fatal(err)
	}
	if len(key) != 33 {
		t.Fatalf("key length = %d, want 33", len(key))
	}
	if key[0] != agentPebbleRecordChunkStatus {
		t.Fatalf("key prefix = 0x%x, want 0x%x", key[0], agentPebbleRecordChunkStatus)
	}
	if got := hex.EncodeToString(key[1:]); got != hash {
		t.Fatalf("key hash = %s, want %s", got, hash)
	}
}

func TestPebbleChunkCatalogMarkChunksInvalidationAndReset(t *testing.T) {
	catalog, err := openPebbleChunkCatalog(filepath.Join(t.TempDir(), "catalog.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Close()

	hashA := testChunkHash("chunk-a")
	hashB := testChunkHash("chunk-b")
	if err := catalog.markChunks([]agentChunkStatusUpdate{
		{Hash: hashA, OriginalSize: 123, Status: "confirmed", Generation: 7, Uploaded: true},
		{Hash: hashB, OriginalSize: 456, Status: "missing"},
	}); err != nil {
		t.Fatal(err)
	}
	status, generation, ok, err := catalog.chunkStatus(hashA)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || status != "confirmed" || generation != 7 {
		t.Fatalf("hashA status=%q generation=%d ok=%v, want confirmed generation 7", status, generation, ok)
	}
	status, generation, ok, err = catalog.chunkStatus(hashB)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || status != "missing" || generation != 0 {
		t.Fatalf("hashB status=%q generation=%d ok=%v, want missing generation 0", status, generation, ok)
	}

	if err := catalog.applyInvalidations([]string{hashA}, 9); err != nil {
		t.Fatal(err)
	}
	status, generation, ok, err = catalog.chunkStatus(hashA)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || status != "unknown" || generation != 0 {
		t.Fatalf("invalidated hashA status=%q generation=%d ok=%v, want unknown generation 0", status, generation, ok)
	}

	if err := catalog.markChunk(hashB, 456, "confirmed", 7, false); err != nil {
		t.Fatal(err)
	}
	if err := catalog.applyInvalidations(nil, 11); err != nil {
		t.Fatal(err)
	}
	status, generation, ok, err = catalog.chunkStatus(hashB)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || status != "confirmed" || generation != 11 {
		t.Fatalf("advanced hashB status=%q generation=%d ok=%v, want confirmed generation 11", status, generation, ok)
	}

	if err := catalog.resetServerChunks(); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := catalog.chunkStatus(hashB); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("chunk status still present after reset")
	}
}

func TestPebbleFileKeyAndRecordRoundTrip(t *testing.T) {
	rootID := "/data/app"
	path := "dir/文件 name.txt"
	key := encodePebbleFileKey(rootID, path)
	decodedRootID, decodedPath, ok := decodePebbleFileKey(key)
	if !ok {
		t.Fatal("decodePebbleFileKey returned not ok")
	}
	if decodedRootID != rootID || decodedPath != path {
		t.Fatalf("decoded key root=%q path=%q, want root=%q path=%q", decodedRootID, decodedPath, rootID, path)
	}

	catalog, err := openPebbleChunkCatalog(filepath.Join(t.TempDir(), "catalog.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Close()

	chunkA := testChunkHash("file-a")
	chunkB := testChunkHash("file-b")
	record := catalogFileRecord{
		RootID:      rootID,
		Path:        path,
		Type:        string(repository.EntryTypeFile),
		Size:        579,
		Mode:        0o100644,
		UID:         1000,
		GID:         1001,
		MTimeNS:     123456789,
		Dev:         42,
		Inode:       84,
		Fingerprint: "fp",
	}
	chunks := []catalogChunkRecord{
		{Hash: chunkA, OriginalSize: 123},
		{Hash: chunkB, OriginalSize: 456},
	}
	if err := catalog.replaceFile(record, chunks); err != nil {
		t.Fatal(err)
	}
	gotRecord, gotChunks, ok, err := catalog.fileRecordWithChunks(rootID, path)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("file record missing")
	}
	if gotRecord != record {
		t.Fatalf("record = %#v, want %#v", gotRecord, record)
	}
	if len(gotChunks) != len(chunks) {
		t.Fatalf("chunks = %#v, want %#v", gotChunks, chunks)
	}
	for i := range chunks {
		if gotChunks[i] != chunks[i] {
			t.Fatalf("chunk %d = %#v, want %#v", i, gotChunks[i], chunks[i])
		}
	}
}

func TestValidateChunkCheckResponseRejectsMalformedCoverage(t *testing.T) {
	hashA := testChunkHash("check-a")
	hashB := testChunkHash("check-b")
	other := testChunkHash("check-other")
	requested := []string{hashA, hashB}

	if err := validateChunkCheckResponse(requested, "repo-test", checkChunksResponse{
		RepositoryID: "repo-test",
		Exists:       []string{hashA},
		Missing:      []string{hashB},
	}, false); err != nil {
		t.Fatalf("valid response rejected: %v", err)
	}

	tests := []struct {
		name    string
		checked checkChunksResponse
		want    string
	}{
		{
			name:    "omitted",
			checked: checkChunksResponse{RepositoryID: "repo-test", Exists: []string{hashA}},
			want:    "omitted chunk " + hashB,
		},
		{
			name:    "unexpected exists",
			checked: checkChunksResponse{RepositoryID: "repo-test", Exists: []string{hashA, other}, Missing: []string{hashB}},
			want:    "unexpected existing chunk " + other,
		},
		{
			name:    "duplicate exists",
			checked: checkChunksResponse{RepositoryID: "repo-test", Exists: []string{hashA, hashA}, Missing: []string{hashB}},
			want:    "duplicate existing chunk " + hashA,
		},
		{
			name:    "both exists and missing",
			checked: checkChunksResponse{RepositoryID: "repo-test", Exists: []string{hashA}, Missing: []string{hashA, hashB}},
			want:    "as both existing and missing",
		},
		{
			name:    "repository mismatch",
			checked: checkChunksResponse{RepositoryID: "other-repo", Exists: []string{hashA}, Missing: []string{hashB}},
			want:    `repository_id = "other-repo"`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateChunkCheckResponse(requested, "repo-test", tc.checked, false)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestValidateChunkCheckResponseAcceptsCompactMissingOnly(t *testing.T) {
	hashA := testChunkHash("compact-check-a")
	hashB := testChunkHash("compact-check-b")
	checked := checkChunksResponse{
		RepositoryID: "repo-test",
		Missing:      []string{hashB},
	}
	requested := []string{hashA, hashB}
	if err := validateChunkCheckResponse(requested, "repo-test", checked, true); err != nil {
		t.Fatalf("compact response rejected: %v", err)
	}
	exists := existingChunkHashes(requested, checked, true)
	if len(exists) != 1 || exists[0] != hashA {
		t.Fatalf("compact inferred exists = %v, want [%s]", exists, hashA)
	}

	other := testChunkHash("compact-check-other")
	err := validateChunkCheckResponse(requested, "repo-test", checkChunksResponse{Missing: []string{other}}, true)
	if err == nil || !strings.Contains(err.Error(), "unexpected missing chunk "+other) {
		t.Fatalf("unknown compact missing error = %v", err)
	}
}

func TestEnsureCatalogChunksConfirmedRejectsIncompleteCheckResponse(t *testing.T) {
	t.Setenv("TURBK_AGENT_CATALOG_BACKEND", "hybrid")
	hash := testChunkHash("catalog reuse stale chunk")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/agent/v1/chunks/check" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"repository_id":    "repo-test",
			"chunk_generation": 7,
			"exists":           []string{},
			"missing":          []string{},
		})
	}))
	defer server.Close()

	catalog, err := openAgentCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Close()

	client := newAgentClient(server.URL, "", "")
	ok, err := client.ensureCatalogChunksConfirmed(catalog, []catalogChunkRecord{
		{Hash: hash, OriginalSize: 123},
	}, backupRunOptions{RepositoryID: "repo-test", ChunkGeneration: 7})
	if err == nil || !strings.Contains(err.Error(), "omitted chunk "+hash) {
		t.Fatalf("ensureCatalogChunksConfirmed() ok=%v error=%v, want omitted chunk error", ok, err)
	}
	if ok {
		t.Fatal("ensureCatalogChunksConfirmed() ok=true for incomplete server response")
	}
}

func TestEnsureCatalogChunksConfirmedSplitsCheckRequests(t *testing.T) {
	t.Setenv("TURBK_AGENT_CATALOG_BACKEND", "hybrid")
	chunks := []catalogChunkRecord{
		{Hash: testChunkHash("split-check-a"), OriginalSize: 101},
		{Hash: testChunkHash("split-check-b"), OriginalSize: 102},
		{Hash: testChunkHash("split-check-c"), OriginalSize: 103},
		{Hash: testChunkHash("split-check-d"), OriginalSize: 104},
		{Hash: testChunkHash("split-check-e"), OriginalSize: 105},
	}
	var batchSizes []int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/agent/v1/chunks/check" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		var req struct {
			Hashes []string `json:"hashes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode check request: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(req.Hashes) > 2 {
			t.Errorf("check batch size = %d, want <= 2", len(req.Hashes))
			http.Error(w, "too many hashes", http.StatusBadRequest)
			return
		}
		batchSizes = append(batchSizes, len(req.Hashes))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"repository_id":    "repo-test",
			"chunk_generation": 9,
			"exists":           req.Hashes,
			"missing":          []string{},
		})
	}))
	defer server.Close()

	catalog, err := openAgentCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Close()

	client := newAgentClient(server.URL, "", "")
	ok, err := client.ensureCatalogChunksConfirmed(catalog, chunks, backupRunOptions{
		RepositoryID:       "repo-test",
		ChunkGeneration:    9,
		MaxChunkCheckBatch: 2,
	})
	if err != nil {
		t.Fatalf("ensureCatalogChunksConfirmed() error = %v", err)
	}
	if !ok {
		t.Fatal("ensureCatalogChunksConfirmed() ok=false, want true")
	}
	if len(batchSizes) != 3 || batchSizes[0] != 2 || batchSizes[1] != 2 || batchSizes[2] != 1 {
		t.Fatalf("check batch sizes = %v, want [2 2 1]", batchSizes)
	}
	for _, chunk := range chunks {
		status, generation, ok, err := catalog.chunkStatus(chunk.Hash)
		if err != nil {
			t.Fatal(err)
		}
		if !ok || status != "confirmed" || generation != 9 {
			t.Fatalf("catalog chunk %s = status=%q generation=%d ok=%v, want confirmed generation 9", chunk.Hash, status, generation, ok)
		}
	}
}

func TestValidateChunkUploadResponseRejectsMalformedCoverage(t *testing.T) {
	hashA := testChunkHash("upload-a")
	hashB := testChunkHash("upload-b")
	other := testChunkHash("upload-other")
	requested := []*pendingBatchChunk{
		{hash: hashA, originalSize: 123},
		{hash: hashB, originalSize: 123},
	}
	validChunk := func(hash string) uploadChunkResponse {
		return uploadChunkResponse{
			Hash:     hash,
			Exists:   boolPtr(true),
			Uploaded: true,
			Ref:      repository.ChunkRef{Hash: hash, OriginalSize: 123},
		}
	}
	if _, err := validateChunkUploadResponse(requested, "repo-test", uploadChunksResponse{
		RepositoryID: "repo-test",
		Chunks:       []uploadChunkResponse{validChunk(hashA), validChunk(hashB)},
	}, false); err != nil {
		t.Fatalf("valid upload response rejected: %v", err)
	}
	tests := []struct {
		name     string
		response uploadChunksResponse
		want     string
	}{
		{
			name:     "omitted",
			response: uploadChunksResponse{RepositoryID: "repo-test", Chunks: []uploadChunkResponse{validChunk(hashA)}},
			want:     "omitted chunk " + hashB,
		},
		{
			name:     "unexpected",
			response: uploadChunksResponse{RepositoryID: "repo-test", Chunks: []uploadChunkResponse{validChunk(hashA), validChunk(hashB), validChunk(other)}},
			want:     "unexpected chunk " + other,
		},
		{
			name:     "duplicate",
			response: uploadChunksResponse{RepositoryID: "repo-test", Chunks: []uploadChunkResponse{validChunk(hashA), validChunk(hashA), validChunk(hashB)}},
			want:     "duplicate chunk " + hashA,
		},
		{
			name:     "repository mismatch",
			response: uploadChunksResponse{RepositoryID: "other-repo", Chunks: []uploadChunkResponse{validChunk(hashA), validChunk(hashB)}},
			want:     `repository_id = "other-repo"`,
		},
		{
			name: "not exists",
			response: uploadChunksResponse{RepositoryID: "repo-test", Chunks: []uploadChunkResponse{
				validChunk(hashA),
				{Hash: hashB, Exists: boolPtr(false), Ref: repository.ChunkRef{Hash: hashB, OriginalSize: 123}},
			}},
			want: "did not confirm chunk " + hashB,
		},
		{
			name: "ref mismatch",
			response: uploadChunksResponse{RepositoryID: "repo-test", Chunks: []uploadChunkResponse{
				validChunk(hashA),
				{Hash: hashB, Exists: boolPtr(true), Ref: repository.ChunkRef{Hash: hashA, OriginalSize: 123}},
			}},
			want: "returned ref " + hashA,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := validateChunkUploadResponse(requested, "repo-test", tc.response, false)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestValidateChunkUploadResponseAcceptsCompactRefLessChunks(t *testing.T) {
	hashA := testChunkHash("compact-upload-a")
	hashB := testChunkHash("compact-upload-b")
	requested := []*pendingBatchChunk{
		{hash: hashA, originalSize: 321},
		{hash: hashB, originalSize: 654},
	}
	uploadedByHash, err := validateChunkUploadResponse(requested, "repo-test", uploadChunksResponse{
		RepositoryID: "repo-test",
		Chunks: []uploadChunkResponse{
			{Hash: hashA, Uploaded: true, OriginalSize: 321},
			{Hash: hashB, Uploaded: false, OriginalSize: 654},
		},
	}, true)
	if err != nil {
		t.Fatalf("compact upload response rejected: %v", err)
	}
	if uploadedByHash[hashA].Ref.Hash != hashA || uploadedByHash[hashA].Ref.OriginalSize != 321 {
		t.Fatalf("compact upload ref for hashA = %+v", uploadedByHash[hashA].Ref)
	}
	if uploadedByHash[hashB].Ref.Hash != hashB || uploadedByHash[hashB].Ref.OriginalSize != 654 {
		t.Fatalf("compact upload ref for hashB = %+v", uploadedByHash[hashB].Ref)
	}

	other := testChunkHash("compact-upload-other")
	_, err = validateChunkUploadResponse(requested, "repo-test", uploadChunksResponse{
		Chunks: []uploadChunkResponse{{Hash: other, Uploaded: true, OriginalSize: 1}},
	}, true)
	if err == nil || !strings.Contains(err.Error(), "unexpected chunk "+other) {
		t.Fatalf("unknown compact upload error = %v", err)
	}
}

func TestOpenAgentCatalogSQLiteBackendUsesSQLiteChunks(t *testing.T) {
	t.Setenv("TURBK_AGENT_CATALOG_BACKEND", "sqlite")
	catalog, err := openAgentCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Close()

	hash := testChunkHash("sqlite backend")
	if err := catalog.markChunk(hash, 321, "confirmed", 4, false); err != nil {
		t.Fatal(err)
	}
	status, generation, ok, err := catalog.chunkStatus(hash)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || status != "confirmed" || generation != 4 {
		t.Fatalf("chunk status=%q generation=%d ok=%v, want confirmed generation 4", status, generation, ok)
	}
	var sqliteChunkRows int
	if err := catalog.db.QueryRow(`SELECT COUNT(*) FROM server_chunks`).Scan(&sqliteChunkRows); err != nil {
		t.Fatal(err)
	}
	if sqliteChunkRows != 1 {
		t.Fatalf("sqlite server_chunks rows = %d, want 1", sqliteChunkRows)
	}
}

func TestRootFlagReadsRootEnvironment(t *testing.T) {
	t.Setenv("TURBK_AGENT_ROOT", "/legacy/root")
	t.Setenv("TURBK_AGENT_ROOTS", "/data/app,/var/log/myapp")

	flag := newRootFlagFromEnv()
	roots, err := rootset.Normalize(flag.Values())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"/data/app", "/var/log/myapp"}
	if len(roots) != len(want) || roots[0] != want[0] || roots[1] != want[1] {
		t.Fatalf("roots = %#v, want %#v", roots, want)
	}
}

func TestRootFlagReadsLegacyRootEnvironment(t *testing.T) {
	t.Setenv("TURBK_AGENT_ROOT", "/legacy/root")
	t.Setenv("TURBK_AGENT_ROOTS", "")

	flag := newRootFlagFromEnv()
	roots, err := rootset.Normalize(flag.Values())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"/legacy/root"}
	if len(roots) != len(want) || roots[0] != want[0] {
		t.Fatalf("roots = %#v, want %#v", roots, want)
	}
}

func TestRootFlagCommandLineOverridesEnvironment(t *testing.T) {
	t.Setenv("TURBK_AGENT_ROOTS", "/env/root")
	flag := newRootFlagFromEnv()
	if err := flag.Set("/cli/one"); err != nil {
		t.Fatal(err)
	}
	if err := flag.Set("/cli/two"); err != nil {
		t.Fatal(err)
	}

	roots, err := rootset.Normalize(flag.Values())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"/cli/one", "/cli/two"}
	if len(roots) != len(want) || roots[0] != want[0] || roots[1] != want[1] {
		t.Fatalf("roots = %#v, want %#v", roots, want)
	}
}

func TestBackupRootsForCommandUsesPayloadRoots(t *testing.T) {
	roots, err := backupRootsForCommand(agentCommand{
		Payload: json.RawMessage(`{"roots":["/server/root","/server/logs"]}`),
	}, []string{"/local/root"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"/server/root", "/server/logs"}
	if len(roots) != len(want) || roots[0] != want[0] || roots[1] != want[1] {
		t.Fatalf("roots = %#v, want %#v", roots, want)
	}
}

func TestBackupRootsForCommandFallsBackToLocalRoots(t *testing.T) {
	roots, err := backupRootsForCommand(agentCommand{Payload: json.RawMessage(`{"job_id":7}`)}, []string{"/local/root"})
	if err != nil {
		t.Fatal(err)
	}
	if len(roots) != 1 || roots[0] != "/local/root" {
		t.Fatalf("roots = %#v, want local fallback", roots)
	}
}

func TestParseBackupScheduleOrDefault(t *testing.T) {
	if got := parseBackupScheduleOrDefault("*/15 * * * *", defaultAgentBackupSchedule); got != "*/15 * * * *" {
		t.Fatalf("valid schedule = %q", got)
	}
	if got := parseBackupScheduleOrDefault("24h", defaultAgentBackupSchedule); got != defaultAgentBackupSchedule {
		t.Fatalf("invalid duration schedule = %q, want default", got)
	}
}

func TestDueByCronChecksWindowWithoutDuplicateMinute(t *testing.T) {
	now := time.Date(2026, 6, 22, 10, 0, 30, 0, time.UTC)
	if !dueByCron("@hourly", time.Time{}, now) {
		t.Fatal("expected first check inside matching minute to be due")
	}
	if dueByCron("@hourly", time.Time{}, now.Add(time.Minute)) {
		t.Fatal("did not expect first check outside matching minute to be due")
	}
	if !dueByCron("@hourly", now.Add(-10*time.Minute), now.Add(5*time.Minute)) {
		t.Fatal("expected missed matching minute inside check window to be due")
	}
	if dueByCron("@hourly", now, now.Add(20*time.Second)) {
		t.Fatal("did not expect same matching minute to trigger twice")
	}
}

type decodedAgentChunkBatch struct {
	hash string
	data []byte
}

func testChunkHash(value string) string {
	sum := blake3.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func decodeAgentChunkBatchRequest(t *testing.T, body io.Reader) []decodedAgentChunkBatch {
	t.Helper()
	data, err := io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(data, agentChunkBatchMagic) {
		t.Fatalf("chunk batch magic mismatch: %q", data[:min(len(data), len(agentChunkBatchMagic))])
	}
	offset := len(agentChunkBatchMagic)
	if len(data) < offset+4 {
		t.Fatalf("chunk batch missing count")
	}
	count := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	chunks := make([]decodedAgentChunkBatch, 0, count)
	for i := uint32(0); i < count; i++ {
		if len(data) < offset+32+8 {
			t.Fatalf("chunk %d missing header", i)
		}
		hashBytes := data[offset : offset+32]
		offset += 32
		length := binary.BigEndian.Uint64(data[offset : offset+8])
		offset += 8
		if length > uint64(len(data)-offset) {
			t.Fatalf("chunk %d length %d exceeds remaining body %d", i, length, len(data)-offset)
		}
		chunk := data[offset : offset+int(length)]
		offset += int(length)
		sum := blake3.Sum256(chunk)
		if !bytes.Equal(sum[:], hashBytes) {
			t.Fatalf("chunk %d hash mismatch", i)
		}
		chunks = append(chunks, decodedAgentChunkBatch{
			hash: hex.EncodeToString(hashBytes),
			data: append([]byte(nil), chunk...),
		})
	}
	if offset != len(data) {
		t.Fatalf("chunk batch has %d trailing bytes", len(data)-offset)
	}
	return chunks
}
