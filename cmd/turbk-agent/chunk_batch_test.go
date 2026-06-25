package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tursom/turbk/internal/fsfilter"
	"github.com/tursom/turbk/internal/repository"
	"github.com/zeebo/blake3"
)

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

func TestScanAndUploadRetriesBatchUpload500(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "retry.txt"), []byte("retry batch upload payload"), 0o644); err != nil {
		t.Fatal(err)
	}

	var uploadCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/agent/v1/runs/1/progress":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "accepted"})
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
			uploadCalls++
			if uploadCalls <= 2 {
				http.Error(w, "temporary upload failure", http.StatusInternalServerError)
				return
			}
			chunks := decodeAgentChunkBatchRequest(t, r.Body)
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
	manifest, err := client.scanAndUpload(1, []string{root}, logger, fsfilter.Options{}, backupRunOptions{
		RepositoryID:                  "repo-test",
		ChunkGeneration:               7,
		MaxChunkCheckBatch:            10000,
		MaxChunkUploadBatchBytes:      defaultAgentChunkUploadBatchBytes,
		MaxChunkResponseBytes:         defaultAgentMaxChunkResponseBytes,
		ChunkBatchUpload:              true,
		CompactChunkCheckResponse:     true,
		CompactChunkUploadResponse:    true,
		ChunkBatchMaxRetries:          2,
		ChunkBatchRetryInitialBackoff: time.Millisecond,
		ChunkBatchRetryMaxBackoff:     time.Millisecond,
		ChunkBatchSplitOn413:          true,
	})
	if err != nil {
		t.Fatalf("scanAndUpload() error = %v", err)
	}
	if uploadCalls != 3 {
		t.Fatalf("upload calls = %d, want 3", uploadCalls)
	}
	entry, ok := manifest.Find("retry.txt")
	if !ok || len(entry.Chunks) != 1 || entry.Chunks[0].Hash == "" {
		t.Fatalf("retry.txt manifest entry = %+v ok=%v", entry, ok)
	}
}

func TestCheckChunksRetriesAfterHeader(t *testing.T) {
	hash := testChunkHash("retry-after check")
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/agent/v1/chunks/check" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		calls++
		w.Header().Set("Content-Type", "application/json")
		if calls == 1 {
			w.Header().Set("Retry-After", "0")
			http.Error(w, "busy", http.StatusTooManyRequests)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"repository_id":    "repo-test",
			"chunk_generation": 7,
			"exists":           []string{hash},
			"missing":          []string{},
		})
	}))
	defer server.Close()

	client := newAgentClient(server.URL, "", "")
	checked, err := client.checkChunks([]string{hash}, "repo-test", 7, false, chunkBatchRetryOptions{
		MaxRetries:     1,
		InitialBackoff: time.Hour,
		MaxBackoff:     time.Hour,
	})
	if err != nil {
		t.Fatalf("checkChunks() error = %v", err)
	}
	if calls != 2 || checked.RetryCount != 1 {
		t.Fatalf("calls=%d retry_count=%d, want 2/1", calls, checked.RetryCount)
	}
}

func TestUploadChunksBatchSplitsOn413(t *testing.T) {
	first := pendingChunkForTest([]byte("first split upload"))
	second := pendingChunkForTest([]byte("second split upload"))
	var uploadCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/agent/v1/chunks/upload" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		uploadCalls++
		chunks := decodeAgentChunkBatchRequest(t, r.Body)
		if len(chunks) > 1 {
			http.Error(w, "too large", http.StatusRequestEntityTooLarge)
			return
		}
		w.Header().Set("Content-Type", "application/json")
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
	}))
	defer server.Close()

	client := newAgentClient(server.URL, "", "")
	uploaded, err := client.uploadChunksBatch([]*pendingBatchChunk{first, second}, true, chunkBatchRetryOptions{
		MaxRetries:     1,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     time.Millisecond,
		SplitOn413:     true,
	})
	if err != nil {
		t.Fatalf("uploadChunksBatch() error = %v", err)
	}
	if uploadCalls != 3 {
		t.Fatalf("upload calls = %d, want initial plus two split requests", uploadCalls)
	}
	if uploaded.SplitCount != 1 || len(uploaded.Chunks) != 2 {
		t.Fatalf("uploaded split_count=%d chunks=%d, want 1/2", uploaded.SplitCount, len(uploaded.Chunks))
	}
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
	uploaded, err := client.uploadChunksBatch(chunks, false, chunkBatchRetryOptions{})
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
	uploaded, err := client.uploadChunksBatch(requested, true, chunkBatchRetryOptions{})
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
