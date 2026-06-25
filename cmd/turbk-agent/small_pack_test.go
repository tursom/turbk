package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tursom/turbk/internal/fsfilter"
	"github.com/tursom/turbk/internal/repository"
)

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
