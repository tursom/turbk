package main

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"testing"

	"github.com/tursom/turbk/internal/fsfilter"
	"github.com/tursom/turbk/internal/repository"
	"github.com/tursom/turbk/internal/rootset"
)

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

func TestScanAndUploadSkipsPathRemovedBeforeStat(t *testing.T) {
	root := t.TempDir()
	vanishedPath := filepath.Join(root, "vanished.txt")
	if err := os.WriteFile(vanishedPath, []byte("removed during scan"), 0o644); err != nil {
		t.Fatal(err)
	}

	originalLstat := agentLstat
	t.Cleanup(func() {
		agentLstat = originalLstat
	})
	agentLstat = func(path string) (os.FileInfo, error) {
		if path == vanishedPath {
			return nil, &os.PathError{Op: "lstat", Path: path, Err: os.ErrNotExist}
		}
		return originalLstat(path)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/agent/v1/runs/1/progress" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "accepted"})
	}))
	defer server.Close()

	client := newAgentClient(server.URL, "", "")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manifest, err := client.scanAndUpload(1, []string{root}, logger, fsfilter.Options{}, backupRunOptions{})
	if err != nil {
		t.Fatalf("scanAndUpload() error = %v", err)
	}
	if _, ok := manifest.Find("vanished.txt"); ok {
		t.Fatal("vanished file was included in manifest")
	}
	if entry, ok := manifest.Find("."); !ok || entry.Type != repository.EntryTypeDir {
		t.Fatalf("root manifest entry = %+v ok=%v, want dir", entry, ok)
	}
}

func TestScanAndUploadParallelSkipsPathRemovedBeforeRead(t *testing.T) {
	root := t.TempDir()
	vanishedPath := filepath.Join(root, "vanished.txt")
	if err := os.WriteFile(vanishedPath, []byte("removed before read"), 0o644); err != nil {
		t.Fatal(err)
	}

	originalOpen := agentOpen
	t.Cleanup(func() {
		agentOpen = originalOpen
	})
	agentOpen = func(path string) (*os.File, error) {
		if path == vanishedPath {
			return nil, &os.PathError{Op: "open", Path: path, Err: os.ErrNotExist}
		}
		return originalOpen(path)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/agent/v1/runs/1/progress" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "accepted"})
	}))
	defer server.Close()

	client := newAgentClient(server.URL, "", "")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manifest, err := client.scanAndUpload(1, []string{root}, logger, fsfilter.Options{}, backupRunOptions{
		ScanParallelEnabled: true,
		FileReadWorkers:     1,
	})
	if err != nil {
		t.Fatalf("scanAndUpload() error = %v", err)
	}
	if _, ok := manifest.Find("vanished.txt"); ok {
		t.Fatal("vanished file was included in manifest")
	}
	if entry, ok := manifest.Find("."); !ok || entry.Type != repository.EntryTypeDir {
		t.Fatalf("root manifest entry = %+v ok=%v, want dir", entry, ok)
	}
}

func TestScanAndUploadRetriesFileChangedDuringRead(t *testing.T) {
	root := t.TempDir()
	changedPath := filepath.Join(root, "changed.log")
	if err := os.WriteFile(changedPath, []byte("initial"), 0o644); err != nil {
		t.Fatal(err)
	}
	finalData := []byte("initial plus appended log data")

	originalOpen := agentOpen
	t.Cleanup(func() {
		agentOpen = originalOpen
	})
	changed := false
	agentOpen = func(path string) (*os.File, error) {
		if path == changedPath && !changed {
			changed = true
			if err := os.WriteFile(path, finalData, 0o644); err != nil {
				return nil, err
			}
		}
		return originalOpen(path)
	}

	server := newAgentScanLegacyChunkServer(t)
	defer server.Close()

	client := newAgentClient(server.URL, "", "")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manifest, err := client.scanAndUpload(1, []string{root}, logger, fsfilter.Options{}, backupRunOptions{})
	if err != nil {
		t.Fatalf("scanAndUpload() error = %v", err)
	}
	if !changed {
		t.Fatal("test did not mutate file during read")
	}
	entry, ok := manifest.Find("changed.log")
	if !ok {
		t.Fatal("changed file was not included after retry")
	}
	if entry.Size != int64(len(finalData)) {
		t.Fatalf("changed file size = %d, want %d", entry.Size, len(finalData))
	}
	if chunkBytes(entry) != entry.Size {
		t.Fatalf("changed file chunk bytes = %d, want %d", chunkBytes(entry), entry.Size)
	}
	if entry, ok := manifest.Find("."); !ok || entry.Type != repository.EntryTypeDir {
		t.Fatalf("root manifest entry = %+v ok=%v, want dir", entry, ok)
	}
}

func TestScanAndUploadSkipsFileChangedAfterRetries(t *testing.T) {
	root := t.TempDir()
	changedPath := filepath.Join(root, "changed.log")
	if err := os.WriteFile(changedPath, []byte("initial"), 0o644); err != nil {
		t.Fatal(err)
	}

	originalOpen := agentOpen
	t.Cleanup(func() {
		agentOpen = originalOpen
	})
	opens := 0
	agentOpen = func(path string) (*os.File, error) {
		if path == changedPath {
			opens++
			if err := os.WriteFile(path, []byte(strings.Repeat("changed ", opens+1)), 0o644); err != nil {
				return nil, err
			}
		}
		return originalOpen(path)
	}

	server := newAgentScanLegacyChunkServer(t)
	defer server.Close()

	client := newAgentClient(server.URL, "", "")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manifest, err := client.scanAndUpload(1, []string{root}, logger, fsfilter.Options{}, backupRunOptions{})
	if err != nil {
		t.Fatalf("scanAndUpload() error = %v", err)
	}
	if opens != defaultAgentChangedFileReadMaxRetries+1 {
		t.Fatalf("changed file opens = %d, want %d", opens, defaultAgentChangedFileReadMaxRetries+1)
	}
	if _, ok := manifest.Find("changed.log"); ok {
		t.Fatal("changed file was included after retry exhaustion")
	}
	if entry, ok := manifest.Find("."); !ok || entry.Type != repository.EntryTypeDir {
		t.Fatalf("root manifest entry = %+v ok=%v, want dir", entry, ok)
	}
}

func TestScanAndUploadParallelRetriesFileChangedDuringRead(t *testing.T) {
	root := t.TempDir()
	changedPath := filepath.Join(root, "changed.log")
	if err := os.WriteFile(changedPath, []byte("initial"), 0o644); err != nil {
		t.Fatal(err)
	}
	finalData := []byte("initial plus appended log data")

	originalOpen := agentOpen
	t.Cleanup(func() {
		agentOpen = originalOpen
	})
	changed := false
	agentOpen = func(path string) (*os.File, error) {
		if path == changedPath && !changed {
			changed = true
			if err := os.WriteFile(path, finalData, 0o644); err != nil {
				return nil, err
			}
		}
		return originalOpen(path)
	}

	server := newAgentScanLegacyChunkServer(t)
	defer server.Close()

	client := newAgentClient(server.URL, "", "")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manifest, err := client.scanAndUpload(1, []string{root}, logger, fsfilter.Options{}, backupRunOptions{
		ScanParallelEnabled: true,
		FileReadWorkers:     1,
	})
	if err != nil {
		t.Fatalf("scanAndUpload() error = %v", err)
	}
	if !changed {
		t.Fatal("test did not mutate file during read")
	}
	entry, ok := manifest.Find("changed.log")
	if !ok {
		t.Fatal("changed file was not included after retry")
	}
	if entry.Size != int64(len(finalData)) {
		t.Fatalf("changed file size = %d, want %d", entry.Size, len(finalData))
	}
	if chunkBytes(entry) != entry.Size {
		t.Fatalf("changed file chunk bytes = %d, want %d", chunkBytes(entry), entry.Size)
	}
	if entry, ok := manifest.Find("."); !ok || entry.Type != repository.EntryTypeDir {
		t.Fatalf("root manifest entry = %+v ok=%v, want dir", entry, ok)
	}
}

func TestScanAndUploadParallelSkipsFileChangedAfterRetries(t *testing.T) {
	root := t.TempDir()
	changedPath := filepath.Join(root, "changed.log")
	if err := os.WriteFile(changedPath, []byte("initial"), 0o644); err != nil {
		t.Fatal(err)
	}

	originalOpen := agentOpen
	t.Cleanup(func() {
		agentOpen = originalOpen
	})
	opens := 0
	agentOpen = func(path string) (*os.File, error) {
		if path == changedPath {
			opens++
			if err := os.WriteFile(path, []byte(strings.Repeat("changed ", opens+1)), 0o644); err != nil {
				return nil, err
			}
		}
		return originalOpen(path)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/agent/v1/runs/1/progress" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "accepted"})
	}))
	defer server.Close()

	client := newAgentClient(server.URL, "", "")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manifest, err := client.scanAndUpload(1, []string{root}, logger, fsfilter.Options{}, backupRunOptions{
		ScanParallelEnabled: true,
		FileReadWorkers:     1,
	})
	if err != nil {
		t.Fatalf("scanAndUpload() error = %v", err)
	}
	if opens != defaultAgentChangedFileReadMaxRetries+1 {
		t.Fatalf("changed file opens = %d, want %d", opens, defaultAgentChangedFileReadMaxRetries+1)
	}
	if _, ok := manifest.Find("changed.log"); ok {
		t.Fatal("changed file was included after retry exhaustion")
	}
	if entry, ok := manifest.Find("."); !ok || entry.Type != repository.EntryTypeDir {
		t.Fatalf("root manifest entry = %+v ok=%v, want dir", entry, ok)
	}
}

func TestRegularFileSnapshotReaderDetectsGrowthDuringRead(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "live.log")
	initial := []byte(strings.Repeat("a", 64*1024))
	if err := os.WriteFile(path, initial, 0o644); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	record := catalogRecordFromFile(root, "live.log", info)
	record.Type = string(repository.EntryTypeFile)
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	reader := newRegularFileSnapshotReader(root, "live.log", path, file, record)
	buf := make([]byte, 1024)
	if n, err := reader.Read(buf); err != nil || n == 0 {
		t.Fatalf("first Read() = %d, %v; want bytes without error", n, err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("appended")); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Read(buf); !errors.Is(err, errAgentPathChanged) {
		t.Fatalf("second Read() error = %v, want errAgentPathChanged", err)
	}
}

func newAgentScanLegacyChunkServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/agent/v1/runs/1/progress":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "accepted"})
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/agent/v1/chunks/"):
			_ = json.NewEncoder(w).Encode(map[string]any{"exists": false})
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/agent/v1/chunks/"):
			data, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read chunk body: %v", err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			hash := strings.TrimPrefix(r.URL.Path, "/agent/v1/chunks/")
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"exists":   true,
				"uploaded": true,
				"ref": map[string]any{
					"hash":          hash,
					"original_size": len(data),
				},
			})
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
}

func chunkBytes(entry repository.FileEntry) int64 {
	var total int64
	for _, chunk := range entry.Chunks {
		total += chunk.OriginalSize
	}
	return total
}

func TestScanAndUploadSkipsPathDeniedBeforeRead(t *testing.T) {
	root := t.TempDir()
	deniedPath := filepath.Join(root, "denied.txt")
	if err := os.WriteFile(deniedPath, []byte("permission denied during scan"), 0o644); err != nil {
		t.Fatal(err)
	}

	originalOpen := agentOpen
	t.Cleanup(func() {
		agentOpen = originalOpen
	})
	agentOpen = func(path string) (*os.File, error) {
		if path == deniedPath {
			return nil, &os.PathError{Op: "open", Path: path, Err: os.ErrPermission}
		}
		return originalOpen(path)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/agent/v1/runs/1/progress" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "accepted"})
	}))
	defer server.Close()

	client := newAgentClient(server.URL, "", "")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manifest, err := client.scanAndUpload(1, []string{root}, logger, fsfilter.Options{}, backupRunOptions{})
	if err != nil {
		t.Fatalf("scanAndUpload() error = %v", err)
	}
	if _, ok := manifest.Find("denied.txt"); ok {
		t.Fatal("permission-denied file was included in manifest")
	}
	if entry, ok := manifest.Find("."); !ok || entry.Type != repository.EntryTypeDir {
		t.Fatalf("root manifest entry = %+v ok=%v, want dir", entry, ok)
	}
}

func TestScanAndUploadSkipsPathIOErrorBeforeStat(t *testing.T) {
	root := t.TempDir()
	brokenPath := filepath.Join(root, "broken.txt")
	if err := os.WriteFile(brokenPath, []byte("io error during scan"), 0o644); err != nil {
		t.Fatal(err)
	}

	originalLstat := agentLstat
	t.Cleanup(func() {
		agentLstat = originalLstat
	})
	agentLstat = func(path string) (os.FileInfo, error) {
		if path == brokenPath {
			return nil, &os.PathError{Op: "lstat", Path: path, Err: syscall.EIO}
		}
		return originalLstat(path)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/agent/v1/runs/1/progress" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "accepted"})
	}))
	defer server.Close()

	client := newAgentClient(server.URL, "", "")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manifest, err := client.scanAndUpload(1, []string{root}, logger, fsfilter.Options{}, backupRunOptions{})
	if err != nil {
		t.Fatalf("scanAndUpload() error = %v", err)
	}
	if _, ok := manifest.Find("broken.txt"); ok {
		t.Fatal("IO-error file was included in manifest")
	}
	if entry, ok := manifest.Find("."); !ok || entry.Type != repository.EntryTypeDir {
		t.Fatalf("root manifest entry = %+v ok=%v, want dir", entry, ok)
	}
}

func TestAgentPathErrorSkipEventRootPolicy(t *testing.T) {
	root := filepath.Join(t.TempDir(), "source")
	if _, skip := agentPathErrorSkipEvent(root, root, os.ErrNotExist); skip {
		t.Fatal("missing root should not be skipped")
	}
	if event, skip := agentPathErrorSkipEvent(root, root, os.ErrPermission); !skip || event.Reason != "permission denied during scan" {
		t.Fatalf("root permission error skip = %v event=%+v, want permission skip", skip, event)
	}
	if event, skip := agentPathErrorSkipEvent(root, root, syscall.EIO); !skip || event.Reason != "filesystem IO error during scan" {
		t.Fatalf("root IO error skip = %v event=%+v, want IO skip", skip, event)
	}
}

func TestScanAndUploadReturnsErrorWhenRootMissing(t *testing.T) {
	root := filepath.Join(t.TempDir(), "missing")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/agent/v1/runs/1/progress" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "accepted"})
	}))
	defer server.Close()

	client := newAgentClient(server.URL, "", "")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if _, err := client.scanAndUpload(1, []string{root}, logger, fsfilter.Options{}, backupRunOptions{}); !os.IsNotExist(err) {
		t.Fatalf("scanAndUpload() error = %v, want not-exist error", err)
	}
}

func TestScanAndUploadParallelManifestMatchesSerial(t *testing.T) {
	root := t.TempDir()
	firstRoot := filepath.Join(root, "first")
	secondRoot := filepath.Join(root, "second")
	for _, dir := range []string{filepath.Join(firstRoot, "sub"), filepath.Join(secondRoot, "nested")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	files := map[string][]byte{
		filepath.Join(firstRoot, "a.txt"):            []byte("parallel alpha payload"),
		filepath.Join(firstRoot, "sub", "b.txt"):     []byte("parallel beta payload"),
		filepath.Join(secondRoot, "nested", "c.txt"): []byte("parallel gamma payload"),
		filepath.Join(secondRoot, "nested", "empty"): nil,
	}
	for name, data := range files {
		if err := os.WriteFile(name, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	roots := []string{firstRoot, secondRoot}

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
	baseOpts := backupRunOptions{
		RepositoryID:               "repo-test",
		ChunkGeneration:            7,
		MaxChunkCheckBatch:         2,
		MaxChunkUploadBatchBytes:   defaultAgentChunkUploadBatchBytes,
		MaxChunkResponseBytes:      defaultAgentMaxChunkResponseBytes,
		ChunkBatchUpload:           true,
		CompactChunkCheckResponse:  true,
		CompactChunkUploadResponse: true,
	}
	serial, err := client.scanAndUpload(1, roots, logger, fsfilter.Options{}, baseOpts)
	if err != nil {
		t.Fatalf("serial scanAndUpload() error = %v", err)
	}
	parallelOpts := baseOpts
	parallelOpts.ScanParallelEnabled = true
	parallelOpts.FileReadWorkers = 2
	parallelOpts.FileReadPipelineBytes = 32
	parallel, err := client.scanAndUpload(1, roots, logger, fsfilter.Options{}, parallelOpts)
	if err != nil {
		t.Fatalf("parallel scanAndUpload() error = %v", err)
	}
	if !reflect.DeepEqual(serial.SourceRoots, roots) || !reflect.DeepEqual(parallel.SourceRoots, roots) {
		t.Fatalf("source roots differ serial=%#v parallel=%#v want=%#v", serial.SourceRoots, parallel.SourceRoots, roots)
	}
	if !reflect.DeepEqual(comparableManifestEntries(serial), comparableManifestEntries(parallel)) {
		t.Fatalf("parallel manifest entries differ\nserial=%#v\nparallel=%#v", comparableManifestEntries(serial), comparableManifestEntries(parallel))
	}
}

type comparableManifestEntry struct {
	Path       string
	Type       repository.EntryType
	Size       int64
	Mode       uint32
	LinkTarget string
	Chunks     []repository.ChunkRef
	Pack       *repository.PackFileRef
}

func comparableManifestEntries(manifest *repository.SnapshotManifest) []comparableManifestEntry {
	entries := make([]comparableManifestEntry, 0, len(manifest.Entries))
	for _, entry := range manifest.Entries {
		chunks := make([]repository.ChunkRef, 0, len(entry.Chunks))
		for _, chunk := range entry.Chunks {
			chunks = append(chunks, repository.ChunkRef{Hash: chunk.Hash, OriginalSize: chunk.OriginalSize})
		}
		entries = append(entries, comparableManifestEntry{
			Path:       entry.Path,
			Type:       entry.Type,
			Size:       entry.Size,
			Mode:       entry.Mode,
			LinkTarget: entry.LinkTarget,
			Chunks:     chunks,
			Pack:       entry.Pack,
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Path < entries[j].Path
	})
	return entries
}
