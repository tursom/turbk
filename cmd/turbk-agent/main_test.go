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
	manifest, err := client.scanAndUpload(1, []string{root}, logger, fsfilter.Options{}, backupRunOptions{
		RepositoryID:             "repo-test",
		ChunkGeneration:          7,
		MaxChunkCheckBatch:       10000,
		MaxChunkUploadBatchBytes: defaultAgentChunkUploadBatchBytes,
		ChunkBatchUpload:         true,
	})
	if err != nil {
		t.Fatalf("scanAndUpload() error = %v", err)
	}
	if checkCalls != 1 || uploadCalls != 1 {
		t.Fatalf("batch calls check=%d upload=%d, want 1/1", checkCalls, uploadCalls)
	}
	if len(checkedHashes) != 2 || len(uploadedHashes) != 2 {
		t.Fatalf("batched hashes check=%v upload=%v, want two chunks", checkedHashes, uploadedHashes)
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
