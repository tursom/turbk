package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/tursom/turbk/internal/fsfilter"
	"github.com/tursom/turbk/internal/repository"
	"github.com/tursom/turbk/internal/rootset"
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
