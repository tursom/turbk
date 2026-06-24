package httpapi

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tursom/turbk/internal/config"
	"github.com/tursom/turbk/internal/repository"
	"github.com/tursom/turbk/internal/state"
	"github.com/zeebo/blake3"
)

func TestWebAssetsAvoidStaleIndexWhiteScreen(t *testing.T) {
	root := t.TempDir()
	webDir := filepath.Join(root, "web")
	assetDir := filepath.Join(webDir, "assets")
	if err := os.MkdirAll(assetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(webDir, "index.html"), []byte(`<!doctype html><script type="module" src="/assets/current.js"></script>`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(assetDir, "current.js"), []byte(`console.log("current");`), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := config.Default()
	cfg.Server.WebDir = webDir
	cfg.Paths.StateDir = filepath.Join(root, "state")
	cfg.Paths.RepoDir = filepath.Join(root, "repo")
	cfg.Paths.RestoreRoots = []string{filepath.Join(root, "restore")}
	store, err := state.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("state.Open() error = %v", err)
	}
	defer store.Close()
	repo, err := repository.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("repository.Open() error = %v", err)
	}
	defer repo.Close()

	server := httptest.NewServer(New(cfg, store, repo, nil).Handler())
	defer server.Close()

	indexResp, err := http.Get(server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer indexResp.Body.Close()
	if indexResp.StatusCode != http.StatusOK {
		t.Fatalf("index status = %d", indexResp.StatusCode)
	}
	if got := indexResp.Header.Get("Cache-Control"); !strings.Contains(got, "no-cache") {
		t.Fatalf("index Cache-Control = %q, want no-cache", got)
	}

	currentResp, err := http.Get(server.URL + "/assets/current.js")
	if err != nil {
		t.Fatal(err)
	}
	defer currentResp.Body.Close()
	if currentResp.StatusCode != http.StatusOK {
		t.Fatalf("current asset status = %d", currentResp.StatusCode)
	}
	if got := currentResp.Header.Get("Cache-Control"); !strings.Contains(got, "immutable") {
		t.Fatalf("current asset Cache-Control = %q, want immutable", got)
	}

	staleResp, err := http.Get(server.URL + "/assets/old-build.js")
	if err != nil {
		t.Fatal(err)
	}
	defer staleResp.Body.Close()
	staleBody, err := io.ReadAll(staleResp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if staleResp.StatusCode != http.StatusOK {
		t.Fatalf("stale js status = %d body=%s", staleResp.StatusCode, string(staleBody))
	}
	if contentType := staleResp.Header.Get("Content-Type"); !strings.Contains(contentType, "text/javascript") {
		t.Fatalf("stale js Content-Type = %q", contentType)
	}
	if strings.Contains(string(staleBody), "<!doctype") || !strings.Contains(string(staleBody), "_turbk_reload") {
		t.Fatalf("stale js body should be reload script, got %q", string(staleBody))
	}

	missingResp, err := http.Get(server.URL + "/assets/missing.png")
	if err != nil {
		t.Fatal(err)
	}
	defer missingResp.Body.Close()
	if missingResp.StatusCode != http.StatusNotFound {
		t.Fatalf("missing image status = %d, want 404", missingResp.StatusCode)
	}
}

func TestManagementListAPIsReturnEmptyArrays(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.StateDir = filepath.Join(root, "state")
	cfg.Paths.RepoDir = filepath.Join(root, "repo")
	cfg.Paths.RestoreRoots = []string{filepath.Join(root, "restore")}
	store, err := state.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("state.Open() error = %v", err)
	}
	defer store.Close()
	repo, err := repository.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("repository.Open() error = %v", err)
	}
	defer repo.Close()

	server := httptest.NewServer(New(cfg, store, repo, nil).Handler())
	defer server.Close()
	cookie := login(t, server.URL)

	cases := []struct {
		path string
		key  string
	}{
		{"/api/v1/hosts", "hosts"},
		{"/api/v1/credentials", "credentials"},
		{"/api/v1/jobs", "jobs"},
		{"/api/v1/runs", "runs"},
		{"/api/v1/runs/1/logs", "logs"},
		{"/api/v1/snapshots", "snapshots"},
		{"/api/v1/restore/tasks", "tasks"},
	}
	for _, tc := range cases {
		status, body := getAuthed(t, server.URL+tc.path, cookie)
		if status != http.StatusOK {
			t.Fatalf("%s status = %d body=%s", tc.path, status, string(body))
		}
		var payload map[string]json.RawMessage
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatalf("%s response is not JSON: %v", tc.path, err)
		}
		raw, ok := payload[tc.key]
		if !ok {
			t.Fatalf("%s response missing %q: %s", tc.path, tc.key, string(body))
		}
		if !bytes.Equal(bytes.TrimSpace(raw), []byte("[]")) {
			t.Fatalf("%s %q = %s, want []", tc.path, tc.key, string(raw))
		}
	}
}

func TestJSONSuccessResponsesSupportGzip(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.StateDir = filepath.Join(root, "state")
	cfg.Paths.RepoDir = filepath.Join(root, "repo")
	cfg.Paths.RestoreRoots = []string{filepath.Join(root, "restore")}
	store, err := state.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("state.Open() error = %v", err)
	}
	defer store.Close()
	repo, err := repository.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("repository.Open() error = %v", err)
	}
	defer repo.Close()

	server := httptest.NewServer(New(cfg, store, repo, nil).Handler())
	defer server.Close()
	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}

	req, err := http.NewRequest(http.MethodGet, server.URL+"/api/v1/health", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.Header.Get("Content-Encoding") != "gzip" {
		t.Fatalf("success Content-Encoding = %q, want gzip", resp.Header.Get("Content-Encoding"))
	}
	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(gz)
	if err != nil {
		t.Fatal(err)
	}
	_ = gz.Close()
	if !bytes.Contains(body, []byte(`"status"`)) {
		t.Fatalf("gzip body = %s", string(body))
	}

	req, err = http.NewRequest(http.MethodPost, server.URL+"/agent/v1/heartbeat", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 400 {
		t.Fatalf("bad heartbeat status = %d, want error", resp.StatusCode)
	}
	if resp.Header.Get("Content-Encoding") == "gzip" {
		t.Fatal("error response should not be gzip encoded")
	}
}

func TestLocalJobRunPublishesSnapshotAndDeduplicates(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.MkdirAll(source, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "data.txt"), bytes.Repeat([]byte("local-backup-"), 500), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := config.Default()
	cfg.Paths.StateDir = filepath.Join(root, "state")
	cfg.Paths.RepoDir = filepath.Join(root, "repo")
	cfg.Paths.RestoreRoots = []string{filepath.Join(root, "restore")}
	store, err := state.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("state.Open() error = %v", err)
	}
	defer store.Close()
	repo, err := repository.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("repository.Open() error = %v", err)
	}
	defer repo.Close()

	server := httptest.NewServer(New(cfg, store, repo, nil).Handler())
	defer server.Close()
	cookie := login(t, server.URL)
	localHost, err := store.CreateHost(context.Background(), state.CreateHostInput{
		Name:       "local server",
		SourceType: "local",
	})
	if err != nil {
		t.Fatal(err)
	}

	createStatus, createBody := postJSONAuthed(t, server.URL+"/api/v1/jobs", cookie, map[string]any{
		"name":    "local job",
		"host_id": localHost.ID,
		"source_config": map[string]any{
			"root": source,
		},
		"enabled":             true,
		"max_runtime_seconds": 900,
		"retry_attempts":      1,
	})
	if createStatus != http.StatusCreated {
		t.Fatalf("create job status = %d body=%s", createStatus, string(createBody))
	}
	var created struct {
		Job state.Job `json:"job"`
	}
	if err := json.Unmarshal(createBody, &created); err != nil {
		t.Fatal(err)
	}
	if created.Job.ID == 0 || created.Job.MaxRuntimeSeconds != 900 || created.Job.RetryAttempts != 1 {
		t.Fatalf("created job missing ID: %+v", created.Job)
	}

	runURL := server.URL + "/api/v1/jobs/" + strconv.FormatInt(created.Job.ID, 10) + "/run"
	firstStatus, firstBody := postJSONAuthed(t, runURL, cookie, nil)
	if firstStatus != http.StatusOK {
		t.Fatalf("first run status = %d body=%s", firstStatus, string(firstBody))
	}
	firstStats, err := repo.Stats()
	if err != nil {
		t.Fatal(err)
	}
	secondStatus, secondBody := postJSONAuthed(t, runURL, cookie, nil)
	if secondStatus != http.StatusOK {
		t.Fatalf("second run status = %d body=%s", secondStatus, string(secondBody))
	}
	secondStats, err := repo.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if secondStats.Chunks != firstStats.Chunks || secondStats.SegmentBytes != firstStats.SegmentBytes {
		t.Fatalf("second run appended duplicate chunks: first=%+v second=%+v", firstStats, secondStats)
	}

	updateStatus, updateBody := requestJSONAuthed(t, http.MethodPatch, server.URL+"/api/v1/jobs/"+strconv.FormatInt(created.Job.ID, 10), cookie, map[string]any{
		"name":     "local job updated",
		"enabled":  false,
		"schedule": "0 3 * * *",
		"timezone": "UTC",
		"source_config": map[string]any{
			"root": source,
		},
		"max_runtime_seconds": 1800,
		"retry_attempts":      2,
	})
	if updateStatus != http.StatusOK {
		t.Fatalf("update job status = %d body=%s", updateStatus, string(updateBody))
	}
	var updated struct {
		Job state.Job `json:"job"`
	}
	if err := json.Unmarshal(updateBody, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Job.Name != "local job updated" || updated.Job.Enabled || nullStringValue(updated.Job.Schedule) != "0 3 * * *" || updated.Job.Timezone != "UTC" || updated.Job.MaxRuntimeSeconds != 1800 || updated.Job.RetryAttempts != 2 {
		t.Fatalf("unexpected updated job: %+v", updated.Job)
	}

	counts, err := store.Counts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if counts.Jobs != 1 || counts.Runs != 2 || counts.Snapshots != 2 {
		t.Fatalf("unexpected counts after runs: %+v", counts)
	}
	logStatus, _ := getAuthed(t, server.URL+"/api/v1/runs/1/logs", cookie)
	if logStatus != http.StatusOK {
		t.Fatalf("logs status = %d", logStatus)
	}
	runsStatus, runsBody := getAuthed(t, server.URL+"/api/v1/runs", cookie)
	if runsStatus != http.StatusOK {
		t.Fatalf("runs status = %d body=%s", runsStatus, string(runsBody))
	}
	var listedRuns struct {
		Runs []state.Run `json:"runs"`
	}
	if err := json.Unmarshal(runsBody, &listedRuns); err != nil {
		t.Fatal(err)
	}
	var completedWithProgress bool
	for _, run := range listedRuns.Runs {
		if run.Progress != nil && run.Progress.Phase == "completed" && run.Progress.ProcessedFiles > 0 && run.Progress.ProcessedBytes > 0 {
			completedWithProgress = true
			break
		}
	}
	if !completedWithProgress {
		t.Fatalf("runs response missing completed progress: %+v", listedRuns.Runs)
	}
}

func TestScheduledJobPublishesSnapshot(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.MkdirAll(source, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "scheduled.txt"), []byte("scheduled-data"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.Scheduler.MaxConcurrentRuns = 1
	cfg.Paths.StateDir = filepath.Join(root, "state")
	cfg.Paths.RepoDir = filepath.Join(root, "repo")
	cfg.Paths.RestoreRoots = []string{filepath.Join(root, "restore")}
	store, err := state.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("state.Open() error = %v", err)
	}
	defer store.Close()
	repo, err := repository.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("repository.Open() error = %v", err)
	}
	defer repo.Close()

	host, err := store.CreateHost(context.Background(), state.CreateHostInput{
		Name:       "local scheduler",
		SourceType: "local",
	})
	if err != nil {
		t.Fatal(err)
	}
	job, err := store.CreateJob(context.Background(), state.CreateJobInput{
		Name:         "scheduled local",
		HostID:       host.ID,
		SourceConfig: json.RawMessage(`{"root":"` + filepath.ToSlash(source) + `"}`),
		Enabled:      true,
		Schedule:     sql.NullString{String: "* * * * *", Valid: true},
		Timezone:     "UTC",
	})
	if err != nil {
		t.Fatal(err)
	}
	server := New(cfg, store, repo, nil)
	if scheduled := server.runDueScheduledJobs(context.Background(), time.Now().UTC()); scheduled != 1 {
		t.Fatalf("scheduled jobs = %d, want 1 for job %+v", scheduled, job)
	}
	waitFor(t, time.Second, func() bool {
		counts, err := store.Counts(context.Background())
		return err == nil && counts.Runs == 1 && counts.Snapshots == 1
	})
	runAgain := server.runDueScheduledJobs(context.Background(), time.Now().UTC())
	if runAgain != 0 {
		t.Fatalf("same-minute scheduled jobs = %d, want 0", runAgain)
	}
}

func TestScheduledJobRetriesFailedRunsWithinLimit(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.Scheduler.MaxConcurrentRuns = 1
	cfg.Paths.StateDir = filepath.Join(root, "state")
	cfg.Paths.RepoDir = filepath.Join(root, "repo")
	cfg.Paths.RestoreRoots = []string{filepath.Join(root, "restore")}
	store, err := state.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("state.Open() error = %v", err)
	}
	defer store.Close()
	repo, err := repository.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("repository.Open() error = %v", err)
	}
	defer repo.Close()

	missingSource := filepath.Join(root, "missing-source")
	host, err := store.CreateHost(context.Background(), state.CreateHostInput{
		Name:       "local scheduler",
		SourceType: "local",
	})
	if err != nil {
		t.Fatal(err)
	}
	job, err := store.CreateJob(context.Background(), state.CreateJobInput{
		Name:              "scheduled missing local",
		HostID:            host.ID,
		SourceConfig:      json.RawMessage(`{"root":"` + filepath.ToSlash(missingSource) + `"}`),
		Enabled:           true,
		Schedule:          sql.NullString{String: "* * * * *", Valid: true},
		Timezone:          "UTC",
		RetryAttempts:     1,
		MaxRuntimeSeconds: 60,
	})
	if err != nil {
		t.Fatal(err)
	}
	server := New(cfg, store, repo, nil)
	now := time.Now().UTC()
	if scheduled := server.runDueScheduledJobs(context.Background(), now); scheduled != 1 {
		t.Fatalf("scheduled jobs = %d, want initial run for job %+v", scheduled, job)
	}
	waitFor(t, time.Second, func() bool {
		runs, err := store.ListRuns(context.Background())
		return err == nil && len(runs) == 1 && runs[0].Status == "failed"
	})
	if scheduled := server.runDueScheduledJobs(context.Background(), now); scheduled != 1 {
		t.Fatalf("scheduled jobs = %d, want one retry", scheduled)
	}
	waitFor(t, time.Second, func() bool {
		runs, err := store.ListRuns(context.Background())
		if err != nil || len(runs) != 2 {
			return false
		}
		return runs[0].Status == "failed" && runs[1].Status == "failed"
	})
	if scheduled := server.runDueScheduledJobs(context.Background(), now); scheduled != 0 {
		t.Fatalf("scheduled jobs = %d, want retry limit reached", scheduled)
	}
}

func TestScheduledMaintenanceRecordsCleanupAndCompactSkip(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.Maintenance.CleanupSchedule = "* * * * *"
	cfg.Maintenance.CompactSchedule = "* * * * *"
	cfg.Maintenance.CompactMinReclaimRatio = 1
	cfg.Maintenance.CompactMinReclaimBytes = "1TiB"
	cfg.Paths.StateDir = filepath.Join(root, "state")
	cfg.Paths.RepoDir = filepath.Join(root, "repo")
	cfg.Paths.RestoreRoots = []string{filepath.Join(root, "restore")}
	store, err := state.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("state.Open() error = %v", err)
	}
	defer store.Close()
	repo, err := repository.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("repository.Open() error = %v", err)
	}
	defer repo.Close()

	server := New(cfg, store, repo, nil)
	now := time.Date(2026, 6, 21, 3, 30, 0, 0, time.UTC)
	if scheduled := server.runDueMaintenance(context.Background(), now); scheduled != 2 {
		t.Fatalf("scheduled maintenance = %d, want cleanup and compact", scheduled)
	}
	if scheduled := server.runDueMaintenance(context.Background(), now); scheduled != 0 {
		t.Fatalf("same-minute scheduled maintenance = %d, want 0", scheduled)
	}
	waitFor(t, time.Second, func() bool {
		runs, err := store.ListMaintenanceRuns(context.Background(), 10)
		if err != nil || len(runs) < 3 {
			return false
		}
		seenRetention := false
		seenCleanup := false
		seenCompactSkip := false
		for _, run := range runs {
			switch run.Mode {
			case "retention":
				seenRetention = run.Status == "completed"
			case "cleanup-errors":
				seenCleanup = run.Status == "completed"
			case "compact":
				seenCompactSkip = run.Status == "skipped" && run.SkippedReason.String == "compact reclaim threshold not met"
			}
		}
		return seenRetention && seenCleanup && seenCompactSkip
	})
}

func TestStorageMaintenanceExpiresOldSnapshotsAndReportsUtilization(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.MkdirAll(source, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "data.txt"), bytes.Repeat([]byte("maintenance-"), 200), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := config.Default()
	cfg.Retention.KeepLast = 1
	cfg.Retention.KeepDaily = 0
	cfg.Retention.KeepWeekly = 0
	cfg.Paths.StateDir = filepath.Join(root, "state")
	cfg.Paths.RepoDir = filepath.Join(root, "repo")
	cfg.Paths.RestoreRoots = []string{filepath.Join(root, "restore")}
	store, err := state.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("state.Open() error = %v", err)
	}
	defer store.Close()
	repo, err := repository.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("repository.Open() error = %v", err)
	}
	defer repo.Close()

	server := httptest.NewServer(New(cfg, store, repo, nil).Handler())
	defer server.Close()
	cookie := login(t, server.URL)
	localHost, err := store.CreateHost(context.Background(), state.CreateHostInput{
		Name:       "local server",
		SourceType: "local",
	})
	if err != nil {
		t.Fatal(err)
	}
	createStatus, createBody := postJSONAuthed(t, server.URL+"/api/v1/jobs", cookie, map[string]any{
		"name":          "retention local",
		"host_id":       localHost.ID,
		"source_config": map[string]any{"root": source},
		"enabled":       true,
	})
	if createStatus != http.StatusCreated {
		t.Fatalf("create job status = %d body=%s", createStatus, string(createBody))
	}
	var created struct {
		Job state.Job `json:"job"`
	}
	if err := json.Unmarshal(createBody, &created); err != nil {
		t.Fatal(err)
	}
	runURL := server.URL + "/api/v1/jobs/" + strconv.FormatInt(created.Job.ID, 10) + "/run"
	if status, body := postJSONAuthed(t, runURL, cookie, nil); status != http.StatusOK {
		t.Fatalf("first run status = %d body=%s", status, string(body))
	}
	if status, body := postJSONAuthed(t, runURL, cookie, nil); status != http.StatusOK {
		t.Fatalf("second run status = %d body=%s", status, string(body))
	}

	status, body := postJSONAuthed(t, server.URL+"/api/v1/storage/maintenance", cookie, map[string]any{"mode": "retention"})
	if status != http.StatusOK {
		t.Fatalf("maintenance status = %d body=%s", status, string(body))
	}
	var report struct {
		Retention struct {
			ExpiredSnapshots int64 `json:"expired_snapshots"`
			ActiveSnapshots  int64 `json:"active_snapshots"`
			DeletedSnapshots int64 `json:"deleted_snapshots"`
		} `json:"retention"`
		Segment struct {
			Bytes       int64   `json:"bytes"`
			Utilization float64 `json:"utilization"`
		} `json:"segment"`
		Verify struct {
			VerifiedChunks int64    `json:"verified_chunks"`
			MissingIndex   int64    `json:"missing_index"`
			CorruptChunks  int64    `json:"corrupt_chunks"`
			Errors         []string `json:"errors"`
		} `json:"verify"`
		Compact struct {
			RewrittenChunks     int64  `json:"rewritten_chunks"`
			RewrittenBytes      int64  `json:"rewritten_bytes"`
			RemovedChunks       int64  `json:"removed_chunks"`
			RemovedSegments     int    `json:"removed_segments"`
			RemovedSegmentBytes int64  `json:"removed_segment_bytes"`
			SkippedReason       string `json:"skipped_reason"`
		} `json:"compact"`
	}
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatal(err)
	}
	if report.Retention.ExpiredSnapshots != 1 || report.Retention.ActiveSnapshots != 1 || report.Retention.DeletedSnapshots != 1 {
		t.Fatalf("unexpected maintenance retention report: %+v", report.Retention)
	}
	if report.Segment.Bytes <= 0 || report.Segment.Utilization <= 0 {
		t.Fatalf("unexpected segment utilization report: %+v", report.Segment)
	}
	snapshots, err := store.ListSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 1 {
		t.Fatalf("active snapshots = %d, want 1", len(snapshots))
	}

	status, body = postJSONAuthed(t, server.URL+"/api/v1/storage/maintenance", cookie, map[string]any{"mode": "verify"})
	if status != http.StatusOK {
		t.Fatalf("verify status = %d body=%s", status, string(body))
	}
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatal(err)
	}
	if report.Verify.VerifiedChunks == 0 || report.Verify.MissingIndex != 0 || report.Verify.CorruptChunks != 0 || len(report.Verify.Errors) != 0 {
		t.Fatalf("unexpected clean verify report: %+v", report.Verify)
	}
	snapshots, err = store.ListSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 1 || snapshots[0].Health != "healthy" || !snapshots[0].VerifiedAt.Valid {
		t.Fatalf("clean verify did not mark snapshot healthy: %+v", snapshots)
	}
	status, body = postJSONAuthed(t, server.URL+"/api/v1/storage/maintenance", cookie, map[string]any{"mode": "compact"})
	if status != http.StatusOK {
		t.Fatalf("compact status = %d body=%s", status, string(body))
	}
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatal(err)
	}
	if report.Compact.SkippedReason != "" || report.Compact.RewrittenChunks == 0 || report.Compact.RewrittenBytes == 0 || report.Compact.RemovedSegments == 0 || report.Compact.RemovedSegmentBytes == 0 {
		t.Fatalf("unexpected compact report: %+v", report.Compact)
	}
	status, body = postJSONAuthed(t, server.URL+"/api/v1/storage/maintenance", cookie, map[string]any{"mode": "verify"})
	if status != http.StatusOK {
		t.Fatalf("post-compact verify status = %d body=%s", status, string(body))
	}
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatal(err)
	}
	if report.Verify.VerifiedChunks == 0 || report.Verify.MissingIndex != 0 || report.Verify.CorruptChunks != 0 || len(report.Verify.Errors) != 0 {
		t.Fatalf("unexpected post-compact verify report: %+v", report.Verify)
	}
	segmentFiles, err := filepath.Glob(filepath.Join(cfg.Paths.RepoDir, "segments", "*.seg"))
	if err != nil {
		t.Fatal(err)
	}
	if len(segmentFiles) == 0 {
		t.Fatal("no segment files written")
	}
	segment, err := os.OpenFile(segmentFiles[0], os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	info, err := segment.Stat()
	if err != nil {
		_ = segment.Close()
		t.Fatal(err)
	}
	if info.Size() == 0 {
		_ = segment.Close()
		t.Fatal("segment file is empty")
	}
	var last [1]byte
	if _, err := segment.ReadAt(last[:], info.Size()-1); err != nil {
		_ = segment.Close()
		t.Fatal(err)
	}
	last[0] ^= 0xff
	if _, err := segment.WriteAt(last[:], info.Size()-1); err != nil {
		_ = segment.Close()
		t.Fatal(err)
	}
	if err := segment.Close(); err != nil {
		t.Fatal(err)
	}
	status, body = postJSONAuthed(t, server.URL+"/api/v1/storage/maintenance", cookie, map[string]any{"mode": "verify"})
	if status != http.StatusOK {
		t.Fatalf("verify corrupt status = %d body=%s", status, string(body))
	}
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatal(err)
	}
	if report.Verify.CorruptChunks == 0 || len(report.Verify.Errors) == 0 {
		t.Fatalf("corrupt verify report did not flag errors: %+v", report.Verify)
	}
	snapshots, err = store.ListSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 1 || snapshots[0].Health != "corrupt" || !snapshots[0].HealthMessage.Valid {
		t.Fatalf("corrupt verify did not mark snapshot corrupt: %+v", snapshots)
	}
}

func TestSnapshotDeleteAPISoftDeletesAndIsIdempotent(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.StateDir = filepath.Join(root, "state")
	cfg.Paths.RepoDir = filepath.Join(root, "repo")
	cfg.Paths.RestoreRoots = []string{filepath.Join(root, "restore")}
	store, err := state.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("state.Open() error = %v", err)
	}
	defer store.Close()
	repo, err := repository.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("repository.Open() error = %v", err)
	}
	defer repo.Close()

	server := httptest.NewServer(New(cfg, store, repo, nil).Handler())
	defer server.Close()
	cookie := login(t, server.URL)

	snapshot, err := store.CreateSnapshot(context.Background(), state.CreateSnapshotInput{
		SourceType:  "local",
		ManifestRef: "delete-api",
		FileCount:   1,
		TotalSize:   128,
	})
	if err != nil {
		t.Fatal(err)
	}
	task, err := store.CreateRestoreTask(context.Background(), state.CreateRestoreTaskInput{
		SnapshotID: snapshot.ID,
		TargetPath: filepath.Join(cfg.Paths.RestoreRoots[0], "target"),
		Status:     "running",
	})
	if err != nil {
		t.Fatal(err)
	}
	deleteURL := server.URL + "/api/v1/snapshots/" + strconv.FormatInt(snapshot.ID, 10)
	status, body := requestJSONAuthed(t, http.MethodDelete, deleteURL, cookie, nil)
	if status != http.StatusConflict {
		t.Fatalf("delete active restore snapshot status = %d body=%s", status, string(body))
	}
	if _, err := store.UpdateRestoreTaskStatus(context.Background(), task.ID, "completed"); err != nil {
		t.Fatal(err)
	}
	status, body = requestJSONAuthed(t, http.MethodDelete, deleteURL, cookie, nil)
	if status != http.StatusOK {
		t.Fatalf("delete snapshot status = %d body=%s", status, string(body))
	}
	if !bytes.Contains(body, []byte(`"status":"deleted"`)) || !bytes.Contains(body, []byte(`"requires_compact":true`)) {
		t.Fatalf("delete response missing deletion status or compact hint: %s", string(body))
	}
	status, body = getAuthed(t, server.URL+"/api/v1/snapshots", cookie)
	if status != http.StatusOK {
		t.Fatalf("list snapshots status = %d body=%s", status, string(body))
	}
	if !bytes.Contains(body, []byte(`"snapshots":[]`)) {
		t.Fatalf("deleted snapshot still visible: %s", string(body))
	}
	status, body = requestJSONAuthed(t, http.MethodDelete, deleteURL, cookie, nil)
	if status != http.StatusOK {
		t.Fatalf("idempotent delete status = %d body=%s", status, string(body))
	}
	if !bytes.Contains(body, []byte(`"deleted":false`)) {
		t.Fatalf("idempotent delete did not report deleted=false: %s", string(body))
	}
	status, body = postJSONAuthed(t, server.URL+"/api/v1/snapshots/delete", cookie, map[string]any{"snapshot_ids": []int64{snapshot.ID, 999999}})
	if status != http.StatusOK {
		t.Fatalf("batch delete status = %d body=%s", status, string(body))
	}
	if !bytes.Contains(body, []byte(`"results"`)) || !bytes.Contains(body, []byte(`snapshot 999999 not found`)) {
		t.Fatalf("batch delete did not include partial failure: %s", string(body))
	}
}

func TestCleanupErrorsRemovesOrphanChunkAndManifest(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.Maintenance.ErrorGracePeriod = "0s"
	cfg.Paths.StateDir = filepath.Join(root, "state")
	cfg.Paths.RepoDir = filepath.Join(root, "repo")
	cfg.Paths.RestoreRoots = []string{filepath.Join(root, "restore")}
	store, err := state.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("state.Open() error = %v", err)
	}
	defer store.Close()
	repo, err := repository.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("repository.Open() error = %v", err)
	}
	defer repo.Close()

	if _, existed, err := repo.PutChunk(context.Background(), []byte("orphan chunk")); err != nil {
		t.Fatal(err)
	} else if existed {
		t.Fatal("unexpected duplicate orphan chunk")
	}
	manifestID := "orphan-cleanup"
	if err := repo.WriteManifest(&repository.SnapshotManifest{
		ID:         manifestID,
		CreatedAt:  time.Now().Add(-time.Hour).UTC(),
		SourceType: "local",
	}); err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(cfg.Paths.StateDir, "manifests", manifestID+".json")
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(manifestPath, old, old); err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(New(cfg, store, repo, nil).Handler())
	defer server.Close()
	cookie := login(t, server.URL)

	status, body := postJSONAuthed(t, server.URL+"/api/v1/storage/maintenance", cookie, map[string]any{"mode": "cleanup-errors"})
	if status != http.StatusOK {
		t.Fatalf("cleanup-errors status = %d body=%s", status, string(body))
	}
	var report struct {
		Cleanup struct {
			RemovedChunks        int64 `json:"removed_chunks"`
			RemovedManifests     int   `json:"removed_manifests"`
			RemovedManifestBytes int64 `json:"removed_manifest_bytes"`
		} `json:"cleanup"`
		Chunks struct {
			Indexed int64 `json:"indexed"`
		} `json:"chunks"`
	}
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatal(err)
	}
	if report.Cleanup.RemovedChunks != 1 || report.Cleanup.RemovedManifests != 1 || report.Cleanup.RemovedManifestBytes == 0 || report.Chunks.Indexed != 0 {
		t.Fatalf("unexpected cleanup report: %+v", report)
	}
	if _, err := os.Stat(manifestPath); !os.IsNotExist(err) {
		t.Fatalf("orphan manifest still exists or stat failed with non-ENOENT: %v", err)
	}
	stats, err := repo.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.Chunks != 0 {
		t.Fatalf("orphan chunk index count = %d, want 0", stats.Chunks)
	}
	status, body = getAuthed(t, server.URL+"/api/v1/storage/maintenance/runs", cookie)
	if status != http.StatusOK {
		t.Fatalf("maintenance history status = %d body=%s", status, string(body))
	}
	if !bytes.Contains(body, []byte(`"mode":"cleanup-errors"`)) {
		t.Fatalf("maintenance history missing cleanup run: %s", string(body))
	}
}

func TestCompactMaintenanceAndBackupRunsAreMutuallyExclusive(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.MkdirAll(source, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "data.txt"), []byte("gate-data"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := config.Default()
	cfg.Scheduler.MaxConcurrentRuns = 1
	cfg.Paths.StateDir = filepath.Join(root, "state")
	cfg.Paths.RepoDir = filepath.Join(root, "repo")
	cfg.Paths.RestoreRoots = []string{filepath.Join(root, "restore")}
	store, err := state.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("state.Open() error = %v", err)
	}
	defer store.Close()
	repo, err := repository.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("repository.Open() error = %v", err)
	}
	defer repo.Close()

	server := New(cfg, store, repo, nil)
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()
	cookie := login(t, httpServer.URL)
	host, err := store.CreateHost(context.Background(), state.CreateHostInput{
		Name:       "local gate",
		SourceType: "local",
	})
	if err != nil {
		t.Fatal(err)
	}
	job, err := store.CreateJob(context.Background(), state.CreateJobInput{
		Name:         "gate local",
		HostID:       host.ID,
		SourceConfig: json.RawMessage(`{"root":"` + filepath.ToSlash(source) + `"}`),
		Enabled:      true,
		Schedule:     sql.NullString{String: "* * * * *", Valid: true},
		Timezone:     "UTC",
	})
	if err != nil {
		t.Fatal(err)
	}

	server.runGate.Lock()
	status, body := postJSONAuthed(t, httpServer.URL+"/api/v1/jobs/"+strconv.FormatInt(job.ID, 10)+"/run", cookie, nil)
	if status != http.StatusConflict {
		server.runGate.Unlock()
		t.Fatalf("manual run during compact status = %d body=%s", status, string(body))
	}
	if scheduled := server.runDueScheduledJobs(context.Background(), time.Now().UTC()); scheduled != 0 {
		server.runGate.Unlock()
		t.Fatalf("scheduled jobs during compact = %d, want 0", scheduled)
	}
	server.runGate.Unlock()

	server.runGate.RLock()
	report, err := server.runStorageMaintenance(context.Background(), "compact")
	server.runGate.RUnlock()
	if err != nil {
		t.Fatal(err)
	}
	if report.Compact.SkippedReason == "" {
		t.Fatalf("compact did not skip while backup gate was busy: %+v", report.Compact)
	}
}

func TestSnapshotBrowseDownloadAndRestore(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.MkdirAll(filepath.Join(source, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	content := bytes.Repeat([]byte("restore-data-"), 256)
	if err := os.WriteFile(filepath.Join(source, "sub", "file.txt"), content, 0o640); err != nil {
		t.Fatal(err)
	}

	cfg := config.Default()
	cfg.Paths.StateDir = filepath.Join(root, "state")
	cfg.Paths.RepoDir = filepath.Join(root, "repo")
	cfg.Paths.RestoreRoots = []string{filepath.Join(root, "restore")}
	store, err := state.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("state.Open() error = %v", err)
	}
	defer store.Close()
	repo, err := repository.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("repository.Open() error = %v", err)
	}
	defer repo.Close()

	server := httptest.NewServer(New(cfg, store, repo, nil).Handler())
	defer server.Close()
	cookie := login(t, server.URL)
	localHost, err := store.CreateHost(context.Background(), state.CreateHostInput{
		Name:       "local server",
		SourceType: "local",
	})
	if err != nil {
		t.Fatal(err)
	}
	createStatus, createBody := postJSONAuthed(t, server.URL+"/api/v1/jobs", cookie, map[string]any{
		"name":          "restore local",
		"host_id":       localHost.ID,
		"source_config": map[string]any{"root": source},
		"enabled":       true,
	})
	if createStatus != http.StatusCreated {
		t.Fatalf("create job status = %d body=%s", createStatus, string(createBody))
	}
	var created struct {
		Job state.Job `json:"job"`
	}
	if err := json.Unmarshal(createBody, &created); err != nil {
		t.Fatal(err)
	}
	runStatus, runBody := postJSONAuthed(t, server.URL+"/api/v1/jobs/"+strconv.FormatInt(created.Job.ID, 10)+"/run", cookie, nil)
	if runStatus != http.StatusOK {
		t.Fatalf("run status = %d body=%s", runStatus, string(runBody))
	}
	snapshots, err := store.ListSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 1 {
		t.Fatalf("snapshots = %d, want 1", len(snapshots))
	}
	snapshotID := snapshots[0].ID

	treeStatus, treeBody := getAuthed(t, server.URL+"/api/v1/snapshots/"+strconv.FormatInt(snapshotID, 10)+"/tree?path=.", cookie)
	if treeStatus != http.StatusOK {
		t.Fatalf("tree status = %d body=%s", treeStatus, string(treeBody))
	}
	if !bytes.Contains(treeBody, []byte(`"path":"sub"`)) {
		t.Fatalf("tree response missing sub directory: %s", string(treeBody))
	}

	fileStatus, fileBody := getAuthed(t, server.URL+"/api/v1/snapshots/"+strconv.FormatInt(snapshotID, 10)+"/files/sub/file.txt", cookie)
	if fileStatus != http.StatusOK {
		t.Fatalf("file status = %d body=%s", fileStatus, string(fileBody))
	}
	if !bytes.Equal(fileBody, content) {
		t.Fatal("downloaded file content differs")
	}

	dirStatus, dirBody := getAuthed(t, server.URL+"/api/v1/snapshots/"+strconv.FormatInt(snapshotID, 10)+"/files/sub", cookie)
	if dirStatus != http.StatusOK {
		t.Fatalf("dir status = %d body=%s", dirStatus, string(dirBody))
	}
	files := readTarGz(t, dirBody)
	if !bytes.Equal(files["sub/file.txt"], content) {
		t.Fatalf("tar.gz file content differs, entries=%v", mapsKeys(files))
	}

	outsideStatus, outsideBody := postJSONAuthed(t, server.URL+"/api/v1/restore", cookie, map[string]any{
		"snapshot_id": snapshotID,
		"path":        "sub",
		"target_path": filepath.Join(root, "outside"),
	})
	if outsideStatus != http.StatusBadRequest {
		t.Fatalf("outside restore status = %d body=%s", outsideStatus, string(outsideBody))
	}

	target := filepath.Join(cfg.Paths.RestoreRoots[0], "restored-sub")
	status, body := postJSONAuthed(t, server.URL+"/api/v1/restore", cookie, map[string]any{
		"snapshot_id": snapshotID,
		"path":        "sub",
		"target_path": target,
	})
	if status != http.StatusCreated {
		t.Fatalf("restore status = %d body=%s", status, string(body))
	}
	if !bytes.Contains(body, []byte(`"status":"completed"`)) {
		t.Fatalf("restore response not completed: %s", string(body))
	}
	restored, err := os.ReadFile(filepath.Join(target, "file.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(restored, content) {
		t.Fatal("restored file content differs")
	}
	taskStatus, taskBody := getAuthed(t, server.URL+"/api/v1/restore/tasks", cookie)
	if taskStatus != http.StatusOK {
		t.Fatalf("restore tasks status = %d body=%s", taskStatus, string(taskBody))
	}
	if !bytes.Contains(taskBody, []byte(`"status":"completed"`)) {
		t.Fatalf("restore tasks missing completed task: %s", string(taskBody))
	}
}

func TestManagementAuthSessionFlow(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.StateDir = filepath.Join(root, "state")
	cfg.Paths.RepoDir = filepath.Join(root, "repo")
	cfg.Paths.RestoreRoots = []string{filepath.Join(root, "restore")}
	store, err := state.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("state.Open() error = %v", err)
	}
	defer store.Close()
	repo, err := repository.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("repository.Open() error = %v", err)
	}
	defer repo.Close()

	server := httptest.NewServer(New(cfg, store, repo, nil).Handler())
	defer server.Close()

	status, body := requestJSON(t, http.MethodGet, server.URL+"/api/v1/jobs", "", nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("unauthenticated jobs status = %d body=%s", status, string(body))
	}
	status, body = requestJSON(t, http.MethodPost, server.URL+"/api/v1/auth/login", "", map[string]any{
		"username": "admin",
		"password": "wrong",
	})
	if status != http.StatusUnauthorized {
		t.Fatalf("bad login status = %d body=%s", status, string(body))
	}

	cookie := login(t, server.URL)
	status, body = requestJSONAuthed(t, http.MethodGet, server.URL+"/api/v1/auth/session", cookie, nil)
	if status != http.StatusOK {
		t.Fatalf("session status = %d body=%s", status, string(body))
	}
	status, body = requestJSONAuthed(t, http.MethodGet, server.URL+"/api/v1/jobs", cookie, nil)
	if status != http.StatusOK {
		t.Fatalf("authenticated jobs status = %d body=%s", status, string(body))
	}
	status, body = requestJSONAuthed(t, http.MethodPost, server.URL+"/api/v1/auth/logout", cookie, nil)
	if status != http.StatusOK {
		t.Fatalf("logout status = %d body=%s", status, string(body))
	}
	status, body = requestJSONAuthed(t, http.MethodGet, server.URL+"/api/v1/jobs", cookie, nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("post-logout jobs status = %d body=%s", status, string(body))
	}
}

func TestSettingsAPIUpdatesAdminAndRetention(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.StateDir = filepath.Join(root, "state")
	cfg.Paths.RepoDir = filepath.Join(root, "repo")
	cfg.Paths.RestoreRoots = []string{filepath.Join(root, "restore")}
	store, err := state.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("state.Open() error = %v", err)
	}
	defer store.Close()
	repo, err := repository.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("repository.Open() error = %v", err)
	}
	defer repo.Close()

	server := httptest.NewServer(New(cfg, store, repo, nil).Handler())
	cookie := login(t, server.URL)
	status, body := requestJSONAuthed(t, http.MethodPatch, server.URL+"/api/v1/settings", cookie, map[string]any{
		"admin_username":    "operator",
		"current_password":  "admin",
		"admin_password":    "new-admin-secret",
		"session_ttl_hours": 6,
		"retention": map[string]any{
			"keep_last":   2,
			"keep_daily":  1,
			"keep_weekly": 0,
		},
	})
	if status != http.StatusOK {
		t.Fatalf("update settings status = %d body=%s", status, string(body))
	}
	var updated struct {
		Settings struct {
			Auth struct {
				Username        string `json:"username"`
				SessionTTLHours int    `json:"session_ttl_hours"`
			} `json:"auth"`
			Retention struct {
				KeepLast   int `json:"keep_last"`
				KeepDaily  int `json:"keep_daily"`
				KeepWeekly int `json:"keep_weekly"`
			} `json:"retention"`
		} `json:"settings"`
	}
	if err := json.Unmarshal(body, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Settings.Auth.Username != "operator" || updated.Settings.Auth.SessionTTLHours != 6 ||
		updated.Settings.Retention.KeepLast != 2 || updated.Settings.Retention.KeepDaily != 1 || updated.Settings.Retention.KeepWeekly != 0 {
		t.Fatalf("unexpected settings response: %+v", updated.Settings)
	}
	status, body = requestJSON(t, http.MethodPost, server.URL+"/api/v1/auth/login", "", map[string]any{
		"username": "admin",
		"password": "admin",
	})
	if status != http.StatusUnauthorized {
		t.Fatalf("old admin login status = %d body=%s", status, string(body))
	}
	server.Close()

	server = httptest.NewServer(New(cfg, store, repo, nil).Handler())
	defer server.Close()
	cookie = loginWith(t, server.URL, "operator", "new-admin-secret")
	status, body = requestJSONAuthed(t, http.MethodGet, server.URL+"/api/v1/bootstrap", cookie, nil)
	if status != http.StatusOK {
		t.Fatalf("bootstrap status = %d body=%s", status, string(body))
	}
	var bootstrap struct {
		Auth struct {
			Username        string `json:"username"`
			SessionTTLHours int    `json:"session_ttl_hours"`
		} `json:"auth"`
		Retention struct {
			KeepLast   int `json:"keep_last"`
			KeepDaily  int `json:"keep_daily"`
			KeepWeekly int `json:"keep_weekly"`
		} `json:"retention"`
	}
	if err := json.Unmarshal(body, &bootstrap); err != nil {
		t.Fatal(err)
	}
	if bootstrap.Auth.Username != "operator" || bootstrap.Auth.SessionTTLHours != 6 ||
		bootstrap.Retention.KeepLast != 2 || bootstrap.Retention.KeepDaily != 1 || bootstrap.Retention.KeepWeekly != 0 {
		t.Fatalf("bootstrap did not use persisted settings: %+v", bootstrap)
	}
	status, body = postJSONAuthed(t, server.URL+"/api/v1/storage/maintenance", cookie, map[string]any{"mode": "retention"})
	if status != http.StatusOK {
		t.Fatalf("maintenance status = %d body=%s", status, string(body))
	}
	if !bytes.Contains(body, []byte(`"keep_last":2`)) || !bytes.Contains(body, []byte(`"keep_daily":1`)) || !bytes.Contains(body, []byte(`"keep_weekly":0`)) {
		t.Fatalf("maintenance did not report persisted retention policy: %s", string(body))
	}
}

func TestHostAPIAndAgentHeartbeatPublishesHostStatus(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.StateDir = filepath.Join(root, "state")
	cfg.Paths.RepoDir = filepath.Join(root, "repo")
	cfg.Paths.RestoreRoots = []string{filepath.Join(root, "restore")}
	store, err := state.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("state.Open() error = %v", err)
	}
	defer store.Close()
	repo, err := repository.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("repository.Open() error = %v", err)
	}
	defer repo.Close()

	server := httptest.NewServer(New(cfg, store, repo, nil).Handler())
	defer server.Close()
	cookie := login(t, server.URL)

	status, body := postJSONAuthed(t, server.URL+"/api/v1/credentials", cookie, map[string]any{
		"name": "sftp root",
		"type": "sftp",
		"payload": map[string]any{
			"username": "root",
			"password": "secret",
		},
	})
	if status != http.StatusCreated {
		t.Fatalf("create credential status = %d body=%s", status, string(body))
	}
	var createdSFTP struct {
		Credential state.Credential `json:"credential"`
	}
	if err := json.Unmarshal(body, &createdSFTP); err != nil {
		t.Fatal(err)
	}

	status, body = postJSONAuthed(t, server.URL+"/api/v1/hosts", cookie, map[string]any{
		"name":          "sftp host",
		"source_type":   "sftp",
		"address":       "10.0.0.10:22",
		"credential_id": createdSFTP.Credential.ID,
	})
	if status != http.StatusCreated {
		t.Fatalf("create host status = %d body=%s", status, string(body))
	}

	status, body = postJSONAuthed(t, server.URL+"/api/v1/hosts", cookie, map[string]any{
		"name":        "local with address",
		"source_type": "local",
		"address":     "/mnt/source",
	})
	if status != http.StatusBadRequest {
		t.Fatalf("local host with address status = %d body=%s", status, string(body))
	}

	status, body = postJSONAuthed(t, server.URL+"/api/v1/hosts", cookie, map[string]any{
		"name":        "server local",
		"source_type": "local",
	})
	if status != http.StatusCreated {
		t.Fatalf("create local host status = %d body=%s", status, string(body))
	}
	var createdLocalHost struct {
		Host state.Host `json:"host"`
	}
	if err := json.Unmarshal(body, &createdLocalHost); err != nil {
		t.Fatal(err)
	}
	status, body = requestJSONAuthed(t, http.MethodPatch, server.URL+"/api/v1/hosts/"+strconv.FormatInt(createdLocalHost.Host.ID, 10), cookie, map[string]any{
		"address": "/mnt/other",
	})
	if status != http.StatusBadRequest {
		t.Fatalf("patch local host address status = %d body=%s", status, string(body))
	}

	status, body = postJSONAuthed(t, server.URL+"/api/v1/hosts", cookie, map[string]any{
		"name":        "agent-subject",
		"source_type": "agent",
		"address":     "should-not-be-stored",
	})
	if status != http.StatusBadRequest {
		t.Fatalf("agent host with address status = %d body=%s", status, string(body))
	}

	status, body = postJSONAuthed(t, server.URL+"/api/v1/hosts", cookie, map[string]any{
		"name":        "agent-subject",
		"source_type": "agent",
	})
	if status != http.StatusCreated {
		t.Fatalf("create agent host status = %d body=%s", status, string(body))
	}
	var createdAgentHost struct {
		Host       state.Host       `json:"host"`
		Credential state.Credential `json:"credential"`
		Agent      struct {
			ClientID     string `json:"client_id"`
			ClientSecret string `json:"client_secret"`
		} `json:"agent"`
	}
	if err := json.Unmarshal(body, &createdAgentHost); err != nil {
		t.Fatal(err)
	}
	if createdAgentHost.Host.Address.Valid {
		t.Fatalf("agent host should not store manual address: %+v", createdAgentHost.Host)
	}
	if createdAgentHost.Agent.ClientID == "" || createdAgentHost.Agent.ClientSecret == "" || !createdAgentHost.Host.CredentialID.Valid {
		t.Fatalf("agent credential missing client credentials: %s", string(body))
	}
	status, body = requestJSONAuthed(t, http.MethodPatch, server.URL+"/api/v1/hosts/"+strconv.FormatInt(createdAgentHost.Host.ID, 10), cookie, map[string]any{
		"address": "manual-agent-hostname",
	})
	if status != http.StatusBadRequest {
		t.Fatalf("patch agent host address status = %d body=%s", status, string(body))
	}
	status, body = requestJSONAuthed(t, http.MethodPatch, server.URL+"/api/v1/hosts/"+strconv.FormatInt(createdAgentHost.Host.ID, 10), cookie, map[string]any{
		"agent_setup": map[string]any{
			"roots":           []string{"/var/lib/docker", "/etc"},
			"backup_schedule": "6h",
		},
	})
	if status != http.StatusBadRequest {
		t.Fatalf("patch invalid agent setup schedule status = %d body=%s", status, string(body))
	}
	status, body = requestJSONAuthed(t, http.MethodPatch, server.URL+"/api/v1/hosts/"+strconv.FormatInt(createdAgentHost.Host.ID, 10), cookie, map[string]any{
		"agent_setup": map[string]any{
			"roots":           []string{"/var/lib/docker", "/etc"},
			"backup_schedule": "0 */6 * * *",
		},
	})
	if status != http.StatusOK {
		t.Fatalf("patch agent setup status = %d body=%s", status, string(body))
	}
	var patchedAgentHost struct {
		Host state.Host `json:"host"`
	}
	if err := json.Unmarshal(body, &patchedAgentHost); err != nil {
		t.Fatal(err)
	}
	if patchedAgentHost.Host.AgentSetup == nil || !reflect.DeepEqual(patchedAgentHost.Host.AgentSetup.Roots, []string{"/var/lib/docker", "/etc"}) || patchedAgentHost.Host.AgentSetup.BackupSchedule != "0 */6 * * *" {
		t.Fatalf("agent setup not saved: %+v", patchedAgentHost.Host.AgentSetup)
	}
	status, body = requestJSONAgent(t, http.MethodPost, server.URL+"/agent/v1/heartbeat", createdAgentHost.Agent.ClientID, createdAgentHost.Agent.ClientSecret, map[string]any{
		"hostname": "agent-hostname",
		"version":  "test",
	})
	if status != http.StatusAccepted {
		t.Fatalf("agent heartbeat status = %d body=%s", status, string(body))
	}

	status, body = getAuthed(t, server.URL+"/api/v1/hosts", cookie)
	if status != http.StatusOK {
		t.Fatalf("list hosts status = %d body=%s", status, string(body))
	}
	var listed struct {
		Hosts []state.Host `json:"hosts"`
	}
	if err := json.Unmarshal(body, &listed); err != nil {
		t.Fatal(err)
	}
	seenManual := false
	seenAgent := false
	seenAgentSetup := false
	for _, host := range listed.Hosts {
		if host.Name == "sftp host" && host.SourceType == "sftp" && nullStringValue(host.Address) == "10.0.0.10:22" {
			seenManual = true
		}
		if host.Name == "agent-subject" && host.SourceType == "agent" && host.Status == "online" && nullStringValue(host.Address) == "agent-hostname" && host.LastSeenAt.Valid {
			seenAgent = true
			seenAgentSetup = host.AgentSetup != nil && reflect.DeepEqual(host.AgentSetup.Roots, []string{"/var/lib/docker", "/etc"}) && host.AgentSetup.BackupSchedule == "0 */6 * * *"
		}
	}
	if !seenManual || !seenAgent || !seenAgentSetup {
		t.Fatalf("hosts did not include manual=%v agent=%v agentSetup=%v: %+v", seenManual, seenAgent, seenAgentSetup, listed.Hosts)
	}
}

func TestCredentialAPIEncryptsAndPullJobReferencesCredential(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.StateDir = filepath.Join(root, "state")
	cfg.Paths.RepoDir = filepath.Join(root, "repo")
	cfg.Paths.RestoreRoots = []string{filepath.Join(root, "restore")}
	store, err := state.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("state.Open() error = %v", err)
	}
	defer store.Close()
	repo, err := repository.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("repository.Open() error = %v", err)
	}
	defer repo.Close()

	server := httptest.NewServer(New(cfg, store, repo, nil).Handler())
	defer server.Close()
	cookie := login(t, server.URL)

	status, body := postJSONAuthed(t, server.URL+"/api/v1/credentials", cookie, map[string]any{
		"name": "remote sftp",
		"type": "sftp",
		"payload": map[string]any{
			"username": "root",
			"password": "super-secret",
		},
	})
	if status != http.StatusCreated {
		t.Fatalf("create credential status = %d body=%s", status, string(body))
	}
	if bytes.Contains(body, []byte("super-secret")) {
		t.Fatalf("credential response leaked secret: %s", string(body))
	}
	var created struct {
		Credential state.Credential `json:"credential"`
	}
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	status, body = postJSONAuthed(t, server.URL+"/api/v1/hosts", cookie, map[string]any{
		"name":          "remote sftp",
		"source_type":   "sftp",
		"address":       "127.0.0.1:22",
		"credential_id": created.Credential.ID,
	})
	if status != http.StatusCreated {
		t.Fatalf("create pull host status = %d body=%s", status, string(body))
	}
	var createdHost struct {
		Host state.Host `json:"host"`
	}
	if err := json.Unmarshal(body, &createdHost); err != nil {
		t.Fatal(err)
	}
	status, body = postJSONAuthed(t, server.URL+"/api/v1/jobs", cookie, map[string]any{
		"name":          "legacy source type",
		"host_id":       createdHost.Host.ID,
		"source_type":   "sftp",
		"source_config": map[string]any{"root": "/srv"},
		"enabled":       true,
	})
	if status != http.StatusBadRequest {
		t.Fatalf("create job with source_type status = %d body=%s", status, string(body))
	}
	status, body = postJSONAuthed(t, server.URL+"/api/v1/jobs", cookie, map[string]any{
		"name":          "legacy credential",
		"host_id":       createdHost.Host.ID,
		"credential_id": created.Credential.ID,
		"source_config": map[string]any{"root": "/srv"},
		"enabled":       true,
	})
	if status != http.StatusBadRequest {
		t.Fatalf("create job with credential_id status = %d body=%s", status, string(body))
	}
	status, body = postJSONAuthed(t, server.URL+"/api/v1/jobs", cookie, map[string]any{
		"name":          "pull sftp",
		"host_id":       createdHost.Host.ID,
		"source_config": map[string]any{"root": "/srv"},
		"enabled":       true,
	})
	if status != http.StatusCreated {
		t.Fatalf("create pull job status = %d body=%s", status, string(body))
	}
	if bytes.Contains(body, []byte("super-secret")) {
		t.Fatalf("job response leaked secret: %s", string(body))
	}
	var createdJob struct {
		Job state.Job `json:"job"`
	}
	if err := json.Unmarshal(body, &createdJob); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		payload map[string]any
	}{
		{
			name:    "host_id",
			payload: map[string]any{"host_id": createdHost.Host.ID},
		},
		{
			name:    "source_type",
			payload: map[string]any{"source_type": "sftp"},
		},
		{
			name:    "credential_id",
			payload: map[string]any{"credential_id": created.Credential.ID},
		},
	} {
		t.Run("reject patch "+tc.name, func(t *testing.T) {
			status, body := requestJSONAuthed(t, http.MethodPatch, server.URL+"/api/v1/jobs/"+strconv.FormatInt(createdJob.Job.ID, 10), cookie, tc.payload)
			if status != http.StatusBadRequest {
				t.Fatalf("patch job with %s status = %d body=%s", tc.name, status, string(body))
			}
		})
	}
	counts, err := store.Counts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if counts.Credentials != 1 || counts.Hosts != 1 || counts.Jobs != 1 {
		t.Fatalf("unexpected counts: %+v", counts)
	}
}

func TestCredentialAPIRejectsEndpointFields(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.StateDir = filepath.Join(root, "state")
	cfg.Paths.RepoDir = filepath.Join(root, "repo")
	cfg.Paths.RestoreRoots = []string{filepath.Join(root, "restore")}
	store, err := state.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("state.Open() error = %v", err)
	}
	defer store.Close()
	repo, err := repository.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("repository.Open() error = %v", err)
	}
	defer repo.Close()

	server := httptest.NewServer(New(cfg, store, repo, nil).Handler())
	defer server.Close()
	cookie := login(t, server.URL)

	for _, tc := range []struct {
		name           string
		credentialType string
		payload        map[string]any
	}{
		{
			name:           "address",
			credentialType: "sftp",
			payload: map[string]any{
				"address":  "10.0.0.10:22",
				"username": "root",
				"password": "secret",
			},
		},
		{
			name:           "base_url",
			credentialType: "webdav",
			payload: map[string]any{
				"base_url":     "https://storage.example.com/dav",
				"bearer_token": "secret",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := postJSONAuthed(t, server.URL+"/api/v1/credentials", cookie, map[string]any{
				"name":    "invalid endpoint field",
				"type":    tc.credentialType,
				"payload": tc.payload,
			})
			if status != http.StatusBadRequest {
				t.Fatalf("create credential status = %d body=%s", status, string(body))
			}
			if !bytes.Contains(body, []byte("must not include")) {
				t.Fatalf("create credential error did not mention endpoint field: %s", string(body))
			}
		})
	}
}

func TestWebDAVPullJobRunPublishesSnapshot(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.StateDir = filepath.Join(root, "state")
	cfg.Paths.RepoDir = filepath.Join(root, "repo")
	cfg.Paths.RestoreRoots = []string{filepath.Join(root, "restore")}
	store, err := state.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("state.Open() error = %v", err)
	}
	defer store.Close()
	repo, err := repository.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("repository.Open() error = %v", err)
	}
	defer repo.Close()

	const token = "webdav-pull-token"
	const content = "webdav pull body"
	modTime := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	webdav := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch r.Method {
		case "PROPFIND":
			if r.URL.Path != "/dav/root" {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/xml; charset=utf-8")
			w.WriteHeader(http.StatusMultiStatus)
			_, _ = w.Write([]byte(webDAVMultiStatusXML(
				webDAVTestResource{href: "/dav/root/", collection: true, modified: modTime},
				webDAVTestResource{href: "/dav/root/file.txt", size: int64(len(content)), modified: modTime},
			)))
		case http.MethodGet:
			if r.URL.Path != "/dav/root/file.txt" {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			_, _ = w.Write([]byte(content))
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}))
	defer webdav.Close()

	server := httptest.NewServer(New(cfg, store, repo, nil).Handler())
	defer server.Close()
	cookie := login(t, server.URL)
	status, body := postJSONAuthed(t, server.URL+"/api/v1/credentials", cookie, map[string]any{
		"name": "webdav pull",
		"type": "webdav",
		"payload": map[string]any{
			"bearer_token": token,
		},
	})
	if status != http.StatusCreated {
		t.Fatalf("create webdav credential status = %d body=%s", status, string(body))
	}
	var createdCredential struct {
		Credential state.Credential `json:"credential"`
	}
	if err := json.Unmarshal(body, &createdCredential); err != nil {
		t.Fatal(err)
	}
	status, body = postJSONAuthed(t, server.URL+"/api/v1/hosts", cookie, map[string]any{
		"name":          "webdav pull",
		"source_type":   "webdav",
		"address":       webdav.URL + "/dav",
		"credential_id": createdCredential.Credential.ID,
	})
	if status != http.StatusCreated {
		t.Fatalf("create webdav host status = %d body=%s", status, string(body))
	}
	var createdHost struct {
		Host state.Host `json:"host"`
	}
	if err := json.Unmarshal(body, &createdHost); err != nil {
		t.Fatal(err)
	}
	status, body = postJSONAuthed(t, server.URL+"/api/v1/jobs", cookie, map[string]any{
		"name":          "webdav pull",
		"host_id":       createdHost.Host.ID,
		"source_config": map[string]any{"root": "/root"},
		"enabled":       true,
	})
	if status != http.StatusCreated {
		t.Fatalf("create webdav job status = %d body=%s", status, string(body))
	}
	var createdJob struct {
		Job state.Job `json:"job"`
	}
	if err := json.Unmarshal(body, &createdJob); err != nil {
		t.Fatal(err)
	}
	status, body = postJSONAuthed(t, server.URL+"/api/v1/jobs/"+strconv.FormatInt(createdJob.Job.ID, 10)+"/run", cookie, nil)
	if status != http.StatusOK {
		t.Fatalf("run webdav job status = %d body=%s", status, string(body))
	}
	snapshots, err := store.ListSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 1 || snapshots[0].SourceType != "webdav" || snapshots[0].FileCount != 1 || snapshots[0].TotalSize != int64(len(content)) {
		t.Fatalf("unexpected webdav snapshot: %+v", snapshots)
	}
	status, body = getAuthed(t, server.URL+"/api/v1/snapshots/"+strconv.FormatInt(snapshots[0].ID, 10)+"/files/file.txt", cookie)
	if status != http.StatusOK {
		t.Fatalf("download webdav snapshot file status = %d body=%s", status, string(body))
	}
	if string(body) != content {
		t.Fatalf("downloaded webdav snapshot body = %q", string(body))
	}
}

func TestAgentPushPublishesSnapshotAndDeduplicates(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.StateDir = filepath.Join(root, "state")
	cfg.Paths.RepoDir = filepath.Join(root, "repo")
	cfg.Paths.RestoreRoots = []string{filepath.Join(root, "restore")}
	store, err := state.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("state.Open() error = %v", err)
	}
	defer store.Close()
	repo, err := repository.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("repository.Open() error = %v", err)
	}
	defer repo.Close()

	server := httptest.NewServer(New(cfg, store, repo, nil).Handler())
	defer server.Close()
	cookie := login(t, server.URL)

	unauthorizedStatus, _ := postJSON(t, server.URL+"/agent/v1/heartbeat", map[string]any{
		"hostname": "agent-host",
		"version":  "test",
	})
	if unauthorizedStatus != http.StatusUnauthorized {
		t.Fatalf("unauthorized heartbeat status = %d", unauthorizedStatus)
	}

	status, body := postJSONAuthed(t, server.URL+"/api/v1/credentials", cookie, map[string]any{
		"name": "agent dev",
		"type": "agent",
		"payload": map[string]any{
			"token":   "dev-token",
			"subject": "dev-host",
		},
	})
	if status != http.StatusBadRequest {
		t.Fatalf("legacy agent credential status = %d body=%s", status, string(body))
	}

	status, body = postJSONAuthed(t, server.URL+"/api/v1/hosts", cookie, map[string]any{
		"name":        "dev-host",
		"source_type": "agent",
	})
	if status != http.StatusCreated {
		t.Fatalf("create agent host status = %d body=%s", status, string(body))
	}
	var createdAgent struct {
		Host       state.Host       `json:"host"`
		Credential state.Credential `json:"credential"`
		Agent      struct {
			ClientID     string `json:"client_id"`
			ClientSecret string `json:"client_secret"`
		} `json:"agent"`
	}
	if err := json.Unmarshal(body, &createdAgent); err != nil {
		t.Fatal(err)
	}
	if createdAgent.Agent.ClientID == "" || createdAgent.Agent.ClientSecret == "" || !createdAgent.Host.CredentialID.Valid {
		t.Fatalf("agent credential missing client credentials: %s", string(body))
	}
	if createdAgent.Credential.Subject != "dev-host" {
		t.Fatalf("agent credential subject = %q", createdAgent.Credential.Subject)
	}

	status, body = getAuthed(t, server.URL+"/api/v1/credentials", cookie)
	if status != http.StatusOK {
		t.Fatalf("list credentials status = %d body=%s", status, string(body))
	}
	if bytes.Contains(body, []byte(createdAgent.Agent.ClientID)) {
		t.Fatalf("credential list exposed agent client id: %s", string(body))
	}
	if bytes.Contains(body, []byte(createdAgent.Agent.ClientSecret)) {
		t.Fatalf("credential list exposed agent client secret: %s", string(body))
	}
	if bytes.Contains(body, []byte(`"subject":"dev-host"`)) {
		t.Fatalf("credential list exposed agent subject: %s", string(body))
	}

	status, body = postJSONAuthed(t, server.URL+"/api/v1/jobs", cookie, map[string]any{
		"name":          "manual agent job",
		"host_id":       createdAgent.Host.ID,
		"source_config": map[string]any{"root": "/srv/data"},
		"enabled":       true,
	})
	if status != http.StatusBadRequest {
		t.Fatalf("manual agent job status = %d body=%s", status, string(body))
	}

	status, body = requestJSON(t, http.MethodPost, server.URL+"/agent/v1/runs", "dev-token", map[string]any{
		"hostname": "agent-host",
		"root":     "/srv/data",
		"job_name": "agent smoke",
	})
	if status != http.StatusUnauthorized {
		t.Fatalf("legacy bearer agent run status = %d body=%s", status, string(body))
	}

	status, body = requestJSONAgent(t, http.MethodPost, server.URL+"/agent/v1/runs", createdAgent.Agent.ClientID, createdAgent.Agent.ClientSecret, map[string]any{
		"hostname": "agent-host",
		"root":     "/srv/data",
		"job_name": "agent smoke",
	})
	if status != http.StatusCreated {
		t.Fatalf("create agent run status = %d body=%s", status, string(body))
	}
	var createdRun struct {
		Job state.Job `json:"job"`
		Run state.Run `json:"run"`
	}
	if err := json.Unmarshal(body, &createdRun); err != nil {
		t.Fatal(err)
	}
	if createdRun.Run.ID == 0 {
		t.Fatalf("created run missing ID: %+v", createdRun.Run)
	}
	if createdRun.Job.Name != "Agent backup - dev-host" {
		t.Fatalf("agent job name = %q, want server-derived name", createdRun.Job.Name)
	}
	status, body = requestJSONAgent(t, http.MethodPost, server.URL+"/agent/v1/runs/"+strconv.FormatInt(createdRun.Run.ID, 10)+"/progress", createdAgent.Agent.ClientID, createdAgent.Agent.ClientSecret, map[string]any{
		"phase":           "uploading",
		"processed_files": 1,
		"processed_bytes": 32,
		"uploaded_chunks": 1,
		"message":         "data.txt",
	})
	if status != http.StatusAccepted {
		t.Fatalf("agent progress status = %d body=%s", status, string(body))
	}
	status, body = requestJSONAgent(t, http.MethodPost, server.URL+"/agent/v1/runs", createdAgent.Agent.ClientID, createdAgent.Agent.ClientSecret, map[string]any{
		"hostname": "agent-host",
		"root":     "/srv/data",
		"job_name": "agent smoke should be ignored",
	})
	if status != http.StatusOK {
		t.Fatalf("idempotent create agent run status = %d body=%s", status, string(body))
	}
	var resumedRun struct {
		Job state.Job `json:"job"`
		Run state.Run `json:"run"`
	}
	if err := json.Unmarshal(body, &resumedRun); err != nil {
		t.Fatal(err)
	}
	if resumedRun.Run.ID != createdRun.Run.ID {
		t.Fatalf("resumed run ID = %d, want %d", resumedRun.Run.ID, createdRun.Run.ID)
	}
	if resumedRun.Job.ID != createdRun.Job.ID || resumedRun.Job.Name != createdRun.Job.Name {
		t.Fatalf("resumed job = %+v, want %+v", resumedRun.Job, createdRun.Job)
	}

	data := bytes.Repeat([]byte("agent-push-data-"), 400)
	sum := blake3.Sum256(data)
	hash := hex.EncodeToString(sum[:])

	status, body = requestJSONAgent(t, http.MethodGet, server.URL+"/agent/v1/chunks/"+hash, createdAgent.Agent.ClientID, createdAgent.Agent.ClientSecret, nil)
	if status != http.StatusOK {
		t.Fatalf("get missing chunk status = %d body=%s", status, string(body))
	}
	var chunkLookup struct {
		Exists bool `json:"exists"`
	}
	if err := json.Unmarshal(body, &chunkLookup); err != nil {
		t.Fatal(err)
	}
	if chunkLookup.Exists {
		t.Fatal("chunk unexpectedly existed before upload")
	}

	firstStatus, firstBody := putRawAgent(t, server.URL+"/agent/v1/chunks/"+hash, createdAgent.Agent.ClientID, createdAgent.Agent.ClientSecret, data)
	if firstStatus != http.StatusCreated {
		t.Fatalf("first chunk upload status = %d body=%s", firstStatus, string(firstBody))
	}
	statsAfterFirst, err := repo.Stats()
	if err != nil {
		t.Fatal(err)
	}
	secondStatus, secondBody := putRawAgent(t, server.URL+"/agent/v1/chunks/"+hash, createdAgent.Agent.ClientID, createdAgent.Agent.ClientSecret, data)
	if secondStatus != http.StatusOK {
		t.Fatalf("second chunk upload status = %d body=%s", secondStatus, string(secondBody))
	}
	statsAfterSecond, err := repo.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if statsAfterSecond.Chunks != statsAfterFirst.Chunks || statsAfterSecond.SegmentBytes != statsAfterFirst.SegmentBytes {
		t.Fatalf("duplicate upload changed storage stats: first=%+v second=%+v", statsAfterFirst, statsAfterSecond)
	}
	var uploaded struct {
		Ref repository.ChunkRef `json:"ref"`
	}
	if err := json.Unmarshal(firstBody, &uploaded); err != nil {
		t.Fatal(err)
	}
	if uploaded.Ref.Hash != hash {
		t.Fatalf("uploaded ref hash = %q, want %q", uploaded.Ref.Hash, hash)
	}

	manifest := repository.SnapshotManifest{
		SourceType: "agent",
		SourceRoot: "/srv/data",
		Entries: []repository.FileEntry{
			{Path: ".", Type: repository.EntryTypeDir, Mode: uint32(os.ModeDir | 0o755)},
			{Path: "data.txt", Type: repository.EntryTypeFile, Size: int64(len(data)), Mode: 0o644, Chunks: []repository.ChunkRef{uploaded.Ref}},
		},
	}
	status, body = requestJSONAgent(t, http.MethodPost, server.URL+"/agent/v1/manifests", createdAgent.Agent.ClientID, createdAgent.Agent.ClientSecret, map[string]any{
		"run_id":   createdRun.Run.ID,
		"manifest": manifest,
	})
	if status != http.StatusCreated {
		t.Fatalf("post agent manifest status = %d body=%s", status, string(body))
	}
	status, body = requestJSONAgent(t, http.MethodPost, server.URL+"/agent/v1/manifests", createdAgent.Agent.ClientID, createdAgent.Agent.ClientSecret, map[string]any{
		"run_id":   createdRun.Run.ID,
		"manifest": manifest,
	})
	if status != http.StatusOK {
		t.Fatalf("idempotent manifest status = %d body=%s", status, string(body))
	}

	counts, err := store.Counts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if counts.Credentials != 1 || counts.Jobs != 1 || counts.Runs != 1 || counts.Snapshots != 1 {
		t.Fatalf("unexpected counts after agent push: %+v", counts)
	}
	run, err := store.GetRun(context.Background(), createdRun.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" {
		t.Fatalf("run status = %q, want completed", run.Status)
	}
	if run.Progress == nil || run.Progress.Phase != "completed" || run.Progress.ProcessedFiles != 1 || run.Progress.UploadedChunks != 1 {
		t.Fatalf("unexpected agent run progress: %+v", run.Progress)
	}
}

func TestAgentBatchChunkUploadUploadsAndDeduplicates(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.StateDir = filepath.Join(root, "state")
	cfg.Paths.RepoDir = filepath.Join(root, "repo")
	cfg.Paths.RestoreRoots = []string{filepath.Join(root, "restore")}
	store, err := state.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("state.Open() error = %v", err)
	}
	defer store.Close()
	repo, err := repository.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("repository.Open() error = %v", err)
	}
	defer repo.Close()

	server := httptest.NewServer(New(cfg, store, repo, nil).Handler())
	defer server.Close()
	cookie := login(t, server.URL)
	status, body := postJSONAuthed(t, server.URL+"/api/v1/hosts", cookie, map[string]any{
		"name":        "batch-agent",
		"source_type": "agent",
	})
	if status != http.StatusCreated {
		t.Fatalf("create agent host status = %d body=%s", status, string(body))
	}
	var createdAgent struct {
		Agent struct {
			ClientID     string `json:"client_id"`
			ClientSecret string `json:"client_secret"`
		} `json:"agent"`
	}
	if err := json.Unmarshal(body, &createdAgent); err != nil {
		t.Fatal(err)
	}

	first := bytes.Repeat([]byte("batch-first-"), 200)
	second := bytes.Repeat([]byte("batch-second-"), 200)
	status, body = postRawAgent(t, server.URL+"/agent/v1/chunks/upload", createdAgent.Agent.ClientID, createdAgent.Agent.ClientSecret, agentChunkBatchContentType, encodeAgentChunkBatch(t, first))
	if status != http.StatusConflict {
		t.Fatalf("batch upload without active run status = %d body=%s", status, string(body))
	}
	status, body = requestJSONAgent(t, http.MethodPost, server.URL+"/agent/v1/runs", createdAgent.Agent.ClientID, createdAgent.Agent.ClientSecret, map[string]any{
		"hostname": "batch-agent",
		"root":     "/batch/source",
	})
	if status != http.StatusCreated {
		t.Fatalf("create agent run status = %d body=%s", status, string(body))
	}
	status, body = postRawAgent(t, server.URL+"/agent/v1/chunks/upload", createdAgent.Agent.ClientID, createdAgent.Agent.ClientSecret, agentChunkBatchContentType, encodeAgentChunkBatch(t, first, second))
	if status != http.StatusAccepted {
		t.Fatalf("batch upload status = %d body=%s", status, string(body))
	}
	var uploaded struct {
		Chunks []struct {
			Hash     string              `json:"hash"`
			Uploaded bool                `json:"uploaded"`
			Ref      repository.ChunkRef `json:"ref"`
		} `json:"chunks"`
	}
	if err := json.Unmarshal(body, &uploaded); err != nil {
		t.Fatal(err)
	}
	if len(uploaded.Chunks) != 2 || !uploaded.Chunks[0].Uploaded || !uploaded.Chunks[1].Uploaded {
		t.Fatalf("unexpected first batch response: %+v", uploaded.Chunks)
	}
	statsAfterFirst, err := repo.Stats()
	if err != nil {
		t.Fatal(err)
	}

	status, body = postRawAgent(t, server.URL+"/agent/v1/chunks/upload", createdAgent.Agent.ClientID, createdAgent.Agent.ClientSecret, agentChunkBatchContentType, encodeAgentChunkBatch(t, first, second))
	if status != http.StatusAccepted {
		t.Fatalf("duplicate batch upload status = %d body=%s", status, string(body))
	}
	var duplicate struct {
		Chunks []struct {
			Uploaded bool `json:"uploaded"`
		} `json:"chunks"`
	}
	if err := json.Unmarshal(body, &duplicate); err != nil {
		t.Fatal(err)
	}
	if len(duplicate.Chunks) != 2 || duplicate.Chunks[0].Uploaded || duplicate.Chunks[1].Uploaded {
		t.Fatalf("unexpected duplicate batch response: %+v", duplicate.Chunks)
	}
	statsAfterSecond, err := repo.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if statsAfterSecond.Chunks != statsAfterFirst.Chunks || statsAfterSecond.SegmentBytes != statsAfterFirst.SegmentBytes {
		t.Fatalf("duplicate batch upload changed storage stats: first=%+v second=%+v", statsAfterFirst, statsAfterSecond)
	}

	status, body = postRawAgent(t, server.URL+"/agent/v1/chunks/upload?compact_response=1", createdAgent.Agent.ClientID, createdAgent.Agent.ClientSecret, agentChunkBatchContentType, encodeAgentChunkBatch(t, first, second))
	if status != http.StatusAccepted {
		t.Fatalf("compact duplicate batch upload status = %d body=%s", status, string(body))
	}
	var compactUpload struct {
		Chunks []map[string]json.RawMessage `json:"chunks"`
	}
	if err := json.Unmarshal(body, &compactUpload); err != nil {
		t.Fatal(err)
	}
	if len(compactUpload.Chunks) != 2 {
		t.Fatalf("compact upload chunks = %d, want 2", len(compactUpload.Chunks))
	}
	for _, chunk := range compactUpload.Chunks {
		if _, ok := chunk["ref"]; ok {
			t.Fatalf("compact upload chunk unexpectedly includes ref: %s", string(body))
		}
		if _, ok := chunk["exists"]; ok {
			t.Fatalf("compact upload chunk unexpectedly includes exists: %s", string(body))
		}
		if len(chunk["hash"]) == 0 || len(chunk["uploaded"]) == 0 || len(chunk["original_size"]) == 0 {
			t.Fatalf("compact upload chunk missing compact fields: %v", chunk)
		}
	}

	firstHash, err := repository.HashBytes(first)
	if err != nil {
		t.Fatal(err)
	}
	missingHash, err := repository.HashBytes([]byte("missing compact check chunk"))
	if err != nil {
		t.Fatal(err)
	}
	status, body = requestJSONAgent(t, http.MethodPost, server.URL+"/agent/v1/chunks/check", createdAgent.Agent.ClientID, createdAgent.Agent.ClientSecret, map[string]any{
		"hashes":           []string{firstHash, missingHash},
		"compact_response": true,
	})
	if status != http.StatusOK {
		t.Fatalf("compact chunk check status = %d body=%s", status, string(body))
	}
	var compactCheck map[string]json.RawMessage
	if err := json.Unmarshal(body, &compactCheck); err != nil {
		t.Fatal(err)
	}
	if _, ok := compactCheck["exists"]; ok {
		t.Fatalf("compact chunk check unexpectedly includes exists: %s", string(body))
	}
	var missing []string
	if err := json.Unmarshal(compactCheck["missing"], &missing); err != nil {
		t.Fatal(err)
	}
	if len(missing) != 1 || missing[0] != missingHash {
		t.Fatalf("compact chunk check missing = %v, want [%s]", missing, missingHash)
	}

	badBody := encodeAgentChunkBatch(t, []byte("bad hash body"))
	badBody[len(agentChunkBatchMagic)+4] ^= 0xff
	status, body = postRawAgent(t, server.URL+"/agent/v1/chunks/upload", createdAgent.Agent.ClientID, createdAgent.Agent.ClientSecret, agentChunkBatchContentType, badBody)
	if status != http.StatusBadRequest {
		t.Fatalf("bad hash batch status = %d body=%s", status, string(body))
	}
}

func TestAgentBatchChunkUploadRejectsLimits(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.StateDir = filepath.Join(root, "state")
	cfg.Paths.RepoDir = filepath.Join(root, "repo")
	cfg.Paths.RestoreRoots = []string{filepath.Join(root, "restore")}
	cfg.Agent.MaxChunkCheckBatch = 1
	cfg.Agent.MaxChunkUploadBatchBytes = "64B"
	store, err := state.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("state.Open() error = %v", err)
	}
	defer store.Close()
	repo, err := repository.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("repository.Open() error = %v", err)
	}
	defer repo.Close()

	server := httptest.NewServer(New(cfg, store, repo, nil).Handler())
	defer server.Close()
	cookie := login(t, server.URL)
	status, body := postJSONAuthed(t, server.URL+"/api/v1/hosts", cookie, map[string]any{
		"name":        "batch-limit-agent",
		"source_type": "agent",
	})
	if status != http.StatusCreated {
		t.Fatalf("create agent host status = %d body=%s", status, string(body))
	}
	var createdAgent struct {
		Agent struct {
			ClientID     string `json:"client_id"`
			ClientSecret string `json:"client_secret"`
		} `json:"agent"`
	}
	if err := json.Unmarshal(body, &createdAgent); err != nil {
		t.Fatal(err)
	}
	status, body = requestJSONAgent(t, http.MethodPost, server.URL+"/agent/v1/runs", createdAgent.Agent.ClientID, createdAgent.Agent.ClientSecret, map[string]any{
		"hostname": "batch-limit-agent",
		"root":     "/batch/limit/source",
	})
	if status != http.StatusCreated {
		t.Fatalf("create agent run status = %d body=%s", status, string(body))
	}

	status, body = postRawAgent(t, server.URL+"/agent/v1/chunks/upload", createdAgent.Agent.ClientID, createdAgent.Agent.ClientSecret, agentChunkBatchContentType, encodeAgentChunkBatch(t, []byte("a"), []byte("b")))
	if status != http.StatusBadRequest {
		t.Fatalf("oversized count batch status = %d body=%s", status, string(body))
	}
	status, body = postRawAgent(t, server.URL+"/agent/v1/chunks/upload", createdAgent.Agent.ClientID, createdAgent.Agent.ClientSecret, agentChunkBatchContentType, encodeAgentChunkBatch(t, bytes.Repeat([]byte("x"), 100)))
	if status != http.StatusRequestEntityTooLarge && status != http.StatusBadRequest {
		t.Fatalf("oversized byte batch status = %d body=%s", status, string(body))
	}
}

func TestPackedManifestCanonicalizeRestoreVerifyAndCompact(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.StateDir = filepath.Join(root, "state")
	cfg.Paths.RepoDir = filepath.Join(root, "repo")
	cfg.Paths.RestoreRoots = []string{filepath.Join(root, "restore")}
	store, err := state.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("state.Open() error = %v", err)
	}
	defer store.Close()
	repo, err := repository.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("repository.Open() error = %v", err)
	}
	defer repo.Close()
	api := New(cfg, store, repo, nil)

	packData, indexes, err := repository.EncodePack([]repository.PackFilePayload{
		{Path: "a.txt", Mode: 0o644, ModTime: time.Unix(1710000000, 0), Data: []byte("alpha")},
		{Path: "dir/b.txt", Mode: 0o600, ModTime: time.Unix(1710000010, 0), Data: []byte("bravo-data")},
	})
	if err != nil {
		t.Fatal(err)
	}
	packRef, _, err := repo.PutChunk(context.Background(), packData)
	if err != nil {
		t.Fatal(err)
	}
	regularRef, _, err := repo.PutChunk(context.Background(), []byte("regular-data"))
	if err != nil {
		t.Fatal(err)
	}
	manifest := &repository.SnapshotManifest{
		ID:         "packed-snapshot",
		CreatedAt:  time.Now().UTC(),
		SourceType: "agent",
		Packs: []repository.PackManifest{{
			ID:     "pack-1",
			Format: repository.PackFormatTBKPack1,
			Chunks: []repository.ChunkRef{{Hash: packRef.Hash, OriginalSize: packRef.OriginalSize}},
		}},
		Entries: []repository.FileEntry{
			{
				Path:    ".",
				Type:    repository.EntryTypeDir,
				Mode:    0o755,
				ModTime: time.Unix(1710000000, 0),
			},
			{
				Path:    "dir",
				Type:    repository.EntryTypeDir,
				Mode:    0o755,
				ModTime: time.Unix(1710000000, 0),
			},
			{
				Path:    "a.txt",
				Type:    repository.EntryTypePackedFile,
				Size:    int64(len("alpha")),
				Mode:    0o644,
				ModTime: time.Unix(1710000000, 0),
				Pack:    &repository.PackFileRef{ID: "pack-1", Offset: indexes[0].Offset, Length: indexes[0].Length},
			},
			{
				Path:    "dir/b.txt",
				Type:    repository.EntryTypePackedFile,
				Size:    int64(len("bravo-data")),
				Mode:    0o600,
				ModTime: time.Unix(1710000010, 0),
				Pack:    &repository.PackFileRef{ID: "pack-1", Offset: indexes[1].Offset, Length: indexes[1].Length},
			},
			{
				Path:    "regular.txt",
				Type:    repository.EntryTypeFile,
				Size:    int64(len("regular-data")),
				Mode:    0o644,
				ModTime: time.Unix(1710000020, 0),
				Chunks:  []repository.ChunkRef{{Hash: regularRef.Hash, OriginalSize: regularRef.OriginalSize}},
			},
			{
				Path:    "empty.txt",
				Type:    repository.EntryTypeFile,
				Size:    0,
				Mode:    0o644,
				ModTime: time.Unix(1710000030, 0),
			},
			{
				Path:       "link",
				Type:       repository.EntryTypeSymlink,
				Mode:       0o777,
				ModTime:    time.Unix(1710000040, 0),
				LinkTarget: "a.txt",
			},
		},
	}
	if err := api.canonicalizeManifestChunks(context.Background(), manifest); err != nil {
		t.Fatalf("canonicalize packed manifest: %v", err)
	}
	if manifest.Packs[0].Chunks[0].SegmentID == 0 || manifest.Packs[0].Chunks[0].Length == 0 {
		t.Fatalf("pack chunk was not canonicalized: %+v", manifest.Packs[0].Chunks[0])
	}
	var file bytes.Buffer
	if err := api.writeFileEntry(context.Background(), &file, manifest, manifest.Entries[3]); err != nil {
		t.Fatalf("write packed file: %v", err)
	}
	if file.String() != "bravo-data" {
		t.Fatalf("packed file bytes = %q", file.String())
	}
	restoreRoot := filepath.Join(root, "restore")
	restoreTarget := filepath.Join(restoreRoot, "snapshot")
	if err := api.restoreEntry(context.Background(), manifest, manifest.Entries[0], ".", restoreTarget, restoreRoot); err != nil {
		t.Fatalf("restore packed directory: %v", err)
	}
	for path, want := range map[string]string{
		"a.txt":       "alpha",
		"dir/b.txt":   "bravo-data",
		"regular.txt": "regular-data",
		"empty.txt":   "",
	} {
		data, err := os.ReadFile(filepath.Join(restoreTarget, filepath.FromSlash(path)))
		if err != nil {
			t.Fatalf("read restored %s: %v", path, err)
		}
		if string(data) != want {
			t.Fatalf("restored %s = %q, want %q", path, string(data), want)
		}
	}
	linkTarget, err := os.Readlink(filepath.Join(restoreTarget, "link"))
	if err != nil {
		t.Fatalf("read restored symlink: %v", err)
	}
	if linkTarget != "a.txt" {
		t.Fatalf("restored symlink target = %q", linkTarget)
	}
	if err := repo.WriteManifest(manifest); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.CreateSnapshot(context.Background(), state.CreateSnapshotInput{
		SourceType:  "agent",
		ManifestRef: manifest.ID,
		FileCount:   4,
		TotalSize:   int64(len("alpha") + len("bravo-data") + len("regular-data")),
	})
	if err != nil {
		t.Fatal(err)
	}
	verify := api.verifyReferencedChunks(context.Background(), []state.Snapshot{snapshot})
	if verify.MissingIndex != 0 || verify.CorruptChunks != 0 || len(verify.Errors) != 0 || verify.VerifiedChunks == 0 {
		t.Fatalf("verify packed snapshot = %+v", verify)
	}
	compact, err := api.compactRepository(context.Background(), []state.Snapshot{snapshot})
	if err != nil {
		t.Fatalf("compact packed snapshot: %v", err)
	}
	if compact.RewrittenChunks == 0 {
		t.Fatalf("compact report = %+v, want rewritten pack chunk", compact)
	}
	compactedManifest, err := repo.ReadManifest(manifest.ID)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := repo.ReadPackedFile(context.Background(), compactedManifest, compactedManifest.Entries[2])
	if err != nil {
		t.Fatalf("read compacted packed file: %v", err)
	}
	if string(restored) != "alpha" {
		t.Fatalf("compacted packed file bytes = %q", string(restored))
	}
}

func TestPackedManifestCanonicalizeRejectsInvalidRange(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.StateDir = filepath.Join(root, "state")
	cfg.Paths.RepoDir = filepath.Join(root, "repo")
	cfg.Paths.RestoreRoots = []string{filepath.Join(root, "restore")}
	store, err := state.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("state.Open() error = %v", err)
	}
	defer store.Close()
	repo, err := repository.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("repository.Open() error = %v", err)
	}
	defer repo.Close()
	api := New(cfg, store, repo, nil)

	packData, indexes, err := repository.EncodePack([]repository.PackFilePayload{
		{Path: "a.txt", Mode: 0o644, ModTime: time.Now(), Data: []byte("alpha")},
	})
	if err != nil {
		t.Fatal(err)
	}
	packRef, _, err := repo.PutChunk(context.Background(), packData)
	if err != nil {
		t.Fatal(err)
	}
	manifest := &repository.SnapshotManifest{
		ID:         "bad-packed-snapshot",
		CreatedAt:  time.Now().UTC(),
		SourceType: "agent",
		Packs: []repository.PackManifest{{
			ID:     "pack-1",
			Format: repository.PackFormatTBKPack1,
			Chunks: []repository.ChunkRef{{Hash: packRef.Hash, OriginalSize: packRef.OriginalSize}},
		}},
		Entries: []repository.FileEntry{{
			Path: "a.txt",
			Type: repository.EntryTypePackedFile,
			Size: int64(len("alpha")),
			Pack: &repository.PackFileRef{ID: "pack-1", Offset: indexes[0].Offset, Length: indexes[0].Length + 1},
		}},
	}
	err = api.canonicalizeManifestChunks(context.Background(), manifest)
	if err == nil || !strings.Contains(err.Error(), "range does not match") {
		t.Fatalf("canonicalize invalid packed range error = %v", err)
	}
}

func TestAgentRunAcceptsMultipleRoots(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.StateDir = filepath.Join(root, "state")
	cfg.Paths.RepoDir = filepath.Join(root, "repo")
	cfg.Paths.RestoreRoots = []string{filepath.Join(root, "restore")}
	store, err := state.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("state.Open() error = %v", err)
	}
	defer store.Close()
	repo, err := repository.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("repository.Open() error = %v", err)
	}
	defer repo.Close()

	server := httptest.NewServer(New(cfg, store, repo, nil).Handler())
	defer server.Close()
	cookie := login(t, server.URL)
	status, body := postJSONAuthed(t, server.URL+"/api/v1/hosts", cookie, map[string]any{
		"name":        "multi-root-agent",
		"source_type": "agent",
	})
	if status != http.StatusCreated {
		t.Fatalf("create agent host status = %d body=%s", status, string(body))
	}
	var createdAgent struct {
		Agent struct {
			ClientID     string `json:"client_id"`
			ClientSecret string `json:"client_secret"`
		} `json:"agent"`
	}
	if err := json.Unmarshal(body, &createdAgent); err != nil {
		t.Fatal(err)
	}

	status, body = requestJSONAgent(t, http.MethodPost, server.URL+"/agent/v1/runs", createdAgent.Agent.ClientID, createdAgent.Agent.ClientSecret, map[string]any{
		"hostname": "multi-root-agent",
		"roots":    []string{"/data", "/data/app"},
	})
	if status != http.StatusBadRequest {
		t.Fatalf("nested roots status = %d body=%s", status, string(body))
	}

	roots := []string{"/data/app", "/var/log/myapp"}
	status, body = requestJSONAgent(t, http.MethodPost, server.URL+"/agent/v1/runs", createdAgent.Agent.ClientID, createdAgent.Agent.ClientSecret, map[string]any{
		"hostname": "multi-root-agent",
		"roots":    roots,
	})
	if status != http.StatusCreated {
		t.Fatalf("create multi-root agent run status = %d body=%s", status, string(body))
	}
	var createdRun struct {
		Job state.Job `json:"job"`
		Run state.Run `json:"run"`
	}
	if err := json.Unmarshal(body, &createdRun); err != nil {
		t.Fatal(err)
	}
	var sourceConfig struct {
		Roots []string `json:"roots"`
	}
	if err := json.Unmarshal([]byte(createdRun.Job.SourceConfig), &sourceConfig); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sourceConfig.Roots, roots) {
		t.Fatalf("source_config.roots = %#v, want %#v", sourceConfig.Roots, roots)
	}

	manifest := repository.SnapshotManifest{
		SourceType:  "agent",
		SourceRoots: roots,
		Entries: []repository.FileEntry{
			{Path: "data/app", Type: repository.EntryTypeDir, Mode: uint32(os.ModeDir | 0o755)},
			{Path: "var/log/myapp", Type: repository.EntryTypeDir, Mode: uint32(os.ModeDir | 0o755)},
		},
	}
	status, body = requestJSONAgent(t, http.MethodPost, server.URL+"/agent/v1/manifests", createdAgent.Agent.ClientID, createdAgent.Agent.ClientSecret, map[string]any{
		"run_id":   createdRun.Run.ID,
		"manifest": manifest,
	})
	if status != http.StatusCreated {
		t.Fatalf("post multi-root manifest status = %d body=%s", status, string(body))
	}
	var postedManifest struct {
		Snapshot state.Snapshot `json:"snapshot"`
	}
	if err := json.Unmarshal(body, &postedManifest); err != nil {
		t.Fatal(err)
	}
	loaded, err := repo.ReadManifest("run-" + strconv.FormatInt(createdRun.Run.ID, 10))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.SourceRoot != "" {
		t.Fatalf("loaded SourceRoot = %q, want empty", loaded.SourceRoot)
	}
	if !reflect.DeepEqual(loaded.SourceRoots, roots) {
		t.Fatalf("loaded SourceRoots = %#v, want %#v", loaded.SourceRoots, roots)
	}

	status, body = getAuthed(t, server.URL+"/api/v1/snapshots/"+strconv.FormatInt(postedManifest.Snapshot.ID, 10)+"/tree?path=.", cookie)
	if status != http.StatusOK {
		t.Fatalf("snapshot tree status = %d body=%s", status, string(body))
	}
	var tree struct {
		Manifest struct {
			SourceRoots []string `json:"source_roots"`
		} `json:"manifest"`
	}
	if err := json.Unmarshal(body, &tree); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(tree.Manifest.SourceRoots, roots) {
		t.Fatalf("tree source_roots = %#v, want %#v", tree.Manifest.SourceRoots, roots)
	}
}

func TestAgentHeartbeatReturnsRepositoryStateAndCommands(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.StateDir = filepath.Join(root, "state")
	cfg.Paths.RepoDir = filepath.Join(root, "repo")
	cfg.Paths.RestoreRoots = []string{filepath.Join(root, "restore")}
	store, err := state.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("state.Open() error = %v", err)
	}
	defer store.Close()
	repo, err := repository.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("repository.Open() error = %v", err)
	}
	defer repo.Close()

	server := httptest.NewServer(New(cfg, store, repo, nil).Handler())
	defer server.Close()
	cookie := login(t, server.URL)

	status, body := postJSONAuthed(t, server.URL+"/api/v1/hosts", cookie, map[string]any{
		"name":        "daemon-host",
		"source_type": "agent",
	})
	if status != http.StatusCreated {
		t.Fatalf("create agent host status = %d body=%s", status, string(body))
	}
	var createdAgent struct {
		Host       state.Host       `json:"host"`
		Credential state.Credential `json:"credential"`
		Agent      struct {
			ClientID     string `json:"client_id"`
			ClientSecret string `json:"client_secret"`
		} `json:"agent"`
	}
	if err := json.Unmarshal(body, &createdAgent); err != nil {
		t.Fatal(err)
	}
	job, _, err := store.FindOrCreateAgentJob(context.Background(), state.AgentJobInput{
		HostID:       createdAgent.Host.ID,
		CredentialID: createdAgent.Credential.ID,
		Name:         "Agent backup - daemon-host",
		SourceConfig: json.RawMessage(`{"root":"/backup/source"}`),
		Timezone:     "Asia/Shanghai",
	})
	if err != nil {
		t.Fatal(err)
	}
	status, body = postJSONAuthed(t, server.URL+"/api/v1/jobs/"+strconv.FormatInt(job.ID, 10)+"/run", cookie, nil)
	if status != http.StatusAccepted {
		t.Fatalf("queue agent job status = %d body=%s", status, string(body))
	}

	status, body = requestJSONAgent(t, http.MethodPost, server.URL+"/agent/v1/heartbeat", createdAgent.Agent.ClientID, createdAgent.Agent.ClientSecret, map[string]any{
		"hostname":       "daemon-hostname",
		"version":        "test",
		"mode":           "daemon",
		"state_dir":      "/var/lib/turbk-agent",
		"catalog_status": "ok",
	})
	if status != http.StatusAccepted {
		t.Fatalf("agent heartbeat status = %d body=%s", status, string(body))
	}
	var heartbeat struct {
		Repository struct {
			ID              string `json:"id"`
			ChunkGeneration int64  `json:"chunk_generation"`
		} `json:"repository"`
		Agent struct {
			PollIntervalSeconds        int64 `json:"poll_interval_seconds"`
			CommandGeneration          int64 `json:"command_generation"`
			MaxChunkUploadBatchBytes   int64 `json:"max_chunk_upload_batch_bytes"`
			MaxChunkResponseBytes      int64 `json:"max_chunk_response_bytes"`
			ChunkBatchUpload           bool  `json:"chunk_batch_upload"`
			CompactChunkCheckResponse  bool  `json:"compact_chunk_check_response"`
			CompactChunkUploadResponse bool  `json:"compact_chunk_upload_response"`
			SmallFilePack              bool  `json:"small_file_pack"`
			SmallFilePackEnabled       bool  `json:"small_file_pack_enabled"`
			SmallFilePackMaxFileSize   int64 `json:"small_file_pack_max_file_size"`
			SmallFilePackTargetSize    int64 `json:"small_file_pack_target_size"`
		} `json:"agent"`
		Commands []struct {
			ID     int64  `json:"id"`
			Type   string `json:"type"`
			JobID  int64  `json:"job_id"`
			Status string `json:"status"`
		} `json:"commands"`
	}
	if err := json.Unmarshal(body, &heartbeat); err != nil {
		t.Fatal(err)
	}
	if heartbeat.Repository.ID == "" || heartbeat.Agent.PollIntervalSeconds != 600 {
		t.Fatalf("heartbeat missing repository/poll state: %+v", heartbeat)
	}
	if !heartbeat.Agent.ChunkBatchUpload || heartbeat.Agent.MaxChunkUploadBatchBytes != 64*1024*1024 || heartbeat.Agent.MaxChunkResponseBytes != 64*1024*1024 {
		t.Fatalf("heartbeat missing chunk batch upload capability: %+v", heartbeat.Agent)
	}
	if !heartbeat.Agent.CompactChunkCheckResponse || !heartbeat.Agent.CompactChunkUploadResponse {
		t.Fatalf("heartbeat missing compact chunk response capabilities: %+v", heartbeat.Agent)
	}
	if !heartbeat.Agent.SmallFilePack || heartbeat.Agent.SmallFilePackEnabled || heartbeat.Agent.SmallFilePackMaxFileSize != 64*1024 || heartbeat.Agent.SmallFilePackTargetSize != 8*1024*1024 {
		t.Fatalf("heartbeat missing small-file pack capabilities: %+v", heartbeat.Agent)
	}
	if len(heartbeat.Commands) != 1 || heartbeat.Commands[0].Type != "run-backup" || heartbeat.Commands[0].JobID != job.ID {
		t.Fatalf("unexpected heartbeat commands: %+v", heartbeat.Commands)
	}

	status, body = requestJSONAgent(t, http.MethodPost, server.URL+"/agent/v1/commands/"+strconv.FormatInt(heartbeat.Commands[0].ID, 10)+"/ack", createdAgent.Agent.ClientID, createdAgent.Agent.ClientSecret, map[string]any{
		"status": "dropped",
		"reason": "agent_busy",
	})
	if status != http.StatusAccepted {
		t.Fatalf("ack command status = %d body=%s", status, string(body))
	}
	updatedHost, err := store.GetHost(context.Background(), createdAgent.Host.ID)
	if err != nil {
		t.Fatal(err)
	}
	if updatedHost.AgentStatus == nil || updatedHost.AgentStatus.LastDroppedReason.String != "agent_busy" {
		t.Fatalf("agent dropped reason was not recorded: %+v", updatedHost.AgentStatus)
	}
}

func TestAgentSetupCreatesTriggerableJobAndCommandRootsDriveRun(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.StateDir = filepath.Join(root, "state")
	cfg.Paths.RepoDir = filepath.Join(root, "repo")
	cfg.Paths.RestoreRoots = []string{filepath.Join(root, "restore")}
	store, err := state.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("state.Open() error = %v", err)
	}
	defer store.Close()
	repo, err := repository.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("repository.Open() error = %v", err)
	}
	defer repo.Close()

	server := httptest.NewServer(New(cfg, store, repo, nil).Handler())
	defer server.Close()
	cookie := login(t, server.URL)

	status, body := postJSONAuthed(t, server.URL+"/api/v1/hosts", cookie, map[string]any{
		"name":        "daemon-trigger-host",
		"source_type": "agent",
	})
	if status != http.StatusCreated {
		t.Fatalf("create agent host status = %d body=%s", status, string(body))
	}
	var createdAgent struct {
		Host  state.Host `json:"host"`
		Agent struct {
			ClientID     string `json:"client_id"`
			ClientSecret string `json:"client_secret"`
		} `json:"agent"`
	}
	if err := json.Unmarshal(body, &createdAgent); err != nil {
		t.Fatal(err)
	}

	roots := []string{"/var/lib/docker", "/etc"}
	status, body = requestJSONAuthed(t, http.MethodPatch, server.URL+"/api/v1/hosts/"+strconv.FormatInt(createdAgent.Host.ID, 10), cookie, map[string]any{
		"agent_setup": map[string]any{
			"roots":           roots,
			"backup_schedule": "0 2 * * *",
		},
	})
	if status != http.StatusOK {
		t.Fatalf("patch agent setup status = %d body=%s", status, string(body))
	}
	jobs, err := store.ListJobs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var agentJob state.Job
	for _, job := range jobs {
		if job.SourceType == "agent" && job.HostID.Valid && job.HostID.Int64 == createdAgent.Host.ID {
			agentJob = job
			break
		}
	}
	if agentJob.ID == 0 {
		t.Fatalf("agent setup did not create a triggerable job: %+v", jobs)
	}
	var sourceConfig struct {
		Root  string   `json:"root"`
		Roots []string `json:"roots"`
	}
	if err := json.Unmarshal([]byte(agentJob.SourceConfig), &sourceConfig); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sourceConfig.Roots, roots) {
		t.Fatalf("agent job roots = %#v, want %#v", sourceConfig.Roots, roots)
	}

	status, body = postJSONAuthed(t, server.URL+"/api/v1/jobs/"+strconv.FormatInt(agentJob.ID, 10)+"/run", cookie, nil)
	if status != http.StatusAccepted {
		t.Fatalf("queue agent job status = %d body=%s", status, string(body))
	}
	type commandPayload struct {
		ID      int64           `json:"id"`
		JobID   int64           `json:"job_id"`
		Status  string          `json:"status"`
		Payload json.RawMessage `json:"payload"`
	}
	var queued struct {
		Command commandPayload `json:"command"`
	}
	if err := json.Unmarshal(body, &queued); err != nil {
		t.Fatal(err)
	}
	var queuedPayload struct {
		JobID int64    `json:"job_id"`
		Roots []string `json:"roots"`
	}
	if err := json.Unmarshal(queued.Command.Payload, &queuedPayload); err != nil {
		t.Fatal(err)
	}
	if queuedPayload.JobID != agentJob.ID || !reflect.DeepEqual(queuedPayload.Roots, roots) {
		t.Fatalf("queued command payload = %+v, want job=%d roots=%#v", queuedPayload, agentJob.ID, roots)
	}

	status, body = requestJSONAgent(t, http.MethodPost, server.URL+"/agent/v1/heartbeat", createdAgent.Agent.ClientID, createdAgent.Agent.ClientSecret, map[string]any{
		"hostname":       "daemon-trigger-hostname",
		"version":        "test",
		"mode":           "daemon",
		"state_dir":      "/var/lib/turbk-agent",
		"catalog_status": "ok",
	})
	if status != http.StatusAccepted {
		t.Fatalf("agent heartbeat status = %d body=%s", status, string(body))
	}
	var heartbeat struct {
		Commands []commandPayload `json:"commands"`
	}
	if err := json.Unmarshal(body, &heartbeat); err != nil {
		t.Fatal(err)
	}
	if len(heartbeat.Commands) != 1 || heartbeat.Commands[0].ID != queued.Command.ID {
		t.Fatalf("heartbeat commands = %+v, want command %d", heartbeat.Commands, queued.Command.ID)
	}
	var heartbeatPayload struct {
		Roots []string `json:"roots"`
	}
	if err := json.Unmarshal(heartbeat.Commands[0].Payload, &heartbeatPayload); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(heartbeatPayload.Roots, roots) {
		t.Fatalf("heartbeat command roots = %#v, want %#v", heartbeatPayload.Roots, roots)
	}

	status, body = requestJSONAgent(t, http.MethodPost, server.URL+"/agent/v1/runs", createdAgent.Agent.ClientID, createdAgent.Agent.ClientSecret, map[string]any{
		"hostname":   "daemon-trigger-hostname",
		"command_id": queued.Command.ID,
		"roots":      heartbeatPayload.Roots,
		"trigger":    "manual",
	})
	if status != http.StatusCreated {
		t.Fatalf("agent command create run status = %d body=%s", status, string(body))
	}
	var createdRun struct {
		Run state.Run `json:"run"`
		Job state.Job `json:"job"`
	}
	if err := json.Unmarshal(body, &createdRun); err != nil {
		t.Fatal(err)
	}
	if createdRun.Run.ID == 0 || !createdRun.Run.JobID.Valid || createdRun.Run.JobID.Int64 != agentJob.ID || createdRun.Job.ID != agentJob.ID {
		t.Fatalf("created run/job = %+v / %+v, want job %d", createdRun.Run, createdRun.Job, agentJob.ID)
	}
	command, err := store.GetAgentCommand(context.Background(), queued.Command.ID)
	if err != nil {
		t.Fatal(err)
	}
	if command.Status != "running" || !command.RunID.Valid || command.RunID.Int64 != createdRun.Run.ID {
		t.Fatalf("command after create run = %+v, want running run %d", command, createdRun.Run.ID)
	}
}

func TestAgentManifestMissingChunksIsRetryable(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.StateDir = filepath.Join(root, "state")
	cfg.Paths.RepoDir = filepath.Join(root, "repo")
	cfg.Paths.RestoreRoots = []string{filepath.Join(root, "restore")}
	store, err := state.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("state.Open() error = %v", err)
	}
	defer store.Close()
	repo, err := repository.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("repository.Open() error = %v", err)
	}
	defer repo.Close()

	server := httptest.NewServer(New(cfg, store, repo, nil).Handler())
	defer server.Close()
	cookie := login(t, server.URL)
	status, body := postJSONAuthed(t, server.URL+"/api/v1/hosts", cookie, map[string]any{
		"name":        "repair-host",
		"source_type": "agent",
	})
	if status != http.StatusCreated {
		t.Fatalf("create agent host status = %d body=%s", status, string(body))
	}
	var createdAgent struct {
		Agent struct {
			ClientID     string `json:"client_id"`
			ClientSecret string `json:"client_secret"`
		} `json:"agent"`
	}
	if err := json.Unmarshal(body, &createdAgent); err != nil {
		t.Fatal(err)
	}
	status, body = requestJSONAgent(t, http.MethodPost, server.URL+"/agent/v1/runs", createdAgent.Agent.ClientID, createdAgent.Agent.ClientSecret, map[string]any{
		"hostname": "repair-host",
		"root":     "/backup/source",
	})
	if status != http.StatusCreated {
		t.Fatalf("create agent run status = %d body=%s", status, string(body))
	}
	var createdRun struct {
		Run state.Run `json:"run"`
	}
	if err := json.Unmarshal(body, &createdRun); err != nil {
		t.Fatal(err)
	}

	missingHash := strings.Repeat("a", 64)
	manifest := repository.SnapshotManifest{
		SourceType: "agent",
		SourceRoot: "/backup/source",
		Entries: []repository.FileEntry{
			{Path: ".", Type: repository.EntryTypeDir, Mode: uint32(os.ModeDir | 0o755)},
			{Path: "data.txt", Type: repository.EntryTypeFile, Size: 10, Mode: 0o644, Chunks: []repository.ChunkRef{{Hash: missingHash, OriginalSize: 10}}},
		},
	}
	status, body = requestJSONAgent(t, http.MethodPost, server.URL+"/agent/v1/manifests", createdAgent.Agent.ClientID, createdAgent.Agent.ClientSecret, map[string]any{
		"run_id":   createdRun.Run.ID,
		"manifest": manifest,
	})
	if status != http.StatusConflict {
		t.Fatalf("missing manifest status = %d body=%s", status, string(body))
	}
	var missingResp struct {
		Status        string `json:"status"`
		Retryable     bool   `json:"retryable"`
		MissingChunks []struct {
			Hash  string   `json:"hash"`
			Paths []string `json:"paths"`
		} `json:"missing_chunks"`
	}
	if err := json.Unmarshal(body, &missingResp); err != nil {
		t.Fatal(err)
	}
	if missingResp.Status != "missing_chunks" || !missingResp.Retryable || len(missingResp.MissingChunks) != 1 || missingResp.MissingChunks[0].Hash != missingHash {
		t.Fatalf("unexpected missing chunks response: %+v", missingResp)
	}
	run, err := store.GetRun(context.Background(), createdRun.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "running" {
		t.Fatalf("run status after missing chunks = %q, want running", run.Status)
	}
	status, body = requestJSONAgent(t, http.MethodPost, server.URL+"/agent/v1/runs/"+strconv.FormatInt(createdRun.Run.ID, 10)+"/finish", createdAgent.Agent.ClientID, createdAgent.Agent.ClientSecret, map[string]any{
		"status": "failed",
		"error":  "scan failed",
	})
	if status != http.StatusOK {
		t.Fatalf("fail run status = %d body=%s", status, string(body))
	}
	run, err = store.GetRun(context.Background(), createdRun.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "failed" || run.ErrorMessage.String != "scan failed" {
		t.Fatalf("run after fail = %+v, want failed scan failed", run)
	}
}

func postJSON(t *testing.T, url string, value any) (int, []byte) {
	t.Helper()
	return requestJSON(t, http.MethodPost, url, "", value)
}

func postJSONAuthed(t *testing.T, url string, cookie *http.Cookie, value any) (int, []byte) {
	t.Helper()
	return requestJSONAuthed(t, http.MethodPost, url, cookie, value)
}

func getAuthed(t *testing.T, url string, cookie *http.Cookie) (int, []byte) {
	t.Helper()
	return requestJSONAuthed(t, http.MethodGet, url, cookie, nil)
}

func requestJSONAuthed(t *testing.T, method, url string, cookie *http.Cookie, value any) (int, []byte) {
	t.Helper()
	return requestJSONWithCookie(t, method, url, "", cookie, value)
}

func requestJSON(t *testing.T, method, url, token string, value any) (int, []byte) {
	t.Helper()
	return requestJSONWithCookie(t, method, url, token, nil, value)
}

func requestJSONAgent(t *testing.T, method, url, clientID, clientSecret string, value any) (int, []byte) {
	t.Helper()
	var body []byte
	var err error
	if value != nil {
		body, err = json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", "application/json")
	if value != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.SetBasicAuth(clientID, clientSecret)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, respBody
}

func requestJSONWithCookie(t *testing.T, method, url, token string, cookie *http.Cookie, value any) (int, []byte) {
	t.Helper()
	var body []byte
	var err error
	if value != nil {
		body, err = json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", "application/json")
	if value != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if cookie != nil {
		req.AddCookie(cookie)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, respBody
}

func login(t *testing.T, baseURL string) *http.Cookie {
	t.Helper()
	return loginWith(t, baseURL, "admin", "admin")
}

func loginWith(t *testing.T, baseURL, username, password string) *http.Cookie {
	t.Helper()
	body, err := json.Marshal(map[string]string{
		"username": username,
		"password": password,
	})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, baseURL+"/api/v1/auth/login", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("login status = %d body=%s", resp.StatusCode, string(respBody))
	}
	for _, cookie := range resp.Cookies() {
		if cookie.Name == sessionCookieName {
			return cookie
		}
	}
	t.Fatalf("login response did not set %s cookie: %s", sessionCookieName, string(respBody))
	return nil
}

func putRawAgent(t *testing.T, url, clientID, clientSecret string, body []byte) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/octet-stream")
	req.SetBasicAuth(clientID, clientSecret)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, respBody
}

func postRawAgent(t *testing.T, url, clientID, clientSecret, contentType string, body []byte) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", "application/json")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.SetBasicAuth(clientID, clientSecret)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, respBody
}

func encodeAgentChunkBatch(t *testing.T, chunks ...[]byte) []byte {
	t.Helper()
	var body bytes.Buffer
	body.Write(agentChunkBatchMagic)
	var count [4]byte
	binary.BigEndian.PutUint32(count[:], uint32(len(chunks)))
	body.Write(count[:])
	for _, chunk := range chunks {
		sum := blake3.Sum256(chunk)
		body.Write(sum[:])
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(chunk)))
		body.Write(length[:])
		body.Write(chunk)
	}
	return body.Bytes()
}

func waitFor(t *testing.T, timeout time.Duration, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !check() {
		t.Fatal("condition was not met before timeout")
	}
}

func readTarGz(t *testing.T, data []byte) map[string][]byte {
	t.Helper()
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	files := make(map[string][]byte)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if header.Typeflag != tar.TypeReg {
			continue
		}
		body, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		files[header.Name] = body
	}
	return files
}

func mapsKeys(values map[string][]byte) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}

func nullStringValue(value sql.NullString) string {
	if !value.Valid {
		return ""
	}
	return value.String
}

type webDAVTestResource struct {
	href       string
	collection bool
	size       int64
	modified   time.Time
}

func webDAVMultiStatusXML(resources ...webDAVTestResource) string {
	out := `<?xml version="1.0" encoding="utf-8"?><D:multistatus xmlns:D="DAV:">`
	for _, resource := range resources {
		out += `<D:response><D:href>` + resource.href + `</D:href><D:propstat><D:prop>`
		if resource.collection {
			out += `<D:resourcetype><D:collection/></D:resourcetype>`
		} else {
			out += `<D:resourcetype/><D:getcontentlength>` + strconv.FormatInt(resource.size, 10) + `</D:getcontentlength>`
		}
		out += `<D:getlastmodified>` + resource.modified.Format(http.TimeFormat) + `</D:getlastmodified>`
		out += `</D:prop></D:propstat></D:response>`
	}
	out += `</D:multistatus>`
	return out
}
