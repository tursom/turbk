package httpapi

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/turbk/turbk/internal/config"
	"github.com/turbk/turbk/internal/repository"
	"github.com/turbk/turbk/internal/state"
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

	createStatus, createBody := postJSONAuthed(t, server.URL+"/api/v1/jobs", cookie, map[string]any{
		"name":        "local job",
		"source_type": "local",
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

	job, err := store.CreateJob(context.Background(), state.CreateJobInput{
		Name:         "scheduled local",
		SourceType:   "local",
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
	job, err := store.CreateJob(context.Background(), state.CreateJobInput{
		Name:              "scheduled missing local",
		SourceType:        "local",
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
	createStatus, createBody := postJSONAuthed(t, server.URL+"/api/v1/jobs", cookie, map[string]any{
		"name":          "retention local",
		"source_type":   "local",
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
	job, err := store.CreateJob(context.Background(), state.CreateJobInput{
		Name:         "gate local",
		SourceType:   "local",
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
	createStatus, createBody := postJSONAuthed(t, server.URL+"/api/v1/jobs", cookie, map[string]any{
		"name":          "restore local",
		"source_type":   "local",
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

	status, body := postJSONAuthed(t, server.URL+"/api/v1/hosts", cookie, map[string]any{
		"name":        "sftp host",
		"source_type": "sftp",
		"address":     "10.0.0.10:22",
	})
	if status != http.StatusCreated {
		t.Fatalf("create host status = %d body=%s", status, string(body))
	}

	status, body = postJSONAuthed(t, server.URL+"/api/v1/hosts", cookie, map[string]any{
		"name":        "agent-subject",
		"source_type": "agent",
		"address":     "should-not-be-stored",
	})
	if status != http.StatusCreated {
		t.Fatalf("create agent host status = %d body=%s", status, string(body))
	}
	var createdAgentHost struct {
		Host state.Host `json:"host"`
	}
	if err := json.Unmarshal(body, &createdAgentHost); err != nil {
		t.Fatal(err)
	}
	if createdAgentHost.Host.Address.Valid {
		t.Fatalf("agent host should not store manual address: %+v", createdAgentHost.Host)
	}

	status, body = postJSONAuthed(t, server.URL+"/api/v1/credentials", cookie, map[string]any{
		"name": "agent dev",
		"type": "agent",
		"payload": map[string]any{
			"subject": "agent-subject",
		},
	})
	if status != http.StatusCreated {
		t.Fatalf("create agent credential status = %d body=%s", status, string(body))
	}
	var createdAgent struct {
		Credential   state.Credential `json:"credential"`
		ClientSecret string           `json:"client_secret"`
	}
	if err := json.Unmarshal(body, &createdAgent); err != nil {
		t.Fatal(err)
	}
	if createdAgent.Credential.ClientID == "" || createdAgent.ClientSecret == "" {
		t.Fatalf("agent credential missing client credentials: %s", string(body))
	}
	status, body = requestJSONAgent(t, http.MethodPost, server.URL+"/agent/v1/heartbeat", createdAgent.Credential.ClientID, createdAgent.ClientSecret, map[string]any{
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
	for _, host := range listed.Hosts {
		if host.Name == "sftp host" && host.SourceType == "sftp" && nullStringValue(host.Address) == "10.0.0.10:22" {
			seenManual = true
		}
		if host.Name == "agent-subject" && host.SourceType == "agent" && host.Status == "online" && nullStringValue(host.Address) == "agent-hostname" && host.LastSeenAt.Valid {
			seenAgent = true
		}
	}
	if !seenManual || !seenAgent {
		t.Fatalf("hosts did not include manual=%v agent=%v: %+v", seenManual, seenAgent, listed.Hosts)
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
			"address":  "127.0.0.1:22",
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
	status, body = postJSONAuthed(t, server.URL+"/api/v1/jobs", cookie, map[string]any{
		"name":          "pull sftp",
		"source_type":   "sftp",
		"credential_id": created.Credential.ID,
		"source_config": map[string]any{"root": "/srv"},
		"enabled":       true,
	})
	if status != http.StatusCreated {
		t.Fatalf("create pull job status = %d body=%s", status, string(body))
	}
	if bytes.Contains(body, []byte("super-secret")) {
		t.Fatalf("job response leaked secret: %s", string(body))
	}
	counts, err := store.Counts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if counts.Credentials != 1 || counts.Jobs != 1 {
		t.Fatalf("unexpected counts: %+v", counts)
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
			"base_url":     webdav.URL + "/dav",
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
	status, body = postJSONAuthed(t, server.URL+"/api/v1/jobs", cookie, map[string]any{
		"name":          "webdav pull",
		"source_type":   "webdav",
		"credential_id": createdCredential.Credential.ID,
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

	status, body = postJSONAuthed(t, server.URL+"/api/v1/credentials", cookie, map[string]any{
		"name": "agent dev",
		"type": "agent",
		"payload": map[string]any{
			"subject": "dev-host",
		},
	})
	if status != http.StatusCreated {
		t.Fatalf("create agent credential status = %d body=%s", status, string(body))
	}
	var createdAgent struct {
		Credential   state.Credential `json:"credential"`
		ClientSecret string           `json:"client_secret"`
	}
	if err := json.Unmarshal(body, &createdAgent); err != nil {
		t.Fatal(err)
	}
	if createdAgent.Credential.ClientID == "" || createdAgent.ClientSecret == "" {
		t.Fatalf("agent credential missing client credentials: %s", string(body))
	}

	status, body = getAuthed(t, server.URL+"/api/v1/credentials", cookie)
	if status != http.StatusOK {
		t.Fatalf("list credentials status = %d body=%s", status, string(body))
	}
	if !bytes.Contains(body, []byte(createdAgent.Credential.ClientID)) {
		t.Fatalf("credential list missing client id: %s", string(body))
	}
	if !bytes.Contains(body, []byte(createdAgent.ClientSecret)) {
		t.Fatalf("credential list missing client secret: %s", string(body))
	}

	status, body = requestJSON(t, http.MethodPost, server.URL+"/agent/v1/runs", "dev-token", map[string]any{
		"hostname": "agent-host",
		"root":     "/srv/data",
		"job_name": "agent smoke",
	})
	if status != http.StatusUnauthorized {
		t.Fatalf("legacy bearer agent run status = %d body=%s", status, string(body))
	}

	status, body = requestJSONAgent(t, http.MethodPost, server.URL+"/agent/v1/runs", createdAgent.Credential.ClientID, createdAgent.ClientSecret, map[string]any{
		"hostname": "agent-host",
		"root":     "/srv/data",
		"job_name": "agent smoke",
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
	if createdRun.Run.ID == 0 {
		t.Fatalf("created run missing ID: %+v", createdRun.Run)
	}
	status, body = requestJSONAgent(t, http.MethodPost, server.URL+"/agent/v1/runs/"+strconv.FormatInt(createdRun.Run.ID, 10)+"/progress", createdAgent.Credential.ClientID, createdAgent.ClientSecret, map[string]any{
		"phase":           "uploading",
		"processed_files": 1,
		"processed_bytes": 32,
		"uploaded_chunks": 1,
		"message":         "data.txt",
	})
	if status != http.StatusAccepted {
		t.Fatalf("agent progress status = %d body=%s", status, string(body))
	}
	status, body = requestJSONAgent(t, http.MethodPost, server.URL+"/agent/v1/runs", createdAgent.Credential.ClientID, createdAgent.ClientSecret, map[string]any{
		"hostname": "agent-host",
		"root":     "/srv/data",
		"job_name": "agent smoke",
	})
	if status != http.StatusOK {
		t.Fatalf("idempotent create agent run status = %d body=%s", status, string(body))
	}
	var resumedRun struct {
		Run state.Run `json:"run"`
	}
	if err := json.Unmarshal(body, &resumedRun); err != nil {
		t.Fatal(err)
	}
	if resumedRun.Run.ID != createdRun.Run.ID {
		t.Fatalf("resumed run ID = %d, want %d", resumedRun.Run.ID, createdRun.Run.ID)
	}

	data := bytes.Repeat([]byte("agent-push-data-"), 400)
	sum := blake3.Sum256(data)
	hash := hex.EncodeToString(sum[:])

	status, body = requestJSONAgent(t, http.MethodGet, server.URL+"/agent/v1/chunks/"+hash, createdAgent.Credential.ClientID, createdAgent.ClientSecret, nil)
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

	firstStatus, firstBody := putRawAgent(t, server.URL+"/agent/v1/chunks/"+hash, createdAgent.Credential.ClientID, createdAgent.ClientSecret, data)
	if firstStatus != http.StatusCreated {
		t.Fatalf("first chunk upload status = %d body=%s", firstStatus, string(firstBody))
	}
	statsAfterFirst, err := repo.Stats()
	if err != nil {
		t.Fatal(err)
	}
	secondStatus, secondBody := putRawAgent(t, server.URL+"/agent/v1/chunks/"+hash, createdAgent.Credential.ClientID, createdAgent.ClientSecret, data)
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
	status, body = requestJSONAgent(t, http.MethodPost, server.URL+"/agent/v1/manifests", createdAgent.Credential.ClientID, createdAgent.ClientSecret, map[string]any{
		"run_id":   createdRun.Run.ID,
		"manifest": manifest,
	})
	if status != http.StatusCreated {
		t.Fatalf("post agent manifest status = %d body=%s", status, string(body))
	}
	status, body = requestJSONAgent(t, http.MethodPost, server.URL+"/agent/v1/manifests", createdAgent.Credential.ClientID, createdAgent.ClientSecret, map[string]any{
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
