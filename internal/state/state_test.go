package state

import (
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/tursom/turbk/internal/config"
)

func TestOpenInitializesSQLiteAndDirs(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.StateDir = filepath.Join(root, "state")
	cfg.Paths.RepoDir = filepath.Join(root, "repo")
	cfg.Paths.RestoreRoots = []string{filepath.Join(root, "restore")}

	store, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.Ping(context.Background()); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}
	counts, err := store.Counts(context.Background())
	if err != nil {
		t.Fatalf("Counts() error = %v", err)
	}
	if counts.Hosts != 0 || counts.Jobs != 0 || counts.Runs != 0 || counts.Snapshots != 0 {
		t.Fatalf("expected empty new store, got %+v", counts)
	}
}

func TestJobRunSnapshotStateFlow(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.StateDir = filepath.Join(root, "state")
	cfg.Paths.RepoDir = filepath.Join(root, "repo")
	cfg.Paths.RestoreRoots = []string{filepath.Join(root, "restore")}

	store, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	job, err := store.CreateJob(ctx, CreateJobInput{
		Name:              "local source",
		SourceType:        "local",
		SourceConfig:      []byte(`{"root":"/tmp/source"}`),
		Enabled:           true,
		MaxRuntimeSeconds: 3600,
		RetryAttempts:     2,
	})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	if job.ID == 0 || job.SourceConfig == "" || job.MaxRuntimeSeconds != 3600 || job.RetryAttempts != 2 {
		t.Fatalf("unexpected job: %+v", job)
	}
	updatedJob, err := store.UpdateJob(ctx, UpdateJobInput{
		ID:                job.ID,
		Name:              "local source updated",
		SourceConfig:      []byte(`{"root":"/tmp/source"}`),
		Enabled:           true,
		MaxRuntimeSeconds: 7200,
		RetryAttempts:     3,
	})
	if err != nil {
		t.Fatalf("UpdateJob() error = %v", err)
	}
	if updatedJob.MaxRuntimeSeconds != 7200 || updatedJob.RetryAttempts != 3 {
		t.Fatalf("updated job lost runtime fields: %+v", updatedJob)
	}
	job = updatedJob

	run, err := store.CreateRun(ctx, job)
	if err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}
	if run.Status != "pending" {
		t.Fatalf("run status = %q, want pending", run.Status)
	}
	if _, err := store.CreateRun(ctx, job); err == nil {
		t.Fatal("CreateRun allowed a second active run")
	}

	now := time.Now().UTC()
	if err := store.MarkRunRunning(ctx, run.ID, now); err != nil {
		t.Fatalf("MarkRunRunning() error = %v", err)
	}
	if err := store.AppendRunLog(ctx, run.ID, "info", "started"); err != nil {
		t.Fatalf("AppendRunLog() error = %v", err)
	}
	if err := store.CompleteRun(ctx, run.ID, now.Add(time.Second)); err != nil {
		t.Fatalf("CompleteRun() error = %v", err)
	}
	completed, err := store.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRun() error = %v", err)
	}
	if completed.Status != "completed" || !completed.StartedAt.Valid || !completed.FinishedAt.Valid {
		t.Fatalf("completed run missing state: %+v", completed)
	}
	logs, err := store.ListRunLogs(ctx, run.ID)
	if err != nil {
		t.Fatalf("ListRunLogs() error = %v", err)
	}
	if len(logs) != 1 || logs[0].Message != "started" {
		t.Fatalf("unexpected logs: %+v", logs)
	}

	snapshot, err := store.CreateSnapshot(ctx, CreateSnapshotInput{
		JobID:       sql.NullInt64{Int64: job.ID, Valid: true},
		RunID:       sql.NullInt64{Int64: run.ID, Valid: true},
		SourceType:  "local",
		ManifestRef: "snap-1",
		FileCount:   3,
		TotalSize:   128,
	})
	if err != nil {
		t.Fatalf("CreateSnapshot() error = %v", err)
	}
	if snapshot.ManifestRef != "snap-1" || snapshot.FileCount != 3 || snapshot.TotalSize != 128 {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}
	counts, err := store.Counts(ctx)
	if err != nil {
		t.Fatalf("Counts() error = %v", err)
	}
	if counts.Jobs != 1 || counts.Runs != 1 || counts.Snapshots != 1 {
		t.Fatalf("unexpected counts: %+v", counts)
	}
}

func TestCredentialsAreEncryptedAtRest(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.StateDir = filepath.Join(root, "state")
	cfg.Paths.RepoDir = filepath.Join(root, "repo")
	cfg.Paths.RestoreRoots = []string{filepath.Join(root, "restore")}

	store, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	payload := []byte(`{"address":"127.0.0.1:22","username":"root","password":"super-secret"}`)
	credential, err := store.CreateCredential(ctx, CreateCredentialInput{
		Name:    "sftp root",
		Type:    "sftp",
		Payload: payload,
	})
	if err != nil {
		t.Fatalf("CreateCredential() error = %v", err)
	}
	if credential.ID == 0 || credential.Type != "sftp" {
		t.Fatalf("unexpected credential: %+v", credential)
	}
	credentials, err := store.ListCredentials(ctx)
	if err != nil {
		t.Fatalf("ListCredentials() error = %v", err)
	}
	if len(credentials) != 1 || credentials[0].Name != "sftp root" {
		t.Fatalf("unexpected credentials: %+v", credentials)
	}
	var raw []byte
	if err := store.db.QueryRowContext(ctx, `SELECT encrypted_payload FROM credentials WHERE id = ?`, credential.ID).Scan(&raw); err != nil {
		t.Fatalf("query encrypted payload: %v", err)
	}
	if bytes.Contains(raw, []byte("super-secret")) {
		t.Fatalf("encrypted payload contains plaintext secret: %s", string(raw))
	}
	loaded, decrypted, err := store.GetCredentialPayload(ctx, credential.ID)
	if err != nil {
		t.Fatalf("GetCredentialPayload() error = %v", err)
	}
	if loaded.ID != credential.ID || !bytes.Equal(decrypted, payload) {
		t.Fatalf("unexpected decrypted payload: credential=%+v payload=%s", loaded, string(decrypted))
	}
}

func TestRetentionPolicyExpiresOlderSnapshots(t *testing.T) {
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	snapshots := []Snapshot{
		{ID: 3, JobID: sql.NullInt64{Int64: 1, Valid: true}, CreatedAt: now},
		{ID: 2, JobID: sql.NullInt64{Int64: 1, Valid: true}, CreatedAt: now.Add(-time.Hour)},
		{ID: 1, JobID: sql.NullInt64{Int64: 1, Valid: true}, CreatedAt: now.Add(-2 * time.Hour)},
	}
	expired := ExpiredSnapshotIDs(snapshots, RetentionPolicy{
		KeepLast:   1,
		KeepDaily:  0,
		KeepWeekly: 0,
	})
	if len(expired) != 2 || expired[0] != 2 || expired[1] != 1 {
		t.Fatalf("expired IDs = %+v, want [2 1]", expired)
	}
}
