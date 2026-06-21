package state

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
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
	host, err := store.CreateHost(ctx, CreateHostInput{
		Name:       "local server",
		SourceType: "local",
	})
	if err != nil {
		t.Fatalf("CreateHost() error = %v", err)
	}
	job, err := store.CreateJob(ctx, CreateJobInput{
		Name:              "local source",
		HostID:            host.ID,
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
	payload := []byte(`{"username":"root","password":"super-secret"}`)
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

func TestAgentCredentialIndexedLookup(t *testing.T) {
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
	clientID := "agt_state_lookup"
	clientSecret := "ags_state_lookup"
	payload := []byte(`{"client_id":"agt_state_lookup","client_secret":"ags_state_lookup","secret_hash":"` + HashAgentSecret(clientID, clientSecret) + `","subject":"agent-host"}`)
	host, credential, err := store.CreateAgentHost(ctx, CreateAgentHostInput{
		Name:         "agent-host",
		Payload:      payload,
		ClientID:     clientID,
		ClientSecret: clientSecret,
		SecretHash:   HashAgentSecret(clientID, clientSecret),
		Subject:      "agent-host",
	})
	if err != nil {
		t.Fatalf("CreateAgentHost() error = %v", err)
	}
	if !host.CredentialID.Valid || host.CredentialID.Int64 != credential.ID || host.Agent == nil || host.Agent.ClientSecret != clientSecret {
		t.Fatalf("agent host did not expose bound credential: host=%+v credential=%+v", host, credential)
	}

	rows, err := store.db.QueryContext(ctx, `PRAGMA index_list(agent_credentials)`)
	if err != nil {
		t.Fatalf("inspect agent credential indexes: %v", err)
	}
	defer rows.Close()
	seenClientIDIndex := false
	for rows.Next() {
		var seq int
		var name string
		var unique int
		var origin string
		var partial int
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			t.Fatalf("scan agent credential index: %v", err)
		}
		if name == "idx_agent_credentials_client_id" {
			seenClientIDIndex = true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate agent credential indexes: %v", err)
	}
	if !seenClientIDIndex {
		t.Fatal("agent_credentials.client_id index is missing")
	}

	auth, err := store.FindAgentCredentialByClientSecret(ctx, clientID, clientSecret)
	if err != nil {
		t.Fatalf("FindAgentCredentialByClientSecret() error = %v", err)
	}
	if auth.HostID != host.ID || auth.Credential.ID != credential.ID || auth.Subject != "agent-host" {
		t.Fatalf("unexpected agent auth context: %+v", auth)
	}
	if _, err := store.FindAgentCredentialByClientSecret(ctx, clientID, "wrong"); !errors.Is(err, ErrAgentCredentialNotFound) {
		t.Fatalf("wrong secret error = %v, want ErrAgentCredentialNotFound", err)
	}
}

func TestAgentJobIsOneToOneWithHost(t *testing.T) {
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
	clientID := "agt_one_to_one"
	clientSecret := "ags_one_to_one"
	secretHash := HashAgentSecret(clientID, clientSecret)
	payload := []byte(`{"client_id":"agt_one_to_one","client_secret":"ags_one_to_one","secret_hash":"` + secretHash + `","subject":"agent-host"}`)
	host, credential, err := store.CreateAgentHost(ctx, CreateAgentHostInput{
		Name:         "agent-host",
		Payload:      payload,
		ClientID:     clientID,
		ClientSecret: clientSecret,
		SecretHash:   secretHash,
		Subject:      "agent-host",
	})
	if err != nil {
		t.Fatalf("CreateAgentHost() error = %v", err)
	}

	first, created, err := store.FindOrCreateAgentJob(ctx, AgentJobInput{
		HostID:       host.ID,
		CredentialID: credential.ID,
		Name:         "server generated one",
		SourceConfig: []byte(`{"root":"/srv/one"}`),
	})
	if err != nil {
		t.Fatalf("FindOrCreateAgentJob(first) error = %v", err)
	}
	if !created {
		t.Fatal("first agent job was not created")
	}
	second, created, err := store.FindOrCreateAgentJob(ctx, AgentJobInput{
		HostID:       host.ID,
		CredentialID: credential.ID,
		Name:         "server generated two",
		SourceConfig: []byte(`{"root":"/srv/two"}`),
	})
	if err != nil {
		t.Fatalf("FindOrCreateAgentJob(second) error = %v", err)
	}
	if created {
		t.Fatal("second agent job created a duplicate for the same host")
	}
	if second.ID != first.ID {
		t.Fatalf("second agent job ID = %d, want %d", second.ID, first.ID)
	}
	if second.Name != "server generated two" || second.SourceConfig != `{"root":"/srv/two"}` {
		t.Fatalf("agent job was not updated from server-owned fields: %+v", second)
	}

	jobs, err := store.ListJobs(ctx)
	if err != nil {
		t.Fatalf("ListJobs() error = %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("agent host has %d jobs, want 1: %+v", len(jobs), jobs)
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

func TestDeleteSnapshotSoftDeletesAndIsIdempotent(t *testing.T) {
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
	snapshot, err := store.CreateSnapshot(ctx, CreateSnapshotInput{
		SourceType:  "local",
		ManifestRef: "delete-test",
		FileCount:   1,
		TotalSize:   42,
	})
	if err != nil {
		t.Fatalf("CreateSnapshot() error = %v", err)
	}
	task, err := store.CreateRestoreTask(ctx, CreateRestoreTaskInput{
		SnapshotID: snapshot.ID,
		TargetPath: filepath.Join(cfg.Paths.RestoreRoots[0], "target"),
		Status:     "running",
	})
	if err != nil {
		t.Fatalf("CreateRestoreTask() error = %v", err)
	}
	if _, _, err := store.DeleteSnapshot(ctx, DeleteSnapshotInput{ID: snapshot.ID}); !errors.Is(err, ErrSnapshotInUse) {
		t.Fatalf("DeleteSnapshot() error = %v, want ErrSnapshotInUse", err)
	}
	if _, err := store.UpdateRestoreTaskStatus(ctx, task.ID, "completed"); err != nil {
		t.Fatalf("UpdateRestoreTaskStatus() error = %v", err)
	}
	deletedAt := time.Date(2026, 6, 21, 3, 0, 0, 0, time.UTC)
	deleted, changed, err := store.DeleteSnapshot(ctx, DeleteSnapshotInput{
		ID:        snapshot.ID,
		Reason:    "manual",
		DeletedBy: "admin",
		Now:       deletedAt,
	})
	if err != nil {
		t.Fatalf("DeleteSnapshot() error = %v", err)
	}
	if !changed || !deleted.DeletedAt.Valid || !deleted.DeletedAt.Time.Equal(deletedAt) || deleted.DeleteReason.String != "manual" || deleted.DeletedBy.String != "admin" {
		t.Fatalf("unexpected deleted snapshot: changed=%v snapshot=%+v", changed, deleted)
	}
	active, err := store.ListSnapshots(ctx)
	if err != nil {
		t.Fatalf("ListSnapshots() error = %v", err)
	}
	if len(active) != 0 {
		t.Fatalf("active snapshots after delete = %+v, want empty", active)
	}
	again, changed, err := store.DeleteSnapshot(ctx, DeleteSnapshotInput{ID: snapshot.ID})
	if err != nil {
		t.Fatalf("DeleteSnapshot(idempotent) error = %v", err)
	}
	if changed || !again.DeletedAt.Valid {
		t.Fatalf("idempotent delete changed=%v snapshot=%+v", changed, again)
	}
}

func TestMarkStaleRunsFailed(t *testing.T) {
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
	host, err := store.CreateHost(ctx, CreateHostInput{Name: "local", SourceType: "local"})
	if err != nil {
		t.Fatal(err)
	}
	job, err := store.CreateJob(ctx, CreateJobInput{
		Name:         "stale",
		HostID:       host.ID,
		SourceConfig: []byte(`{"root":"/tmp/source"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := store.CreateRun(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour)
	if _, err := store.db.ExecContext(ctx, `UPDATE runs SET created_at = ? WHERE id = ?`, old, run.ID); err != nil {
		t.Fatalf("backdate run: %v", err)
	}
	changed, err := store.MarkStaleRunsFailed(ctx, now.Add(-6*time.Hour), now.Add(-time.Hour), now)
	if err != nil {
		t.Fatalf("MarkStaleRunsFailed() error = %v", err)
	}
	if changed != 1 {
		t.Fatalf("stale runs changed = %d, want 1", changed)
	}
	loaded, err := store.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Status != "failed" || !loaded.FinishedAt.Valid || loaded.ErrorMessage.String != "stale run expired by maintenance" {
		t.Fatalf("unexpected stale run: %+v", loaded)
	}
}
