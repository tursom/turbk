package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/turbk/turbk/internal/config"
	_ "modernc.org/sqlite"
)

type Store struct {
	db        *sql.DB
	stateDir  string
	repoDir   string
	dbPath    string
	startedAt time.Time
}

func Open(ctx context.Context, cfg config.Config) (*Store, error) {
	if err := ensureRuntimeDirs(cfg); err != nil {
		return nil, err
	}
	dbPath := filepath.Join(cfg.Paths.StateDir, "turbk.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	store := &Store{
		db:        db,
		stateDir:  cfg.Paths.StateDir,
		repoDir:   cfg.Paths.RepoDir,
		dbPath:    dbPath,
		startedAt: time.Now().UTC(),
	}
	if err := store.init(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func ensureRuntimeDirs(cfg config.Config) error {
	dirs := []string{cfg.Paths.StateDir, cfg.Paths.RepoDir, filepath.Join(cfg.Paths.StateDir, "tmp")}
	dirs = append(dirs, cfg.Paths.RestoreRoots...)
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return fmt.Errorf("create runtime dir %q: %w", dir, err)
		}
	}
	return nil
}

func (s *Store) init(ctx context.Context) error {
	pragmas := []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA foreign_keys = ON",
		"PRAGMA busy_timeout = 5000",
	}
	for _, stmt := range pragmas {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("apply sqlite pragma %q: %w", stmt, err)
		}
	}
	for _, stmt := range schema {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("apply sqlite schema: %w", err)
		}
	}
	if err := s.ensureJobRuntimeColumns(ctx); err != nil {
		return err
	}
	return nil
}

func (s *Store) ensureJobRuntimeColumns(ctx context.Context) error {
	columns := []struct {
		name string
		stmt string
	}{
		{"max_runtime_seconds", `ALTER TABLE jobs ADD COLUMN max_runtime_seconds INTEGER NOT NULL DEFAULT 0`},
		{"retry_attempts", `ALTER TABLE jobs ADD COLUMN retry_attempts INTEGER NOT NULL DEFAULT 0`},
	}
	for _, column := range columns {
		exists, err := s.columnExists(ctx, "jobs", column.name)
		if err != nil {
			return err
		}
		if exists {
			continue
		}
		if _, err := s.db.ExecContext(ctx, column.stmt); err != nil {
			return fmt.Errorf("add jobs.%s column: %w", column.name, err)
		}
	}
	return nil
}

func (s *Store) columnExists(ctx context.Context, table, column string) (bool, error) {
	rows, err := s.db.QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
	if err != nil {
		return false, fmt.Errorf("inspect table %s: %w", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, dataType string
		var notNull int
		var defaultValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &pk); err != nil {
			return false, fmt.Errorf("scan table %s columns: %w", table, err)
		}
		if name == column {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate table %s columns: %w", table, err)
	}
	return false, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *Store) DBPath() string {
	return s.dbPath
}

func (s *Store) StartedAt() time.Time {
	return s.startedAt
}

func (s *Store) Counts(ctx context.Context) (Counts, error) {
	var counts Counts
	queries := []struct {
		name string
		sql  string
		dst  *int64
	}{
		{"hosts", "SELECT COUNT(*) FROM hosts", &counts.Hosts},
		{"credentials", "SELECT COUNT(*) FROM credentials", &counts.Credentials},
		{"jobs", "SELECT COUNT(*) FROM jobs", &counts.Jobs},
		{"runs", "SELECT COUNT(*) FROM runs", &counts.Runs},
		{"snapshots", "SELECT COUNT(*) FROM snapshots WHERE deleted_at IS NULL", &counts.Snapshots},
	}
	for _, query := range queries {
		row := s.db.QueryRowContext(ctx, query.sql)
		if err := row.Scan(query.dst); err != nil {
			return Counts{}, fmt.Errorf("count %s: %w", query.name, err)
		}
	}
	return counts, nil
}

func (s *Store) ListHosts(ctx context.Context) ([]Host, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, name, source_type, address, status, last_seen_at, created_at, updated_at
		FROM hosts
		ORDER BY created_at DESC, id DESC
		LIMIT 200`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var hosts []Host
	for rows.Next() {
		var host Host
		if err := rows.Scan(&host.ID, &host.Name, &host.SourceType, &host.Address, &host.Status, &host.LastSeenAt, &host.CreatedAt, &host.UpdatedAt); err != nil {
			return nil, err
		}
		hosts = append(hosts, host)
	}
	return hosts, rows.Err()
}

type CreateHostInput struct {
	Name       string
	SourceType string
	Address    sql.NullString
	Status     string
	LastSeenAt sql.NullTime
}

func (s *Store) CreateHost(ctx context.Context, input CreateHostInput) (Host, error) {
	input.Name = strings.TrimSpace(input.Name)
	input.SourceType = strings.TrimSpace(input.SourceType)
	input.Status = strings.TrimSpace(input.Status)
	if input.Name == "" {
		return Host{}, errors.New("host name is required")
	}
	if input.SourceType == "" {
		return Host{}, errors.New("host source_type is required")
	}
	if input.Status == "" {
		input.Status = "unknown"
	}
	result, err := s.db.ExecContext(ctx, `
		INSERT INTO hosts (name, source_type, address, status, last_seen_at)
		VALUES (?, ?, ?, ?, ?)`,
		input.Name, input.SourceType, input.Address, input.Status, input.LastSeenAt)
	if err != nil {
		return Host{}, fmt.Errorf("create host: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return Host{}, fmt.Errorf("get created host id: %w", err)
	}
	return s.GetHost(ctx, id)
}

func (s *Store) GetHost(ctx context.Context, id int64) (Host, error) {
	var host Host
	err := s.db.QueryRowContext(ctx, `
		SELECT id, name, source_type, address, status, last_seen_at, created_at, updated_at
		FROM hosts
		WHERE id = ?`, id).
		Scan(&host.ID, &host.Name, &host.SourceType, &host.Address, &host.Status, &host.LastSeenAt, &host.CreatedAt, &host.UpdatedAt)
	if err == sql.ErrNoRows {
		return Host{}, fmt.Errorf("host %d not found", id)
	}
	if err != nil {
		return Host{}, fmt.Errorf("get host %d: %w", id, err)
	}
	return host, nil
}

func (s *Store) UpsertHostStatus(ctx context.Context, name, sourceType, address, status string, lastSeenAt time.Time) (Host, error) {
	name = strings.TrimSpace(name)
	sourceType = strings.TrimSpace(sourceType)
	address = strings.TrimSpace(address)
	status = strings.TrimSpace(status)
	if name == "" {
		return Host{}, errors.New("host name is required")
	}
	if sourceType == "" {
		return Host{}, errors.New("host source_type is required")
	}
	if status == "" {
		status = "unknown"
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Host{}, fmt.Errorf("begin upsert host transaction: %w", err)
	}
	defer tx.Rollback()

	var id int64
	err = tx.QueryRowContext(ctx, `
		SELECT id
		FROM hosts
		WHERE source_type = ? AND name = ?
		ORDER BY id DESC
		LIMIT 1`, sourceType, name).Scan(&id)
	addressValue := sql.NullString{String: address, Valid: address != ""}
	lastSeenValue := sql.NullTime{Time: lastSeenAt.UTC(), Valid: !lastSeenAt.IsZero()}
	if err == nil {
		if _, err := tx.ExecContext(ctx, `
			UPDATE hosts
			SET address = ?, status = ?, last_seen_at = ?, updated_at = CURRENT_TIMESTAMP
			WHERE id = ?`, addressValue, status, lastSeenValue, id); err != nil {
			return Host{}, fmt.Errorf("update host status: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return Host{}, fmt.Errorf("commit host status update: %w", err)
		}
		return s.GetHost(ctx, id)
	}
	if err != sql.ErrNoRows {
		return Host{}, fmt.Errorf("find host: %w", err)
	}

	result, err := tx.ExecContext(ctx, `
		INSERT INTO hosts (name, source_type, address, status, last_seen_at)
		VALUES (?, ?, ?, ?, ?)`, name, sourceType, addressValue, status, lastSeenValue)
	if err != nil {
		return Host{}, fmt.Errorf("create host status: %w", err)
	}
	id, err = result.LastInsertId()
	if err != nil {
		return Host{}, fmt.Errorf("get created host status id: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Host{}, fmt.Errorf("commit host status create: %w", err)
	}
	return s.GetHost(ctx, id)
}

func (s *Store) ListJobs(ctx context.Context) ([]Job, error) {
	rows, err := s.db.QueryContext(ctx, `
			SELECT id, host_id, credential_id, name, source_type, source_config, enabled, schedule, timezone, max_runtime_seconds, retry_attempts, created_at, updated_at
			FROM jobs
			ORDER BY created_at DESC, id DESC
			LIMIT 200`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		var job Job
		if err := rows.Scan(&job.ID, &job.HostID, &job.CredentialID, &job.Name, &job.SourceType, &job.SourceConfig, &job.Enabled, &job.Schedule, &job.Timezone, &job.MaxRuntimeSeconds, &job.RetryAttempts, &job.CreatedAt, &job.UpdatedAt); err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (s *Store) ListRuns(ctx context.Context) ([]Run, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, job_id, host_id, status, started_at, finished_at, error_message, created_at
		FROM runs
		ORDER BY created_at DESC, id DESC
		LIMIT 200`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runs []Run
	for rows.Next() {
		var run Run
		if err := rows.Scan(&run.ID, &run.JobID, &run.HostID, &run.Status, &run.StartedAt, &run.FinishedAt, &run.ErrorMessage, &run.CreatedAt); err != nil {
			return nil, err
		}
		if err := s.hydrateRunProgress(ctx, &run); err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, rows.Err()
}

func (s *Store) ListSnapshots(ctx context.Context) ([]Snapshot, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, job_id, host_id, run_id, created_at, source_type, manifest_ref, file_count, total_size, deleted_at
		FROM snapshots
		WHERE deleted_at IS NULL
		ORDER BY created_at DESC, id DESC
		LIMIT 200`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var snapshots []Snapshot
	for rows.Next() {
		var snapshot Snapshot
		if err := rows.Scan(&snapshot.ID, &snapshot.JobID, &snapshot.HostID, &snapshot.RunID, &snapshot.CreatedAt, &snapshot.SourceType, &snapshot.ManifestRef, &snapshot.FileCount, &snapshot.TotalSize, &snapshot.DeletedAt); err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots, rows.Err()
}

func (s *Store) UpsertAgentHeartbeat(ctx context.Context, subject, hostname, version string, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO agent_heartbeats (token_subject, hostname, agent_version, last_seen_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(token_subject) DO UPDATE SET
			hostname = excluded.hostname,
			agent_version = excluded.agent_version,
			last_seen_at = excluded.last_seen_at`,
		subject, hostname, version, now.UTC())
	if err != nil {
		return fmt.Errorf("upsert agent heartbeat: %w", err)
	}
	if _, err := s.UpsertHostStatus(ctx, subject, "agent", hostname, "online", now); err != nil {
		return fmt.Errorf("upsert agent host: %w", err)
	}
	return nil
}

type Counts struct {
	Hosts       int64 `json:"hosts"`
	Credentials int64 `json:"credentials"`
	Jobs        int64 `json:"jobs"`
	Runs        int64 `json:"runs"`
	Snapshots   int64 `json:"snapshots"`
}

type Host struct {
	ID         int64          `json:"id"`
	Name       string         `json:"name"`
	SourceType string         `json:"source_type"`
	Address    sql.NullString `json:"address"`
	Status     string         `json:"status"`
	LastSeenAt sql.NullTime   `json:"last_seen_at"`
	CreatedAt  time.Time      `json:"created_at"`
	UpdatedAt  time.Time      `json:"updated_at"`
}

type Job struct {
	ID                int64          `json:"id"`
	HostID            sql.NullInt64  `json:"host_id"`
	CredentialID      sql.NullInt64  `json:"credential_id"`
	Name              string         `json:"name"`
	SourceType        string         `json:"source_type"`
	SourceConfig      string         `json:"source_config"`
	Enabled           bool           `json:"enabled"`
	Schedule          sql.NullString `json:"schedule"`
	Timezone          string         `json:"timezone"`
	MaxRuntimeSeconds int64          `json:"max_runtime_seconds"`
	RetryAttempts     int64          `json:"retry_attempts"`
	CreatedAt         time.Time      `json:"created_at"`
	UpdatedAt         time.Time      `json:"updated_at"`
}

type Run struct {
	ID           int64          `json:"id"`
	JobID        sql.NullInt64  `json:"job_id"`
	HostID       sql.NullInt64  `json:"host_id"`
	Status       string         `json:"status"`
	StartedAt    sql.NullTime   `json:"started_at"`
	FinishedAt   sql.NullTime   `json:"finished_at"`
	ErrorMessage sql.NullString `json:"error_message"`
	CreatedAt    time.Time      `json:"created_at"`
	Progress     *RunProgress   `json:"progress,omitempty"`
}

type RunProgress struct {
	RunID          int64     `json:"run_id"`
	Phase          string    `json:"phase"`
	TotalFiles     int64     `json:"total_files"`
	ProcessedFiles int64     `json:"processed_files"`
	TotalBytes     int64     `json:"total_bytes"`
	ProcessedBytes int64     `json:"processed_bytes"`
	UploadedChunks int64     `json:"uploaded_chunks"`
	ReusedChunks   int64     `json:"reused_chunks"`
	Message        string    `json:"message"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type Snapshot struct {
	ID          int64         `json:"id"`
	JobID       sql.NullInt64 `json:"job_id"`
	HostID      sql.NullInt64 `json:"host_id"`
	RunID       sql.NullInt64 `json:"run_id"`
	CreatedAt   time.Time     `json:"created_at"`
	SourceType  string        `json:"source_type"`
	ManifestRef string        `json:"manifest_ref"`
	FileCount   int64         `json:"file_count"`
	TotalSize   int64         `json:"total_size"`
	DeletedAt   sql.NullTime  `json:"deleted_at"`
}

type RunLog struct {
	ID        int64     `json:"id"`
	RunID     int64     `json:"run_id"`
	Level     string    `json:"level"`
	Message   string    `json:"message"`
	CreatedAt time.Time `json:"created_at"`
}

type RestoreTask struct {
	ID         int64     `json:"id"`
	SnapshotID int64     `json:"snapshot_id"`
	TargetPath string    `json:"target_path"`
	Status     string    `json:"status"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

var schema = []string{
	`CREATE TABLE IF NOT EXISTS schema_migrations (
		version INTEGER PRIMARY KEY,
		applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`,
	`CREATE TABLE IF NOT EXISTS hosts (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		source_type TEXT NOT NULL CHECK (source_type IN ('agent', 'sftp', 'ftp', 'ftps', 'webdav', 'local')),
		address TEXT,
		status TEXT NOT NULL DEFAULT 'unknown',
		last_seen_at DATETIME,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`,
	`CREATE TABLE IF NOT EXISTS credentials (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		type TEXT NOT NULL CHECK (type IN ('sftp', 'ftp', 'ftps', 'webdav', 'agent')),
		encrypted_payload BLOB NOT NULL DEFAULT x'',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`,
	`CREATE TABLE IF NOT EXISTS jobs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		host_id INTEGER REFERENCES hosts(id) ON DELETE SET NULL,
		credential_id INTEGER REFERENCES credentials(id) ON DELETE SET NULL,
		name TEXT NOT NULL,
		source_type TEXT NOT NULL CHECK (source_type IN ('agent', 'sftp', 'ftp', 'ftps', 'webdav', 'local')),
		source_config TEXT NOT NULL DEFAULT '{}',
			enabled BOOLEAN NOT NULL DEFAULT 0,
			schedule TEXT,
			timezone TEXT NOT NULL DEFAULT 'Asia/Shanghai',
			max_runtime_seconds INTEGER NOT NULL DEFAULT 0,
			retry_attempts INTEGER NOT NULL DEFAULT 0,
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
	`CREATE TABLE IF NOT EXISTS runs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		job_id INTEGER REFERENCES jobs(id) ON DELETE SET NULL,
		host_id INTEGER REFERENCES hosts(id) ON DELETE SET NULL,
		status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'failed', 'canceled', 'completed')),
		started_at DATETIME,
		finished_at DATETIME,
		error_message TEXT,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`,
	`CREATE TABLE IF NOT EXISTS run_logs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
		level TEXT NOT NULL,
		message TEXT NOT NULL,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`,
	`CREATE TABLE IF NOT EXISTS run_progress (
		run_id INTEGER PRIMARY KEY REFERENCES runs(id) ON DELETE CASCADE,
		phase TEXT NOT NULL,
		total_files INTEGER NOT NULL DEFAULT 0,
		processed_files INTEGER NOT NULL DEFAULT 0,
		total_bytes INTEGER NOT NULL DEFAULT 0,
		processed_bytes INTEGER NOT NULL DEFAULT 0,
		uploaded_chunks INTEGER NOT NULL DEFAULT 0,
		reused_chunks INTEGER NOT NULL DEFAULT 0,
		message TEXT NOT NULL DEFAULT '',
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`,
	`CREATE TABLE IF NOT EXISTS snapshots (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		job_id INTEGER REFERENCES jobs(id) ON DELETE SET NULL,
		host_id INTEGER REFERENCES hosts(id) ON DELETE SET NULL,
		run_id INTEGER REFERENCES runs(id) ON DELETE SET NULL,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		source_type TEXT NOT NULL,
		manifest_ref TEXT NOT NULL DEFAULT '',
		file_count INTEGER NOT NULL DEFAULT 0,
		total_size INTEGER NOT NULL DEFAULT 0,
		deleted_at DATETIME
	)`,
	`CREATE TABLE IF NOT EXISTS restore_tasks (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		snapshot_id INTEGER NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
		target_path TEXT NOT NULL,
		status TEXT NOT NULL DEFAULT 'pending',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`,
	`CREATE TABLE IF NOT EXISTS agent_heartbeats (
		token_subject TEXT PRIMARY KEY,
		hostname TEXT NOT NULL,
		agent_version TEXT NOT NULL,
		last_seen_at DATETIME NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS web_sessions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			token_hash TEXT NOT NULL UNIQUE,
			username TEXT NOT NULL,
			expires_at DATETIME NOT NULL,
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
	`CREATE TABLE IF NOT EXISTS app_settings (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL,
			updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
}
