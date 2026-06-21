package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

var ErrActiveRunExists = errors.New("active run already exists for job")

type CreateJobInput struct {
	Name              string          `json:"name"`
	HostID            int64           `json:"host_id"`
	SourceConfig      json.RawMessage `json:"source_config"`
	Enabled           bool            `json:"enabled"`
	Schedule          sql.NullString  `json:"schedule"`
	Timezone          string          `json:"timezone"`
	MaxRuntimeSeconds int64           `json:"max_runtime_seconds"`
	RetryAttempts     int64           `json:"retry_attempts"`
}

type UpdateJobInput struct {
	ID                int64           `json:"id"`
	Name              string          `json:"name"`
	SourceConfig      json.RawMessage `json:"source_config"`
	Enabled           bool            `json:"enabled"`
	Schedule          sql.NullString  `json:"schedule"`
	Timezone          string          `json:"timezone"`
	MaxRuntimeSeconds int64           `json:"max_runtime_seconds"`
	RetryAttempts     int64           `json:"retry_attempts"`
}

type CreateSnapshotInput struct {
	JobID       sql.NullInt64
	HostID      sql.NullInt64
	RunID       sql.NullInt64
	SourceType  string
	ManifestRef string
	FileCount   int64
	TotalSize   int64
}

type AgentJobInput struct {
	HostID       int64
	CredentialID int64
	Name         string
	SourceConfig json.RawMessage
	Timezone     string
}

type RetentionPolicy struct {
	KeepLast   int `json:"keep_last"`
	KeepDaily  int `json:"keep_daily"`
	KeepWeekly int `json:"keep_weekly"`
}

type SnapshotCounts struct {
	Active  int64 `json:"active"`
	Deleted int64 `json:"deleted"`
}

type RunStatusSummary struct {
	Total     int64
	Pending   int64
	Running   int64
	Failed    int64
	Canceled  int64
	Completed int64
}

func (s RunStatusSummary) Active() int64 {
	return s.Pending + s.Running
}

type UpdateRunProgressInput struct {
	RunID          int64
	Phase          string
	TotalFiles     int64
	ProcessedFiles int64
	TotalBytes     int64
	ProcessedBytes int64
	UploadedChunks int64
	ReusedChunks   int64
	Message        string
}

func (s *Store) CreateJob(ctx context.Context, input CreateJobInput) (Job, error) {
	if input.Name == "" {
		return Job{}, errors.New("job name is required")
	}
	if input.HostID <= 0 {
		return Job{}, errors.New("job host_id is required")
	}
	host, err := s.GetHost(ctx, input.HostID)
	if err != nil {
		return Job{}, err
	}
	if len(input.SourceConfig) == 0 {
		input.SourceConfig = json.RawMessage(`{}`)
	}
	if !json.Valid(input.SourceConfig) {
		return Job{}, errors.New("job source_config must be valid JSON")
	}
	if input.Timezone == "" {
		input.Timezone = "Asia/Shanghai"
	}
	if input.MaxRuntimeSeconds < 0 {
		return Job{}, errors.New("job max_runtime_seconds must be non-negative")
	}
	if input.RetryAttempts < 0 {
		return Job{}, errors.New("job retry_attempts must be non-negative")
	}

	result, err := s.db.ExecContext(ctx, `
			INSERT INTO jobs (host_id, credential_id, name, source_type, source_config, enabled, schedule, timezone, max_runtime_seconds, retry_attempts)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		host.ID, host.CredentialID, input.Name, host.SourceType, string(input.SourceConfig), input.Enabled, input.Schedule, input.Timezone, input.MaxRuntimeSeconds, input.RetryAttempts)
	if err != nil {
		return Job{}, fmt.Errorf("create job: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return Job{}, fmt.Errorf("get created job id: %w", err)
	}
	return s.GetJob(ctx, id)
}

func (s *Store) UpdateJob(ctx context.Context, input UpdateJobInput) (Job, error) {
	if input.ID <= 0 {
		return Job{}, errors.New("job id is required")
	}
	if input.Name == "" {
		return Job{}, errors.New("job name is required")
	}
	if len(input.SourceConfig) == 0 {
		input.SourceConfig = json.RawMessage(`{}`)
	}
	if !json.Valid(input.SourceConfig) {
		return Job{}, errors.New("job source_config must be valid JSON")
	}
	if input.Timezone == "" {
		input.Timezone = "Asia/Shanghai"
	}
	if input.MaxRuntimeSeconds < 0 {
		return Job{}, errors.New("job max_runtime_seconds must be non-negative")
	}
	if input.RetryAttempts < 0 {
		return Job{}, errors.New("job retry_attempts must be non-negative")
	}

	result, err := s.db.ExecContext(ctx, `
			UPDATE jobs
			SET name = ?, source_config = ?, enabled = ?, schedule = ?, timezone = ?, max_runtime_seconds = ?, retry_attempts = ?, updated_at = CURRENT_TIMESTAMP
			WHERE id = ?`,
		input.Name, string(input.SourceConfig), input.Enabled, input.Schedule, input.Timezone, input.MaxRuntimeSeconds, input.RetryAttempts, input.ID)
	if err != nil {
		return Job{}, fmt.Errorf("update job: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return Job{}, fmt.Errorf("get updated job rows affected: %w", err)
	}
	if changed == 0 {
		return Job{}, fmt.Errorf("job %d not found", input.ID)
	}
	return s.GetJob(ctx, input.ID)
}

func (s *Store) FindOrCreateAgentJob(ctx context.Context, input AgentJobInput) (Job, bool, error) {
	if input.HostID <= 0 {
		return Job{}, false, errors.New("agent host_id is required")
	}
	if input.CredentialID <= 0 {
		return Job{}, false, errors.New("agent credential_id is required")
	}
	if input.Name == "" {
		return Job{}, false, errors.New("agent job name is required")
	}
	if len(input.SourceConfig) == 0 {
		input.SourceConfig = json.RawMessage(`{}`)
	}
	if !json.Valid(input.SourceConfig) {
		return Job{}, false, errors.New("agent source_config must be valid JSON")
	}
	if input.Timezone == "" {
		input.Timezone = "Asia/Shanghai"
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, false, fmt.Errorf("begin agent job transaction: %w", err)
	}
	defer tx.Rollback()

	var id int64
	err = tx.QueryRowContext(ctx, `
		SELECT id
		FROM jobs
		WHERE source_type = 'agent' AND host_id = ? AND name = ?
		ORDER BY id DESC
		LIMIT 1`, input.HostID, input.Name).Scan(&id)
	if err == nil {
		if _, err := tx.ExecContext(ctx, `
			UPDATE jobs
			SET source_config = ?, updated_at = CURRENT_TIMESTAMP
			WHERE id = ?`, string(input.SourceConfig), id); err != nil {
			return Job{}, false, fmt.Errorf("update agent job: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return Job{}, false, fmt.Errorf("commit agent job update: %w", err)
		}
		job, err := s.GetJob(ctx, id)
		return job, false, err
	}
	if err != sql.ErrNoRows {
		return Job{}, false, fmt.Errorf("find agent job: %w", err)
	}

	result, err := tx.ExecContext(ctx, `
		INSERT INTO jobs (host_id, credential_id, name, source_type, source_config, enabled, timezone)
		VALUES (?, ?, ?, 'agent', ?, 1, ?)`,
		input.HostID, input.CredentialID, input.Name, string(input.SourceConfig), input.Timezone)
	if err != nil {
		return Job{}, false, fmt.Errorf("create agent job: %w", err)
	}
	id, err = result.LastInsertId()
	if err != nil {
		return Job{}, false, fmt.Errorf("get created agent job id: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Job{}, false, fmt.Errorf("commit agent job create: %w", err)
	}
	job, err := s.GetJob(ctx, id)
	return job, true, err
}

func (s *Store) GetJob(ctx context.Context, id int64) (Job, error) {
	var job Job
	err := s.db.QueryRowContext(ctx, `
			SELECT id, host_id, credential_id, name, source_type, source_config, enabled, schedule, timezone, max_runtime_seconds, retry_attempts, created_at, updated_at
			FROM jobs
			WHERE id = ?`, id).
		Scan(&job.ID, &job.HostID, &job.CredentialID, &job.Name, &job.SourceType, &job.SourceConfig, &job.Enabled, &job.Schedule, &job.Timezone, &job.MaxRuntimeSeconds, &job.RetryAttempts, &job.CreatedAt, &job.UpdatedAt)
	if err == sql.ErrNoRows {
		return Job{}, fmt.Errorf("job %d not found", id)
	}
	if err != nil {
		return Job{}, fmt.Errorf("get job %d: %w", id, err)
	}
	return job, nil
}

func (s *Store) ListScheduledJobs(ctx context.Context) ([]Job, error) {
	rows, err := s.db.QueryContext(ctx, `
			SELECT id, host_id, credential_id, name, source_type, source_config, enabled, schedule, timezone, max_runtime_seconds, retry_attempts, created_at, updated_at
			FROM jobs
		WHERE enabled = 1
			AND schedule IS NOT NULL
			AND TRIM(schedule) != ''
			AND source_type != 'agent'
		ORDER BY id ASC`)
	if err != nil {
		return nil, fmt.Errorf("list scheduled jobs: %w", err)
	}
	defer rows.Close()

	jobs := make([]Job, 0)
	for rows.Next() {
		var job Job
		if err := rows.Scan(&job.ID, &job.HostID, &job.CredentialID, &job.Name, &job.SourceType, &job.SourceConfig, &job.Enabled, &job.Schedule, &job.Timezone, &job.MaxRuntimeSeconds, &job.RetryAttempts, &job.CreatedAt, &job.UpdatedAt); err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (s *Store) CreateRun(ctx context.Context, job Job) (Run, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Run{}, fmt.Errorf("begin create run transaction: %w", err)
	}
	defer tx.Rollback()

	var activeID int64
	err = tx.QueryRowContext(ctx, `
		SELECT id
		FROM runs
		WHERE job_id = ? AND status IN ('pending', 'running')
		ORDER BY id DESC
		LIMIT 1`, job.ID).Scan(&activeID)
	if err == nil {
		return Run{}, fmt.Errorf("%w: %d", ErrActiveRunExists, activeID)
	}
	if err != sql.ErrNoRows {
		return Run{}, fmt.Errorf("check active run: %w", err)
	}

	result, err := tx.ExecContext(ctx, `
		INSERT INTO runs (job_id, host_id, status)
		VALUES (?, ?, 'pending')`, job.ID, job.HostID)
	if err != nil {
		return Run{}, fmt.Errorf("create run: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return Run{}, fmt.Errorf("get created run id: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Run{}, fmt.Errorf("commit create run transaction: %w", err)
	}
	return s.GetRun(ctx, id)
}

func (s *Store) GetRun(ctx context.Context, id int64) (Run, error) {
	var run Run
	err := s.db.QueryRowContext(ctx, `
		SELECT id, job_id, host_id, status, started_at, finished_at, error_message, created_at
		FROM runs
		WHERE id = ?`, id).
		Scan(&run.ID, &run.JobID, &run.HostID, &run.Status, &run.StartedAt, &run.FinishedAt, &run.ErrorMessage, &run.CreatedAt)
	if err == sql.ErrNoRows {
		return Run{}, fmt.Errorf("run %d not found", id)
	}
	if err != nil {
		return Run{}, fmt.Errorf("get run %d: %w", id, err)
	}
	if err := s.hydrateRunProgress(ctx, &run); err != nil {
		return Run{}, err
	}
	return run, nil
}

func (s *Store) GetActiveRunForJob(ctx context.Context, jobID int64) (Run, bool, error) {
	var run Run
	err := s.db.QueryRowContext(ctx, `
		SELECT id, job_id, host_id, status, started_at, finished_at, error_message, created_at
		FROM runs
		WHERE job_id = ? AND status IN ('pending', 'running')
		ORDER BY id DESC
		LIMIT 1`, jobID).
		Scan(&run.ID, &run.JobID, &run.HostID, &run.Status, &run.StartedAt, &run.FinishedAt, &run.ErrorMessage, &run.CreatedAt)
	if err == sql.ErrNoRows {
		return Run{}, false, nil
	}
	if err != nil {
		return Run{}, false, fmt.Errorf("get active run for job %d: %w", jobID, err)
	}
	if err := s.hydrateRunProgress(ctx, &run); err != nil {
		return Run{}, false, err
	}
	return run, true, nil
}

func (s *Store) HasRunCreatedSince(ctx context.Context, jobID int64, since time.Time) (bool, error) {
	var id int64
	err := s.db.QueryRowContext(ctx, `
		SELECT id
		FROM runs
		WHERE job_id = ? AND created_at >= ?
		ORDER BY id DESC
		LIMIT 1`, jobID, since.UTC()).Scan(&id)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check recent run for job %d: %w", jobID, err)
	}
	return true, nil
}

func (s *Store) RunStatusSummarySince(ctx context.Context, jobID int64, since time.Time) (RunStatusSummary, error) {
	rows, err := s.db.QueryContext(ctx, `
			SELECT status, COUNT(*)
			FROM runs
			WHERE job_id = ? AND created_at >= ?
			GROUP BY status`, jobID, since.UTC())
	if err != nil {
		return RunStatusSummary{}, fmt.Errorf("summarize runs for job %d: %w", jobID, err)
	}
	defer rows.Close()

	var summary RunStatusSummary
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return RunStatusSummary{}, fmt.Errorf("scan run summary for job %d: %w", jobID, err)
		}
		summary.Total += count
		switch status {
		case "pending":
			summary.Pending = count
		case "running":
			summary.Running = count
		case "failed":
			summary.Failed = count
		case "canceled":
			summary.Canceled = count
		case "completed":
			summary.Completed = count
		}
	}
	if err := rows.Err(); err != nil {
		return RunStatusSummary{}, fmt.Errorf("iterate run summary for job %d: %w", jobID, err)
	}
	return summary, nil
}

func (s *Store) CountActiveRuns(ctx context.Context) (int64, error) {
	var count int64
	if err := s.db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM runs
			WHERE status IN ('pending', 'running')`).Scan(&count); err != nil {
		return 0, fmt.Errorf("count active runs: %w", err)
	}
	return count, nil
}

func (s *Store) MarkRunRunning(ctx context.Context, id int64, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE runs
		SET status = 'running', started_at = ?
		WHERE id = ? AND status = 'pending'`, now.UTC(), id)
	if err != nil {
		return fmt.Errorf("mark run running: %w", err)
	}
	return nil
}

func (s *Store) CompleteRun(ctx context.Context, id int64, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE runs
		SET status = 'completed', finished_at = ?, error_message = NULL
		WHERE id = ?`, now.UTC(), id)
	if err != nil {
		return fmt.Errorf("complete run: %w", err)
	}
	return nil
}

func (s *Store) FailRun(ctx context.Context, id int64, message string, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE runs
		SET status = 'failed', finished_at = ?, error_message = ?
		WHERE id = ?`, now.UTC(), message, id)
	if err != nil {
		return fmt.Errorf("fail run: %w", err)
	}
	return nil
}

func (s *Store) AppendRunLog(ctx context.Context, runID int64, level, message string) error {
	if level == "" {
		level = "info"
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO run_logs (run_id, level, message)
		VALUES (?, ?, ?)`, runID, level, message)
	if err != nil {
		return fmt.Errorf("append run log: %w", err)
	}
	return nil
}

func (s *Store) UpdateRunProgress(ctx context.Context, input UpdateRunProgressInput) (RunProgress, error) {
	if input.RunID <= 0 {
		return RunProgress{}, errors.New("run id is required")
	}
	if input.Phase == "" {
		input.Phase = "running"
	}
	input.TotalFiles = nonNegative(input.TotalFiles)
	input.ProcessedFiles = nonNegative(input.ProcessedFiles)
	input.TotalBytes = nonNegative(input.TotalBytes)
	input.ProcessedBytes = nonNegative(input.ProcessedBytes)
	input.UploadedChunks = nonNegative(input.UploadedChunks)
	input.ReusedChunks = nonNegative(input.ReusedChunks)
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO run_progress (run_id, phase, total_files, processed_files, total_bytes, processed_bytes, uploaded_chunks, reused_chunks, message, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(run_id) DO UPDATE SET
			phase = excluded.phase,
			total_files = excluded.total_files,
			processed_files = excluded.processed_files,
			total_bytes = excluded.total_bytes,
			processed_bytes = excluded.processed_bytes,
			uploaded_chunks = excluded.uploaded_chunks,
			reused_chunks = excluded.reused_chunks,
			message = excluded.message,
			updated_at = CURRENT_TIMESTAMP`,
		input.RunID, input.Phase, input.TotalFiles, input.ProcessedFiles, input.TotalBytes, input.ProcessedBytes, input.UploadedChunks, input.ReusedChunks, input.Message)
	if err != nil {
		return RunProgress{}, fmt.Errorf("update run progress: %w", err)
	}
	progress, ok, err := s.GetRunProgress(ctx, input.RunID)
	if err != nil {
		return RunProgress{}, err
	}
	if !ok {
		return RunProgress{}, fmt.Errorf("run progress %d not found after update", input.RunID)
	}
	return progress, nil
}

func (s *Store) GetRunProgress(ctx context.Context, runID int64) (RunProgress, bool, error) {
	var progress RunProgress
	err := s.db.QueryRowContext(ctx, `
		SELECT run_id, phase, total_files, processed_files, total_bytes, processed_bytes, uploaded_chunks, reused_chunks, message, updated_at
		FROM run_progress
		WHERE run_id = ?`, runID).
		Scan(&progress.RunID, &progress.Phase, &progress.TotalFiles, &progress.ProcessedFiles, &progress.TotalBytes, &progress.ProcessedBytes, &progress.UploadedChunks, &progress.ReusedChunks, &progress.Message, &progress.UpdatedAt)
	if err == sql.ErrNoRows {
		return RunProgress{}, false, nil
	}
	if err != nil {
		return RunProgress{}, false, fmt.Errorf("get run progress %d: %w", runID, err)
	}
	return progress, true, nil
}

func (s *Store) hydrateRunProgress(ctx context.Context, run *Run) error {
	progress, ok, err := s.GetRunProgress(ctx, run.ID)
	if err != nil {
		return err
	}
	if ok {
		run.Progress = &progress
	}
	return nil
}

func nonNegative(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}

func (s *Store) ListRunLogs(ctx context.Context, runID int64) ([]RunLog, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, run_id, level, message, created_at
		FROM run_logs
		WHERE run_id = ?
		ORDER BY id ASC`, runID)
	if err != nil {
		return nil, fmt.Errorf("list run logs: %w", err)
	}
	defer rows.Close()

	logs := make([]RunLog, 0)
	for rows.Next() {
		var log RunLog
		if err := rows.Scan(&log.ID, &log.RunID, &log.Level, &log.Message, &log.CreatedAt); err != nil {
			return nil, err
		}
		logs = append(logs, log)
	}
	return logs, rows.Err()
}

func (s *Store) CreateSnapshot(ctx context.Context, input CreateSnapshotInput) (Snapshot, error) {
	result, err := s.db.ExecContext(ctx, `
		INSERT INTO snapshots (job_id, host_id, run_id, source_type, manifest_ref, file_count, total_size)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		input.JobID, input.HostID, input.RunID, input.SourceType, input.ManifestRef, input.FileCount, input.TotalSize)
	if err != nil {
		return Snapshot{}, fmt.Errorf("create snapshot: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return Snapshot{}, fmt.Errorf("get created snapshot id: %w", err)
	}
	return s.GetSnapshot(ctx, id)
}

func (s *Store) GetSnapshot(ctx context.Context, id int64) (Snapshot, error) {
	var snapshot Snapshot
	err := s.db.QueryRowContext(ctx, `
		SELECT id, job_id, host_id, run_id, created_at, source_type, manifest_ref, file_count, total_size, deleted_at
		FROM snapshots
		WHERE id = ?`, id).
		Scan(&snapshot.ID, &snapshot.JobID, &snapshot.HostID, &snapshot.RunID, &snapshot.CreatedAt, &snapshot.SourceType, &snapshot.ManifestRef, &snapshot.FileCount, &snapshot.TotalSize, &snapshot.DeletedAt)
	if err == sql.ErrNoRows {
		return Snapshot{}, fmt.Errorf("snapshot %d not found", id)
	}
	if err != nil {
		return Snapshot{}, fmt.Errorf("get snapshot %d: %w", id, err)
	}
	return snapshot, nil
}

func (s *Store) GetSnapshotByRun(ctx context.Context, runID int64) (Snapshot, bool, error) {
	var snapshot Snapshot
	err := s.db.QueryRowContext(ctx, `
		SELECT id, job_id, host_id, run_id, created_at, source_type, manifest_ref, file_count, total_size, deleted_at
		FROM snapshots
		WHERE run_id = ?
		ORDER BY id DESC
		LIMIT 1`, runID).
		Scan(&snapshot.ID, &snapshot.JobID, &snapshot.HostID, &snapshot.RunID, &snapshot.CreatedAt, &snapshot.SourceType, &snapshot.ManifestRef, &snapshot.FileCount, &snapshot.TotalSize, &snapshot.DeletedAt)
	if err == sql.ErrNoRows {
		return Snapshot{}, false, nil
	}
	if err != nil {
		return Snapshot{}, false, fmt.Errorf("get snapshot by run %d: %w", runID, err)
	}
	return snapshot, true, nil
}

func (s *Store) GetLatestSnapshotForJob(ctx context.Context, jobID int64) (Snapshot, bool, error) {
	var snapshot Snapshot
	err := s.db.QueryRowContext(ctx, `
		SELECT id, job_id, host_id, run_id, created_at, source_type, manifest_ref, file_count, total_size, deleted_at
		FROM snapshots
		WHERE job_id = ? AND deleted_at IS NULL
		ORDER BY created_at DESC, id DESC
		LIMIT 1`, jobID).
		Scan(&snapshot.ID, &snapshot.JobID, &snapshot.HostID, &snapshot.RunID, &snapshot.CreatedAt, &snapshot.SourceType, &snapshot.ManifestRef, &snapshot.FileCount, &snapshot.TotalSize, &snapshot.DeletedAt)
	if err == sql.ErrNoRows {
		return Snapshot{}, false, nil
	}
	if err != nil {
		return Snapshot{}, false, fmt.Errorf("get latest snapshot for job %d: %w", jobID, err)
	}
	return snapshot, true, nil
}

func (s *Store) ListActiveSnapshots(ctx context.Context) ([]Snapshot, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, job_id, host_id, run_id, created_at, source_type, manifest_ref, file_count, total_size, deleted_at
		FROM snapshots
		WHERE deleted_at IS NULL
		ORDER BY job_id ASC, created_at DESC, id DESC`)
	if err != nil {
		return nil, fmt.Errorf("list active snapshots: %w", err)
	}
	defer rows.Close()

	snapshots := make([]Snapshot, 0)
	for rows.Next() {
		var snapshot Snapshot
		if err := rows.Scan(&snapshot.ID, &snapshot.JobID, &snapshot.HostID, &snapshot.RunID, &snapshot.CreatedAt, &snapshot.SourceType, &snapshot.ManifestRef, &snapshot.FileCount, &snapshot.TotalSize, &snapshot.DeletedAt); err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots, rows.Err()
}

func (s *Store) ExpireSnapshots(ctx context.Context, ids []int64, now time.Time) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin expire snapshots transaction: %w", err)
	}
	defer tx.Rollback()

	var expired int64
	for _, id := range ids {
		result, err := tx.ExecContext(ctx, `
			UPDATE snapshots
			SET deleted_at = ?
			WHERE id = ? AND deleted_at IS NULL`, now.UTC(), id)
		if err != nil {
			return 0, fmt.Errorf("expire snapshot %d: %w", id, err)
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("expire snapshot rows affected: %w", err)
		}
		expired += changed
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit expire snapshots transaction: %w", err)
	}
	return expired, nil
}

func (s *Store) SnapshotCounts(ctx context.Context) (SnapshotCounts, error) {
	var counts SnapshotCounts
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM snapshots WHERE deleted_at IS NULL`).Scan(&counts.Active); err != nil {
		return SnapshotCounts{}, fmt.Errorf("count active snapshots: %w", err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM snapshots WHERE deleted_at IS NOT NULL`).Scan(&counts.Deleted); err != nil {
		return SnapshotCounts{}, fmt.Errorf("count deleted snapshots: %w", err)
	}
	return counts, nil
}

func ExpiredSnapshotIDs(snapshots []Snapshot, policy RetentionPolicy) []int64 {
	if policy.KeepLast <= 0 {
		policy.KeepLast = 30
	}
	keep := make(map[int64]bool)
	type groupKey struct {
		valid bool
		id    int64
	}
	groups := make(map[groupKey][]Snapshot)
	for _, snapshot := range snapshots {
		key := groupKey{valid: snapshot.JobID.Valid}
		if snapshot.JobID.Valid {
			key.id = snapshot.JobID.Int64
		} else {
			key.id = snapshot.ID
		}
		groups[key] = append(groups[key], snapshot)
	}

	for _, group := range groups {
		for i, snapshot := range group {
			if i < policy.KeepLast {
				keep[snapshot.ID] = true
			}
		}
		keptDays := make(map[string]bool)
		for _, snapshot := range group {
			if policy.KeepDaily <= 0 || len(keptDays) >= policy.KeepDaily {
				break
			}
			day := snapshot.CreatedAt.UTC().Format("2006-01-02")
			if keptDays[day] {
				continue
			}
			keep[snapshot.ID] = true
			keptDays[day] = true
		}
		keptWeeks := make(map[string]bool)
		for _, snapshot := range group {
			if policy.KeepWeekly <= 0 || len(keptWeeks) >= policy.KeepWeekly {
				break
			}
			year, week := snapshot.CreatedAt.UTC().ISOWeek()
			key := fmt.Sprintf("%04d-W%02d", year, week)
			if keptWeeks[key] {
				continue
			}
			keep[snapshot.ID] = true
			keptWeeks[key] = true
		}
	}

	var expired []int64
	for _, snapshot := range snapshots {
		if !keep[snapshot.ID] {
			expired = append(expired, snapshot.ID)
		}
	}
	return expired
}
