package state

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type MaintenanceRun struct {
	ID            int64          `json:"id"`
	Mode          string         `json:"mode"`
	Status        string         `json:"status"`
	StartedAt     time.Time      `json:"started_at"`
	FinishedAt    sql.NullTime   `json:"finished_at"`
	SkippedReason sql.NullString `json:"skipped_reason"`
	ReportJSON    string         `json:"report_json"`
	ErrorMessage  sql.NullString `json:"error_message"`
}

type RecordMaintenanceRunInput struct {
	Mode          string
	Status        string
	StartedAt     time.Time
	FinishedAt    time.Time
	SkippedReason string
	ReportJSON    string
	ErrorMessage  string
}

func (s *Store) RecordMaintenanceRun(ctx context.Context, input RecordMaintenanceRunInput) (MaintenanceRun, error) {
	if input.Mode == "" {
		return MaintenanceRun{}, fmt.Errorf("maintenance mode is required")
	}
	if input.Status == "" {
		input.Status = "completed"
	}
	if input.StartedAt.IsZero() {
		input.StartedAt = time.Now().UTC()
	}
	if input.ReportJSON == "" {
		input.ReportJSON = "{}"
	}
	finished := sql.NullTime{Time: input.FinishedAt.UTC(), Valid: !input.FinishedAt.IsZero()}
	skippedReason := sql.NullString{String: input.SkippedReason, Valid: input.SkippedReason != ""}
	errorMessage := sql.NullString{String: input.ErrorMessage, Valid: input.ErrorMessage != ""}
	result, err := s.db.ExecContext(ctx, `
		INSERT INTO maintenance_runs (mode, status, started_at, finished_at, skipped_reason, report_json, error_message)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		input.Mode, input.Status, input.StartedAt.UTC(), finished, skippedReason, input.ReportJSON, errorMessage)
	if err != nil {
		return MaintenanceRun{}, fmt.Errorf("record maintenance run: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return MaintenanceRun{}, fmt.Errorf("get maintenance run id: %w", err)
	}
	return s.GetMaintenanceRun(ctx, id)
}

func (s *Store) GetMaintenanceRun(ctx context.Context, id int64) (MaintenanceRun, error) {
	var run MaintenanceRun
	err := s.db.QueryRowContext(ctx, `
		SELECT id, mode, status, started_at, finished_at, skipped_reason, report_json, error_message
		FROM maintenance_runs
		WHERE id = ?`, id).Scan(&run.ID, &run.Mode, &run.Status, &run.StartedAt, &run.FinishedAt, &run.SkippedReason, &run.ReportJSON, &run.ErrorMessage)
	if err == sql.ErrNoRows {
		return MaintenanceRun{}, fmt.Errorf("maintenance run %d not found", id)
	}
	if err != nil {
		return MaintenanceRun{}, fmt.Errorf("get maintenance run %d: %w", id, err)
	}
	return run, nil
}

func (s *Store) ListMaintenanceRuns(ctx context.Context, limit int) ([]MaintenanceRun, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, mode, status, started_at, finished_at, skipped_reason, report_json, error_message
		FROM maintenance_runs
		ORDER BY started_at DESC, id DESC
		LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("list maintenance runs: %w", err)
	}
	defer rows.Close()

	runs := make([]MaintenanceRun, 0)
	for rows.Next() {
		var run MaintenanceRun
		if err := rows.Scan(&run.ID, &run.Mode, &run.Status, &run.StartedAt, &run.FinishedAt, &run.SkippedReason, &run.ReportJSON, &run.ErrorMessage); err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, rows.Err()
}
