package state

import (
	"context"
	"database/sql"
	"fmt"
)

type CreateRestoreTaskInput struct {
	SnapshotID int64
	TargetPath string
	Status     string
}

func (s *Store) CreateRestoreTask(ctx context.Context, input CreateRestoreTaskInput) (RestoreTask, error) {
	if input.SnapshotID <= 0 {
		return RestoreTask{}, fmt.Errorf("snapshot_id is required")
	}
	if input.TargetPath == "" {
		return RestoreTask{}, fmt.Errorf("target_path is required")
	}
	if input.Status == "" {
		input.Status = "pending"
	}
	result, err := s.db.ExecContext(ctx, `
		INSERT INTO restore_tasks (snapshot_id, target_path, status)
		VALUES (?, ?, ?)`, input.SnapshotID, input.TargetPath, input.Status)
	if err != nil {
		return RestoreTask{}, fmt.Errorf("create restore task: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return RestoreTask{}, fmt.Errorf("get created restore task id: %w", err)
	}
	return s.GetRestoreTask(ctx, id)
}

func (s *Store) UpdateRestoreTaskStatus(ctx context.Context, id int64, status string) (RestoreTask, error) {
	if id <= 0 {
		return RestoreTask{}, fmt.Errorf("restore task id is required")
	}
	if status == "" {
		return RestoreTask{}, fmt.Errorf("restore task status is required")
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE restore_tasks
		SET status = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?`, status, id)
	if err != nil {
		return RestoreTask{}, fmt.Errorf("update restore task status: %w", err)
	}
	return s.GetRestoreTask(ctx, id)
}

func (s *Store) GetRestoreTask(ctx context.Context, id int64) (RestoreTask, error) {
	var task RestoreTask
	err := s.db.QueryRowContext(ctx, `
		SELECT id, snapshot_id, target_path, status, created_at, updated_at
		FROM restore_tasks
		WHERE id = ?`, id).
		Scan(&task.ID, &task.SnapshotID, &task.TargetPath, &task.Status, &task.CreatedAt, &task.UpdatedAt)
	if err == sql.ErrNoRows {
		return RestoreTask{}, fmt.Errorf("restore task %d not found", id)
	}
	if err != nil {
		return RestoreTask{}, fmt.Errorf("get restore task %d: %w", id, err)
	}
	return task, nil
}

func (s *Store) ListRestoreTasks(ctx context.Context) ([]RestoreTask, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, snapshot_id, target_path, status, created_at, updated_at
		FROM restore_tasks
		ORDER BY created_at DESC, id DESC
		LIMIT 200`)
	if err != nil {
		return nil, fmt.Errorf("list restore tasks: %w", err)
	}
	defer rows.Close()

	var tasks []RestoreTask
	for rows.Next() {
		var task RestoreTask
		if err := rows.Scan(&task.ID, &task.SnapshotID, &task.TargetPath, &task.Status, &task.CreatedAt, &task.UpdatedAt); err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}
