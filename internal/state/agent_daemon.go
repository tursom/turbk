package state

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	repositoryMetaIDKey                    = "repository_id"
	repositoryMetaChunkGenerationKey       = "chunk_generation"
	repositoryMetaConfigGenerationKey      = "config_generation"
	repositoryMetaCommandGenerationKey     = "command_generation"
	repositoryMetaInvalidationAvailableKey = "invalidation_available_from"
)

type RepositoryState struct {
	RepositoryID              string `json:"id"`
	ChunkGeneration           int64  `json:"chunk_generation"`
	ConfigGeneration          int64  `json:"config_generation"`
	CommandGeneration         int64  `json:"command_generation"`
	InvalidationAvailableFrom int64  `json:"invalidation_available_from"`
}

type ChunkInvalidations struct {
	RepositoryState RepositoryState
	FromGeneration  int64
	ToGeneration    int64
	Complete        bool
	Reason          string
	Hashes          []string
}

type AgentHeartbeatInput struct {
	CredentialID      int64
	HostID            int64
	Subject           string
	Hostname          string
	Version           string
	Mode              string
	StateDir          string
	CatalogStatus     string
	RepositoryID      string
	ChunkGeneration   int64
	ConfigGeneration  int64
	CommandGeneration int64
	RunningRunID      sql.NullInt64
	LastError         string
	Now               time.Time
}

type AgentHeartbeat struct {
	TokenSubject      string         `json:"token_subject"`
	Hostname          string         `json:"hostname"`
	AgentVersion      string         `json:"agent_version"`
	Mode              string         `json:"mode"`
	StateDir          sql.NullString `json:"state_dir"`
	CatalogStatus     sql.NullString `json:"catalog_status"`
	RepositoryID      sql.NullString `json:"repository_id"`
	ChunkGeneration   int64          `json:"chunk_generation"`
	ConfigGeneration  int64          `json:"config_generation"`
	CommandGeneration int64          `json:"command_generation"`
	RunningRunID      sql.NullInt64  `json:"running_run_id"`
	LastError         sql.NullString `json:"last_error"`
	LastDroppedReason sql.NullString `json:"last_dropped_reason"`
	LastDroppedAt     sql.NullTime   `json:"last_dropped_at"`
	LastSeenAt        time.Time      `json:"last_seen_at"`
}

type AgentCommand struct {
	ID         int64           `json:"id"`
	HostID     int64           `json:"host_id"`
	JobID      sql.NullInt64   `json:"job_id"`
	RunID      sql.NullInt64   `json:"run_id"`
	Type       string          `json:"type"`
	Status     string          `json:"status"`
	Payload    json.RawMessage `json:"payload"`
	Reason     sql.NullString  `json:"reason"`
	CreatedBy  sql.NullString  `json:"created_by"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
	ExpiresAt  time.Time       `json:"expires_at"`
	ClaimedAt  sql.NullTime    `json:"claimed_at"`
	FinishedAt sql.NullTime    `json:"finished_at"`
}

func (s *Store) ensureAgentCommandColumns(ctx context.Context) error {
	columns := []struct {
		name string
		stmt string
	}{
		{"run_id", `ALTER TABLE agent_commands ADD COLUMN run_id INTEGER REFERENCES runs(id) ON DELETE SET NULL`},
	}
	for _, column := range columns {
		exists, err := s.columnExists(ctx, "agent_commands", column.name)
		if err != nil {
			return err
		}
		if exists {
			continue
		}
		if _, err := s.db.ExecContext(ctx, column.stmt); err != nil {
			return fmt.Errorf("add agent_commands.%s column: %w", column.name, err)
		}
	}
	return nil
}

type CreateAgentCommandInput struct {
	HostID    int64
	JobID     sql.NullInt64
	Type      string
	Payload   json.RawMessage
	CreatedBy string
	ExpiresAt time.Time
}

func (s *Store) ensureRepositoryState(ctx context.Context) error {
	repositoryID, err := randomRepositoryID()
	if err != nil {
		return err
	}
	defaults := map[string]string{
		repositoryMetaIDKey:                    repositoryID,
		repositoryMetaChunkGenerationKey:       "0",
		repositoryMetaConfigGenerationKey:      "0",
		repositoryMetaCommandGenerationKey:     "0",
		repositoryMetaInvalidationAvailableKey: "0",
	}
	for key, value := range defaults {
		if _, err := s.db.ExecContext(ctx, `
			INSERT OR IGNORE INTO repository_meta (key, value, updated_at)
			VALUES (?, ?, CURRENT_TIMESTAMP)`, key, value); err != nil {
			return fmt.Errorf("initialize repository meta %s: %w", key, err)
		}
	}
	return nil
}

func (s *Store) ensureAgentHeartbeatColumns(ctx context.Context) error {
	columns := []struct {
		name string
		stmt string
	}{
		{"mode", `ALTER TABLE agent_heartbeats ADD COLUMN mode TEXT NOT NULL DEFAULT 'once'`},
		{"state_dir", `ALTER TABLE agent_heartbeats ADD COLUMN state_dir TEXT`},
		{"catalog_status", `ALTER TABLE agent_heartbeats ADD COLUMN catalog_status TEXT`},
		{"repository_id", `ALTER TABLE agent_heartbeats ADD COLUMN repository_id TEXT`},
		{"chunk_generation", `ALTER TABLE agent_heartbeats ADD COLUMN chunk_generation INTEGER NOT NULL DEFAULT 0`},
		{"config_generation", `ALTER TABLE agent_heartbeats ADD COLUMN config_generation INTEGER NOT NULL DEFAULT 0`},
		{"command_generation", `ALTER TABLE agent_heartbeats ADD COLUMN command_generation INTEGER NOT NULL DEFAULT 0`},
		{"running_run_id", `ALTER TABLE agent_heartbeats ADD COLUMN running_run_id INTEGER`},
		{"last_error", `ALTER TABLE agent_heartbeats ADD COLUMN last_error TEXT`},
		{"last_dropped_reason", `ALTER TABLE agent_heartbeats ADD COLUMN last_dropped_reason TEXT`},
		{"last_dropped_at", `ALTER TABLE agent_heartbeats ADD COLUMN last_dropped_at DATETIME`},
	}
	for _, column := range columns {
		exists, err := s.columnExists(ctx, "agent_heartbeats", column.name)
		if err != nil {
			return err
		}
		if exists {
			continue
		}
		if _, err := s.db.ExecContext(ctx, column.stmt); err != nil {
			return fmt.Errorf("add agent_heartbeats.%s column: %w", column.name, err)
		}
	}
	return nil
}

func randomRepositoryID() (string, error) {
	var raw [18]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate repository id: %w", err)
	}
	return "repo_" + base64.RawURLEncoding.EncodeToString(raw[:]), nil
}

func (s *Store) RepositoryState(ctx context.Context) (RepositoryState, error) {
	if err := s.ensureRepositoryState(ctx); err != nil {
		return RepositoryState{}, err
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT key, value
		FROM repository_meta
		WHERE key IN (?, ?, ?, ?, ?)`,
		repositoryMetaIDKey,
		repositoryMetaChunkGenerationKey,
		repositoryMetaConfigGenerationKey,
		repositoryMetaCommandGenerationKey,
		repositoryMetaInvalidationAvailableKey)
	if err != nil {
		return RepositoryState{}, fmt.Errorf("load repository meta: %w", err)
	}
	defer rows.Close()

	values := make(map[string]string)
	for rows.Next() {
		var key string
		var value string
		if err := rows.Scan(&key, &value); err != nil {
			return RepositoryState{}, err
		}
		values[key] = value
	}
	if err := rows.Err(); err != nil {
		return RepositoryState{}, err
	}
	parse := func(key string) (int64, error) {
		var value int64
		if _, err := fmt.Sscan(values[key], &value); err != nil {
			return 0, fmt.Errorf("parse repository meta %s=%q: %w", key, values[key], err)
		}
		return value, nil
	}
	chunkGeneration, err := parse(repositoryMetaChunkGenerationKey)
	if err != nil {
		return RepositoryState{}, err
	}
	configGeneration, err := parse(repositoryMetaConfigGenerationKey)
	if err != nil {
		return RepositoryState{}, err
	}
	commandGeneration, err := parse(repositoryMetaCommandGenerationKey)
	if err != nil {
		return RepositoryState{}, err
	}
	invalidationAvailableFrom, err := parse(repositoryMetaInvalidationAvailableKey)
	if err != nil {
		return RepositoryState{}, err
	}
	return RepositoryState{
		RepositoryID:              values[repositoryMetaIDKey],
		ChunkGeneration:           chunkGeneration,
		ConfigGeneration:          configGeneration,
		CommandGeneration:         commandGeneration,
		InvalidationAvailableFrom: invalidationAvailableFrom,
	}, nil
}

func (s *Store) BumpConfigGeneration(ctx context.Context) (int64, error) {
	return s.bumpRepositoryCounter(ctx, repositoryMetaConfigGenerationKey)
}

func (s *Store) bumpCommandGeneration(ctx context.Context) (int64, error) {
	return s.bumpRepositoryCounter(ctx, repositoryMetaCommandGenerationKey)
}

func (s *Store) bumpRepositoryCounter(ctx context.Context, key string) (int64, error) {
	if err := s.ensureRepositoryState(ctx); err != nil {
		return 0, err
	}
	if _, err := s.db.ExecContext(ctx, `
		UPDATE repository_meta
		SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT), updated_at = CURRENT_TIMESTAMP
		WHERE key = ?`, key); err != nil {
		return 0, fmt.Errorf("bump repository meta %s: %w", key, err)
	}
	var value int64
	if err := s.db.QueryRowContext(ctx, `SELECT CAST(value AS INTEGER) FROM repository_meta WHERE key = ?`, key).Scan(&value); err != nil {
		return 0, fmt.Errorf("read bumped repository meta %s: %w", key, err)
	}
	return value, nil
}

func (s *Store) BumpChunkGeneration(ctx context.Context, hashes []string, reason string, now time.Time) (int64, error) {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "unknown"
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin chunk generation transaction: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
		UPDATE repository_meta
		SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT), updated_at = CURRENT_TIMESTAMP
		WHERE key = ?`, repositoryMetaChunkGenerationKey); err != nil {
		return 0, fmt.Errorf("bump chunk generation: %w", err)
	}
	var generation int64
	if err := tx.QueryRowContext(ctx, `SELECT CAST(value AS INTEGER) FROM repository_meta WHERE key = ?`, repositoryMetaChunkGenerationKey).Scan(&generation); err != nil {
		return 0, fmt.Errorf("read chunk generation: %w", err)
	}
	for _, hash := range hashes {
		hash = strings.TrimSpace(hash)
		if hash == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO chunk_invalidations (generation, hash, reason, created_at)
			VALUES (?, ?, ?, ?)`, generation, hash, reason, now.UTC()); err != nil {
			return 0, fmt.Errorf("record chunk invalidation %s: %w", hash, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit chunk generation transaction: %w", err)
	}
	return generation, nil
}

func (s *Store) PruneChunkInvalidations(ctx context.Context, cutoff time.Time) (int64, error) {
	if cutoff.IsZero() {
		return 0, nil
	}
	var maxDeleted sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `
		SELECT MAX(generation)
		FROM chunk_invalidations
		WHERE created_at < ?`, cutoff.UTC()).Scan(&maxDeleted); err != nil {
		return 0, fmt.Errorf("inspect chunk invalidations before prune: %w", err)
	}
	result, err := s.db.ExecContext(ctx, `
		DELETE FROM chunk_invalidations
		WHERE created_at < ?`, cutoff.UTC())
	if err != nil {
		return 0, fmt.Errorf("prune chunk invalidations: %w", err)
	}
	removed, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("get pruned chunk invalidation count: %w", err)
	}
	if maxDeleted.Valid {
		if _, err := s.db.ExecContext(ctx, `
			UPDATE repository_meta
			SET value = CAST(MAX(CAST(value AS INTEGER), ?) AS TEXT), updated_at = CURRENT_TIMESTAMP
			WHERE key = ?`, maxDeleted.Int64, repositoryMetaInvalidationAvailableKey); err != nil {
			return 0, fmt.Errorf("update invalidation available generation: %w", err)
		}
	}
	return removed, nil
}

func (s *Store) ChunkInvalidationsSince(ctx context.Context, sinceGeneration int64, maxHashes int) (ChunkInvalidations, error) {
	repositoryState, err := s.RepositoryState(ctx)
	if err != nil {
		return ChunkInvalidations{}, err
	}
	if sinceGeneration < 0 {
		sinceGeneration = 0
	}
	result := ChunkInvalidations{
		RepositoryState: repositoryState,
		FromGeneration:  sinceGeneration,
		ToGeneration:    repositoryState.ChunkGeneration,
		Hashes:          []string{},
		Complete:        true,
	}
	if sinceGeneration >= repositoryState.ChunkGeneration {
		return result, nil
	}
	if sinceGeneration < repositoryState.InvalidationAvailableFrom {
		result.Complete = false
		result.Reason = "journal_compacted"
		return result, nil
	}
	if maxHashes <= 0 {
		maxHashes = 100000
	}
	var count int64
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM chunk_invalidations
		WHERE generation > ? AND generation <= ?`, sinceGeneration, repositoryState.ChunkGeneration).Scan(&count); err != nil {
		return ChunkInvalidations{}, fmt.Errorf("count chunk invalidations: %w", err)
	}
	if count > int64(maxHashes) {
		result.Complete = false
		result.Reason = "response_limit_exceeded"
		return result, nil
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT hash
		FROM chunk_invalidations
		WHERE generation > ? AND generation <= ?
		ORDER BY generation ASC, hash ASC`, sinceGeneration, repositoryState.ChunkGeneration)
	if err != nil {
		return ChunkInvalidations{}, fmt.Errorf("list chunk invalidations: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var hash string
		if err := rows.Scan(&hash); err != nil {
			return ChunkInvalidations{}, err
		}
		result.Hashes = append(result.Hashes, hash)
	}
	return result, rows.Err()
}

func (s *Store) CreateAgentCommand(ctx context.Context, input CreateAgentCommandInput) (AgentCommand, error) {
	input.Type = strings.TrimSpace(input.Type)
	input.CreatedBy = strings.TrimSpace(input.CreatedBy)
	if input.HostID <= 0 {
		return AgentCommand{}, errors.New("agent command host_id is required")
	}
	if input.Type == "" {
		return AgentCommand{}, errors.New("agent command type is required")
	}
	if len(input.Payload) == 0 {
		input.Payload = json.RawMessage(`{}`)
	}
	if !json.Valid(input.Payload) {
		return AgentCommand{}, errors.New("agent command payload must be valid JSON")
	}
	if input.ExpiresAt.IsZero() {
		return AgentCommand{}, errors.New("agent command expires_at is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AgentCommand{}, fmt.Errorf("begin create agent command transaction: %w", err)
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `
		INSERT INTO agent_commands (host_id, job_id, type, status, payload, created_by, expires_at)
		VALUES (?, ?, ?, 'pending', ?, ?, ?)`,
		input.HostID, input.JobID, input.Type, string(input.Payload), nullString(input.CreatedBy), input.ExpiresAt.UTC())
	if err != nil {
		return AgentCommand{}, fmt.Errorf("create agent command: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return AgentCommand{}, fmt.Errorf("get created agent command id: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE repository_meta
		SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT), updated_at = CURRENT_TIMESTAMP
		WHERE key = ?`, repositoryMetaCommandGenerationKey); err != nil {
		return AgentCommand{}, fmt.Errorf("bump command generation: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return AgentCommand{}, fmt.Errorf("commit create agent command transaction: %w", err)
	}
	return s.GetAgentCommand(ctx, id)
}

func (s *Store) ExpireAgentCommands(ctx context.Context, now time.Time) (int64, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE agent_commands
		SET status = 'expired', updated_at = CURRENT_TIMESTAMP, finished_at = ?
		WHERE status IN ('pending', 'claimed')
			AND expires_at <= ?`, now.UTC(), now.UTC())
	if err != nil {
		return 0, fmt.Errorf("expire agent commands: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("get expired agent command count: %w", err)
	}
	if changed > 0 {
		if _, err := s.bumpCommandGeneration(ctx); err != nil {
			return 0, err
		}
	}
	return changed, nil
}

func (s *Store) ListPendingAgentCommands(ctx context.Context, hostID int64, limit int, now time.Time) ([]AgentCommand, error) {
	if hostID <= 0 {
		return nil, errors.New("agent command host_id is required")
	}
	if limit <= 0 {
		limit = 10
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, host_id, job_id, run_id, type, status, payload, reason, created_by, created_at, updated_at, expires_at, claimed_at, finished_at
		FROM agent_commands
		WHERE host_id = ?
			AND status = 'pending'
			AND expires_at > ?
		ORDER BY id ASC
		LIMIT ?`, hostID, now.UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("list pending agent commands: %w", err)
	}
	defer rows.Close()
	return scanAgentCommands(rows)
}

func (s *Store) GetAgentCommand(ctx context.Context, id int64) (AgentCommand, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, host_id, job_id, run_id, type, status, payload, reason, created_by, created_at, updated_at, expires_at, claimed_at, finished_at
		FROM agent_commands
		WHERE id = ?`, id)
	if err != nil {
		return AgentCommand{}, fmt.Errorf("get agent command %d: %w", id, err)
	}
	defer rows.Close()
	commands, err := scanAgentCommands(rows)
	if err != nil {
		return AgentCommand{}, err
	}
	if len(commands) == 0 {
		return AgentCommand{}, fmt.Errorf("agent command %d not found", id)
	}
	return commands[0], nil
}

func (s *Store) MarkAgentCommandRunning(ctx context.Context, id, hostID, runID int64, now time.Time) (AgentCommand, error) {
	if id <= 0 {
		return AgentCommand{}, errors.New("agent command id is required")
	}
	if hostID <= 0 {
		return AgentCommand{}, errors.New("agent command host_id is required")
	}
	if runID <= 0 {
		return AgentCommand{}, errors.New("agent command run_id is required")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE agent_commands
		SET status = 'running', run_id = ?, claimed_at = COALESCE(claimed_at, ?), updated_at = CURRENT_TIMESTAMP
		WHERE id = ? AND host_id = ? AND status IN ('pending', 'claimed') AND expires_at > ?`,
		runID, now.UTC(), id, hostID, now.UTC())
	if err != nil {
		return AgentCommand{}, fmt.Errorf("mark agent command running: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return AgentCommand{}, fmt.Errorf("get mark agent command running rows affected: %w", err)
	}
	if changed == 0 {
		command, getErr := s.GetAgentCommand(ctx, id)
		if getErr == nil && command.HostID == hostID && command.Status == "running" && command.RunID.Valid && command.RunID.Int64 == runID {
			return command, nil
		}
		return AgentCommand{}, fmt.Errorf("agent command %d is not runnable by this host", id)
	}
	if _, err := s.bumpCommandGeneration(ctx); err != nil {
		return AgentCommand{}, err
	}
	return s.GetAgentCommand(ctx, id)
}

func (s *Store) FinishAgentCommandForRun(ctx context.Context, hostID, runID int64, status, reason string, now time.Time) error {
	status = strings.TrimSpace(status)
	reason = strings.TrimSpace(reason)
	switch status {
	case "completed", "failed", "dropped", "expired":
	default:
		return fmt.Errorf("agent command finish status %q is not supported", status)
	}
	if hostID <= 0 || runID <= 0 {
		return nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE agent_commands
		SET status = ?, reason = ?, finished_at = ?, updated_at = CURRENT_TIMESTAMP
		WHERE host_id = ? AND run_id = ? AND status IN ('pending', 'claimed', 'running')`,
		status, nullString(reason), now.UTC(), hostID, runID)
	if err != nil {
		return fmt.Errorf("finish agent command for run: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get finish agent command rows affected: %w", err)
	}
	if changed > 0 {
		_, err = s.bumpCommandGeneration(ctx)
	}
	return err
}

func (s *Store) AckAgentCommand(ctx context.Context, id, hostID int64, status, reason string, now time.Time) (AgentCommand, error) {
	status = strings.TrimSpace(status)
	reason = strings.TrimSpace(reason)
	if id <= 0 {
		return AgentCommand{}, errors.New("agent command id is required")
	}
	if hostID <= 0 {
		return AgentCommand{}, errors.New("agent command host_id is required")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	var stmt string
	switch status {
	case "claimed":
		stmt = `
			UPDATE agent_commands
			SET status = 'claimed', reason = ?, claimed_at = ?, updated_at = CURRENT_TIMESTAMP
			WHERE id = ? AND host_id = ? AND status = 'pending' AND expires_at > ?`
	case "running":
		stmt = `
			UPDATE agent_commands
			SET status = 'running', reason = ?, claimed_at = COALESCE(claimed_at, ?), updated_at = CURRENT_TIMESTAMP
			WHERE id = ? AND host_id = ? AND status IN ('pending', 'claimed') AND expires_at > ?`
	case "completed", "failed", "dropped", "expired":
		stmt = `
			UPDATE agent_commands
			SET status = ?, reason = ?, finished_at = ?, updated_at = CURRENT_TIMESTAMP
			WHERE id = ? AND host_id = ? AND status IN ('pending', 'claimed', 'running')`
	default:
		return AgentCommand{}, fmt.Errorf("agent command status %q is not supported", status)
	}
	var result sql.Result
	var err error
	if status == "completed" || status == "failed" || status == "dropped" || status == "expired" {
		result, err = s.db.ExecContext(ctx, stmt, status, nullString(reason), now.UTC(), id, hostID)
	} else {
		result, err = s.db.ExecContext(ctx, stmt, nullString(reason), now.UTC(), id, hostID, now.UTC())
	}
	if err != nil {
		return AgentCommand{}, fmt.Errorf("ack agent command %d: %w", id, err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return AgentCommand{}, fmt.Errorf("get ack agent command rows affected: %w", err)
	}
	if changed == 0 {
		return AgentCommand{}, fmt.Errorf("agent command %d is not claimable by this host", id)
	}
	if _, err := s.bumpCommandGeneration(ctx); err != nil {
		return AgentCommand{}, err
	}
	if status == "dropped" {
		_ = s.RecordAgentDroppedCommand(ctx, hostID, reason, now)
	}
	return s.GetAgentCommand(ctx, id)
}

func (s *Store) RecordAgentDroppedCommand(ctx context.Context, hostID int64, reason string, now time.Time) error {
	if hostID <= 0 {
		return nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE agent_heartbeats
		SET last_dropped_reason = ?, last_dropped_at = ?
		WHERE token_subject = (
			SELECT subject
			FROM agent_credentials
			WHERE host_id = ?
			LIMIT 1
		)`, nullString(reason), now.UTC(), hostID)
	if err != nil {
		return fmt.Errorf("record dropped agent command: %w", err)
	}
	return nil
}

func (s *Store) GetAgentHeartbeat(ctx context.Context, subject string) (AgentHeartbeat, bool, error) {
	subject = strings.TrimSpace(subject)
	if subject == "" {
		return AgentHeartbeat{}, false, nil
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT token_subject, hostname, agent_version, mode, state_dir, catalog_status, repository_id,
			chunk_generation, config_generation, command_generation, running_run_id, last_error,
			last_dropped_reason, last_dropped_at, last_seen_at
		FROM agent_heartbeats
		WHERE token_subject = ?`, subject)
	if err != nil {
		return AgentHeartbeat{}, false, fmt.Errorf("get agent heartbeat: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return AgentHeartbeat{}, false, rows.Err()
	}
	heartbeat, err := scanAgentHeartbeat(rows)
	if err != nil {
		return AgentHeartbeat{}, false, err
	}
	return heartbeat, true, rows.Err()
}

func scanAgentHeartbeat(scanner interface{ Scan(dest ...any) error }) (AgentHeartbeat, error) {
	var heartbeat AgentHeartbeat
	if err := scanner.Scan(
		&heartbeat.TokenSubject,
		&heartbeat.Hostname,
		&heartbeat.AgentVersion,
		&heartbeat.Mode,
		&heartbeat.StateDir,
		&heartbeat.CatalogStatus,
		&heartbeat.RepositoryID,
		&heartbeat.ChunkGeneration,
		&heartbeat.ConfigGeneration,
		&heartbeat.CommandGeneration,
		&heartbeat.RunningRunID,
		&heartbeat.LastError,
		&heartbeat.LastDroppedReason,
		&heartbeat.LastDroppedAt,
		&heartbeat.LastSeenAt,
	); err != nil {
		return AgentHeartbeat{}, err
	}
	return heartbeat, nil
}

func scanAgentCommands(rows *sql.Rows) ([]AgentCommand, error) {
	commands := make([]AgentCommand, 0)
	for rows.Next() {
		var command AgentCommand
		var payload string
		if err := rows.Scan(
			&command.ID,
			&command.HostID,
			&command.JobID,
			&command.RunID,
			&command.Type,
			&command.Status,
			&payload,
			&command.Reason,
			&command.CreatedBy,
			&command.CreatedAt,
			&command.UpdatedAt,
			&command.ExpiresAt,
			&command.ClaimedAt,
			&command.FinishedAt,
		); err != nil {
			return nil, err
		}
		command.Payload = json.RawMessage(payload)
		if len(command.Payload) == 0 {
			command.Payload = json.RawMessage(`{}`)
		}
		commands = append(commands, command)
	}
	return commands, rows.Err()
}

func nullString(value string) sql.NullString {
	value = strings.TrimSpace(value)
	return sql.NullString{String: value, Valid: value != ""}
}
