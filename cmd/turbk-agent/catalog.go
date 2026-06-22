package main

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

type agentCatalog struct {
	db       *sql.DB
	lockFile *os.File
}

type catalogServerState struct {
	ServerURL                  string
	ClientID                   string
	RepositoryID               string
	ChunkGeneration            int64
	LastInvalidationGeneration int64
	ConfigGeneration           int64
	CommandGeneration          int64
}

type catalogFileRecord struct {
	RootID      string
	Path        string
	Type        string
	Size        int64
	Mode        int64
	UID         int64
	GID         int64
	MTimeNS     int64
	Dev         int64
	Inode       int64
	LinkTarget  string
	Fingerprint string
}

type catalogChunkRecord struct {
	Hash         string
	OriginalSize int64
}

func openAgentCatalog(stateDir string) (*agentCatalog, error) {
	stateDir = strings.TrimSpace(stateDir)
	if stateDir == "" {
		return nil, errors.New("agent state dir is required")
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return nil, fmt.Errorf("create agent state dir %q: %w", stateDir, err)
	}
	lockFile, err := os.OpenFile(filepath.Join(stateDir, "agent.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open agent state lock: %w", err)
	}
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lockFile.Close()
		return nil, fmt.Errorf("lock agent state dir %q: %w", stateDir, err)
	}
	db, err := sql.Open("sqlite", filepath.Join(stateDir, "catalog.db"))
	if err != nil {
		_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
		_ = lockFile.Close()
		return nil, fmt.Errorf("open agent catalog: %w", err)
	}
	catalog := &agentCatalog{db: db, lockFile: lockFile}
	if err := catalog.init(); err != nil {
		_ = catalog.Close()
		return nil, err
	}
	return catalog, nil
}

func (c *agentCatalog) init() error {
	pragmas := []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA foreign_keys = ON",
		"PRAGMA busy_timeout = 5000",
	}
	for _, stmt := range pragmas {
		if _, err := c.db.Exec(stmt); err != nil {
			return fmt.Errorf("apply catalog pragma %q: %w", stmt, err)
		}
	}
	for _, stmt := range agentCatalogSchema {
		if _, err := c.db.Exec(stmt); err != nil {
			return fmt.Errorf("apply agent catalog schema: %w", err)
		}
	}
	return nil
}

func (c *agentCatalog) Close() error {
	var err error
	if c == nil {
		return nil
	}
	if c.db != nil {
		err = c.db.Close()
	}
	if c.lockFile != nil {
		if flockErr := syscall.Flock(int(c.lockFile.Fd()), syscall.LOCK_UN); err == nil {
			err = flockErr
		}
		if closeErr := c.lockFile.Close(); err == nil {
			err = closeErr
		}
	}
	return err
}

func (c *agentCatalog) serverState(serverURL, clientID string) (catalogServerState, bool, error) {
	var state catalogServerState
	err := c.db.QueryRow(`
		SELECT server_url, client_id, repository_id, chunk_generation, last_invalidation_generation, config_generation, command_generation
		FROM server_state
		WHERE server_url = ? AND client_id = ?`, serverURL, clientID).
		Scan(&state.ServerURL, &state.ClientID, &state.RepositoryID, &state.ChunkGeneration, &state.LastInvalidationGeneration, &state.ConfigGeneration, &state.CommandGeneration)
	if err == sql.ErrNoRows {
		return catalogServerState{}, false, nil
	}
	if err != nil {
		return catalogServerState{}, false, fmt.Errorf("load catalog server state: %w", err)
	}
	return state, true, nil
}

func (c *agentCatalog) upsertServerState(state catalogServerState) error {
	_, err := c.db.Exec(`
		INSERT INTO server_state (server_url, client_id, repository_id, chunk_generation, last_invalidation_generation, config_generation, command_generation, last_heartbeat_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(server_url, client_id) DO UPDATE SET
			repository_id = excluded.repository_id,
			chunk_generation = excluded.chunk_generation,
			last_invalidation_generation = excluded.last_invalidation_generation,
			config_generation = excluded.config_generation,
			command_generation = excluded.command_generation,
			last_heartbeat_at = CURRENT_TIMESTAMP`,
		state.ServerURL, state.ClientID, state.RepositoryID, state.ChunkGeneration, state.LastInvalidationGeneration, state.ConfigGeneration, state.CommandGeneration)
	if err != nil {
		return fmt.Errorf("upsert catalog server state: %w", err)
	}
	return nil
}

func (c *agentCatalog) resetServerChunks() error {
	_, err := c.db.Exec(`
		UPDATE server_chunks
		SET status = 'unknown', confirmed_generation = NULL, updated_at = CURRENT_TIMESTAMP`)
	if err != nil {
		return fmt.Errorf("reset catalog server chunks: %w", err)
	}
	return nil
}

func (c *agentCatalog) applyInvalidations(hashes []string, generation int64) error {
	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("begin invalidation transaction: %w", err)
	}
	defer tx.Rollback()
	for _, hash := range hashes {
		hash = strings.TrimSpace(hash)
		if hash == "" {
			continue
		}
		if _, err := tx.Exec(`
			INSERT INTO server_chunks (hash, original_size, status, updated_at)
			VALUES (?, 0, 'unknown', CURRENT_TIMESTAMP)
			ON CONFLICT(hash) DO UPDATE SET
				status = 'unknown',
				confirmed_generation = NULL,
				updated_at = CURRENT_TIMESTAMP`, hash); err != nil {
			return fmt.Errorf("mark invalidated chunk %s: %w", hash, err)
		}
	}
	if _, err := tx.Exec(`
		UPDATE server_chunks
		SET confirmed_generation = ?, updated_at = CURRENT_TIMESTAMP
		WHERE status = 'confirmed'`, generation); err != nil {
		return fmt.Errorf("advance confirmed chunk generation: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit invalidation transaction: %w", err)
	}
	return nil
}

func (c *agentCatalog) fileRecord(rootID, path string) (catalogFileRecord, bool, error) {
	var record catalogFileRecord
	err := c.db.QueryRow(`
		SELECT root_id, path, type, size, mode, uid, gid, mtime_ns, COALESCE(dev, 0), COALESCE(inode, 0), COALESCE(link_target, ''), COALESCE(chunk_fingerprint, '')
		FROM files
		WHERE root_id = ? AND path = ?`, rootID, path).
		Scan(&record.RootID, &record.Path, &record.Type, &record.Size, &record.Mode, &record.UID, &record.GID, &record.MTimeNS, &record.Dev, &record.Inode, &record.LinkTarget, &record.Fingerprint)
	if err == sql.ErrNoRows {
		return catalogFileRecord{}, false, nil
	}
	if err != nil {
		return catalogFileRecord{}, false, fmt.Errorf("load catalog file record %s: %w", path, err)
	}
	return record, true, nil
}

func (c *agentCatalog) fileChunks(rootID, path string) ([]catalogChunkRecord, error) {
	rows, err := c.db.Query(`
		SELECT hash, original_size
		FROM file_chunks
		WHERE root_id = ? AND path = ?
		ORDER BY ordinal ASC`, rootID, path)
	if err != nil {
		return nil, fmt.Errorf("load file chunks %s: %w", path, err)
	}
	defer rows.Close()
	chunks := make([]catalogChunkRecord, 0)
	for rows.Next() {
		var chunk catalogChunkRecord
		if err := rows.Scan(&chunk.Hash, &chunk.OriginalSize); err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
	}
	return chunks, rows.Err()
}

func (c *agentCatalog) replaceFile(record catalogFileRecord, chunks []catalogChunkRecord) error {
	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("begin replace file transaction: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`
		INSERT INTO files (root_id, path, type, size, mode, uid, gid, mtime_ns, dev, inode, link_target, chunk_fingerprint, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(root_id, path) DO UPDATE SET
			type = excluded.type,
			size = excluded.size,
			mode = excluded.mode,
			uid = excluded.uid,
			gid = excluded.gid,
			mtime_ns = excluded.mtime_ns,
			dev = excluded.dev,
			inode = excluded.inode,
			link_target = excluded.link_target,
			chunk_fingerprint = excluded.chunk_fingerprint,
			updated_at = CURRENT_TIMESTAMP`,
		record.RootID, record.Path, record.Type, record.Size, record.Mode, record.UID, record.GID, record.MTimeNS, record.Dev, record.Inode, nullText(record.LinkTarget), record.Fingerprint); err != nil {
		return fmt.Errorf("upsert file record %s: %w", record.Path, err)
	}
	if _, err := tx.Exec(`DELETE FROM file_chunks WHERE root_id = ? AND path = ?`, record.RootID, record.Path); err != nil {
		return fmt.Errorf("replace file chunks %s: %w", record.Path, err)
	}
	for ordinal, chunk := range chunks {
		if _, err := tx.Exec(`
			INSERT INTO file_chunks (root_id, path, ordinal, hash, original_size)
			VALUES (?, ?, ?, ?, ?)`, record.RootID, record.Path, ordinal, chunk.Hash, chunk.OriginalSize); err != nil {
			return fmt.Errorf("insert file chunk %s:%d: %w", record.Path, ordinal, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit replace file transaction: %w", err)
	}
	return nil
}

func (c *agentCatalog) chunkStatus(hash string) (status string, confirmedGeneration int64, ok bool, err error) {
	err = c.db.QueryRow(`
		SELECT status, COALESCE(confirmed_generation, 0)
		FROM server_chunks
		WHERE hash = ?`, hash).Scan(&status, &confirmedGeneration)
	if err == sql.ErrNoRows {
		return "", 0, false, nil
	}
	if err != nil {
		return "", 0, false, fmt.Errorf("load server chunk %s: %w", hash, err)
	}
	return status, confirmedGeneration, true, nil
}

func (c *agentCatalog) markChunk(hash string, originalSize int64, status string, generation int64, uploaded bool) error {
	_, err := c.db.Exec(`
		INSERT INTO server_chunks (hash, original_size, status, confirmed_generation, last_checked_at, last_uploaded_at, updated_at)
		VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(hash) DO UPDATE SET
			original_size = CASE WHEN excluded.original_size > 0 THEN excluded.original_size ELSE server_chunks.original_size END,
			status = excluded.status,
			confirmed_generation = excluded.confirmed_generation,
			last_checked_at = CURRENT_TIMESTAMP,
			last_uploaded_at = CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE server_chunks.last_uploaded_at END,
			updated_at = CURRENT_TIMESTAMP`,
		hash, originalSize, status, sql.NullInt64{Int64: generation, Valid: generation > 0}, uploadedAt(uploaded), uploaded)
	if err != nil {
		return fmt.Errorf("mark server chunk %s: %w", hash, err)
	}
	return nil
}

func (c *agentCatalog) recordRunStart(localRunID string, serverRunID, commandID int64, trigger string, started time.Time) error {
	if started.IsZero() {
		started = time.Now().UTC()
	}
	_, err := c.db.Exec(`
		INSERT INTO agent_runs (local_run_id, server_run_id, command_id, trigger, status, started_at)
		VALUES (?, ?, ?, ?, 'running', ?)`,
		localRunID,
		sql.NullInt64{Int64: serverRunID, Valid: serverRunID > 0},
		sql.NullInt64{Int64: commandID, Valid: commandID > 0},
		trigger,
		started.UTC())
	if err != nil {
		return fmt.Errorf("record agent run start: %w", err)
	}
	return nil
}

func (c *agentCatalog) recordRunFinish(localRunID, status, message string, finished time.Time) error {
	if finished.IsZero() {
		finished = time.Now().UTC()
	}
	_, err := c.db.Exec(`
		UPDATE agent_runs
		SET status = ?, message = ?, finished_at = ?
		WHERE local_run_id = ?`,
		status, nullText(message), finished.UTC(), localRunID)
	if err != nil {
		return fmt.Errorf("record agent run finish: %w", err)
	}
	return nil
}

func catalogFileMatches(a, b catalogFileRecord) bool {
	return a.RootID == b.RootID &&
		a.Path == b.Path &&
		a.Type == b.Type &&
		a.Size == b.Size &&
		a.Mode == b.Mode &&
		a.UID == b.UID &&
		a.GID == b.GID &&
		a.MTimeNS == b.MTimeNS &&
		a.Dev == b.Dev &&
		a.Inode == b.Inode &&
		a.LinkTarget == b.LinkTarget
}

func nullText(value string) sql.NullString {
	return sql.NullString{String: value, Valid: value != ""}
}

func uploadedAt(uploaded bool) sql.NullTime {
	now := time.Now().UTC()
	return sql.NullTime{Time: now, Valid: uploaded}
}

var agentCatalogSchema = []string{
	`CREATE TABLE IF NOT EXISTS agent_meta (
		key TEXT PRIMARY KEY,
		value TEXT NOT NULL,
		updated_at DATETIME NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS server_state (
		server_url TEXT NOT NULL,
		client_id TEXT NOT NULL,
		repository_id TEXT NOT NULL,
		chunk_generation INTEGER NOT NULL,
		last_invalidation_generation INTEGER NOT NULL,
		config_generation INTEGER NOT NULL,
		command_generation INTEGER NOT NULL,
		last_heartbeat_at DATETIME,
		PRIMARY KEY (server_url, client_id)
	)`,
	`CREATE TABLE IF NOT EXISTS files (
		root_id TEXT NOT NULL,
		path TEXT NOT NULL,
		type TEXT NOT NULL,
		size INTEGER NOT NULL,
		mode INTEGER NOT NULL,
		uid INTEGER NOT NULL,
		gid INTEGER NOT NULL,
		mtime_ns INTEGER NOT NULL,
		dev INTEGER,
		inode INTEGER,
		link_target TEXT,
		chunk_fingerprint TEXT,
		updated_at DATETIME NOT NULL,
		PRIMARY KEY (root_id, path)
	)`,
	`CREATE TABLE IF NOT EXISTS file_chunks (
		root_id TEXT NOT NULL,
		path TEXT NOT NULL,
		ordinal INTEGER NOT NULL,
		hash TEXT NOT NULL,
		original_size INTEGER NOT NULL,
		PRIMARY KEY (root_id, path, ordinal)
	)`,
	`CREATE INDEX IF NOT EXISTS idx_file_chunks_hash ON file_chunks(hash)`,
	`CREATE TABLE IF NOT EXISTS server_chunks (
		hash TEXT PRIMARY KEY,
		original_size INTEGER NOT NULL,
		status TEXT NOT NULL,
		confirmed_generation INTEGER,
		last_checked_at DATETIME,
		last_uploaded_at DATETIME,
		updated_at DATETIME NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS agent_runs (
		local_run_id TEXT PRIMARY KEY,
		server_run_id INTEGER,
		command_id INTEGER,
		trigger TEXT NOT NULL,
		status TEXT NOT NULL,
		started_at DATETIME NOT NULL,
		finished_at DATETIME,
		message TEXT
	)`,
}
