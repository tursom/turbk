package main

import (
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/cockroachdb/pebble"

	_ "modernc.org/sqlite"
)

type agentCatalog struct {
	db           *sql.DB
	lockFile     *os.File
	chunkCatalog agentChunkCatalog
	fileCatalog  agentFileCatalog
	stats        agentCatalogStats
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

type agentChunkStatusUpdate struct {
	Hash         string
	OriginalSize int64
	Status       string
	Generation   int64
	Uploaded     bool
}

type agentFileCatalogUpdate struct {
	Record catalogFileRecord
	Chunks []catalogChunkRecord
}

type agentCatalogStats struct {
	ChunkStatusUpdates int64
	ChunkInvalidations int64
	ChunkResets        int64
	FileRecords        int64
	FileChunkRefs      int64
}

type agentChunkCatalog interface {
	chunkStatus(hash string) (status string, confirmedGeneration int64, ok bool, err error)
	markChunk(hash string, originalSize int64, status string, generation int64, uploaded bool) error
	markChunks(updates []agentChunkStatusUpdate) error
	applyInvalidations(hashes []string, generation int64) error
	resetServerChunks() error
	flush() error
	Close() error
}

type agentFileCatalog interface {
	fileRecord(rootID, path string) (catalogFileRecord, bool, error)
	fileChunks(rootID, path string) ([]catalogChunkRecord, error)
	fileRecordWithChunks(rootID, path string) (catalogFileRecord, []catalogChunkRecord, bool, error)
	replaceFile(record catalogFileRecord, chunks []catalogChunkRecord) error
	replaceFiles(updates []agentFileCatalogUpdate) error
	flush() error
	Close() error
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
	chunkCatalog, fileCatalog, err := openAgentCatalogBackends(stateDir, db, agentCatalogBackendFromEnv())
	if err != nil {
		_ = catalog.Close()
		return nil, err
	}
	catalog.chunkCatalog = chunkCatalog
	catalog.fileCatalog = fileCatalog
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
	if c.chunkCatalog != nil {
		err = c.chunkCatalog.Close()
	}
	if c.fileCatalog != nil && any(c.fileCatalog) != any(c.chunkCatalog) {
		if fileErr := c.fileCatalog.Close(); err == nil {
			err = fileErr
		}
	}
	if c.db != nil {
		if dbErr := c.db.Close(); err == nil {
			err = dbErr
		}
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
	if c.chunkCatalog == nil {
		return nil
	}
	c.stats.ChunkResets++
	return c.chunkCatalog.resetServerChunks()
}

func (c *agentCatalog) applyInvalidations(hashes []string, generation int64) error {
	if c.chunkCatalog == nil {
		return nil
	}
	c.stats.ChunkInvalidations += int64(len(hashes))
	return c.chunkCatalog.applyInvalidations(hashes, generation)
}

func (c *agentCatalog) fileRecord(rootID, path string) (catalogFileRecord, bool, error) {
	if c.fileCatalog == nil {
		return catalogFileRecord{}, false, nil
	}
	return c.fileCatalog.fileRecord(rootID, path)
}

func (c *agentCatalog) fileChunks(rootID, path string) ([]catalogChunkRecord, error) {
	if c.fileCatalog == nil {
		return nil, nil
	}
	return c.fileCatalog.fileChunks(rootID, path)
}

func (c *agentCatalog) fileRecordWithChunks(rootID, path string) (catalogFileRecord, []catalogChunkRecord, bool, error) {
	if c.fileCatalog == nil {
		return catalogFileRecord{}, nil, false, nil
	}
	return c.fileCatalog.fileRecordWithChunks(rootID, path)
}

func (c *agentCatalog) replaceFile(record catalogFileRecord, chunks []catalogChunkRecord) error {
	return c.replaceFiles([]agentFileCatalogUpdate{{Record: record, Chunks: chunks}})
}

func (c *agentCatalog) replaceFiles(updates []agentFileCatalogUpdate) error {
	if c.fileCatalog == nil || len(updates) == 0 {
		return nil
	}
	for _, update := range updates {
		c.stats.FileRecords++
		c.stats.FileChunkRefs += int64(len(update.Chunks))
	}
	return c.fileCatalog.replaceFiles(updates)
}

func (c *agentCatalog) chunkStatus(hash string) (status string, confirmedGeneration int64, ok bool, err error) {
	if c.chunkCatalog == nil {
		return "", 0, false, nil
	}
	return c.chunkCatalog.chunkStatus(hash)
}

func (c *agentCatalog) markChunk(hash string, originalSize int64, status string, generation int64, uploaded bool) error {
	if c.chunkCatalog == nil {
		return nil
	}
	c.stats.ChunkStatusUpdates++
	return c.chunkCatalog.markChunk(hash, originalSize, status, generation, uploaded)
}

func (c *agentCatalog) markChunks(updates []agentChunkStatusUpdate) error {
	if c.chunkCatalog == nil {
		return nil
	}
	c.stats.ChunkStatusUpdates += int64(len(updates))
	return c.chunkCatalog.markChunks(updates)
}

func (c *agentCatalog) flush() error {
	if c == nil {
		return nil
	}
	var err error
	if c.chunkCatalog != nil {
		err = c.chunkCatalog.flush()
	}
	if c.fileCatalog != nil && any(c.fileCatalog) != any(c.chunkCatalog) {
		if fileErr := c.fileCatalog.flush(); err == nil {
			err = fileErr
		}
	}
	return err
}

func (c *agentCatalog) statsSnapshot() agentCatalogStats {
	if c == nil {
		return agentCatalogStats{}
	}
	return c.stats
}

func (s agentCatalogStats) delta(previous agentCatalogStats) agentCatalogStats {
	return agentCatalogStats{
		ChunkStatusUpdates: s.ChunkStatusUpdates - previous.ChunkStatusUpdates,
		ChunkInvalidations: s.ChunkInvalidations - previous.ChunkInvalidations,
		ChunkResets:        s.ChunkResets - previous.ChunkResets,
		FileRecords:        s.FileRecords - previous.FileRecords,
		FileChunkRefs:      s.FileChunkRefs - previous.FileChunkRefs,
	}
}

func (s agentCatalogStats) any() bool {
	return s.ChunkStatusUpdates != 0 ||
		s.ChunkInvalidations != 0 ||
		s.ChunkResets != 0 ||
		s.FileRecords != 0 ||
		s.FileChunkRefs != 0
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

const (
	agentCatalogBackendSQLite = "sqlite"
	agentCatalogBackendHybrid = "hybrid"

	agentPebbleRecordChunkStatus = byte(0x01)
	agentPebbleRecordFile        = byte(0x02)
	agentPebbleValueVersion      = byte(0x01)

	agentChunkStatusUnknown   = byte(0x01)
	agentChunkStatusConfirmed = byte(0x02)
	agentChunkStatusMissing   = byte(0x03)

	agentFileEntryTypeFile    = byte(0x01)
	agentFileEntryTypeDir     = byte(0x02)
	agentFileEntryTypeSymlink = byte(0x03)
)

type sqliteChunkCatalog struct {
	db *sql.DB
}

type sqliteFileCatalog struct {
	db *sql.DB
}

type pebbleChunkCatalog struct {
	db *pebble.DB
}

type pebbleChunkStatusRecord struct {
	Status              string
	OriginalSize        int64
	ConfirmedGeneration int64
	LastCheckedUnix     int64
	LastUploadedUnix    int64
}

func agentCatalogBackendFromEnv() string {
	backend := strings.ToLower(strings.TrimSpace(os.Getenv("TURBK_AGENT_CATALOG_BACKEND")))
	if backend == "" {
		return agentCatalogBackendHybrid
	}
	return backend
}

func openAgentCatalogBackends(stateDir string, db *sql.DB, backend string) (agentChunkCatalog, agentFileCatalog, error) {
	switch backend {
	case agentCatalogBackendSQLite:
		return &sqliteChunkCatalog{db: db}, &sqliteFileCatalog{db: db}, nil
	case agentCatalogBackendHybrid:
		pebbleCatalog, err := openPebbleChunkCatalog(filepath.Join(stateDir, "catalog.pebble"))
		if err != nil {
			return &sqliteChunkCatalog{db: db}, &sqliteFileCatalog{db: db}, nil
		}
		return pebbleCatalog, pebbleCatalog, nil
	case "pebble":
		return nil, nil, errors.New("agent catalog backend pebble is not supported yet; use hybrid or sqlite")
	default:
		return nil, nil, fmt.Errorf("unsupported agent catalog backend %q", backend)
	}
}

func openPebbleChunkCatalog(path string) (*pebbleChunkCatalog, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("open pebble chunk catalog: %w", err)
	}
	return &pebbleChunkCatalog{db: db}, nil
}

func (c *sqliteChunkCatalog) chunkStatus(hash string) (status string, confirmedGeneration int64, ok bool, err error) {
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

func (c *sqliteChunkCatalog) markChunk(hash string, originalSize int64, status string, generation int64, uploaded bool) error {
	return c.markChunks([]agentChunkStatusUpdate{{
		Hash:         hash,
		OriginalSize: originalSize,
		Status:       status,
		Generation:   generation,
		Uploaded:     uploaded,
	}})
}

func (c *sqliteChunkCatalog) markChunks(updates []agentChunkStatusUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("begin mark chunks transaction: %w", err)
	}
	defer tx.Rollback()
	for _, update := range updates {
		hash := strings.TrimSpace(update.Hash)
		if hash == "" {
			continue
		}
		status := strings.TrimSpace(update.Status)
		if _, err := tx.Exec(`
			INSERT INTO server_chunks (hash, original_size, status, confirmed_generation, last_checked_at, last_uploaded_at, updated_at)
			VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?, CURRENT_TIMESTAMP)
			ON CONFLICT(hash) DO UPDATE SET
				original_size = CASE WHEN excluded.original_size > 0 THEN excluded.original_size ELSE server_chunks.original_size END,
				status = excluded.status,
				confirmed_generation = excluded.confirmed_generation,
				last_checked_at = CURRENT_TIMESTAMP,
				last_uploaded_at = CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE server_chunks.last_uploaded_at END,
				updated_at = CURRENT_TIMESTAMP`,
			hash, update.OriginalSize, status, sql.NullInt64{Int64: update.Generation, Valid: update.Generation > 0}, uploadedAt(update.Uploaded), update.Uploaded); err != nil {
			return fmt.Errorf("mark server chunk %s: %w", hash, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit mark chunks transaction: %w", err)
	}
	return nil
}

func (c *sqliteChunkCatalog) applyInvalidations(hashes []string, generation int64) error {
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

func (c *sqliteChunkCatalog) resetServerChunks() error {
	_, err := c.db.Exec(`
		UPDATE server_chunks
		SET status = 'unknown', confirmed_generation = NULL, updated_at = CURRENT_TIMESTAMP`)
	if err != nil {
		return fmt.Errorf("reset catalog server chunks: %w", err)
	}
	return nil
}

func (c *sqliteChunkCatalog) flush() error {
	return nil
}

func (c *sqliteChunkCatalog) Close() error {
	return nil
}

func (c *sqliteFileCatalog) fileRecord(rootID, path string) (catalogFileRecord, bool, error) {
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

func (c *sqliteFileCatalog) fileChunks(rootID, path string) ([]catalogChunkRecord, error) {
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

func (c *sqliteFileCatalog) fileRecordWithChunks(rootID, path string) (catalogFileRecord, []catalogChunkRecord, bool, error) {
	record, ok, err := c.fileRecord(rootID, path)
	if err != nil || !ok {
		return catalogFileRecord{}, nil, false, err
	}
	chunks, err := c.fileChunks(rootID, path)
	if err != nil {
		return catalogFileRecord{}, nil, false, err
	}
	return record, chunks, true, nil
}

func (c *sqliteFileCatalog) replaceFile(record catalogFileRecord, chunks []catalogChunkRecord) error {
	return c.replaceFiles([]agentFileCatalogUpdate{{Record: record, Chunks: chunks}})
}

func (c *sqliteFileCatalog) replaceFiles(updates []agentFileCatalogUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("begin replace file transaction: %w", err)
	}
	defer tx.Rollback()
	for _, update := range updates {
		record := update.Record
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
		for ordinal, chunk := range update.Chunks {
			if _, err := tx.Exec(`
				INSERT INTO file_chunks (root_id, path, ordinal, hash, original_size)
				VALUES (?, ?, ?, ?, ?)`, record.RootID, record.Path, ordinal, chunk.Hash, chunk.OriginalSize); err != nil {
				return fmt.Errorf("insert file chunk %s:%d: %w", record.Path, ordinal, err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit replace file transaction: %w", err)
	}
	return nil
}

func (c *sqliteFileCatalog) flush() error {
	return nil
}

func (c *sqliteFileCatalog) Close() error {
	return nil
}

func (c *pebbleChunkCatalog) chunkStatus(hash string) (status string, confirmedGeneration int64, ok bool, err error) {
	key, err := encodePebbleChunkStatusKey(hash)
	if err != nil {
		return "", 0, false, err
	}
	record, ok, err := c.getRecord(key)
	if err != nil || !ok {
		return "", 0, false, err
	}
	return record.Status, record.ConfirmedGeneration, true, nil
}

func (c *pebbleChunkCatalog) markChunk(hash string, originalSize int64, status string, generation int64, uploaded bool) error {
	return c.markChunks([]agentChunkStatusUpdate{{
		Hash:         hash,
		OriginalSize: originalSize,
		Status:       status,
		Generation:   generation,
		Uploaded:     uploaded,
	}})
}

func (c *pebbleChunkCatalog) markChunks(updates []agentChunkStatusUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	batch := c.db.NewBatch()
	defer batch.Close()
	now := time.Now().UTC().Unix()
	for _, update := range updates {
		hash := strings.TrimSpace(update.Hash)
		if hash == "" {
			continue
		}
		key, err := encodePebbleChunkStatusKey(hash)
		if err != nil {
			return err
		}
		if update.OriginalSize < 0 {
			return fmt.Errorf("invalid original size %d for chunk %s", update.OriginalSize, hash)
		}
		record := pebbleChunkStatusRecord{
			Status:              strings.TrimSpace(update.Status),
			OriginalSize:        update.OriginalSize,
			ConfirmedGeneration: update.Generation,
			LastCheckedUnix:     now,
		}
		if record.Status == "unknown" {
			record.ConfirmedGeneration = 0
			record.LastCheckedUnix = 0
		}
		if update.Uploaded {
			record.LastUploadedUnix = now
		}
		value, err := encodePebbleChunkStatusValue(record)
		if err != nil {
			return err
		}
		if err := batch.Set(key, value, nil); err != nil {
			return fmt.Errorf("mark pebble chunk %s: %w", hash, err)
		}
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("commit pebble chunk batch: %w", err)
	}
	return nil
}

func (c *pebbleChunkCatalog) applyInvalidations(hashes []string, generation int64) error {
	batch := c.db.NewBatch()
	defer batch.Close()
	now := time.Now().UTC().Unix()
	invalidated := make(map[string]struct{}, len(hashes))
	for _, hash := range hashes {
		hash = strings.TrimSpace(hash)
		if hash == "" {
			continue
		}
		key, err := encodePebbleChunkStatusKey(hash)
		if err != nil {
			return err
		}
		record, _, err := c.getRecord(key)
		if err != nil {
			return err
		}
		record.Status = "unknown"
		record.ConfirmedGeneration = 0
		value, err := encodePebbleChunkStatusValue(record)
		if err != nil {
			return err
		}
		if err := batch.Set(key, value, nil); err != nil {
			return fmt.Errorf("mark invalidated pebble chunk %s: %w", hash, err)
		}
		invalidated[string(key)] = struct{}{}
	}

	iter, err := c.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{agentPebbleRecordChunkStatus},
		UpperBound: []byte{agentPebbleRecordChunkStatus + 1},
	})
	if err != nil {
		return fmt.Errorf("iterate pebble chunks: %w", err)
	}
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		key := append([]byte(nil), iter.Key()...)
		if _, ok := invalidated[string(key)]; ok {
			continue
		}
		record, ok := decodePebbleChunkStatusValue(iter.Value())
		if !ok {
			if err := batch.Delete(key, nil); err != nil {
				return fmt.Errorf("delete malformed pebble chunk status: %w", err)
			}
			continue
		}
		if record.Status != "confirmed" {
			continue
		}
		record.ConfirmedGeneration = generation
		record.LastCheckedUnix = now
		value, err := encodePebbleChunkStatusValue(record)
		if err != nil {
			return err
		}
		if err := batch.Set(key, value, nil); err != nil {
			return fmt.Errorf("advance pebble chunk generation: %w", err)
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterate pebble chunks: %w", err)
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("commit pebble invalidations: %w", err)
	}
	return nil
}

func (c *pebbleChunkCatalog) resetServerChunks() error {
	if err := c.db.DeleteRange([]byte{agentPebbleRecordChunkStatus}, []byte{agentPebbleRecordChunkStatus + 1}, pebble.NoSync); err != nil {
		return fmt.Errorf("reset pebble server chunks: %w", err)
	}
	return nil
}

func (c *pebbleChunkCatalog) flush() error {
	return c.db.Flush()
}

func (c *pebbleChunkCatalog) Close() error {
	return c.db.Close()
}

func (c *pebbleChunkCatalog) getRecord(key []byte) (pebbleChunkStatusRecord, bool, error) {
	value, closer, err := c.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return pebbleChunkStatusRecord{}, false, nil
	}
	if err != nil {
		return pebbleChunkStatusRecord{}, false, fmt.Errorf("load pebble chunk status: %w", err)
	}
	defer closer.Close()
	record, ok := decodePebbleChunkStatusValue(value)
	if !ok {
		if err := c.db.Delete(key, pebble.NoSync); err != nil {
			return pebbleChunkStatusRecord{}, false, fmt.Errorf("delete malformed pebble chunk status: %w", err)
		}
		return pebbleChunkStatusRecord{}, false, nil
	}
	return record, true, nil
}

func (c *pebbleChunkCatalog) fileRecord(rootID, path string) (catalogFileRecord, bool, error) {
	record, _, ok, err := c.fileRecordWithChunks(rootID, path)
	return record, ok, err
}

func (c *pebbleChunkCatalog) fileChunks(rootID, path string) ([]catalogChunkRecord, error) {
	_, chunks, ok, err := c.fileRecordWithChunks(rootID, path)
	if err != nil || !ok {
		return nil, err
	}
	return chunks, nil
}

func (c *pebbleChunkCatalog) fileRecordWithChunks(rootID, path string) (catalogFileRecord, []catalogChunkRecord, bool, error) {
	key := encodePebbleFileKey(rootID, path)
	value, closer, err := c.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return catalogFileRecord{}, nil, false, nil
	}
	if err != nil {
		return catalogFileRecord{}, nil, false, fmt.Errorf("load pebble file record %s: %w", path, err)
	}
	defer closer.Close()
	record, chunks, ok := decodePebbleFileValue(value)
	if !ok {
		if err := c.db.Delete(key, pebble.NoSync); err != nil {
			return catalogFileRecord{}, nil, false, fmt.Errorf("delete malformed pebble file record %s: %w", path, err)
		}
		return catalogFileRecord{}, nil, false, nil
	}
	record.RootID = filepath.Clean(rootID)
	record.Path = path
	return record, chunks, true, nil
}

func (c *pebbleChunkCatalog) replaceFile(record catalogFileRecord, chunks []catalogChunkRecord) error {
	return c.replaceFiles([]agentFileCatalogUpdate{{Record: record, Chunks: chunks}})
}

func (c *pebbleChunkCatalog) replaceFiles(updates []agentFileCatalogUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	batch := c.db.NewBatch()
	defer batch.Close()
	for _, update := range updates {
		key := encodePebbleFileKey(update.Record.RootID, update.Record.Path)
		value, err := encodePebbleFileValue(update.Record, update.Chunks)
		if err != nil {
			return err
		}
		if err := batch.Set(key, value, nil); err != nil {
			return fmt.Errorf("replace pebble file record %s: %w", update.Record.Path, err)
		}
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("commit pebble file records: %w", err)
	}
	return nil
}

func encodePebbleFileKey(rootID, path string) []byte {
	key := []byte{agentPebbleRecordFile}
	key = appendLengthPrefixedBytes(key, []byte(filepath.Clean(rootID)))
	key = appendLengthPrefixedBytes(key, []byte(path))
	return key
}

func decodePebbleFileKey(key []byte) (rootID string, path string, ok bool) {
	if len(key) == 0 || key[0] != agentPebbleRecordFile {
		return "", "", false
	}
	offset := 1
	rootBytes, ok := readLengthPrefixedBytes(key, &offset)
	if !ok {
		return "", "", false
	}
	pathBytes, ok := readLengthPrefixedBytes(key, &offset)
	if !ok || offset != len(key) {
		return "", "", false
	}
	return string(rootBytes), string(pathBytes), true
}

func encodePebbleFileValue(record catalogFileRecord, chunks []catalogChunkRecord) ([]byte, error) {
	entryType, err := encodeFileEntryType(record.Type)
	if err != nil {
		return nil, err
	}
	if record.Size < 0 {
		return nil, fmt.Errorf("invalid file size %d for %s", record.Size, record.Path)
	}
	if record.Mode < 0 || record.Mode > int64(^uint32(0)) {
		return nil, fmt.Errorf("invalid file mode %d for %s", record.Mode, record.Path)
	}
	value := []byte{agentPebbleValueVersion, entryType}
	value = appendUint64(value, uint64(record.Size))
	value = appendUint32(value, uint32(record.Mode))
	value = appendInt64(value, record.UID)
	value = appendInt64(value, record.GID)
	value = appendInt64(value, record.MTimeNS)
	value = appendInt64(value, record.Dev)
	value = appendInt64(value, record.Inode)
	value = appendLengthPrefixedBytes(value, []byte(record.LinkTarget))
	value = appendLengthPrefixedBytes(value, []byte(record.Fingerprint))
	value = appendUvarint(value, uint64(len(chunks)))
	for _, chunk := range chunks {
		hashBytes, err := hex.DecodeString(strings.TrimSpace(chunk.Hash))
		if err != nil || len(hashBytes) != 32 {
			return nil, fmt.Errorf("invalid file chunk hash %q for %s", chunk.Hash, record.Path)
		}
		if chunk.OriginalSize < 0 {
			return nil, fmt.Errorf("invalid file chunk size %d for %s", chunk.OriginalSize, record.Path)
		}
		value = append(value, hashBytes...)
		value = appendUint64(value, uint64(chunk.OriginalSize))
	}
	return value, nil
}

func decodePebbleFileValue(value []byte) (catalogFileRecord, []catalogChunkRecord, bool) {
	if len(value) < 2 || value[0] != agentPebbleValueVersion {
		return catalogFileRecord{}, nil, false
	}
	entryType, ok := decodeFileEntryType(value[1])
	if !ok {
		return catalogFileRecord{}, nil, false
	}
	offset := 2
	size, ok := readUint64(value, &offset)
	if !ok {
		return catalogFileRecord{}, nil, false
	}
	mode, ok := readUint32(value, &offset)
	if !ok {
		return catalogFileRecord{}, nil, false
	}
	uid, ok := readInt64(value, &offset)
	if !ok {
		return catalogFileRecord{}, nil, false
	}
	gid, ok := readInt64(value, &offset)
	if !ok {
		return catalogFileRecord{}, nil, false
	}
	mtimeNS, ok := readInt64(value, &offset)
	if !ok {
		return catalogFileRecord{}, nil, false
	}
	dev, ok := readInt64(value, &offset)
	if !ok {
		return catalogFileRecord{}, nil, false
	}
	inode, ok := readInt64(value, &offset)
	if !ok {
		return catalogFileRecord{}, nil, false
	}
	linkTarget, ok := readLengthPrefixedBytes(value, &offset)
	if !ok {
		return catalogFileRecord{}, nil, false
	}
	fingerprint, ok := readLengthPrefixedBytes(value, &offset)
	if !ok {
		return catalogFileRecord{}, nil, false
	}
	chunkCount, ok := readUvarint(value, &offset)
	if !ok {
		return catalogFileRecord{}, nil, false
	}
	if size > uint64(1<<63-1) || chunkCount > uint64((len(value)-offset)/40) || chunkCount > uint64(int(^uint(0)>>1)) {
		return catalogFileRecord{}, nil, false
	}
	chunks := make([]catalogChunkRecord, 0, int(chunkCount))
	for i := uint64(0); i < chunkCount; i++ {
		if len(value)-offset < 40 {
			return catalogFileRecord{}, nil, false
		}
		hash := hex.EncodeToString(value[offset : offset+32])
		offset += 32
		originalSize, ok := readUint64(value, &offset)
		if !ok {
			return catalogFileRecord{}, nil, false
		}
		if originalSize > uint64(1<<63-1) {
			return catalogFileRecord{}, nil, false
		}
		chunks = append(chunks, catalogChunkRecord{
			Hash:         hash,
			OriginalSize: int64(originalSize),
		})
	}
	if offset != len(value) {
		return catalogFileRecord{}, nil, false
	}
	return catalogFileRecord{
		Type:        entryType,
		Size:        int64(size),
		Mode:        int64(mode),
		UID:         uid,
		GID:         gid,
		MTimeNS:     mtimeNS,
		Dev:         dev,
		Inode:       inode,
		LinkTarget:  string(linkTarget),
		Fingerprint: string(fingerprint),
	}, chunks, true
}

func encodePebbleChunkStatusKey(hash string) ([]byte, error) {
	hash = strings.TrimSpace(hash)
	hashBytes, err := hex.DecodeString(hash)
	if err != nil || len(hashBytes) != 32 {
		return nil, fmt.Errorf("invalid chunk hash %q", hash)
	}
	key := make([]byte, 1+len(hashBytes))
	key[0] = agentPebbleRecordChunkStatus
	copy(key[1:], hashBytes)
	return key, nil
}

func encodePebbleChunkStatusValue(record pebbleChunkStatusRecord) ([]byte, error) {
	statusCode, err := encodeChunkStatusCode(record.Status)
	if err != nil {
		return nil, err
	}
	if record.OriginalSize < 0 {
		return nil, fmt.Errorf("invalid chunk original size %d", record.OriginalSize)
	}
	if record.ConfirmedGeneration < 0 {
		return nil, fmt.Errorf("invalid chunk generation %d", record.ConfirmedGeneration)
	}
	value := make([]byte, 34)
	value[0] = agentPebbleValueVersion
	value[1] = statusCode
	binary.BigEndian.PutUint64(value[2:10], uint64(record.OriginalSize))
	binary.BigEndian.PutUint64(value[10:18], uint64(record.ConfirmedGeneration))
	binary.BigEndian.PutUint64(value[18:26], uint64(record.LastCheckedUnix))
	binary.BigEndian.PutUint64(value[26:34], uint64(record.LastUploadedUnix))
	return value, nil
}

func decodePebbleChunkStatusValue(value []byte) (pebbleChunkStatusRecord, bool) {
	if len(value) != 34 || value[0] != agentPebbleValueVersion {
		return pebbleChunkStatusRecord{}, false
	}
	status, ok := decodeChunkStatusCode(value[1])
	if !ok {
		return pebbleChunkStatusRecord{}, false
	}
	return pebbleChunkStatusRecord{
		Status:              status,
		OriginalSize:        int64(binary.BigEndian.Uint64(value[2:10])),
		ConfirmedGeneration: int64(binary.BigEndian.Uint64(value[10:18])),
		LastCheckedUnix:     int64(binary.BigEndian.Uint64(value[18:26])),
		LastUploadedUnix:    int64(binary.BigEndian.Uint64(value[26:34])),
	}, true
}

func encodeChunkStatusCode(status string) (byte, error) {
	switch strings.TrimSpace(status) {
	case "unknown":
		return agentChunkStatusUnknown, nil
	case "confirmed":
		return agentChunkStatusConfirmed, nil
	case "missing":
		return agentChunkStatusMissing, nil
	default:
		return 0, fmt.Errorf("invalid chunk status %q", status)
	}
}

func decodeChunkStatusCode(code byte) (string, bool) {
	switch code {
	case agentChunkStatusUnknown:
		return "unknown", true
	case agentChunkStatusConfirmed:
		return "confirmed", true
	case agentChunkStatusMissing:
		return "missing", true
	default:
		return "", false
	}
}

func encodeFileEntryType(entryType string) (byte, error) {
	switch strings.TrimSpace(entryType) {
	case "file":
		return agentFileEntryTypeFile, nil
	case "dir":
		return agentFileEntryTypeDir, nil
	case "symlink":
		return agentFileEntryTypeSymlink, nil
	default:
		return 0, fmt.Errorf("invalid file entry type %q", entryType)
	}
}

func decodeFileEntryType(code byte) (string, bool) {
	switch code {
	case agentFileEntryTypeFile:
		return "file", true
	case agentFileEntryTypeDir:
		return "dir", true
	case agentFileEntryTypeSymlink:
		return "symlink", true
	default:
		return "", false
	}
}

func appendUint32(dst []byte, value uint32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], value)
	return append(dst, buf[:]...)
}

func appendUint64(dst []byte, value uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)
	return append(dst, buf[:]...)
}

func appendInt64(dst []byte, value int64) []byte {
	return appendUint64(dst, uint64(value))
}

func appendUvarint(dst []byte, value uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], value)
	return append(dst, buf[:n]...)
}

func appendLengthPrefixedBytes(dst []byte, value []byte) []byte {
	dst = appendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func readUint32(data []byte, offset *int) (uint32, bool) {
	if len(data)-*offset < 4 {
		return 0, false
	}
	value := binary.BigEndian.Uint32(data[*offset : *offset+4])
	*offset += 4
	return value, true
}

func readUint64(data []byte, offset *int) (uint64, bool) {
	if len(data)-*offset < 8 {
		return 0, false
	}
	value := binary.BigEndian.Uint64(data[*offset : *offset+8])
	*offset += 8
	return value, true
}

func readInt64(data []byte, offset *int) (int64, bool) {
	value, ok := readUint64(data, offset)
	return int64(value), ok
}

func readUvarint(data []byte, offset *int) (uint64, bool) {
	if *offset > len(data) {
		return 0, false
	}
	value, n := binary.Uvarint(data[*offset:])
	if n <= 0 {
		return 0, false
	}
	*offset += n
	return value, true
}

func readLengthPrefixedBytes(data []byte, offset *int) ([]byte, bool) {
	length, ok := readUvarint(data, offset)
	if !ok || length > uint64(len(data)-*offset) {
		return nil, false
	}
	value := data[*offset : *offset+int(length)]
	*offset += int(length)
	return value, true
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
