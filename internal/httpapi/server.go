package httpapi

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	pathpkg "path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tursom/turbk/internal/config"
	"github.com/tursom/turbk/internal/repository"
	"github.com/tursom/turbk/internal/source"
	"github.com/tursom/turbk/internal/state"
	"github.com/tursom/turbk/internal/version"
)

type Server struct {
	cfg          config.Config
	store        *state.Store
	repo         *repository.Repository
	logger       *slog.Logger
	mux          *http.ServeMux
	schedulerSem chan struct{}
	runGate      sync.RWMutex
	settingsMu   sync.RWMutex
	settings     runtimeSettings
}

func New(cfg config.Config, store *state.Store, repo *repository.Repository, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	maxScheduledRuns := cfg.Scheduler.MaxConcurrentRuns
	if maxScheduledRuns <= 0 {
		maxScheduledRuns = 1
	}
	s := &Server{
		cfg:          cfg,
		store:        store,
		repo:         repo,
		logger:       logger,
		mux:          http.NewServeMux(),
		schedulerSem: make(chan struct{}, maxScheduledRuns),
		settings:     loadRuntimeSettings(context.Background(), cfg, store, logger),
	}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler {
	return withAccessLog(s.logger, s.withManagementAuth(s.mux))
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /api/v1/health", s.handleHealth)
	s.mux.HandleFunc("POST /api/v1/auth/login", s.handleAuthLogin)
	s.mux.HandleFunc("POST /api/v1/auth/logout", s.handleAuthLogout)
	s.mux.HandleFunc("GET /api/v1/auth/session", s.handleAuthSession)
	s.mux.HandleFunc("GET /api/v1/bootstrap", s.handleBootstrap)
	s.mux.HandleFunc("GET /api/v1/settings", s.handleSettings)
	s.mux.HandleFunc("PATCH /api/v1/settings", s.handleUpdateSettings)
	s.mux.HandleFunc("GET /api/v1/hosts", s.handleHosts)
	s.mux.HandleFunc("POST /api/v1/hosts", s.handleCreateHost)
	s.mux.HandleFunc("PATCH /api/v1/hosts/{id}", s.handleUpdateHost)
	s.mux.HandleFunc("GET /api/v1/credentials", s.handleCredentials)
	s.mux.HandleFunc("POST /api/v1/credentials", s.handleCreateCredential)
	s.mux.HandleFunc("GET /api/v1/jobs", s.handleJobs)
	s.mux.HandleFunc("POST /api/v1/jobs", s.handleCreateJob)
	s.mux.HandleFunc("PATCH /api/v1/jobs/{id}", s.handleUpdateJob)
	s.mux.HandleFunc("POST /api/v1/jobs/{id}/run", s.handleRunJob)
	s.mux.HandleFunc("GET /api/v1/runs", s.handleRuns)
	s.mux.HandleFunc("GET /api/v1/runs/{id}/logs", s.handleRunLogs)
	s.mux.HandleFunc("GET /api/v1/snapshots", s.handleSnapshots)
	s.mux.HandleFunc("GET /api/v1/snapshots/{id}/tree", s.handleSnapshotTree)
	s.mux.HandleFunc("GET /api/v1/snapshots/{id}/files", s.handleSnapshotDownload)
	s.mux.HandleFunc("GET /api/v1/snapshots/{id}/files/{path...}", s.handleSnapshotDownload)
	s.mux.HandleFunc("GET /api/v1/restore/tasks", s.handleRestoreTasks)
	s.mux.HandleFunc("POST /api/v1/restore", s.handleRestore)
	s.mux.HandleFunc("GET /api/v1/storage/health", s.handleStorageHealth)
	s.mux.HandleFunc("POST /api/v1/storage/maintenance", s.handleStorageMaintenance)
	s.mux.HandleFunc("POST /agent/v1/heartbeat", s.handleAgentHeartbeat)
	s.mux.HandleFunc("POST /agent/v1/runs", s.handleAgentCreateRun)
	s.mux.HandleFunc("GET /agent/v1/chunks/{hash}", s.handleAgentGetChunk)
	s.mux.HandleFunc("PUT /agent/v1/chunks/{hash}", s.handleAgentPutChunk)
	s.mux.HandleFunc("POST /agent/v1/manifests", s.handleAgentPostManifest)
	s.mux.HandleFunc("POST /agent/v1/runs/{id}/progress", s.handleAgentProgress)
	s.mux.HandleFunc("POST /agent/v1/runs/{id}/finish", s.handleAgentFinishRun)
	s.mux.HandleFunc("/", s.handleWeb)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	dbStatus := "ok"
	if err := s.store.Ping(ctx); err != nil {
		dbStatus = "error"
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status": "error",
			"error":  err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status":     "ok",
		"version":    version.Version,
		"commit":     version.Commit,
		"date":       version.Date,
		"database":   dbStatus,
		"started_at": s.store.StartedAt(),
		"time":       time.Now().UTC(),
	})
}

func (s *Server) handleBootstrap(w http.ResponseWriter, r *http.Request) {
	counts, err := s.store.Counts(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	settings := s.currentSettings()
	writeJSON(w, http.StatusOK, map[string]any{
		"version": version.Version,
		"counts":  counts,
		"paths": map[string]any{
			"state_dir":     s.cfg.Paths.StateDir,
			"repo_dir":      s.cfg.Paths.RepoDir,
			"restore_roots": s.cfg.Paths.RestoreRoots,
		},
		"repository": s.cfg.Repository,
		"scheduler":  s.cfg.Scheduler,
		"retention":  settings.Retention,
		"auth": map[string]any{
			"username":          settings.AuthUsername,
			"session_ttl_hours": settings.SessionTTLHours,
		},
	})
}

func (s *Server) handleHosts(w http.ResponseWriter, r *http.Request) {
	hosts, err := s.store.ListHosts(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"hosts": hosts})
}

func (s *Server) handleCreateHost(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name         string `json:"name"`
		SourceType   string `json:"source_type"`
		Address      string `json:"address"`
		CredentialID *int64 `json:"credential_id"`
		Status       string `json:"status"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	req.SourceType = strings.TrimSpace(req.SourceType)
	req.Address = strings.TrimSpace(req.Address)
	req.Status = strings.TrimSpace(req.Status)
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, errors.New("host name is required"))
		return
	}
	if req.SourceType == "" {
		req.SourceType = "local"
	}
	if !isSupportedSourceType(req.SourceType) {
		writeError(w, http.StatusBadRequest, fmt.Errorf("source_type %q is not supported", req.SourceType))
		return
	}
	if req.SourceType == "agent" {
		if req.Address != "" {
			writeError(w, http.StatusBadRequest, errors.New("agent host address is updated by heartbeat"))
			return
		}
		if req.CredentialID != nil {
			writeError(w, http.StatusBadRequest, errors.New("agent credential is generated by the server"))
			return
		}
		payload, clientID, clientSecret, subject, err := newAgentCredentialPayload(mustJSON(map[string]any{"subject": req.Name}))
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		host, credential, err := s.store.CreateAgentHost(r.Context(), state.CreateAgentHostInput{
			Name:         req.Name,
			Payload:      payload,
			ClientID:     clientID,
			ClientSecret: clientSecret,
			SecretHash:   state.HashAgentSecret(clientID, clientSecret),
			Subject:      subject,
		})
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, http.StatusCreated, map[string]any{
			"host":       host,
			"credential": credential,
			"agent": map[string]any{
				"client_id":     clientID,
				"client_secret": clientSecret,
			},
		})
		return
	}
	var credentialID sql.NullInt64
	switch req.SourceType {
	case "local":
		if req.Address != "" {
			writeError(w, http.StatusBadRequest, errors.New("local hosts do not use address"))
			return
		}
		if req.CredentialID != nil {
			writeError(w, http.StatusBadRequest, errors.New("local hosts do not use credential_id"))
			return
		}
	default:
		if req.Address == "" {
			writeError(w, http.StatusBadRequest, errors.New("host address is required for pull sources"))
			return
		}
		if req.CredentialID == nil || *req.CredentialID <= 0 {
			writeError(w, http.StatusBadRequest, errors.New("credential_id is required for pull hosts"))
			return
		}
		credential, err := s.store.GetCredential(r.Context(), *req.CredentialID)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if credential.Type != req.SourceType {
			writeError(w, http.StatusBadRequest, fmt.Errorf("credential type %q does not match source_type %q", credential.Type, req.SourceType))
			return
		}
		credentialID = sql.NullInt64{Int64: *req.CredentialID, Valid: true}
	}
	address := sql.NullString{String: req.Address, Valid: req.Address != ""}
	host, err := s.store.CreateHost(r.Context(), state.CreateHostInput{
		Name:         req.Name,
		SourceType:   req.SourceType,
		Address:      address,
		CredentialID: credentialID,
		Status:       req.Status,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"host": host})
}

func (s *Server) handleUpdateHost(w http.ResponseWriter, r *http.Request) {
	hostID, err := parsePathID(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	host, err := s.store.GetHost(r.Context(), hostID)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	var req struct {
		Name         *string `json:"name"`
		Address      *string `json:"address"`
		CredentialID *int64  `json:"credential_id"`
		Status       *string `json:"status"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	name := host.Name
	if req.Name != nil {
		name = strings.TrimSpace(*req.Name)
	}
	status := host.Status
	if req.Status != nil {
		status = strings.TrimSpace(*req.Status)
	}
	address := host.Address
	if req.Address != nil {
		value := strings.TrimSpace(*req.Address)
		address = sql.NullString{String: value, Valid: value != ""}
	}
	credentialID := host.CredentialID
	switch host.SourceType {
	case "agent":
		if req.Address != nil {
			writeError(w, http.StatusBadRequest, errors.New("agent host address is updated by heartbeat"))
			return
		}
		if req.CredentialID != nil {
			writeError(w, http.StatusBadRequest, errors.New("agent credential binding cannot be changed"))
			return
		}
	case "local":
		if req.Address != nil {
			writeError(w, http.StatusBadRequest, errors.New("local hosts do not use address"))
			return
		}
		if req.CredentialID != nil && *req.CredentialID > 0 {
			writeError(w, http.StatusBadRequest, errors.New("local hosts do not use credential_id"))
			return
		}
		credentialID = sql.NullInt64{}
	default:
		if req.CredentialID != nil {
			if *req.CredentialID <= 0 {
				writeError(w, http.StatusBadRequest, errors.New("credential_id is required for pull hosts"))
				return
			}
			credential, err := s.store.GetCredential(r.Context(), *req.CredentialID)
			if err != nil {
				writeError(w, http.StatusBadRequest, err)
				return
			}
			if credential.Type != host.SourceType {
				writeError(w, http.StatusBadRequest, fmt.Errorf("credential type %q does not match source_type %q", credential.Type, host.SourceType))
				return
			}
			credentialID = sql.NullInt64{Int64: *req.CredentialID, Valid: true}
		}
		if !address.Valid || strings.TrimSpace(address.String) == "" {
			writeError(w, http.StatusBadRequest, errors.New("host address is required for pull sources"))
			return
		}
	}
	updated, err := s.store.UpdateHost(r.Context(), state.UpdateHostInput{
		ID:           host.ID,
		Name:         name,
		Address:      address,
		CredentialID: credentialID,
		Status:       status,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"host": updated})
}

func (s *Server) handleCredentials(w http.ResponseWriter, r *http.Request) {
	credentials, err := s.store.ListCredentials(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"credentials": credentials})
}

func (s *Server) handleCreateCredential(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name    string          `json:"name"`
		Type    string          `json:"type"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Type == "agent" {
		writeError(w, http.StatusBadRequest, errors.New("agent credentials are created with agent hosts"))
		return
	}
	if err := validateCredentialPayload(req.Type, req.Payload); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	credential, err := s.store.CreateCredential(r.Context(), state.CreateCredentialInput{
		Name:    req.Name,
		Type:    req.Type,
		Payload: req.Payload,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"credential": credential})
}

func (s *Server) handleJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := s.store.ListJobs(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs})
}

func (s *Server) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name              string          `json:"name"`
		HostID            int64           `json:"host_id"`
		SourceType        string          `json:"source_type"`
		CredentialID      *int64          `json:"credential_id"`
		SourceConfig      json.RawMessage `json:"source_config"`
		Enabled           bool            `json:"enabled"`
		Schedule          string          `json:"schedule"`
		Timezone          string          `json:"timezone"`
		MaxRuntimeSeconds int64           `json:"max_runtime_seconds"`
		RetryAttempts     int64           `json:"retry_attempts"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.HostID <= 0 {
		writeError(w, http.StatusBadRequest, errors.New("host_id is required"))
		return
	}
	if strings.TrimSpace(req.SourceType) != "" {
		writeError(w, http.StatusBadRequest, errors.New("job source_type is derived from host_id"))
		return
	}
	if req.CredentialID != nil {
		writeError(w, http.StatusBadRequest, errors.New("jobs use host credential binding; do not submit credential_id"))
		return
	}
	host, err := s.store.GetHost(r.Context(), req.HostID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if host.SourceType == "agent" {
		writeError(w, http.StatusBadRequest, errors.New("agent jobs are created from the bound agent host"))
		return
	}
	if err := validateSourceConfig(host.SourceType, req.SourceConfig); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if host.SourceType != "local" && !host.CredentialID.Valid {
		writeError(w, http.StatusBadRequest, errors.New("host credential_id is required for non-local jobs"))
		return
	}
	input := state.CreateJobInput{
		Name:              req.Name,
		HostID:            host.ID,
		SourceConfig:      req.SourceConfig,
		Enabled:           req.Enabled,
		Timezone:          req.Timezone,
		MaxRuntimeSeconds: req.MaxRuntimeSeconds,
		RetryAttempts:     req.RetryAttempts,
	}
	if req.Schedule != "" {
		if !validCronExpression(req.Schedule) {
			writeError(w, http.StatusBadRequest, fmt.Errorf("invalid cron schedule %q", req.Schedule))
			return
		}
		input.Schedule = sql.NullString{String: req.Schedule, Valid: true}
	}
	job, err := s.store.CreateJob(r.Context(), input)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"job": job})
}

func (s *Server) handleUpdateJob(w http.ResponseWriter, r *http.Request) {
	jobID, err := parsePathID(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	job, err := s.store.GetJob(r.Context(), jobID)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	var req struct {
		Name              *string          `json:"name"`
		HostID            *int64           `json:"host_id"`
		SourceType        *string          `json:"source_type"`
		CredentialID      *int64           `json:"credential_id"`
		SourceConfig      *json.RawMessage `json:"source_config"`
		Enabled           *bool            `json:"enabled"`
		Schedule          *string          `json:"schedule"`
		Timezone          *string          `json:"timezone"`
		MaxRuntimeSeconds *int64           `json:"max_runtime_seconds"`
		RetryAttempts     *int64           `json:"retry_attempts"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.HostID != nil {
		writeError(w, http.StatusBadRequest, errors.New("job host_id cannot be changed"))
		return
	}
	if req.SourceType != nil {
		writeError(w, http.StatusBadRequest, errors.New("job source_type is derived from host_id"))
		return
	}
	if req.CredentialID != nil {
		writeError(w, http.StatusBadRequest, errors.New("jobs use host credential binding; do not submit credential_id"))
		return
	}

	name := job.Name
	if req.Name != nil {
		name = strings.TrimSpace(*req.Name)
	}
	sourceConfig := json.RawMessage(job.SourceConfig)
	if req.SourceConfig != nil {
		sourceConfig = *req.SourceConfig
	}
	if err := validateSourceConfig(job.SourceType, sourceConfig); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	enabled := job.Enabled
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	timezone := job.Timezone
	if req.Timezone != nil {
		timezone = strings.TrimSpace(*req.Timezone)
	}
	maxRuntimeSeconds := job.MaxRuntimeSeconds
	if req.MaxRuntimeSeconds != nil {
		maxRuntimeSeconds = *req.MaxRuntimeSeconds
	}
	retryAttempts := job.RetryAttempts
	if req.RetryAttempts != nil {
		retryAttempts = *req.RetryAttempts
	}
	schedule := job.Schedule
	if req.Schedule != nil {
		value := strings.TrimSpace(*req.Schedule)
		schedule = sql.NullString{}
		if value != "" {
			if !validCronExpression(value) {
				writeError(w, http.StatusBadRequest, fmt.Errorf("invalid cron schedule %q", value))
				return
			}
			schedule = sql.NullString{String: value, Valid: true}
		}
	}

	updated, err := s.store.UpdateJob(r.Context(), state.UpdateJobInput{
		ID:                job.ID,
		Name:              name,
		SourceConfig:      sourceConfig,
		Enabled:           enabled,
		Schedule:          schedule,
		Timezone:          timezone,
		MaxRuntimeSeconds: maxRuntimeSeconds,
		RetryAttempts:     retryAttempts,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"job": updated})
}

func (s *Server) handleRunJob(w http.ResponseWriter, r *http.Request) {
	jobID, err := parsePathID(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	job, err := s.store.GetJob(r.Context(), jobID)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	if job.SourceType == "agent" {
		writeError(w, http.StatusBadRequest, errors.New("agent jobs must be started by turbk-agent"))
		return
	}
	releaseRunGate, ok := s.tryEnterBackupWrite()
	if !ok {
		writeError(w, http.StatusConflict, errStorageMaintenanceRunning)
		return
	}
	defer releaseRunGate()
	run, err := s.store.CreateRun(r.Context(), job)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, state.ErrActiveRunExists) {
			status = http.StatusConflict
		}
		writeError(w, status, err)
		return
	}
	result, err := s.executeRun(r.Context(), job, run)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, result)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := s.store.ListRuns(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": runs})
}

func (s *Server) handleRunLogs(w http.ResponseWriter, r *http.Request) {
	runID, err := parsePathID(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	logs, err := s.store.ListRunLogs(r.Context(), runID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"logs": logs})
}

func (s *Server) handleSnapshots(w http.ResponseWriter, r *http.Request) {
	snapshots, err := s.store.ListSnapshots(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"snapshots": snapshots})
}

func (s *Server) handleStorageHealth(w http.ResponseWriter, r *http.Request) {
	repoInfo, err := os.Stat(s.cfg.Paths.RepoDir)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	dbInfo, err := os.Stat(s.store.DBPath())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	repoStats, err := s.repo.Stats()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok",
		"repo": map[string]any{
			"path":     s.cfg.Paths.RepoDir,
			"mode":     repoInfo.Mode().String(),
			"modified": repoInfo.ModTime().UTC(),
		},
		"sqlite": map[string]any{
			"path":     s.store.DBPath(),
			"size":     dbInfo.Size(),
			"modified": dbInfo.ModTime().UTC(),
		},
		"segment": map[string]any{
			"size":              s.cfg.Repository.SegmentSize,
			"writeMode":         "append-only",
			"count":             repoStats.Segments,
			"bytes":             repoStats.SegmentBytes,
			"appendOnlyRecords": repoStats.AppendOnlyRecords,
		},
		"chunks": map[string]any{
			"count":            repoStats.Chunks,
			"logical_bytes":    repoStats.LogicalBytes,
			"compressed_bytes": repoStats.CompressedBytes,
			"avg_size":         repoStats.ChunkAvgSize,
		},
		"manifests": map[string]any{
			"count": repoStats.ManifestCount,
		},
	})
}

func (s *Server) handleAgentHeartbeat(w http.ResponseWriter, r *http.Request) {
	agent, ok := s.authenticateAgent(w, r)
	if !ok {
		return
	}
	var req struct {
		Hostname string `json:"hostname"`
		Version  string `json:"version"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Hostname == "" {
		writeError(w, http.StatusBadRequest, errors.New("hostname is required"))
		return
	}
	if req.Version == "" {
		req.Version = "unknown"
	}
	now := time.Now().UTC()
	if err := s.store.UpsertAgentHeartbeat(r.Context(), agent.HostID, agent.Subject, req.Hostname, req.Version, now); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"status":      "accepted",
		"client_id":   agent.Credential.ClientID,
		"subject":     agent.Subject,
		"server_time": now,
	})
}

func (s *Server) handleAgentCreateRun(w http.ResponseWriter, r *http.Request) {
	agent, ok := s.authenticateAgent(w, r)
	if !ok {
		return
	}
	var req struct {
		Hostname string `json:"hostname"`
		Root     string `json:"root"`
		RunKey   string `json:"run_key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	req.Hostname = strings.TrimSpace(req.Hostname)
	req.Root = strings.TrimSpace(req.Root)
	if req.Hostname == "" {
		req.Hostname = agent.Subject
	}
	if req.Root == "" {
		writeError(w, http.StatusBadRequest, errors.New("root is required"))
		return
	}
	agentHost, err := s.store.GetHost(r.Context(), agent.HostID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	sourceConfig, err := json.Marshal(map[string]any{
		"root":     req.Root,
		"hostname": req.Hostname,
		"run_key":  req.RunKey,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	releaseRunGate, ok := s.tryEnterBackupWrite()
	if !ok {
		writeError(w, http.StatusConflict, errStorageMaintenanceRunning)
		return
	}
	defer releaseRunGate()
	job, created, err := s.store.FindOrCreateAgentJob(r.Context(), state.AgentJobInput{
		HostID:       agent.HostID,
		CredentialID: agent.Credential.ID,
		Name:         agentJobName(agentHost),
		SourceConfig: sourceConfig,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	run, err := s.store.CreateRun(r.Context(), job)
	if err != nil {
		if errors.Is(err, state.ErrActiveRunExists) {
			active, exists, activeErr := s.store.GetActiveRunForJob(r.Context(), job.ID)
			if activeErr != nil {
				writeError(w, http.StatusInternalServerError, activeErr)
				return
			}
			if exists {
				if active.Status == "pending" {
					_ = s.store.MarkRunRunning(r.Context(), active.ID, time.Now().UTC())
					active, _ = s.store.GetRun(r.Context(), active.ID)
				}
				_ = s.store.AppendRunLog(r.Context(), active.ID, "info", "agent run resumed")
				writeJSON(w, http.StatusOK, map[string]any{
					"status":      "running",
					"job_created": created,
					"job":         job,
					"run":         active,
					"server_time": time.Now().UTC(),
				})
				return
			}
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	now := time.Now().UTC()
	if err := s.store.MarkRunRunning(r.Context(), run.ID, now); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	_ = s.store.AppendRunLog(r.Context(), run.ID, "info", "agent run started")
	running, err := s.store.GetRun(r.Context(), run.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	status := http.StatusCreated
	writeJSON(w, status, map[string]any{
		"status":      "running",
		"job_created": created,
		"job":         job,
		"run":         running,
		"server_time": now,
	})
}

func (s *Server) handleAgentGetChunk(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authenticateAgent(w, r); !ok {
		return
	}
	hash := r.PathValue("hash")
	if err := validateChunkHash(hash); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	ref, exists, err := s.repo.GetChunkRef(r.Context(), hash)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	resp := map[string]any{"exists": exists}
	if exists {
		resp["ref"] = ref
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleAgentPutChunk(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authenticateAgent(w, r); !ok {
		return
	}
	hash := r.PathValue("hash")
	if err := validateChunkHash(hash); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 256*1024*1024))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	actual, err := repository.HashBytes(body)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if actual != hash {
		writeError(w, http.StatusBadRequest, fmt.Errorf("chunk hash mismatch: path=%s body=%s", hash, actual))
		return
	}
	releaseRunGate, ok := s.tryEnterBackupWrite()
	if !ok {
		writeError(w, http.StatusConflict, errStorageMaintenanceRunning)
		return
	}
	defer releaseRunGate()
	ref, existed, err := s.repo.PutChunk(r.Context(), body)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	status := http.StatusCreated
	if existed {
		status = http.StatusOK
	}
	writeJSON(w, status, map[string]any{
		"exists":   true,
		"uploaded": !existed,
		"ref":      ref,
	})
}

func (s *Server) handleAgentPostManifest(w http.ResponseWriter, r *http.Request) {
	agent, ok := s.authenticateAgent(w, r)
	if !ok {
		return
	}
	var req struct {
		RunID    int64                       `json:"run_id"`
		Manifest repository.SnapshotManifest `json:"manifest"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.RunID <= 0 {
		writeError(w, http.StatusBadRequest, errors.New("run_id is required"))
		return
	}
	job, run, ok := s.authorizeAgentRun(w, r, agent, req.RunID)
	if !ok {
		return
	}
	if snapshot, exists, err := s.store.GetSnapshotByRun(r.Context(), run.ID); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	} else if exists {
		writeJSON(w, http.StatusOK, map[string]any{
			"status":   "completed",
			"run":      run,
			"snapshot": snapshot,
			"manifest": map[string]any{"id": snapshot.ManifestRef},
		})
		return
	}
	if run.Status != "running" && run.Status != "pending" {
		writeError(w, http.StatusConflict, fmt.Errorf("run %d is %s", run.ID, run.Status))
		return
	}
	manifest := req.Manifest
	manifest.ID = fmt.Sprintf("run-%d", run.ID)
	manifest.SourceType = "agent"
	if manifest.SourceRoot == "" {
		var cfg jobSourceConfig
		_ = json.Unmarshal([]byte(job.SourceConfig), &cfg)
		manifest.SourceRoot = cfg.Root
		if manifest.SourceRoot == "" {
			manifest.SourceRoot = cfg.Path
		}
	}
	if err := s.canonicalizeManifestChunks(r.Context(), &manifest); err != nil {
		_ = s.store.AppendRunLog(r.Context(), run.ID, "error", err.Error())
		_ = s.store.FailRun(r.Context(), run.ID, err.Error(), time.Now().UTC())
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := s.repo.WriteManifest(&manifest); err != nil {
		_ = s.store.AppendRunLog(r.Context(), run.ID, "error", err.Error())
		_ = s.store.FailRun(r.Context(), run.ID, err.Error(), time.Now().UTC())
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	fileCount, totalSize := manifestTotals(&manifest)
	snapshot, err := s.store.CreateSnapshot(r.Context(), state.CreateSnapshotInput{
		JobID:       sql.NullInt64{Int64: job.ID, Valid: true},
		HostID:      job.HostID,
		RunID:       sql.NullInt64{Int64: run.ID, Valid: true},
		SourceType:  "agent",
		ManifestRef: manifest.ID,
		FileCount:   fileCount,
		TotalSize:   totalSize,
	})
	if err != nil {
		_ = s.store.AppendRunLog(r.Context(), run.ID, "error", err.Error())
		_ = s.store.FailRun(r.Context(), run.ID, err.Error(), time.Now().UTC())
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	_ = s.store.AppendRunLog(r.Context(), run.ID, "info", fmt.Sprintf("agent snapshot %s published", manifest.ID))
	existingProgress, _, _ := s.store.GetRunProgress(r.Context(), run.ID)
	_, _ = s.store.UpdateRunProgress(r.Context(), state.UpdateRunProgressInput{
		RunID:          run.ID,
		Phase:          "completed",
		TotalFiles:     fileCount,
		ProcessedFiles: fileCount,
		TotalBytes:     totalSize,
		ProcessedBytes: totalSize,
		UploadedChunks: existingProgress.UploadedChunks,
		ReusedChunks:   existingProgress.ReusedChunks,
		Message:        manifest.ID,
	})
	if err := s.store.CompleteRun(r.Context(), run.ID, time.Now().UTC()); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	completed, err := s.store.GetRun(r.Context(), run.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"status":   "completed",
		"run":      completed,
		"snapshot": snapshot,
		"manifest": map[string]any{
			"id":         manifest.ID,
			"entries":    len(manifest.Entries),
			"file_count": fileCount,
			"total_size": totalSize,
		},
	})
}

func (s *Server) handleAgentProgress(w http.ResponseWriter, r *http.Request) {
	agent, ok := s.authenticateAgent(w, r)
	if !ok {
		return
	}
	runID, err := parsePathID(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	_, run, ok := s.authorizeAgentRun(w, r, agent, runID)
	if !ok {
		return
	}
	if run.Status != "running" && run.Status != "pending" {
		writeError(w, http.StatusConflict, fmt.Errorf("run %d is %s", run.ID, run.Status))
		return
	}
	var req struct {
		Phase          string `json:"phase"`
		TotalFiles     int64  `json:"total_files"`
		ProcessedFiles int64  `json:"processed_files"`
		TotalBytes     int64  `json:"total_bytes"`
		ProcessedBytes int64  `json:"processed_bytes"`
		UploadedChunks int64  `json:"uploaded_chunks"`
		ReusedChunks   int64  `json:"reused_chunks"`
		Message        string `json:"message"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	progress, err := s.store.UpdateRunProgress(r.Context(), state.UpdateRunProgressInput{
		RunID:          run.ID,
		Phase:          strings.TrimSpace(req.Phase),
		TotalFiles:     req.TotalFiles,
		ProcessedFiles: req.ProcessedFiles,
		TotalBytes:     req.TotalBytes,
		ProcessedBytes: req.ProcessedBytes,
		UploadedChunks: req.UploadedChunks,
		ReusedChunks:   req.ReusedChunks,
		Message:        req.Message,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"status": "accepted", "progress": progress})
}

func (s *Server) handleAgentFinishRun(w http.ResponseWriter, r *http.Request) {
	agent, ok := s.authenticateAgent(w, r)
	if !ok {
		return
	}
	runID, err := parsePathID(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	_, run, ok := s.authorizeAgentRun(w, r, agent, runID)
	if !ok {
		return
	}
	if run.Status == "completed" {
		writeJSON(w, http.StatusOK, map[string]any{"status": "completed", "run": run})
		return
	}
	if _, exists, err := s.store.GetSnapshotByRun(r.Context(), run.ID); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	} else if !exists {
		writeError(w, http.StatusConflict, errors.New("manifest must be submitted before finishing run"))
		return
	}
	if err := s.store.CompleteRun(r.Context(), run.ID, time.Now().UTC()); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	completed, err := s.store.GetRun(r.Context(), run.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "completed", "run": completed})
}

type agentAuthContext struct {
	Credential state.Credential
	HostID     int64
	Subject    string
}

func (s *Server) authenticateAgent(w http.ResponseWriter, r *http.Request) (agentAuthContext, bool) {
	clientID, clientSecret, ok := r.BasicAuth()
	if !ok || strings.TrimSpace(clientID) == "" || clientSecret == "" {
		writeError(w, http.StatusUnauthorized, errors.New("agent client credentials are required"))
		return agentAuthContext{}, false
	}
	auth, err := s.store.FindAgentCredentialByClientSecret(r.Context(), clientID, clientSecret)
	if err != nil {
		if errors.Is(err, state.ErrAgentCredentialNotFound) {
			writeError(w, http.StatusUnauthorized, errors.New("invalid agent client credentials"))
			return agentAuthContext{}, false
		}
		writeError(w, http.StatusInternalServerError, err)
		return agentAuthContext{}, false
	}
	return agentAuthContext{Credential: auth.Credential, HostID: auth.HostID, Subject: auth.Subject}, true
}

func agentJobName(host state.Host) string {
	name := strings.TrimSpace(host.Name)
	if name == "" {
		name = fmt.Sprintf("host-%d", host.ID)
	}
	return fmt.Sprintf("Agent backup - %s", name)
}

func (s *Server) authorizeAgentRun(w http.ResponseWriter, r *http.Request, agent agentAuthContext, runID int64) (state.Job, state.Run, bool) {
	run, err := s.store.GetRun(r.Context(), runID)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return state.Job{}, state.Run{}, false
	}
	if !run.JobID.Valid {
		writeError(w, http.StatusForbidden, errors.New("run has no job"))
		return state.Job{}, state.Run{}, false
	}
	job, err := s.store.GetJob(r.Context(), run.JobID.Int64)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return state.Job{}, state.Run{}, false
	}
	if job.SourceType != "agent" || !job.HostID.Valid || job.HostID.Int64 != agent.HostID {
		writeError(w, http.StatusForbidden, errors.New("agent credential is not allowed to access this run"))
		return state.Job{}, state.Run{}, false
	}
	return job, run, true
}

func (s *Server) canonicalizeManifestChunks(ctx context.Context, manifest *repository.SnapshotManifest) error {
	if len(manifest.Entries) == 0 {
		return errors.New("manifest entries are required")
	}
	for entryIndex := range manifest.Entries {
		entry := &manifest.Entries[entryIndex]
		switch entry.Type {
		case repository.EntryTypeDir, repository.EntryTypeSymlink:
			continue
		case repository.EntryTypeFile:
			if entry.Size > 0 && len(entry.Chunks) == 0 {
				return fmt.Errorf("file %q has no chunks", entry.Path)
			}
			var chunkTotal int64
			for chunkIndex := range entry.Chunks {
				hash := entry.Chunks[chunkIndex].Hash
				if err := validateChunkHash(hash); err != nil {
					return fmt.Errorf("file %q chunk %d: %w", entry.Path, chunkIndex, err)
				}
				ref, exists, err := s.repo.GetChunkRef(ctx, hash)
				if err != nil {
					return err
				}
				if !exists {
					return fmt.Errorf("file %q references missing chunk %s", entry.Path, hash)
				}
				entry.Chunks[chunkIndex] = ref
				chunkTotal += ref.OriginalSize
			}
			if chunkTotal != entry.Size {
				return fmt.Errorf("file %q size %d does not match chunk bytes %d", entry.Path, entry.Size, chunkTotal)
			}
		default:
			return fmt.Errorf("manifest entry %q has unsupported type %q", entry.Path, entry.Type)
		}
	}
	return nil
}

func (s *Server) executeRun(ctx context.Context, job state.Job, run state.Run) (map[string]any, error) {
	runCtx := ctx
	cancel := func() {}
	if job.MaxRuntimeSeconds > 0 {
		runCtx, cancel = context.WithTimeout(ctx, time.Duration(job.MaxRuntimeSeconds)*time.Second)
	}
	defer cancel()

	now := time.Now().UTC()
	if err := s.store.MarkRunRunning(ctx, run.ID, now); err != nil {
		return map[string]any{"status": "error", "error": err.Error(), "run": run}, err
	}
	_ = s.store.AppendRunLog(ctx, run.ID, "info", "run started")

	var cfg jobSourceConfig
	if err := json.Unmarshal([]byte(job.SourceConfig), &cfg); err != nil {
		_ = s.store.AppendRunLog(ctx, run.ID, "error", err.Error())
		_ = s.store.FailRun(ctx, run.ID, err.Error(), time.Now().UTC())
		failed, _ := s.store.GetRun(ctx, run.ID)
		return map[string]any{"status": "error", "error": err.Error(), "run": failed}, err
	}
	root := cfg.Root
	if root == "" {
		root = cfg.Path
	}
	_ = s.store.AppendRunLog(ctx, run.ID, "info", "scanning "+root)
	_, _ = s.store.UpdateRunProgress(ctx, state.UpdateRunProgressInput{
		RunID:   run.ID,
		Phase:   "scanning",
		Message: root,
	})
	manifestID := fmt.Sprintf("run-%d", run.ID)
	manifest, err := s.backupJobSource(runCtx, job, run.ID, manifestID, root)
	if err != nil {
		if job.MaxRuntimeSeconds > 0 && runCtx.Err() == context.DeadlineExceeded {
			err = fmt.Errorf("run exceeded max_runtime_seconds (%d): %w", job.MaxRuntimeSeconds, context.DeadlineExceeded)
		}
		_ = s.store.AppendRunLog(ctx, run.ID, "error", err.Error())
		_, _ = s.store.UpdateRunProgress(ctx, state.UpdateRunProgressInput{
			RunID:   run.ID,
			Phase:   "failed",
			Message: err.Error(),
		})
		_ = s.store.FailRun(ctx, run.ID, err.Error(), time.Now().UTC())
		failed, _ := s.store.GetRun(ctx, run.ID)
		return map[string]any{"status": "error", "error": err.Error(), "run": failed}, err
	}
	fileCount, totalSize := manifestTotals(manifest)
	snapshot, err := s.store.CreateSnapshot(ctx, state.CreateSnapshotInput{
		JobID:       sql.NullInt64{Int64: job.ID, Valid: true},
		HostID:      job.HostID,
		RunID:       sql.NullInt64{Int64: run.ID, Valid: true},
		SourceType:  job.SourceType,
		ManifestRef: manifest.ID,
		FileCount:   fileCount,
		TotalSize:   totalSize,
	})
	if err != nil {
		_ = s.store.AppendRunLog(ctx, run.ID, "error", err.Error())
		_ = s.store.FailRun(ctx, run.ID, err.Error(), time.Now().UTC())
		failed, _ := s.store.GetRun(ctx, run.ID)
		return map[string]any{"status": "error", "error": err.Error(), "run": failed}, err
	}
	_ = s.store.AppendRunLog(ctx, run.ID, "info", fmt.Sprintf("snapshot %s published", manifest.ID))
	existingProgress, _, _ := s.store.GetRunProgress(ctx, run.ID)
	_, _ = s.store.UpdateRunProgress(ctx, state.UpdateRunProgressInput{
		RunID:          run.ID,
		Phase:          "completed",
		ProcessedFiles: fileCount,
		TotalFiles:     fileCount,
		ProcessedBytes: totalSize,
		TotalBytes:     totalSize,
		UploadedChunks: existingProgress.UploadedChunks,
		ReusedChunks:   existingProgress.ReusedChunks,
		Message:        manifest.ID,
	})
	if err := s.store.CompleteRun(ctx, run.ID, time.Now().UTC()); err != nil {
		return map[string]any{"status": "error", "error": err.Error(), "run": run}, err
	}
	completed, err := s.store.GetRun(ctx, run.ID)
	if err != nil {
		return map[string]any{"status": "error", "error": err.Error(), "run": run}, err
	}
	return map[string]any{
		"status":   "completed",
		"run":      completed,
		"snapshot": snapshot,
		"manifest": map[string]any{
			"id":         manifest.ID,
			"entries":    len(manifest.Entries),
			"file_count": fileCount,
			"total_size": totalSize,
		},
	}, nil
}

func (s *Server) backupJobSource(ctx context.Context, job state.Job, runID int64, manifestID, root string) (*repository.SnapshotManifest, error) {
	previous := s.previousJobManifest(ctx, job)
	progressFn := func(progress repository.BackupProgress) {
		_, _ = s.store.UpdateRunProgress(ctx, state.UpdateRunProgressInput{
			RunID:          runID,
			Phase:          progress.Phase,
			ProcessedFiles: progress.ProcessedFiles,
			ProcessedBytes: progress.ProcessedBytes,
			UploadedChunks: progress.UploadedChunks,
			ReusedChunks:   progress.ReusedChunks,
			Message:        progress.Message,
		})
	}
	if job.SourceType == "local" {
		return s.repo.BackupLocalTreeIncremental(ctx, manifestID, job.SourceType, root, previous, progressFn)
	}
	connector, err := s.connectorForJob(ctx, job)
	if err != nil {
		return nil, err
	}
	defer connector.Close()
	return s.repo.BackupFromSourceIncremental(ctx, manifestID, job.SourceType, root, connector, previous, progressFn)
}

func (s *Server) previousJobManifest(ctx context.Context, job state.Job) *repository.SnapshotManifest {
	snapshot, exists, err := s.store.GetLatestSnapshotForJob(ctx, job.ID)
	if err != nil {
		s.logger.Warn("load latest snapshot", "job", job.ID, "error", err)
		return nil
	}
	if !exists {
		return nil
	}
	manifest, err := s.repo.ReadManifest(snapshot.ManifestRef)
	if err != nil {
		s.logger.Warn("read latest snapshot manifest", "job", job.ID, "snapshot", snapshot.ID, "manifest", snapshot.ManifestRef, "error", err)
		return nil
	}
	return manifest
}

func (s *Server) connectorForJob(ctx context.Context, job state.Job) (source.Connector, error) {
	if !job.HostID.Valid {
		return nil, errors.New("host_id is required")
	}
	host, err := s.store.GetHost(ctx, job.HostID.Int64)
	if err != nil {
		return nil, err
	}
	if host.SourceType != job.SourceType {
		return nil, fmt.Errorf("host source_type %q does not match job source_type %q", host.SourceType, job.SourceType)
	}
	if !host.CredentialID.Valid {
		return nil, errors.New("host credential_id is required")
	}
	if !host.Address.Valid || strings.TrimSpace(host.Address.String) == "" {
		return nil, errors.New("host address is required")
	}
	credential, payload, err := s.store.GetCredentialPayload(ctx, host.CredentialID.Int64)
	if err != nil {
		return nil, err
	}
	if credential.Type != host.SourceType {
		return nil, fmt.Errorf("credential type %q does not match source_type %q", credential.Type, host.SourceType)
	}
	switch job.SourceType {
	case "sftp":
		var cfg sftpCredentialPayload
		if err := json.Unmarshal(payload, &cfg); err != nil {
			return nil, fmt.Errorf("decode sftp credential: %w", err)
		}
		return source.NewSFTP(source.SFTPConfig{
			Address:    strings.TrimSpace(host.Address.String),
			Username:   cfg.Username,
			Password:   cfg.Password,
			PrivateKey: []byte(cfg.PrivateKey),
		})
	case "ftp", "ftps":
		var cfg ftpCredentialPayload
		if err := json.Unmarshal(payload, &cfg); err != nil {
			return nil, fmt.Errorf("decode ftp credential: %w", err)
		}
		return source.NewFTP(source.FTPConfig{
			Address:       strings.TrimSpace(host.Address.String),
			Username:      cfg.Username,
			Password:      cfg.Password,
			TLS:           job.SourceType == "ftps" || cfg.TLS,
			Explicit:      cfg.ExplicitTLS,
			SkipTLSVerify: cfg.SkipTLSVerify,
		})
	case "webdav":
		var cfg webdavCredentialPayload
		if err := json.Unmarshal(payload, &cfg); err != nil {
			return nil, fmt.Errorf("decode webdav credential: %w", err)
		}
		return source.NewWebDAV(source.WebDAVConfig{
			BaseURL:     strings.TrimSpace(host.Address.String),
			Username:    cfg.Username,
			Password:    cfg.Password,
			BearerToken: cfg.BearerToken,
		})
	default:
		return nil, fmt.Errorf("source_type %q is not supported", job.SourceType)
	}
}

type jobSourceConfig struct {
	Root string `json:"root"`
	Path string `json:"path"`
}

type sftpCredentialPayload struct {
	Username   string `json:"username"`
	Password   string `json:"password"`
	PrivateKey string `json:"private_key"`
}

type ftpCredentialPayload struct {
	Username      string `json:"username"`
	Password      string `json:"password"`
	TLS           bool   `json:"tls"`
	ExplicitTLS   bool   `json:"explicit_tls"`
	SkipTLSVerify bool   `json:"skip_tls_verify"`
}

type webdavCredentialPayload struct {
	Username    string `json:"username"`
	Password    string `json:"password"`
	BearerToken string `json:"bearer_token"`
}

type agentCredentialPayload struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret,omitempty"`
	SecretHash   string `json:"secret_hash"`
	Subject      string `json:"subject"`
}

func newAgentCredentialPayload(raw json.RawMessage) (json.RawMessage, string, string, string, error) {
	var fields map[string]json.RawMessage
	if len(raw) > 0 {
		if !json.Valid(raw) {
			return nil, "", "", "", errors.New("credential payload must be valid JSON")
		}
		if err := json.Unmarshal(raw, &fields); err != nil {
			return nil, "", "", "", err
		}
	}
	forbidden := []string{"token", "client_id", "client_secret", "secret_hash"}
	for _, field := range forbidden {
		if _, ok := fields[field]; ok {
			return nil, "", "", "", fmt.Errorf("agent credential %s is generated by the server", field)
		}
	}
	var req struct {
		Subject string `json:"subject"`
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &req); err != nil {
			return nil, "", "", "", err
		}
	}
	clientID, err := randomAgentCredentialPart("agt_", 16)
	if err != nil {
		return nil, "", "", "", err
	}
	clientSecret, err := randomAgentCredentialPart("ags_", 32)
	if err != nil {
		return nil, "", "", "", err
	}
	subject := strings.TrimSpace(req.Subject)
	payload, err := json.Marshal(agentCredentialPayload{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		SecretHash:   state.HashAgentSecret(clientID, clientSecret),
		Subject:      subject,
	})
	if err != nil {
		return nil, "", "", "", err
	}
	return payload, clientID, clientSecret, subject, nil
}

func randomAgentCredentialPart(prefix string, byteCount int) (string, error) {
	data := make([]byte, byteCount)
	if _, err := rand.Read(data); err != nil {
		return "", fmt.Errorf("generate agent credential: %w", err)
	}
	return prefix + base64.RawURLEncoding.EncodeToString(data), nil
}

func validateSourceConfig(sourceType string, raw json.RawMessage) error {
	var cfg jobSourceConfig
	if len(raw) == 0 {
		return errors.New("source_config.root is required")
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return fmt.Errorf("source_config must be valid JSON: %w", err)
	}
	root := cfg.Root
	if root == "" {
		root = cfg.Path
	}
	if root == "" {
		return errors.New("source_config.root is required")
	}
	if sourceType != "local" {
		return nil
	}
	info, err := os.Stat(root)
	if err != nil {
		return fmt.Errorf("stat source_config.root: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("source_config.root %q is not a directory", root)
	}
	return nil
}

func validateCredentialPayload(credentialType string, raw json.RawMessage) error {
	if len(raw) == 0 || !json.Valid(raw) {
		return errors.New("credential payload must be valid JSON")
	}
	if err := rejectCredentialEndpointFields(raw); err != nil {
		return err
	}
	switch credentialType {
	case "sftp":
		var cfg sftpCredentialPayload
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return err
		}
		if cfg.Username == "" || (cfg.Password == "" && cfg.PrivateKey == "") {
			return errors.New("sftp credential requires username and password or private_key")
		}
	case "ftp", "ftps":
		var cfg ftpCredentialPayload
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return err
		}
		if cfg.Username == "" {
			return errors.New("ftp credential requires username")
		}
	case "webdav":
		var cfg webdavCredentialPayload
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return err
		}
		if cfg.BearerToken == "" && cfg.Username == "" {
			return errors.New("webdav credential requires bearer_token or username")
		}
	default:
		return fmt.Errorf("credential type %q is not supported", credentialType)
	}
	return nil
}

func rejectCredentialEndpointFields(raw json.RawMessage) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	for _, field := range []string{"address", "base_url"} {
		if _, ok := fields[field]; ok {
			return fmt.Errorf("credential payload must not include %q; configure endpoints on hosts", field)
		}
	}
	return nil
}

func mustJSON(value any) json.RawMessage {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return data
}

func isSupportedSourceType(sourceType string) bool {
	switch sourceType {
	case "local", "sftp", "ftp", "ftps", "webdav", "agent":
		return true
	default:
		return false
	}
}

func manifestTotals(manifest *repository.SnapshotManifest) (fileCount int64, totalSize int64) {
	for _, entry := range manifest.Entries {
		if entry.Type != repository.EntryTypeFile {
			continue
		}
		fileCount++
		totalSize += entry.Size
	}
	return fileCount, totalSize
}

func (s *Server) handleWeb(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/api/") || strings.HasPrefix(r.URL.Path, "/agent/") {
		http.NotFound(w, r)
		return
	}

	webPath := strings.TrimPrefix(pathpkg.Clean("/"+strings.TrimPrefix(r.URL.Path, "/")), "/")
	if webPath == "" || webPath == "." {
		webPath = "index.html"
	}
	candidate := filepath.Join(s.cfg.Server.WebDir, filepath.FromSlash(webPath))
	if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
		setWebCacheHeaders(w, webPath)
		http.ServeFile(w, r, candidate)
		return
	}
	if strings.HasPrefix(webPath, "assets/") {
		serveMissingWebAsset(w, r, webPath)
		return
	}

	index := filepath.Join(s.cfg.Server.WebDir, "index.html")
	if _, err := os.Stat(index); err == nil {
		setWebCacheHeaders(w, "index.html")
		http.ServeFile(w, r, index)
		return
	}

	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`<!doctype html><html><head><title>Turbk</title></head><body><h1>Turbk</h1><p>Web UI has not been built yet.</p></body></html>`))
}

func setWebCacheHeaders(w http.ResponseWriter, webPath string) {
	if webPath == "index.html" {
		w.Header().Set("Cache-Control", "no-cache, must-revalidate")
		return
	}
	if strings.HasPrefix(webPath, "assets/") {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	}
}

func serveMissingWebAsset(w http.ResponseWriter, r *http.Request, webPath string) {
	w.Header().Set("Cache-Control", "no-store")
	switch strings.ToLower(filepath.Ext(webPath)) {
	case ".js", ".mjs":
		w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `(() => {
  const url = new URL(window.location.href);
  url.searchParams.set("_turbk_reload", Date.now().toString());
  window.location.replace(url.toString());
})();`)
	case ".css":
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
		w.WriteHeader(http.StatusOK)
	default:
		http.NotFound(w, r)
	}
}

var errStorageMaintenanceRunning = errors.New("storage maintenance is running")

func (s *Server) tryEnterBackupWrite() (func(), bool) {
	if !s.runGate.TryRLock() {
		return nil, false
	}
	return s.runGate.RUnlock, true
}

func (s *Server) tryEnterCompactMaintenance() (func(), bool) {
	if !s.runGate.TryLock() {
		return nil, false
	}
	return s.runGate.Unlock, true
}

func validateChunkHash(hash string) error {
	if len(hash) != 64 {
		return fmt.Errorf("chunk hash must be 64 hex characters")
	}
	if strings.ToLower(hash) != hash {
		return fmt.Errorf("chunk hash must be lowercase hex")
	}
	decoded, err := hex.DecodeString(hash)
	if err != nil || len(decoded) != 32 {
		return fmt.Errorf("chunk hash must be valid BLAKE3-256 hex")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]any{
		"status": "error",
		"error":  err.Error(),
	})
}

func parsePathID(r *http.Request, name string) (int64, error) {
	raw := r.PathValue(name)
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("invalid %s %q", name, raw)
	}
	return id, nil
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func withAccessLog(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		logger.Info("http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"duration", time.Since(start).String(),
		)
	})
}
