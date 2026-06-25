package main

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/tursom/turbk/internal/fsfilter"
	"github.com/tursom/turbk/internal/rootset"
)

type backupRunOptions struct {
	Catalog                       *agentCatalog
	RepositoryID                  string
	ChunkGeneration               int64
	MaxChunkCheckBatch            int
	MaxChunkUploadBatchBytes      int64
	MaxChunkResponseBytes         int64
	ChunkBatchMaxRetries          int
	ChunkBatchRetryInitialBackoff time.Duration
	ChunkBatchRetryMaxBackoff     time.Duration
	ChunkBatchSplitOn413          bool
	ChunkPipelineEnabled          bool
	MaxChunkCheckInflight         int
	MaxChunkUploadInflight        int
	MaxChunkPipelineBytes         int64
	ChunkBatchUpload              bool
	CompactChunkCheckResponse     bool
	CompactChunkUploadResponse    bool
	SmallFilePackEnabled          bool
	SmallFilePackMaxFileSize      int64
	SmallFilePackTargetSize       int64
	ScanParallelEnabled           bool
	FileReadWorkers               int
	FileReadPipelineBytes         int64
	CommandID                     int64
	Trigger                       string
	MaxManifestRepairAttempts     int
}

func backupOptionsFromHeartbeat(heartbeat heartbeatResponse, base backupRunOptions) backupRunOptions {
	base.RepositoryID = heartbeat.Repository.ID
	base.ChunkGeneration = heartbeat.Repository.ChunkGeneration
	base.MaxChunkCheckBatch = heartbeat.Agent.MaxChunkCheckBatch
	base.MaxChunkUploadBatchBytes = heartbeat.Agent.MaxChunkUploadBatchBytes
	base.MaxChunkResponseBytes = heartbeat.Agent.MaxChunkResponseBytes
	base.ChunkBatchMaxRetries = heartbeat.Agent.ChunkBatchMaxRetries
	base.ChunkBatchRetryInitialBackoff = heartbeat.Agent.chunkBatchRetryInitialBackoff()
	base.ChunkBatchRetryMaxBackoff = heartbeat.Agent.chunkBatchRetryMaxBackoff()
	base.ChunkBatchSplitOn413 = heartbeat.Agent.ChunkBatchSplitOn413
	base.ChunkPipelineEnabled = heartbeat.Agent.ChunkPipelineEnabled
	base.MaxChunkCheckInflight = heartbeat.Agent.MaxChunkCheckInflight
	base.MaxChunkUploadInflight = heartbeat.Agent.MaxChunkUploadInflight
	base.MaxChunkPipelineBytes = heartbeat.Agent.MaxChunkPipelineBytes
	base.ChunkBatchUpload = heartbeat.Agent.ChunkBatchUpload
	base.CompactChunkCheckResponse = heartbeat.Agent.CompactChunkCheckResponse
	base.CompactChunkUploadResponse = heartbeat.Agent.CompactChunkUploadResponse
	base.SmallFilePackEnabled = heartbeat.Agent.SmallFilePack && heartbeat.Agent.SmallFilePackEnabled
	base.SmallFilePackMaxFileSize = heartbeat.Agent.SmallFilePackMaxFileSize
	base.SmallFilePackTargetSize = heartbeat.Agent.SmallFilePackTargetSize
	base.ScanParallelEnabled = heartbeat.Agent.ScanParallelEnabled
	base.FileReadWorkers = heartbeat.Agent.FileReadWorkers
	base.FileReadPipelineBytes = heartbeat.Agent.FileReadPipelineBytes
	return base
}

func runBackup(client *agentClient, root string, logger *slog.Logger, scanOptions fsfilter.Options) error {
	return runBackupWithOptions(client, []string{root}, logger, scanOptions, backupRunOptions{Trigger: "once", MaxManifestRepairAttempts: 3})
}

func runBackupWithOptions(client *agentClient, roots []string, logger *slog.Logger, scanOptions fsfilter.Options, opts backupRunOptions) error {
	applyAgentRuntimeEnvOverrides(&opts)
	roots, err := rootset.Normalize(roots)
	if err != nil {
		return err
	}
	for _, root := range roots {
		info, err := os.Lstat(root)
		if err != nil {
			return fmt.Errorf("stat root %q: %w", root, err)
		}
		if !info.IsDir() {
			return fmt.Errorf("root %q is not a directory", root)
		}
		if scanOptions.SkipPseudoFilesystems {
			if fsName, ok, err := fsfilter.PseudoFilesystemName(root); err == nil && ok {
				return fmt.Errorf("root %q is on unsupported pseudo filesystem %s", root, fsName)
			}
		}
	}
	if opts.Trigger == "" {
		opts.Trigger = "once"
	}
	if opts.MaxManifestRepairAttempts <= 0 {
		opts.MaxManifestRepairAttempts = 3
	}
	hostname, _ := os.Hostname()
	createRunRequest := map[string]any{
		"hostname":              hostname,
		"command_id":            opts.CommandID,
		"trigger":               opts.Trigger,
		"repository_id":         opts.RepositoryID,
		"base_chunk_generation": opts.ChunkGeneration,
	}
	if len(roots) == 1 {
		createRunRequest["root"] = roots[0]
	} else {
		createRunRequest["roots"] = roots
	}
	var created createRunResponse
	if _, err := client.doJSON(http.MethodPost, "/agent/v1/runs", createRunRequest, &created); err != nil {
		return err
	}
	if created.Run.ID <= 0 {
		return fmt.Errorf("server did not return a run id")
	}
	logger.Info("agent run started", "run", created.Run.ID, "roots", roots)
	localRunID := fmt.Sprintf("run-%d-%d", created.Run.ID, time.Now().UTC().UnixNano())
	runStatus := "failed"
	runMessage := ""
	if opts.Catalog != nil {
		catalogStatsStart := opts.Catalog.statsSnapshot()
		if err := opts.Catalog.recordRunStart(localRunID, created.Run.ID, opts.CommandID, opts.Trigger, time.Now().UTC()); err != nil {
			logger.Warn("agent catalog run start record failed", "run", created.Run.ID, "error", err)
		}
		defer func() {
			if err := opts.Catalog.recordRunFinish(localRunID, runStatus, runMessage, time.Now().UTC()); err != nil {
				logger.Warn("agent catalog run finish record failed", "run", created.Run.ID, "error", err)
			}
		}()
		defer func() {
			stats := opts.Catalog.statsSnapshot().delta(catalogStatsStart)
			if stats.any() {
				logger.Info("agent catalog writes",
					"run", created.Run.ID,
					"chunk_status_updates", stats.ChunkStatusUpdates,
					"chunk_invalidations", stats.ChunkInvalidations,
					"chunk_resets", stats.ChunkResets,
					"file_records", stats.FileRecords,
					"file_chunk_refs", stats.FileChunkRefs,
				)
			}
		}()
		defer func() {
			if err := opts.Catalog.flush(); err != nil {
				logger.Warn("agent catalog flush failed", "run", created.Run.ID, "error", err)
			}
		}()
	}

	var submitted submitManifestResponse
	manifestAccepted := false
	failRemoteRun := func(err error) error {
		runMessage = err.Error()
		if !manifestAccepted {
			if failErr := client.failRun(created.Run.ID, runMessage); failErr != nil {
				logger.Warn("agent run failure report failed", "run", created.Run.ID, "error", failErr)
			}
		}
		return err
	}
	for attempt := 0; attempt <= opts.MaxManifestRepairAttempts; attempt++ {
		manifest, err := client.scanAndUpload(created.Run.ID, roots, logger, scanOptions, opts)
		if err != nil {
			return failRemoteRun(err)
		}
		submitted, err = client.submitManifest(created.Run.ID, manifest)
		if err != nil {
			return failRemoteRun(err)
		}
		if submitted.Status != "missing_chunks" {
			manifestAccepted = true
			logger.Info("agent backup manifest accepted", "run", created.Run.ID, "entries", len(manifest.Entries), "status", submitted.Status, "repair_attempt", attempt)
			break
		}
		if attempt >= opts.MaxManifestRepairAttempts || !submitted.Retryable {
			err := fmt.Errorf("manifest still references %d missing chunks after %d repair attempts", len(submitted.MissingChunks), attempt)
			return failRemoteRun(err)
		}
		logger.Warn("agent manifest references missing chunks; retrying after repair", "run", created.Run.ID, "missing_chunks", len(submitted.MissingChunks), "repair_attempt", attempt+1)
		if opts.Catalog != nil {
			updates := make([]agentChunkStatusUpdate, 0, len(submitted.MissingChunks))
			for _, missing := range submitted.MissingChunks {
				updates = append(updates, agentChunkStatusUpdate{Hash: missing.Hash, Status: "missing"})
			}
			if err := opts.Catalog.markChunks(updates); err != nil {
				logger.Warn("agent catalog missing chunk update failed", "run", created.Run.ID, "error", err)
			}
		}
	}
	if _, err := client.doJSON(http.MethodPost, fmt.Sprintf("/agent/v1/runs/%d/finish", created.Run.ID), nil, nil); err != nil {
		runMessage = err.Error()
		return err
	}
	runStatus = "completed"
	runMessage = submitted.Status
	logger.Info("agent backup completed", "run", created.Run.ID, "status", submitted.Status)
	return nil
}

func syncCatalogWithServer(client *agentClient, catalog *agentCatalog, heartbeat heartbeatResponse) error {
	if catalog == nil || heartbeat.Repository.ID == "" {
		return nil
	}
	local, ok, err := catalog.serverState(client.serverURL, client.clientID)
	if err != nil {
		return err
	}
	next := catalogServerState{
		ServerURL:                  client.serverURL,
		ClientID:                   client.clientID,
		RepositoryID:               heartbeat.Repository.ID,
		ChunkGeneration:            heartbeat.Repository.ChunkGeneration,
		LastInvalidationGeneration: heartbeat.Repository.ChunkGeneration,
		ConfigGeneration:           heartbeat.Agent.ConfigGeneration,
		CommandGeneration:          heartbeat.Agent.CommandGeneration,
	}
	if !ok {
		return catalog.upsertServerState(next)
	}
	next.LastInvalidationGeneration = local.LastInvalidationGeneration
	if local.RepositoryID != "" && local.RepositoryID != heartbeat.Repository.ID {
		if err := catalog.resetServerChunks(); err != nil {
			return err
		}
		next.LastInvalidationGeneration = heartbeat.Repository.ChunkGeneration
		return catalog.upsertServerState(next)
	}
	if heartbeat.Repository.ChunkGeneration > local.LastInvalidationGeneration {
		if local.LastInvalidationGeneration >= heartbeat.Repository.InvalidationAvailableFrom {
			invalidations, err := client.chunkInvalidations(local.LastInvalidationGeneration)
			if err != nil {
				return err
			}
			if invalidations.Complete {
				if err := catalog.applyInvalidations(invalidations.InvalidatedHashes, invalidations.ToGeneration); err != nil {
					return err
				}
				next.LastInvalidationGeneration = invalidations.ToGeneration
			}
		}
	}
	return catalog.upsertServerState(next)
}
