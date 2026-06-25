package main

import (
	"testing"
	"time"
)

func TestBackupOptionsFromHeartbeatMapsServerFieldsAndPreservesBase(t *testing.T) {
	catalog := &agentCatalog{}
	heartbeat := heartbeatResponse{
		Repository: repositoryInfo{
			ID:              "repo-1",
			ChunkGeneration: 42,
		},
		Agent: agentInfo{
			MaxChunkCheckBatch:            7,
			MaxChunkUploadBatchBytes:      8,
			MaxChunkResponseBytes:         9,
			ChunkBatchMaxRetries:          10,
			ChunkBatchRetryInitialBackoff: "2s",
			ChunkBatchRetryMaxBackoff:     "30s",
			ChunkBatchSplitOn413:          true,
			ChunkPipelineEnabled:          true,
			MaxChunkCheckInflight:         11,
			MaxChunkUploadInflight:        12,
			MaxChunkPipelineBytes:         13,
			ChunkBatchUpload:              true,
			CompactChunkCheckResponse:     true,
			CompactChunkUploadResponse:    true,
			SmallFilePack:                 true,
			SmallFilePackEnabled:          true,
			SmallFilePackMaxFileSize:      14,
			SmallFilePackTargetSize:       15,
			ScanParallelEnabled:           true,
			FileReadWorkers:               16,
			FileReadPipelineBytes:         17,
		},
	}

	opts := backupOptionsFromHeartbeat(heartbeat, backupRunOptions{
		Catalog:                   catalog,
		CommandID:                 100,
		Trigger:                   "manual",
		MaxManifestRepairAttempts: 3,
	})

	if opts.Catalog != catalog || opts.CommandID != 100 || opts.Trigger != "manual" || opts.MaxManifestRepairAttempts != 3 {
		t.Fatalf("base options were not preserved: %+v", opts)
	}
	if opts.RepositoryID != "repo-1" || opts.ChunkGeneration != 42 {
		t.Fatalf("repository fields = %q/%d, want repo-1/42", opts.RepositoryID, opts.ChunkGeneration)
	}
	if opts.MaxChunkCheckBatch != 7 ||
		opts.MaxChunkUploadBatchBytes != 8 ||
		opts.MaxChunkResponseBytes != 9 ||
		opts.ChunkBatchMaxRetries != 10 ||
		opts.ChunkBatchRetryInitialBackoff != 2*time.Second ||
		opts.ChunkBatchRetryMaxBackoff != 30*time.Second ||
		!opts.ChunkBatchSplitOn413 ||
		!opts.ChunkPipelineEnabled ||
		opts.MaxChunkCheckInflight != 11 ||
		opts.MaxChunkUploadInflight != 12 ||
		opts.MaxChunkPipelineBytes != 13 ||
		!opts.ChunkBatchUpload ||
		!opts.CompactChunkCheckResponse ||
		!opts.CompactChunkUploadResponse ||
		!opts.SmallFilePackEnabled ||
		opts.SmallFilePackMaxFileSize != 14 ||
		opts.SmallFilePackTargetSize != 15 ||
		!opts.ScanParallelEnabled ||
		opts.FileReadWorkers != 16 ||
		opts.FileReadPipelineBytes != 17 {
		t.Fatalf("server options were not mapped: %+v", opts)
	}

	heartbeat.Agent.SmallFilePack = false
	opts = backupOptionsFromHeartbeat(heartbeat, backupRunOptions{})
	if opts.SmallFilePackEnabled {
		t.Fatal("SmallFilePackEnabled = true, want false when server capability flag is false")
	}
}
