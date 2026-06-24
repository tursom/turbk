package config

import "testing"

func TestDefaultConfigNormalizes(t *testing.T) {
	cfg := Default()
	if err := cfg.Normalize(); err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if cfg.Server.Listen == "" {
		t.Fatal("expected listen address")
	}
	if cfg.Paths.StateDir == "" || cfg.Paths.RepoDir == "" {
		t.Fatal("expected runtime directories")
	}
}

func TestExampleConfigsLoad(t *testing.T) {
	paths := []string{
		"../../configs/turbk.example.yaml",
		"../../deploy/server/config.example.yaml",
	}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			cfg, err := Load(path)
			if err != nil {
				t.Fatalf("Load(%q) error = %v", path, err)
			}
			if cfg.Server.Listen == "" {
				t.Fatal("expected server.listen")
			}
			if cfg.Server.WebDir == "" {
				t.Fatal("expected server.web_dir")
			}
			if cfg.Auth.Username == "" || cfg.Auth.Password == "" {
				t.Fatal("expected auth username and password")
			}
			if cfg.Paths.StateDir == "" || cfg.Paths.RepoDir == "" || len(cfg.Paths.RestoreRoots) == 0 {
				t.Fatal("expected runtime paths")
			}
			if cfg.Repository.SegmentSize == "" || cfg.Repository.ChunkAvgSize == "" || cfg.Repository.Compression == "" || cfg.Repository.Encryption == "" {
				t.Fatal("expected repository config")
			}
			if cfg.Scheduler.Timezone == "" || cfg.Scheduler.MaxConcurrentRuns <= 0 {
				t.Fatal("expected scheduler config")
			}
			if cfg.Retention.KeepLast <= 0 || cfg.Retention.KeepDaily < 0 || cfg.Retention.KeepWeekly < 0 {
				t.Fatal("expected retention config")
			}
			if cfg.Maintenance.Timezone == "" || cfg.Maintenance.CleanupSchedule == "" || cfg.Maintenance.CompactSchedule == "" ||
				cfg.Maintenance.ErrorGracePeriod == "" || cfg.Maintenance.StaleRunAfter == "" || cfg.Maintenance.CompactMinReclaimBytes == "" {
				t.Fatal("expected maintenance config")
			}
		})
	}
}

func TestExampleConfigsUseDefaultInitialAdmin(t *testing.T) {
	paths := []string{
		"../../configs/turbk.example.yaml",
		"../../deploy/server/config.example.yaml",
	}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			cfg, err := Load(path)
			if err != nil {
				t.Fatalf("Load(%q) error = %v", path, err)
			}
			if cfg.Auth.Username != "admin" || cfg.Auth.Password != "admin" {
				t.Fatalf("initial admin = %q/%q, want admin/admin", cfg.Auth.Username, cfg.Auth.Password)
			}
		})
	}
}

func TestEnvOverridesRepositoryConfig(t *testing.T) {
	t.Setenv("TURBK_SEGMENT_SIZE", "256MiB")
	t.Setenv("TURBK_CHUNK_AVG_SIZE", "2MiB")
	t.Setenv("TURBK_COMPRESSION", "none")
	t.Setenv("TURBK_ENCRYPTION", "none")
	t.Setenv("TURBK_AGENT_MAX_CHUNK_UPLOAD_BATCH_BYTES", "32MiB")
	t.Setenv("TURBK_AGENT_MAX_CHUNK_RESPONSE_BYTES", "16MiB")
	t.Setenv("TURBK_AGENT_CHUNK_BATCH_MAX_RETRIES", "7")
	t.Setenv("TURBK_AGENT_CHUNK_BATCH_RETRY_INITIAL_BACKOFF", "250ms")
	t.Setenv("TURBK_AGENT_CHUNK_BATCH_RETRY_MAX_BACKOFF", "5s")
	t.Setenv("TURBK_AGENT_CHUNK_BATCH_SPLIT_ON_413", "false")
	t.Setenv("TURBK_AGENT_CHUNK_PIPELINE_ENABLED", "true")
	t.Setenv("TURBK_AGENT_MAX_CHUNK_CHECK_INFLIGHT", "3")
	t.Setenv("TURBK_AGENT_MAX_CHUNK_UPLOAD_INFLIGHT", "4")
	t.Setenv("TURBK_AGENT_MAX_CHUNK_PIPELINE_BYTES", "256MiB")
	t.Setenv("TURBK_AGENT_MAX_CHUNK_UPLOAD_INFLIGHT_PER_AGENT", "5")
	t.Setenv("TURBK_AGENT_MAX_CHUNK_UPLOAD_INFLIGHT_BYTES_PER_AGENT", "512MiB")
	t.Setenv("TURBK_AGENT_MAX_CHUNK_UPLOAD_INFLIGHT_BYTES_GLOBAL", "2GiB")
	t.Setenv("TURBK_AGENT_CHUNK_UPLOAD_RETRY_AFTER", "1s")
	t.Setenv("TURBK_AGENT_REPO_WRITE_QUEUE_ENABLED", "true")
	t.Setenv("TURBK_AGENT_REPO_WRITE_QUEUE_MAX_REQUESTS", "9")
	t.Setenv("TURBK_AGENT_REPO_WRITE_QUEUE_MAX_BYTES", "768MiB")
	t.Setenv("TURBK_AGENT_REPO_WRITE_COALESCE_WINDOW", "10ms")
	t.Setenv("TURBK_AGENT_REPO_WRITE_COALESCE_MAX_BYTES", "192MiB")
	t.Setenv("TURBK_AGENT_REPO_WRITE_SUB_BATCH_BYTES", "32MiB")
	t.Setenv("TURBK_AGENT_SCAN_PARALLEL_ENABLED", "true")
	t.Setenv("TURBK_AGENT_FILE_READ_WORKERS", "6")
	t.Setenv("TURBK_AGENT_FILE_READ_PIPELINE_BYTES", "384MiB")
	t.Setenv("TURBK_AGENT_SMALL_FILE_PACK_ENABLED", "true")
	t.Setenv("TURBK_AGENT_SMALL_FILE_PACK_MAX_FILE_SIZE", "128KiB")
	t.Setenv("TURBK_AGENT_SMALL_FILE_PACK_TARGET_SIZE", "16MiB")

	cfg, err := Load("../../configs/turbk.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Repository.SegmentSize != "256MiB" ||
		cfg.Repository.ChunkAvgSize != "2MiB" ||
		cfg.Repository.Compression != "none" ||
		cfg.Repository.Encryption != "none" ||
		cfg.Agent.MaxChunkUploadBatchBytes != "32MiB" ||
		cfg.Agent.MaxChunkResponseBytes != "16MiB" ||
		cfg.Agent.ChunkBatchMaxRetries != 7 ||
		cfg.Agent.ChunkBatchRetryInitialBackoff != "250ms" ||
		cfg.Agent.ChunkBatchRetryMaxBackoff != "5s" ||
		cfg.Agent.ChunkBatchSplitOn413 ||
		!cfg.Agent.ChunkPipelineEnabled ||
		cfg.Agent.MaxChunkCheckInflight != 3 ||
		cfg.Agent.MaxChunkUploadInflight != 4 ||
		cfg.Agent.MaxChunkPipelineBytes != "256MiB" ||
		cfg.Agent.MaxChunkUploadInflightPerAgent != 5 ||
		cfg.Agent.MaxChunkUploadInflightBytesPerAgent != "512MiB" ||
		cfg.Agent.MaxChunkUploadInflightBytesGlobal != "2GiB" ||
		cfg.Agent.ChunkUploadRetryAfter != "1s" ||
		!cfg.Agent.RepoWriteQueueEnabled ||
		cfg.Agent.RepoWriteQueueMaxRequests != 9 ||
		cfg.Agent.RepoWriteQueueMaxBytes != "768MiB" ||
		cfg.Agent.RepoWriteCoalesceWindow != "10ms" ||
		cfg.Agent.RepoWriteCoalesceMaxBytes != "192MiB" ||
		cfg.Agent.RepoWriteSubBatchBytes != "32MiB" ||
		!cfg.Agent.ScanParallelEnabled ||
		cfg.Agent.FileReadWorkers != 6 ||
		cfg.Agent.FileReadPipelineBytes != "384MiB" ||
		!cfg.Agent.SmallFilePackEnabled ||
		cfg.Agent.SmallFilePackMaxFileSize != "128KiB" ||
		cfg.Agent.SmallFilePackTargetSize != "16MiB" {
		t.Fatalf("env overrides were not applied: repository=%+v agent=%+v", cfg.Repository, cfg.Agent)
	}
}
