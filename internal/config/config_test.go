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

	cfg, err := Load("../../configs/turbk.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Repository.SegmentSize != "256MiB" ||
		cfg.Repository.ChunkAvgSize != "2MiB" ||
		cfg.Repository.Compression != "none" ||
		cfg.Repository.Encryption != "none" ||
		cfg.Agent.MaxChunkUploadBatchBytes != "32MiB" {
		t.Fatalf("env overrides were not applied: repository=%+v agent=%+v", cfg.Repository, cfg.Agent)
	}
}
