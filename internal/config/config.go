package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server      ServerConfig      `json:"server" yaml:"server"`
	Auth        AuthConfig        `json:"auth" yaml:"auth"`
	Paths       PathsConfig       `json:"paths" yaml:"paths"`
	Repository  RepositoryConfig  `json:"repository" yaml:"repository"`
	Scheduler   SchedulerConfig   `json:"scheduler" yaml:"scheduler"`
	Retention   RetentionConfig   `json:"retention" yaml:"retention"`
	Maintenance MaintenanceConfig `json:"maintenance" yaml:"maintenance"`
	Agent       AgentConfig       `json:"agent" yaml:"agent"`
}

type ServerConfig struct {
	Listen    string `json:"listen" yaml:"listen"`
	PublicURL string `json:"public_url" yaml:"public_url"`
	WebDir    string `json:"web_dir" yaml:"web_dir"`
}

type AuthConfig struct {
	Username        string `json:"username" yaml:"username"`
	Password        string `json:"password" yaml:"password"`
	SessionTTLHours int    `json:"session_ttl_hours" yaml:"session_ttl_hours"`
}

type PathsConfig struct {
	StateDir     string   `json:"state_dir" yaml:"state_dir"`
	RepoDir      string   `json:"repo_dir" yaml:"repo_dir"`
	RestoreRoots []string `json:"restore_roots" yaml:"restore_roots"`
}

type RepositoryConfig struct {
	SegmentSize  string `json:"segment_size" yaml:"segment_size"`
	ChunkAvgSize string `json:"chunk_avg_size" yaml:"chunk_avg_size"`
	Compression  string `json:"compression" yaml:"compression"`
	Encryption   string `json:"encryption" yaml:"encryption"`
}

type SchedulerConfig struct {
	Timezone          string `json:"timezone" yaml:"timezone"`
	MaxConcurrentRuns int    `json:"max_concurrent_runs" yaml:"max_concurrent_runs"`
}

type RetentionConfig struct {
	KeepLast   int `json:"keep_last" yaml:"keep_last"`
	KeepDaily  int `json:"keep_daily" yaml:"keep_daily"`
	KeepWeekly int `json:"keep_weekly" yaml:"keep_weekly"`
}

type MaintenanceConfig struct {
	Enabled                 bool    `json:"enabled" yaml:"enabled"`
	Timezone                string  `json:"timezone" yaml:"timezone"`
	CleanupSchedule         string  `json:"cleanup_schedule" yaml:"cleanup_schedule"`
	CompactEnabled          bool    `json:"compact_enabled" yaml:"compact_enabled"`
	CompactSchedule         string  `json:"compact_schedule" yaml:"compact_schedule"`
	ErrorGracePeriod        string  `json:"error_grace_period" yaml:"error_grace_period"`
	StaleRunAfter           string  `json:"stale_run_after" yaml:"stale_run_after"`
	KeepDeletedMetadataDays int     `json:"keep_deleted_metadata_days" yaml:"keep_deleted_metadata_days"`
	CompactMinReclaimRatio  float64 `json:"compact_min_reclaim_ratio" yaml:"compact_min_reclaim_ratio"`
	CompactMinReclaimBytes  string  `json:"compact_min_reclaim_bytes" yaml:"compact_min_reclaim_bytes"`
}

type AgentConfig struct {
	CommandTTL                    string `json:"command_ttl" yaml:"command_ttl"`
	DefaultPollInterval           string `json:"default_poll_interval" yaml:"default_poll_interval"`
	MaxChunkCheckBatch            int    `json:"max_chunk_check_batch" yaml:"max_chunk_check_batch"`
	MaxChunkUploadBatchBytes      string `json:"max_chunk_upload_batch_bytes" yaml:"max_chunk_upload_batch_bytes"`
	MaxInvalidationResponseHashes int    `json:"max_invalidation_response_hashes" yaml:"max_invalidation_response_hashes"`
	InvalidationRetentionDays     int    `json:"invalidation_retention_days" yaml:"invalidation_retention_days"`
}

func Default() Config {
	return Config{
		Server: ServerConfig{
			Listen:    ":8080",
			PublicURL: "http://localhost:8080",
			WebDir:    "web/dist",
		},
		Auth: AuthConfig{
			Username:        "admin",
			Password:        "admin",
			SessionTTLHours: 24,
		},
		Paths: PathsConfig{
			StateDir:     "./data/state",
			RepoDir:      "./data/repo",
			RestoreRoots: []string{"./data/restore"},
		},
		Repository: RepositoryConfig{
			SegmentSize:  "512MiB",
			ChunkAvgSize: "1MiB",
			Compression:  "zstd",
			Encryption:   "aes-256-gcm",
		},
		Scheduler: SchedulerConfig{
			Timezone:          "Asia/Shanghai",
			MaxConcurrentRuns: 2,
		},
		Retention: RetentionConfig{
			KeepLast:   30,
			KeepDaily:  30,
			KeepWeekly: 12,
		},
		Maintenance: MaintenanceConfig{
			Enabled:                 true,
			Timezone:                "Asia/Shanghai",
			CleanupSchedule:         "0 3 * * *",
			CompactEnabled:          true,
			CompactSchedule:         "30 3 * * 0",
			ErrorGracePeriod:        "24h",
			StaleRunAfter:           "6h",
			KeepDeletedMetadataDays: 30,
			CompactMinReclaimRatio:  0.15,
			CompactMinReclaimBytes:  "1GiB",
		},
		Agent: AgentConfig{
			CommandTTL:                    "30m",
			DefaultPollInterval:           "10m",
			MaxChunkCheckBatch:            10000,
			MaxChunkUploadBatchBytes:      "64MiB",
			MaxInvalidationResponseHashes: 100000,
			InvalidationRetentionDays:     30,
		},
	}
}

func Load(path string) (Config, error) {
	cfg := Default()
	if envPath := os.Getenv("TURBK_CONFIG"); path == "" && envPath != "" {
		path = envPath
	}
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("read config %q: %w", path, err)
		}
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return Config{}, fmt.Errorf("parse config %q: %w", path, err)
		}
	}
	applyEnv(&cfg)
	if err := cfg.Normalize(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c *Config) Normalize() error {
	if c.Server.Listen == "" {
		return errors.New("server.listen is required")
	}
	if c.Paths.StateDir == "" {
		return errors.New("paths.state_dir is required")
	}
	if c.Paths.RepoDir == "" {
		return errors.New("paths.repo_dir is required")
	}
	if c.Server.WebDir == "" {
		c.Server.WebDir = "web/dist"
	}
	if c.Auth.Username == "" {
		c.Auth.Username = "admin"
	}
	if c.Auth.Password == "" {
		c.Auth.Password = "admin"
	}
	if c.Auth.SessionTTLHours <= 0 {
		c.Auth.SessionTTLHours = 24
	}
	if len(c.Paths.RestoreRoots) == 0 {
		c.Paths.RestoreRoots = []string{filepath.Join(c.Paths.StateDir, "restore")}
	}
	if c.Scheduler.Timezone == "" {
		c.Scheduler.Timezone = "Asia/Shanghai"
	}
	if c.Scheduler.MaxConcurrentRuns <= 0 {
		c.Scheduler.MaxConcurrentRuns = 1
	}
	if c.Retention.KeepLast <= 0 {
		c.Retention.KeepLast = 30
	}
	if c.Retention.KeepDaily < 0 || c.Retention.KeepWeekly < 0 {
		return errors.New("retention keep values must be non-negative")
	}
	if c.Maintenance.Timezone == "" {
		c.Maintenance.Timezone = c.Scheduler.Timezone
	}
	if c.Maintenance.CleanupSchedule == "" {
		c.Maintenance.CleanupSchedule = "0 3 * * *"
	}
	if c.Maintenance.CompactSchedule == "" {
		c.Maintenance.CompactSchedule = "30 3 * * 0"
	}
	if c.Maintenance.ErrorGracePeriod == "" {
		c.Maintenance.ErrorGracePeriod = "24h"
	}
	if duration, err := time.ParseDuration(c.Maintenance.ErrorGracePeriod); err != nil {
		return fmt.Errorf("maintenance.error_grace_period must be a duration: %w", err)
	} else if duration < 0 {
		return errors.New("maintenance.error_grace_period must be non-negative")
	}
	if c.Maintenance.StaleRunAfter == "" {
		c.Maintenance.StaleRunAfter = "6h"
	}
	if duration, err := time.ParseDuration(c.Maintenance.StaleRunAfter); err != nil {
		return fmt.Errorf("maintenance.stale_run_after must be a duration: %w", err)
	} else if duration < 0 {
		return errors.New("maintenance.stale_run_after must be non-negative")
	}
	if c.Maintenance.KeepDeletedMetadataDays < 0 {
		return errors.New("maintenance.keep_deleted_metadata_days must be non-negative")
	}
	if c.Maintenance.CompactMinReclaimRatio < 0 {
		return errors.New("maintenance.compact_min_reclaim_ratio must be non-negative")
	}
	if c.Maintenance.CompactMinReclaimBytes == "" {
		c.Maintenance.CompactMinReclaimBytes = "1GiB"
	}
	if c.Agent.CommandTTL == "" {
		c.Agent.CommandTTL = "30m"
	}
	if duration, err := time.ParseDuration(c.Agent.CommandTTL); err != nil {
		return fmt.Errorf("agent.command_ttl must be a duration: %w", err)
	} else if duration <= 0 {
		return errors.New("agent.command_ttl must be positive")
	}
	if c.Agent.DefaultPollInterval == "" {
		c.Agent.DefaultPollInterval = "10m"
	}
	if duration, err := time.ParseDuration(c.Agent.DefaultPollInterval); err != nil {
		return fmt.Errorf("agent.default_poll_interval must be a duration: %w", err)
	} else if duration <= 0 {
		return errors.New("agent.default_poll_interval must be positive")
	}
	if c.Agent.MaxChunkCheckBatch <= 0 {
		c.Agent.MaxChunkCheckBatch = 10000
	}
	if strings.TrimSpace(c.Agent.MaxChunkUploadBatchBytes) == "" {
		c.Agent.MaxChunkUploadBatchBytes = "64MiB"
	}
	if bytes, err := parseConfigByteSize(c.Agent.MaxChunkUploadBatchBytes); err != nil {
		return fmt.Errorf("agent.max_chunk_upload_batch_bytes must be a byte size: %w", err)
	} else if bytes <= 0 {
		return errors.New("agent.max_chunk_upload_batch_bytes must be positive")
	}
	if c.Agent.MaxInvalidationResponseHashes <= 0 {
		c.Agent.MaxInvalidationResponseHashes = 100000
	}
	if c.Agent.InvalidationRetentionDays < 0 {
		return errors.New("agent.invalidation_retention_days must be non-negative")
	}
	return nil
}

func applyEnv(c *Config) {
	applyString := func(env string, dst *string) {
		if v := os.Getenv(env); v != "" {
			*dst = v
		}
	}
	applyInt := func(env string, dst *int) {
		if v := os.Getenv(env); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				*dst = n
			}
		}
	}
	applyBool := func(env string, dst *bool) {
		if v := os.Getenv(env); v != "" {
			if parsed, err := strconv.ParseBool(v); err == nil {
				*dst = parsed
			}
		}
	}
	applyFloat := func(env string, dst *float64) {
		if v := os.Getenv(env); v != "" {
			if parsed, err := strconv.ParseFloat(v, 64); err == nil {
				*dst = parsed
			}
		}
	}

	applyString("TURBK_LISTEN", &c.Server.Listen)
	applyString("TURBK_PUBLIC_URL", &c.Server.PublicURL)
	applyString("TURBK_WEB_DIR", &c.Server.WebDir)
	applyString("TURBK_ADMIN_USERNAME", &c.Auth.Username)
	applyString("TURBK_ADMIN_PASSWORD", &c.Auth.Password)
	applyInt("TURBK_SESSION_TTL_HOURS", &c.Auth.SessionTTLHours)
	applyString("TURBK_STATE_DIR", &c.Paths.StateDir)
	applyString("TURBK_REPO_DIR", &c.Paths.RepoDir)
	if v := os.Getenv("TURBK_RESTORE_ROOTS"); v != "" {
		c.Paths.RestoreRoots = splitList(v)
	}
	applyString("TURBK_SEGMENT_SIZE", &c.Repository.SegmentSize)
	applyString("TURBK_CHUNK_AVG_SIZE", &c.Repository.ChunkAvgSize)
	applyString("TURBK_COMPRESSION", &c.Repository.Compression)
	applyString("TURBK_ENCRYPTION", &c.Repository.Encryption)
	applyString("TURBK_TIMEZONE", &c.Scheduler.Timezone)
	applyInt("TURBK_MAX_CONCURRENT_RUNS", &c.Scheduler.MaxConcurrentRuns)
	applyInt("TURBK_KEEP_LAST", &c.Retention.KeepLast)
	applyInt("TURBK_KEEP_DAILY", &c.Retention.KeepDaily)
	applyInt("TURBK_KEEP_WEEKLY", &c.Retention.KeepWeekly)
	applyBool("TURBK_MAINTENANCE_ENABLED", &c.Maintenance.Enabled)
	applyString("TURBK_MAINTENANCE_TIMEZONE", &c.Maintenance.Timezone)
	applyString("TURBK_MAINTENANCE_CLEANUP_SCHEDULE", &c.Maintenance.CleanupSchedule)
	applyBool("TURBK_MAINTENANCE_COMPACT_ENABLED", &c.Maintenance.CompactEnabled)
	applyString("TURBK_MAINTENANCE_COMPACT_SCHEDULE", &c.Maintenance.CompactSchedule)
	applyString("TURBK_MAINTENANCE_ERROR_GRACE_PERIOD", &c.Maintenance.ErrorGracePeriod)
	applyString("TURBK_MAINTENANCE_STALE_RUN_AFTER", &c.Maintenance.StaleRunAfter)
	applyInt("TURBK_MAINTENANCE_KEEP_DELETED_METADATA_DAYS", &c.Maintenance.KeepDeletedMetadataDays)
	applyFloat("TURBK_MAINTENANCE_COMPACT_MIN_RECLAIM_RATIO", &c.Maintenance.CompactMinReclaimRatio)
	applyString("TURBK_MAINTENANCE_COMPACT_MIN_RECLAIM_BYTES", &c.Maintenance.CompactMinReclaimBytes)
	applyString("TURBK_AGENT_COMMAND_TTL", &c.Agent.CommandTTL)
	applyString("TURBK_AGENT_DEFAULT_POLL_INTERVAL", &c.Agent.DefaultPollInterval)
	applyInt("TURBK_AGENT_MAX_CHUNK_CHECK_BATCH", &c.Agent.MaxChunkCheckBatch)
	applyString("TURBK_AGENT_MAX_CHUNK_UPLOAD_BATCH_BYTES", &c.Agent.MaxChunkUploadBatchBytes)
	applyInt("TURBK_AGENT_MAX_INVALIDATION_RESPONSE_HASHES", &c.Agent.MaxInvalidationResponseHashes)
	applyInt("TURBK_AGENT_INVALIDATION_RETENTION_DAYS", &c.Agent.InvalidationRetentionDays)
}

func parseConfigByteSize(value string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, errors.New("size is required")
	}
	units := []struct {
		suffix string
		mult   float64
	}{
		{"tib", 1024 * 1024 * 1024 * 1024},
		{"tb", 1000 * 1000 * 1000 * 1000},
		{"gib", 1024 * 1024 * 1024},
		{"gb", 1000 * 1000 * 1000},
		{"mib", 1024 * 1024},
		{"mb", 1000 * 1000},
		{"kib", 1024},
		{"kb", 1000},
		{"b", 1},
	}
	lower := strings.ToLower(value)
	multiplier := float64(1)
	number := lower
	for _, unit := range units {
		if strings.HasSuffix(lower, unit.suffix) {
			multiplier = unit.mult
			number = strings.TrimSpace(strings.TrimSuffix(lower, unit.suffix))
			break
		}
	}
	parsed, err := strconv.ParseFloat(number, 64)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("invalid size %q", value)
	}
	return int64(parsed * multiplier), nil
}

func splitList(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
