package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server     ServerConfig     `json:"server" yaml:"server"`
	Auth       AuthConfig       `json:"auth" yaml:"auth"`
	Paths      PathsConfig      `json:"paths" yaml:"paths"`
	Repository RepositoryConfig `json:"repository" yaml:"repository"`
	Scheduler  SchedulerConfig  `json:"scheduler" yaml:"scheduler"`
	Retention  RetentionConfig  `json:"retention" yaml:"retention"`
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
