package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tursom/turbk/internal/cronexpr"
	"github.com/tursom/turbk/internal/rootset"
)

type rootFlag struct {
	values  []string
	changed bool
}

func newRootFlagFromEnv() rootFlag {
	roots := rootset.SplitList(os.Getenv("TURBK_AGENT_ROOTS"))
	if len(roots) == 0 {
		if root := strings.TrimSpace(os.Getenv("TURBK_AGENT_ROOT")); root != "" {
			roots = []string{root}
		}
	}
	return rootFlag{values: roots}
}

func (f *rootFlag) Set(value string) error {
	if !f.changed {
		f.values = nil
		f.changed = true
	}
	f.values = append(f.values, value)
	return nil
}

func (f *rootFlag) String() string {
	return strings.Join(f.values, ",")
}

func (f *rootFlag) Values() []string {
	if len(f.values) == 0 {
		return nil
	}
	values := make([]string, len(f.values))
	copy(values, f.values)
	return values
}

func envBool(name string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	switch strings.ToLower(value) {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return fallback
	}
}

func envString(name, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	return value
}

func envInt(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func parseDurationOrDefault(value string, fallback time.Duration) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration < 0 {
		return fallback
	}
	return duration
}

func parseAgentByteSize(value string) (int64, error) {
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

func parseBackupScheduleOrDefault(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	if !cronexpr.Valid(value) {
		return fallback
	}
	return value
}

func applyAgentRuntimeEnvOverrides(opts *backupRunOptions) {
	if opts == nil {
		return
	}
	if raw := strings.TrimSpace(os.Getenv("TURBK_AGENT_CHUNK_PIPELINE_ENABLED")); raw != "" {
		if parsed, ok := parseAgentBool(raw); ok && !parsed {
			opts.ChunkPipelineEnabled = false
		}
	}
	if value := envInt("TURBK_AGENT_MAX_CHUNK_CHECK_INFLIGHT", 0); value > 0 {
		opts.MaxChunkCheckInflight = minPositiveInt(opts.MaxChunkCheckInflight, value)
	}
	if value := envInt("TURBK_AGENT_MAX_CHUNK_UPLOAD_INFLIGHT", 0); value > 0 {
		opts.MaxChunkUploadInflight = minPositiveInt(opts.MaxChunkUploadInflight, value)
	}
	if raw := strings.TrimSpace(os.Getenv("TURBK_AGENT_MAX_CHUNK_PIPELINE_BYTES")); raw != "" {
		if value, err := parseAgentByteSize(raw); err == nil && value > 0 {
			opts.MaxChunkPipelineBytes = minPositiveInt64(opts.MaxChunkPipelineBytes, value)
		}
	}
	if value := envInt("TURBK_AGENT_CHUNK_BATCH_MAX_RETRIES", 0); value > 0 {
		opts.ChunkBatchMaxRetries = value
	}
	if raw := strings.TrimSpace(os.Getenv("TURBK_AGENT_CHUNK_BATCH_RETRY_INITIAL_BACKOFF")); raw != "" {
		if value, err := time.ParseDuration(raw); err == nil && value > 0 {
			opts.ChunkBatchRetryInitialBackoff = value
		}
	}
	if raw := strings.TrimSpace(os.Getenv("TURBK_AGENT_CHUNK_BATCH_RETRY_MAX_BACKOFF")); raw != "" {
		if value, err := time.ParseDuration(raw); err == nil && value > 0 {
			opts.ChunkBatchRetryMaxBackoff = value
		}
	}
	if raw := strings.TrimSpace(os.Getenv("TURBK_AGENT_CHUNK_BATCH_SPLIT_ON_413")); raw != "" {
		if parsed, ok := parseAgentBool(raw); ok {
			opts.ChunkBatchSplitOn413 = parsed
		}
	}
	if raw := strings.TrimSpace(os.Getenv("TURBK_AGENT_SCAN_PARALLEL_ENABLED")); raw != "" {
		if parsed, ok := parseAgentBool(raw); ok && !parsed {
			opts.ScanParallelEnabled = false
		}
	}
	if value := envInt("TURBK_AGENT_FILE_READ_WORKERS", 0); value > 0 {
		opts.FileReadWorkers = minPositiveInt(opts.FileReadWorkers, value)
	}
	if raw := strings.TrimSpace(os.Getenv("TURBK_AGENT_FILE_READ_PIPELINE_BYTES")); raw != "" {
		if value, err := parseAgentByteSize(raw); err == nil && value > 0 {
			opts.FileReadPipelineBytes = minPositiveInt64(opts.FileReadPipelineBytes, value)
		}
	}
}

func parseAgentBool(value string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "y", "on":
		return true, true
	case "0", "false", "no", "n", "off":
		return false, true
	default:
		return false, false
	}
}

func minPositiveInt(serverValue, localValue int) int {
	if serverValue <= 0 {
		return localValue
	}
	if localValue <= 0 || localValue > serverValue {
		return serverValue
	}
	return localValue
}

func minPositiveInt64(serverValue, localValue int64) int64 {
	if serverValue <= 0 {
		return localValue
	}
	if localValue <= 0 || localValue > serverValue {
		return serverValue
	}
	return localValue
}
