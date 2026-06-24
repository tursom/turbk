package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/tursom/turbk/internal/cronexpr"
	"github.com/tursom/turbk/internal/fsfilter"
	"github.com/tursom/turbk/internal/repository"
	"github.com/tursom/turbk/internal/rootset"
	"github.com/tursom/turbk/internal/version"
	"github.com/zeebo/blake3"
)

const agentChunkAvgSize = 1024 * 1024
const defaultAgentBackupSchedule = "0 0 * * *"
const defaultAgentChunkUploadBatchBytes = int64(64 * 1024 * 1024)
const defaultAgentFileCatalogBatchFiles = 1000
const defaultAgentFileCatalogBatchBytes = int64(16 * 1024 * 1024)
const defaultAgentResponseBodyLimit = int64(64 * 1024 * 1024)
const defaultAgentMaxChunkResponseBytes = int64(64 * 1024 * 1024)
const defaultAgentMaxChunkPipelineBytes = int64(128 * 1024 * 1024)
const defaultAgentFileReadPipelineBytes = int64(512 * 1024 * 1024)
const defaultAgentChunkBatchMaxRetries = 5
const defaultAgentChunkBatchRetryInitialBackoff = 500 * time.Millisecond
const defaultAgentChunkBatchRetryMaxBackoff = 30 * time.Second
const agentChunkBatchContentType = "application/vnd.turbk.chunk-batch.v1"

const estimatedChunkCheckResponseBytesPerHash = int64(80)
const estimatedCompactChunkUploadResponseBytesPerChunk = int64(140)
const estimatedFullChunkUploadResponseBytesPerChunk = int64(360)
const estimatedChunkBatchResponseBaseBytes = int64(512)

var (
	agentChunkBatchMagic = []byte("TBKCHB1\n")
	agentLstat           = os.Lstat
	agentOpen            = os.Open
)

type agentClient struct {
	serverURL    string
	clientID     string
	clientSecret string
	http         *http.Client
}

type runRef struct {
	ID int64 `json:"id"`
}

type agentCommand struct {
	ID        int64           `json:"id"`
	Type      string          `json:"type"`
	JobID     int64           `json:"job_id"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
	ExpiresAt time.Time       `json:"expires_at"`
}

type heartbeatRequest struct {
	Hostname                   string `json:"hostname"`
	Version                    string `json:"version"`
	Mode                       string `json:"mode,omitempty"`
	StateDir                   string `json:"state_dir,omitempty"`
	CatalogStatus              string `json:"catalog_status,omitempty"`
	RepositoryID               string `json:"repository_id,omitempty"`
	ChunkGeneration            int64  `json:"chunk_generation,omitempty"`
	ConfigGeneration           int64  `json:"config_generation,omitempty"`
	CommandGeneration          int64  `json:"command_generation,omitempty"`
	RunningRunID               int64  `json:"running_run_id,omitempty"`
	LastError                  string `json:"last_error,omitempty"`
	CompactChunkCheckResponse  bool   `json:"compact_chunk_check_response,omitempty"`
	CompactChunkUploadResponse bool   `json:"compact_chunk_upload_response,omitempty"`
	SmallFilePack              bool   `json:"small_file_pack,omitempty"`
	ChunkPipeline              bool   `json:"chunk_pipeline,omitempty"`
	ScanParallel               bool   `json:"scan_parallel,omitempty"`
}

type heartbeatResponse struct {
	Status     string         `json:"status"`
	ClientID   string         `json:"client_id"`
	Subject    string         `json:"subject"`
	ServerTime time.Time      `json:"server_time"`
	Repository repositoryInfo `json:"repository"`
	Agent      agentInfo      `json:"agent"`
	Commands   []agentCommand `json:"commands"`
}

type repositoryInfo struct {
	ID                        string `json:"id"`
	ChunkGeneration           int64  `json:"chunk_generation"`
	InvalidationAvailableFrom int64  `json:"invalidation_available_from"`
}

type agentInfo struct {
	ConfigGeneration              int64  `json:"config_generation"`
	CommandGeneration             int64  `json:"command_generation"`
	PollIntervalSeconds           int64  `json:"poll_interval_seconds"`
	DefaultPollInterval           string `json:"default_poll_interval"`
	MaxChunkCheckBatch            int    `json:"max_chunk_check_batch"`
	MaxChunkUploadBatchBytes      int64  `json:"max_chunk_upload_batch_bytes"`
	MaxChunkResponseBytes         int64  `json:"max_chunk_response_bytes"`
	ChunkBatchMaxRetries          int    `json:"chunk_batch_max_retries"`
	ChunkBatchRetryInitialBackoff string `json:"chunk_batch_retry_initial_backoff"`
	ChunkBatchRetryMaxBackoff     string `json:"chunk_batch_retry_max_backoff"`
	ChunkBatchSplitOn413          bool   `json:"chunk_batch_split_on_413"`
	ChunkPipelineEnabled          bool   `json:"chunk_pipeline_enabled"`
	MaxChunkCheckInflight         int    `json:"max_chunk_check_inflight"`
	MaxChunkUploadInflight        int    `json:"max_chunk_upload_inflight"`
	MaxChunkPipelineBytes         int64  `json:"max_chunk_pipeline_bytes"`
	ChunkBatchUpload              bool   `json:"chunk_batch_upload"`
	CompactChunkCheckResponse     bool   `json:"compact_chunk_check_response"`
	CompactChunkUploadResponse    bool   `json:"compact_chunk_upload_response"`
	SmallFilePack                 bool   `json:"small_file_pack"`
	SmallFilePackEnabled          bool   `json:"small_file_pack_enabled"`
	SmallFilePackMaxFileSize      int64  `json:"small_file_pack_max_file_size"`
	SmallFilePackTargetSize       int64  `json:"small_file_pack_target_size"`
	ScanParallelEnabled           bool   `json:"scan_parallel_enabled"`
	FileReadWorkers               int    `json:"file_read_workers"`
	FileReadPipelineBytes         int64  `json:"file_read_pipeline_bytes"`
	MaxManifestRepairAttempts     int    `json:"max_manifest_repair_attempts"`
}

func (a agentInfo) chunkBatchRetryInitialBackoff() time.Duration {
	return parseDurationOrDefault(a.ChunkBatchRetryInitialBackoff, defaultAgentChunkBatchRetryInitialBackoff)
}

func (a agentInfo) chunkBatchRetryMaxBackoff() time.Duration {
	return parseDurationOrDefault(a.ChunkBatchRetryMaxBackoff, defaultAgentChunkBatchRetryMaxBackoff)
}

type createRunResponse struct {
	Status string `json:"status"`
	Run    runRef `json:"run"`
}

type chunkResponse struct {
	Exists   bool                `json:"exists"`
	Uploaded bool                `json:"uploaded"`
	Ref      repository.ChunkRef `json:"ref"`
}

type submitManifestResponse struct {
	Status          string                 `json:"status"`
	Run             runRef                 `json:"run"`
	RepositoryID    string                 `json:"repository_id"`
	ChunkGeneration int64                  `json:"chunk_generation"`
	MissingChunks   []missingChunkResponse `json:"missing_chunks"`
	Retryable       bool                   `json:"retryable"`
}

type missingChunkResponse struct {
	Hash  string   `json:"hash"`
	Paths []string `json:"paths"`
}

type checkChunksResponse struct {
	RepositoryID    string   `json:"repository_id"`
	ChunkGeneration int64    `json:"chunk_generation"`
	Exists          []string `json:"exists"`
	Missing         []string `json:"missing"`
	ResponseBytes   int64    `json:"-"`
	RetryCount      int64    `json:"-"`
}

func (r *checkChunksResponse) setResponseBodyBytes(bytes int64) {
	r.ResponseBytes = bytes
}

type uploadChunksResponse struct {
	Status          string                `json:"status"`
	RepositoryID    string                `json:"repository_id"`
	ChunkGeneration int64                 `json:"chunk_generation"`
	Chunks          []uploadChunkResponse `json:"chunks"`
	RequestBytes    int64                 `json:"-"`
	ResponseBytes   int64                 `json:"-"`
	RetryCount      int64                 `json:"-"`
	SplitCount      int64                 `json:"-"`
}

func (r *uploadChunksResponse) setResponseBodyBytes(bytes int64) {
	r.ResponseBytes = bytes
}

type uploadChunkResponse struct {
	Hash         string              `json:"hash"`
	Exists       *bool               `json:"exists,omitempty"`
	Uploaded     bool                `json:"uploaded"`
	OriginalSize int64               `json:"original_size,omitempty"`
	Ref          repository.ChunkRef `json:"ref"`
}

type invalidationsResponse struct {
	RepositoryID      string   `json:"repository_id"`
	FromGeneration    int64    `json:"from_generation"`
	ToGeneration      int64    `json:"to_generation"`
	Complete          bool     `json:"complete"`
	InvalidatedHashes []string `json:"invalidated_hashes"`
	Reason            string   `json:"reason"`
}

type agentProgress struct {
	Phase          string `json:"phase"`
	TotalFiles     int64  `json:"total_files"`
	ProcessedFiles int64  `json:"processed_files"`
	TotalBytes     int64  `json:"total_bytes"`
	ProcessedBytes int64  `json:"processed_bytes"`
	UploadedChunks int64  `json:"uploaded_chunks"`
	ReusedChunks   int64  `json:"reused_chunks"`
	Message        string `json:"message"`
}

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

func main() {
	var serverURL string
	var clientID string
	var clientSecret string
	rootsFlag := newRootFlagFromEnv()
	var once bool
	var daemon bool
	var stateDir string
	var pollIntervalValue string
	var pollJitterValue string
	var backupScheduleValue string
	var maxManifestRepairAttempts int
	var excludeValue string
	var skipPseudoFS bool
	var throughputBench bool
	var throughputFiles int
	var throughputFileSizeValue string
	var throughputModesValue string
	var throughputRTTValue string
	var throughputHTTP500 int
	var throughputHTTP429 int
	var throughputHTTP413Value string
	var throughputMaxCheckBatch int
	flag.StringVar(&serverURL, "server", os.Getenv("TURBK_SERVER_URL"), "Turbk server URL")
	flag.StringVar(&clientID, "client-id", os.Getenv("TURBK_AGENT_ID"), "Agent client ID")
	flag.StringVar(&clientSecret, "client-secret", os.Getenv("TURBK_AGENT_SECRET"), "Agent client secret")
	flag.Var(&rootsFlag, "root", "Root directory to back up; may be repeated")
	flag.StringVar(&stateDir, "state-dir", envString("TURBK_AGENT_STATE_DIR", "/var/lib/turbk-agent"), "Persistent agent state directory")
	flag.StringVar(&pollIntervalValue, "poll-interval", os.Getenv("TURBK_AGENT_POLL_INTERVAL"), "Daemon poll interval override")
	flag.StringVar(&pollJitterValue, "poll-jitter", envString("TURBK_AGENT_POLL_JITTER", "1m"), "Daemon poll jitter")
	flag.StringVar(&backupScheduleValue, "backup-schedule", envString("TURBK_AGENT_BACKUP_SCHEDULE", defaultAgentBackupSchedule), "Daemon local backup cron schedule")
	flag.IntVar(&maxManifestRepairAttempts, "max-manifest-repair-attempts", envInt("TURBK_AGENT_MAX_MANIFEST_REPAIR_ATTEMPTS", 3), "Maximum manifest missing chunk repair attempts")
	flag.StringVar(&excludeValue, "exclude", os.Getenv("TURBK_AGENT_EXCLUDES"), "Comma or newline separated path patterns to skip, relative to root")
	flag.BoolVar(&skipPseudoFS, "skip-pseudo-fs", envBool("TURBK_AGENT_SKIP_PSEUDO_FS", true), "Skip Linux pseudo filesystems such as procfs and sysfs")
	flag.BoolVar(&daemon, "daemon", envBool("TURBK_AGENT_DAEMON", false), "Run as a long-lived daemon")
	flag.BoolVar(&once, "once", false, "Send one heartbeat or run one backup and exit")
	flag.BoolVar(&throughputBench, "throughput-bench", false, "Run local agent throughput benchmark against a mock server")
	flag.IntVar(&throughputFiles, "throughput-files", 32, "Throughput benchmark file count")
	flag.StringVar(&throughputFileSizeValue, "throughput-file-size", "64KiB", "Throughput benchmark file size")
	flag.StringVar(&throughputModesValue, "throughput-modes", "serial,pipeline,parallel", "Comma-separated throughput benchmark modes: serial,pipeline,parallel")
	flag.StringVar(&throughputRTTValue, "throughput-rtt", "0s", "Artificial RTT delay for chunk check/upload requests")
	flag.IntVar(&throughputHTTP500, "throughput-http-500", 0, "Return HTTP 500 for the first N upload requests per mode")
	flag.IntVar(&throughputHTTP429, "throughput-http-429", 0, "Return HTTP 429 for the first N upload requests per mode")
	flag.StringVar(&throughputHTTP413Value, "throughput-http-413-threshold", "", "Return HTTP 413 when an upload request body exceeds this size")
	flag.IntVar(&throughputMaxCheckBatch, "throughput-max-check-batch", 1, "Max chunk check batch size used by benchmark agent modes")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if throughputBench {
		opts, err := newAgentThroughputBenchmarkOptions(throughputFiles, throughputFileSizeValue, throughputModesValue, throughputRTTValue, throughputHTTP500, throughputHTTP429, throughputHTTP413Value, throughputMaxCheckBatch)
		if err != nil {
			logger.Error("throughput benchmark options are invalid", "error", err)
			os.Exit(1)
		}
		if err := runAgentThroughputBenchmark(opts, os.Stdout); err != nil {
			logger.Error("throughput benchmark failed", "error", err)
			os.Exit(1)
		}
		return
	}
	if serverURL == "" {
		printReady()
		return
	}
	if clientID == "" || clientSecret == "" {
		logger.Error("agent client credentials are required", "client_id_set", clientID != "", "client_secret_set", clientSecret != "")
		os.Exit(1)
	}
	client := newAgentClient(serverURL, clientID, clientSecret)
	roots, err := normalizeOptionalRoots(rootsFlag.Values())
	if err != nil {
		logger.Error("agent roots are invalid", "error", err)
		os.Exit(1)
	}
	scanOptions := fsfilter.Options{
		ExcludePatterns:       fsfilter.SplitPatterns(excludeValue),
		SkipPseudoFilesystems: skipPseudoFS,
	}
	if daemon {
		opts := daemonOptions{
			Roots:                     roots,
			StateDir:                  stateDir,
			PollInterval:              parseDurationOrDefault(pollIntervalValue, 0),
			PollJitter:                parseDurationOrDefault(pollJitterValue, time.Minute),
			BackupSchedule:            parseBackupScheduleOrDefault(backupScheduleValue, defaultAgentBackupSchedule),
			MaxManifestRepairAttempts: maxManifestRepairAttempts,
			ScanOptions:               scanOptions,
		}
		if err := runDaemon(client, logger, opts); err != nil {
			logger.Error("agent daemon failed", "error", err)
			os.Exit(1)
		}
		return
	}
	if len(roots) > 0 {
		catalog, err := openAgentCatalog(stateDir)
		if err != nil {
			logger.Warn("agent catalog unavailable; falling back to stateless once mode", "error", err)
		}
		if catalog != nil {
			defer catalog.Close()
		}
		heartbeat, err := client.sendHeartbeatWithState(catalog, stateDir, "once", 0, "")
		if err != nil {
			logger.Error("agent heartbeat failed", "error", err)
			os.Exit(1)
		}
		if catalog != nil {
			if err := syncCatalogWithServer(client, catalog, heartbeat); err != nil {
				logger.Warn("agent catalog sync failed; continuing with server checks", "error", err)
			}
		}
		if err := runBackupWithOptions(client, roots, logger, scanOptions, backupRunOptions{
			Catalog:                       catalog,
			RepositoryID:                  heartbeat.Repository.ID,
			ChunkGeneration:               heartbeat.Repository.ChunkGeneration,
			MaxChunkCheckBatch:            heartbeat.Agent.MaxChunkCheckBatch,
			MaxChunkUploadBatchBytes:      heartbeat.Agent.MaxChunkUploadBatchBytes,
			MaxChunkResponseBytes:         heartbeat.Agent.MaxChunkResponseBytes,
			ChunkBatchMaxRetries:          heartbeat.Agent.ChunkBatchMaxRetries,
			ChunkBatchRetryInitialBackoff: heartbeat.Agent.chunkBatchRetryInitialBackoff(),
			ChunkBatchRetryMaxBackoff:     heartbeat.Agent.chunkBatchRetryMaxBackoff(),
			ChunkBatchSplitOn413:          heartbeat.Agent.ChunkBatchSplitOn413,
			ChunkPipelineEnabled:          heartbeat.Agent.ChunkPipelineEnabled,
			MaxChunkCheckInflight:         heartbeat.Agent.MaxChunkCheckInflight,
			MaxChunkUploadInflight:        heartbeat.Agent.MaxChunkUploadInflight,
			MaxChunkPipelineBytes:         heartbeat.Agent.MaxChunkPipelineBytes,
			ChunkBatchUpload:              heartbeat.Agent.ChunkBatchUpload,
			CompactChunkCheckResponse:     heartbeat.Agent.CompactChunkCheckResponse,
			CompactChunkUploadResponse:    heartbeat.Agent.CompactChunkUploadResponse,
			SmallFilePackEnabled:          heartbeat.Agent.SmallFilePack && heartbeat.Agent.SmallFilePackEnabled,
			SmallFilePackMaxFileSize:      heartbeat.Agent.SmallFilePackMaxFileSize,
			SmallFilePackTargetSize:       heartbeat.Agent.SmallFilePackTargetSize,
			ScanParallelEnabled:           heartbeat.Agent.ScanParallelEnabled,
			FileReadWorkers:               heartbeat.Agent.FileReadWorkers,
			FileReadPipelineBytes:         heartbeat.Agent.FileReadPipelineBytes,
			Trigger:                       "once",
			MaxManifestRepairAttempts:     maxManifestRepairAttempts,
		}); err != nil {
			logger.Error("agent backup failed", "error", err)
			os.Exit(1)
		}
		return
	}
	if err := client.sendHeartbeat(); err != nil {
		logger.Error("agent heartbeat failed", "error", err)
		os.Exit(1)
	}
	logger.Info("agent heartbeat accepted", "server", serverURL)
	if once {
		return
	}
	logger.Info("agent idle; pass -root to run a backup")
}

func normalizeOptionalRoots(roots []string) ([]string, error) {
	if len(roots) == 0 {
		return nil, nil
	}
	return rootset.Normalize(roots)
}

func printReady() {
	hostname, _ := os.Hostname()
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
		"name":     "turbk-agent",
		"version":  version.Version,
		"hostname": hostname,
		"status":   "ready",
	})
}

func newAgentClient(serverURL, clientID, clientSecret string) *agentClient {
	return &agentClient{
		serverURL:    strings.TrimRight(serverURL, "/"),
		clientID:     clientID,
		clientSecret: clientSecret,
		http:         newAgentHTTPClient(),
	}
}

func newAgentHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ForceAttemptHTTP2 = true
	transport.MaxIdleConns = envInt("TURBK_AGENT_HTTP_MAX_IDLE_CONNS", 100)
	transport.MaxIdleConnsPerHost = envInt("TURBK_AGENT_HTTP_MAX_IDLE_CONNS_PER_HOST", 16)
	transport.MaxConnsPerHost = envInt("TURBK_AGENT_HTTP_MAX_CONNS_PER_HOST", 0)
	transport.IdleConnTimeout = parseDurationOrDefault(envString("TURBK_AGENT_HTTP_IDLE_CONN_TIMEOUT", "90s"), 90*time.Second)
	transport.TLSHandshakeTimeout = parseDurationOrDefault(envString("TURBK_AGENT_HTTP_TLS_HANDSHAKE_TIMEOUT", "10s"), 10*time.Second)
	transport.ResponseHeaderTimeout = parseDurationOrDefault(os.Getenv("TURBK_AGENT_HTTP_RESPONSE_HEADER_TIMEOUT"), 0)
	transport.ExpectContinueTimeout = parseDurationOrDefault(envString("TURBK_AGENT_HTTP_EXPECT_CONTINUE_TIMEOUT", "1s"), time.Second)
	return &http.Client{
		Timeout:   parseDurationOrDefault(envString("TURBK_AGENT_HTTP_TIMEOUT", "60s"), 60*time.Second),
		Transport: transport,
	}
}

func (c *agentClient) sendHeartbeat() error {
	_, err := c.sendHeartbeatWithState(nil, "", "once", 0, "")
	return err
}

func (c *agentClient) sendHeartbeatWithState(catalog *agentCatalog, stateDir, mode string, runningRunID int64, lastError string) (heartbeatResponse, error) {
	hostname, _ := os.Hostname()
	req := heartbeatRequest{
		Hostname:                   hostname,
		Version:                    version.Version,
		Mode:                       mode,
		StateDir:                   stateDir,
		RunningRunID:               runningRunID,
		LastError:                  lastError,
		CompactChunkCheckResponse:  true,
		CompactChunkUploadResponse: true,
		SmallFilePack:              true,
		ChunkPipeline:              true,
		ScanParallel:               true,
	}
	if catalog != nil {
		req.CatalogStatus = "ok"
		if state, ok, err := catalog.serverState(c.serverURL, c.clientID); err == nil && ok {
			req.RepositoryID = state.RepositoryID
			req.ChunkGeneration = state.ChunkGeneration
			req.ConfigGeneration = state.ConfigGeneration
			req.CommandGeneration = state.CommandGeneration
		} else if err != nil {
			req.CatalogStatus = "error: " + err.Error()
		}
	}
	var resp heartbeatResponse
	_, err := c.doJSON(http.MethodPost, "/agent/v1/heartbeat", req, &resp)
	return resp, err
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

type agentThroughputBenchmarkOptions struct {
	Files              int
	FileSize           int64
	Modes              []string
	RTT                time.Duration
	HTTP500Uploads     int
	HTTP429Uploads     int
	HTTP413Threshold   int64
	MaxChunkCheckBatch int
}

type agentThroughputBenchmarkResult struct {
	Mode                  string  `json:"mode"`
	Files                 int     `json:"files"`
	FileSize              int64   `json:"file_size"`
	Bytes                 int64   `json:"bytes"`
	DurationSeconds       float64 `json:"duration_seconds"`
	ThroughputBytesPerSec float64 `json:"throughput_bytes_per_sec"`
	CheckRequests         int64   `json:"check_requests"`
	UploadRequests        int64   `json:"upload_requests"`
	UploadRequestBytes    int64   `json:"upload_request_bytes"`
	UploadResponseBytes   int64   `json:"upload_response_bytes"`
	Injected500           int64   `json:"injected_500"`
	Injected429           int64   `json:"injected_429"`
	Injected413           int64   `json:"injected_413"`
	ManifestEquivalent    bool    `json:"manifest_equivalent"`
	ManifestEntries       int     `json:"manifest_entries"`
	AllocBytes            uint64  `json:"alloc_bytes"`
	PeakHeapAllocBytes    uint64  `json:"peak_heap_alloc_bytes"`
}

type agentThroughputBenchmarkMemorySampler struct {
	done chan struct{}
	wg   sync.WaitGroup
	peak atomic.Uint64
}

type agentThroughputMockServer struct {
	rtt              time.Duration
	http500Uploads   int
	http429Uploads   int
	http413Threshold int64
	mu               sync.Mutex
	run              agentThroughputMockRun
}

type agentThroughputMockRun struct {
	remaining500        int
	remaining429        int
	checkRequests       int64
	uploadRequests      int64
	uploadRequestBytes  int64
	uploadResponseBytes int64
	injected500         int64
	injected429         int64
	injected413         int64
}

func newAgentThroughputBenchmarkOptions(files int, fileSizeValue, modesValue, rttValue string, http500Uploads, http429Uploads int, http413ThresholdValue string, maxChunkCheckBatch int) (agentThroughputBenchmarkOptions, error) {
	if files <= 0 {
		return agentThroughputBenchmarkOptions{}, errors.New("throughput-files must be positive")
	}
	fileSize, err := parseAgentByteSize(fileSizeValue)
	if err != nil {
		return agentThroughputBenchmarkOptions{}, fmt.Errorf("throughput-file-size: %w", err)
	}
	if fileSize < 0 {
		return agentThroughputBenchmarkOptions{}, errors.New("throughput-file-size must be non-negative")
	}
	rtt, err := time.ParseDuration(strings.TrimSpace(rttValue))
	if err != nil {
		return agentThroughputBenchmarkOptions{}, fmt.Errorf("throughput-rtt: %w", err)
	}
	if rtt < 0 {
		return agentThroughputBenchmarkOptions{}, errors.New("throughput-rtt must be non-negative")
	}
	var threshold int64
	if strings.TrimSpace(http413ThresholdValue) != "" {
		threshold, err = parseAgentByteSize(http413ThresholdValue)
		if err != nil {
			return agentThroughputBenchmarkOptions{}, fmt.Errorf("throughput-http-413-threshold: %w", err)
		}
		if threshold <= 0 {
			return agentThroughputBenchmarkOptions{}, errors.New("throughput-http-413-threshold must be positive")
		}
	}
	modes := splitBenchmarkModes(modesValue)
	if len(modes) == 0 {
		return agentThroughputBenchmarkOptions{}, errors.New("throughput-modes is required")
	}
	if maxChunkCheckBatch <= 0 {
		maxChunkCheckBatch = 1
	}
	return agentThroughputBenchmarkOptions{
		Files:              files,
		FileSize:           fileSize,
		Modes:              modes,
		RTT:                rtt,
		HTTP500Uploads:     http500Uploads,
		HTTP429Uploads:     http429Uploads,
		HTTP413Threshold:   threshold,
		MaxChunkCheckBatch: maxChunkCheckBatch,
	}, nil
}

func splitBenchmarkModes(value string) []string {
	parts := strings.Split(value, ",")
	modes := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		mode := strings.ToLower(strings.TrimSpace(part))
		if mode == "" {
			continue
		}
		if _, ok := seen[mode]; ok {
			continue
		}
		seen[mode] = struct{}{}
		modes = append(modes, mode)
	}
	return modes
}

func runAgentThroughputBenchmark(opts agentThroughputBenchmarkOptions, output io.Writer) error {
	root, err := os.MkdirTemp("", "turbk-agent-throughput-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(root)
	if err := generateBenchmarkFiles(root, opts.Files, opts.FileSize); err != nil {
		return err
	}
	mock := &agentThroughputMockServer{
		rtt:              opts.RTT,
		http500Uploads:   opts.HTTP500Uploads,
		http429Uploads:   opts.HTTP429Uploads,
		http413Threshold: opts.HTTP413Threshold,
	}
	server := httptest.NewServer(mock.handler())
	defer server.Close()
	client := newAgentClient(server.URL, "", "")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	results := make([]agentThroughputBenchmarkResult, 0, len(opts.Modes))
	var baseline string
	for _, mode := range opts.Modes {
		mock.reset()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		sampler := startAgentThroughputBenchmarkMemorySampler(10 * time.Millisecond)
		started := time.Now()
		manifest, err := client.scanAndUpload(1, []string{root}, logger, fsfilter.Options{}, benchmarkRunOptions(opts, mode))
		peakHeapAlloc := sampler.stop()
		if err != nil {
			return fmt.Errorf("mode %s: %w", mode, err)
		}
		duration := time.Since(started)
		runtime.ReadMemStats(&after)
		fingerprint := benchmarkManifestFingerprint(manifest)
		if baseline == "" {
			baseline = fingerprint
		}
		stats := mock.stats()
		totalBytes := int64(opts.Files) * opts.FileSize
		throughput := float64(0)
		if duration > 0 {
			throughput = float64(totalBytes) / duration.Seconds()
		}
		result := agentThroughputBenchmarkResult{
			Mode:                  mode,
			Files:                 opts.Files,
			FileSize:              opts.FileSize,
			Bytes:                 totalBytes,
			DurationSeconds:       duration.Seconds(),
			ThroughputBytesPerSec: throughput,
			CheckRequests:         stats.checkRequests,
			UploadRequests:        stats.uploadRequests,
			UploadRequestBytes:    stats.uploadRequestBytes,
			UploadResponseBytes:   stats.uploadResponseBytes,
			Injected500:           stats.injected500,
			Injected429:           stats.injected429,
			Injected413:           stats.injected413,
			ManifestEquivalent:    fingerprint == baseline,
			ManifestEntries:       len(manifest.Entries),
			AllocBytes:            after.TotalAlloc - before.TotalAlloc,
			PeakHeapAllocBytes:    peakHeapAlloc,
		}
		results = append(results, result)
	}
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	return encoder.Encode(results)
}

func startAgentThroughputBenchmarkMemorySampler(interval time.Duration) *agentThroughputBenchmarkMemorySampler {
	if interval <= 0 {
		interval = 10 * time.Millisecond
	}
	sampler := &agentThroughputBenchmarkMemorySampler{
		done: make(chan struct{}),
	}
	sample := func() {
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		atomicMaxUint64(&sampler.peak, stats.HeapAlloc)
	}
	sampler.wg.Add(1)
	go func() {
		defer sampler.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		sample()
		for {
			select {
			case <-ticker.C:
				sample()
			case <-sampler.done:
				sample()
				return
			}
		}
	}()
	return sampler
}

func (s *agentThroughputBenchmarkMemorySampler) stop() uint64 {
	close(s.done)
	s.wg.Wait()
	return s.peak.Load()
}

func atomicMaxUint64(value *atomic.Uint64, candidate uint64) {
	for {
		current := value.Load()
		if candidate <= current {
			return
		}
		if value.CompareAndSwap(current, candidate) {
			return
		}
	}
}

func benchmarkRunOptions(opts agentThroughputBenchmarkOptions, mode string) backupRunOptions {
	runOpts := backupRunOptions{
		RepositoryID:                  "benchmark-repo",
		ChunkGeneration:               1,
		MaxChunkCheckBatch:            opts.MaxChunkCheckBatch,
		MaxChunkUploadBatchBytes:      defaultAgentChunkUploadBatchBytes,
		MaxChunkResponseBytes:         defaultAgentMaxChunkResponseBytes,
		ChunkBatchMaxRetries:          defaultAgentChunkBatchMaxRetries,
		ChunkBatchRetryInitialBackoff: time.Millisecond,
		ChunkBatchRetryMaxBackoff:     10 * time.Millisecond,
		ChunkBatchSplitOn413:          true,
		ChunkBatchUpload:              true,
		CompactChunkCheckResponse:     true,
		CompactChunkUploadResponse:    true,
		MaxChunkCheckInflight:         1,
		MaxChunkUploadInflight:        1,
		MaxChunkPipelineBytes:         defaultAgentMaxChunkPipelineBytes,
		FileReadWorkers:               2,
		FileReadPipelineBytes:         defaultAgentFileReadPipelineBytes,
		MaxManifestRepairAttempts:     1,
	}
	switch mode {
	case "serial":
	case "pipeline", "write_queue":
		runOpts.ChunkPipelineEnabled = true
		runOpts.MaxChunkCheckInflight = 4
		runOpts.MaxChunkUploadInflight = 4
	case "parallel":
		runOpts.ChunkPipelineEnabled = true
		runOpts.MaxChunkCheckInflight = 4
		runOpts.MaxChunkUploadInflight = 4
		runOpts.ScanParallelEnabled = true
		runOpts.FileReadWorkers = 4
	default:
		runOpts.Trigger = mode
	}
	return runOpts
}

func generateBenchmarkFiles(root string, files int, size int64) error {
	for i := 0; i < files; i++ {
		data := make([]byte, size)
		for j := range data {
			data[j] = byte((i + j) % 251)
		}
		name := filepath.Join(root, fmt.Sprintf("file-%05d.bin", i))
		if err := os.WriteFile(name, data, 0o644); err != nil {
			return err
		}
	}
	return nil
}

func (m *agentThroughputMockServer) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.run = agentThroughputMockRun{
		remaining500: m.http500Uploads,
		remaining429: m.http429Uploads,
	}
}

func (m *agentThroughputMockServer) stats() agentThroughputMockRun {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.run
}

func (m *agentThroughputMockServer) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/progress"):
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "accepted"})
		case r.Method == http.MethodPost && r.URL.Path == "/agent/v1/chunks/check":
			time.Sleep(m.rtt)
			m.handleBenchmarkCheck(w, r)
		case r.Method == http.MethodPost && r.URL.Path == "/agent/v1/chunks/upload":
			time.Sleep(m.rtt)
			m.handleBenchmarkUpload(w, r)
		default:
			http.NotFound(w, r)
		}
	})
}

func (m *agentThroughputMockServer) handleBenchmarkCheck(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Hashes []string `json:"hashes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	m.mu.Lock()
	m.run.checkRequests++
	m.mu.Unlock()
	_ = json.NewEncoder(w).Encode(map[string]any{
		"repository_id":    "benchmark-repo",
		"chunk_generation": 1,
		"missing":          req.Hashes,
	})
}

func (m *agentThroughputMockServer) handleBenchmarkUpload(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	m.mu.Lock()
	m.run.uploadRequests++
	m.run.uploadRequestBytes += int64(len(body))
	if m.run.remaining500 > 0 {
		m.run.remaining500--
		m.run.injected500++
		m.mu.Unlock()
		http.Error(w, "injected 500", http.StatusInternalServerError)
		return
	}
	if m.run.remaining429 > 0 {
		m.run.remaining429--
		m.run.injected429++
		m.mu.Unlock()
		w.Header().Set("Retry-After", "0")
		http.Error(w, "injected 429", http.StatusTooManyRequests)
		return
	}
	if m.http413Threshold > 0 && int64(len(body)) > m.http413Threshold {
		m.run.injected413++
		m.mu.Unlock()
		http.Error(w, "injected 413", http.StatusRequestEntityTooLarge)
		return
	}
	m.mu.Unlock()

	chunks, err := decodeAgentChunkBatchData(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	respChunks := make([]map[string]any, 0, len(chunks))
	for _, chunk := range chunks {
		respChunks = append(respChunks, map[string]any{
			"hash":          chunk.hash,
			"uploaded":      true,
			"original_size": len(chunk.data),
		})
	}
	response := map[string]any{
		"status":           "accepted",
		"repository_id":    "benchmark-repo",
		"chunk_generation": 1,
		"chunks":           respChunks,
	}
	if data, err := json.Marshal(response); err == nil {
		m.mu.Lock()
		m.run.uploadResponseBytes += int64(len(data))
		m.mu.Unlock()
	}
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(response)
}

type decodedAgentChunk struct {
	hash string
	data []byte
}

func decodeAgentChunkBatchData(data []byte) ([]decodedAgentChunk, error) {
	if !bytes.HasPrefix(data, agentChunkBatchMagic) {
		return nil, errors.New("chunk batch magic mismatch")
	}
	offset := len(agentChunkBatchMagic)
	if len(data) < offset+4 {
		return nil, errors.New("chunk batch missing count")
	}
	count := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	chunks := make([]decodedAgentChunk, 0, count)
	for i := uint32(0); i < count; i++ {
		if len(data) < offset+32+8 {
			return nil, fmt.Errorf("chunk %d missing header", i)
		}
		hashBytes := data[offset : offset+32]
		offset += 32
		length := binary.BigEndian.Uint64(data[offset : offset+8])
		offset += 8
		if length > uint64(len(data)-offset) {
			return nil, fmt.Errorf("chunk %d length %d exceeds remaining body %d", i, length, len(data)-offset)
		}
		chunk := data[offset : offset+int(length)]
		offset += int(length)
		sum := blake3.Sum256(chunk)
		if !bytes.Equal(sum[:], hashBytes) {
			return nil, fmt.Errorf("chunk %d hash mismatch", i)
		}
		chunks = append(chunks, decodedAgentChunk{
			hash: hex.EncodeToString(hashBytes),
			data: chunk,
		})
	}
	if offset != len(data) {
		return nil, errors.New("chunk batch has trailing bytes")
	}
	return chunks, nil
}

func benchmarkManifestFingerprint(manifest *repository.SnapshotManifest) string {
	entries := make([]repository.FileEntry, len(manifest.Entries))
	copy(entries, manifest.Entries)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Path < entries[j].Path
	})
	type chunk struct {
		Hash         string `json:"hash"`
		OriginalSize int64  `json:"original_size"`
	}
	type entry struct {
		Path       string               `json:"path"`
		Type       repository.EntryType `json:"type"`
		Size       int64                `json:"size"`
		Mode       uint32               `json:"mode"`
		LinkTarget string               `json:"link_target,omitempty"`
		Chunks     []chunk              `json:"chunks,omitempty"`
	}
	fingerprint := make([]entry, 0, len(entries))
	for _, manifestEntry := range entries {
		next := entry{
			Path:       manifestEntry.Path,
			Type:       manifestEntry.Type,
			Size:       manifestEntry.Size,
			Mode:       manifestEntry.Mode,
			LinkTarget: manifestEntry.LinkTarget,
		}
		for _, ref := range manifestEntry.Chunks {
			next.Chunks = append(next.Chunks, chunk{Hash: ref.Hash, OriginalSize: ref.OriginalSize})
		}
		fingerprint = append(fingerprint, next)
	}
	data, err := json.Marshal(fingerprint)
	if err != nil {
		return ""
	}
	return string(data)
}

type daemonOptions struct {
	Roots                     []string
	StateDir                  string
	PollInterval              time.Duration
	PollJitter                time.Duration
	BackupSchedule            string
	MaxManifestRepairAttempts int
	ScanOptions               fsfilter.Options
}

func runDaemon(client *agentClient, logger *slog.Logger, opts daemonOptions) error {
	catalog, err := openAgentCatalog(opts.StateDir)
	if err != nil {
		return err
	}
	defer catalog.Close()
	if opts.MaxManifestRepairAttempts <= 0 {
		opts.MaxManifestRepairAttempts = 3
	}
	if strings.TrimSpace(opts.BackupSchedule) == "" || !cronexpr.Valid(opts.BackupSchedule) {
		opts.BackupSchedule = defaultAgentBackupSchedule
	}

	var running bool
	var lastBackupStarted time.Time
	var lastBackupFinished time.Time
	var lastScheduleChecked time.Time
	var runningRunID int64
	var lastError string
	for {
		heartbeat, err := client.sendHeartbeatWithState(catalog, opts.StateDir, "daemon", runningRunID, lastError)
		if err != nil {
			lastError = err.Error()
			logger.Error("agent heartbeat failed", "error", err)
			time.Sleep(nextPollDelay(opts.PollInterval, opts.PollJitter, heartbeat))
			continue
		}
		lastError = ""
		if err := syncCatalogWithServer(client, catalog, heartbeat); err != nil {
			lastError = err.Error()
			logger.Warn("agent catalog sync failed", "error", err)
		}

		backupCommandHandled := false
		for _, command := range heartbeat.Commands {
			switch command.Type {
			case "refresh-config":
				if err := syncCatalogWithServer(client, catalog, heartbeat); err != nil {
					lastError = err.Error()
					_ = client.ackCommand(command.ID, "failed", err.Error())
				} else {
					_ = client.ackCommand(command.ID, "completed", "")
				}
				continue
			case "cancel-run":
				_ = client.ackCommand(command.ID, "dropped", "no_running_run")
				continue
			case "run-backup":
			default:
				if err := client.ackCommand(command.ID, "dropped", "unsupported_command"); err != nil {
					logger.Warn("agent command ack failed", "command", command.ID, "error", err)
				}
				continue
			}
			if running || backupCommandHandled || commandCreatedDuringLastRun(command, lastBackupStarted, lastBackupFinished) {
				if err := client.ackCommand(command.ID, "dropped", "agent_busy"); err != nil {
					logger.Warn("agent busy command drop failed", "command", command.ID, "error", err)
				}
				continue
			}
			commandRoots, err := backupRootsForCommand(command, opts.Roots)
			if err != nil {
				lastError = err.Error()
				if err := client.ackCommand(command.ID, "failed", err.Error()); err != nil {
					logger.Warn("agent invalid command ack failed", "command", command.ID, "error", err)
				}
				continue
			}
			if len(commandRoots) == 0 {
				if err := client.ackCommand(command.ID, "dropped", "root_not_configured"); err != nil {
					logger.Warn("agent root-not-configured command drop failed", "command", command.ID, "error", err)
				}
				continue
			}
			running = true
			backupCommandHandled = true
			lastBackupStarted = time.Now().UTC()
			err = runBackupWithOptions(client, commandRoots, logger, opts.ScanOptions, backupRunOptions{
				Catalog:                       catalog,
				RepositoryID:                  heartbeat.Repository.ID,
				ChunkGeneration:               heartbeat.Repository.ChunkGeneration,
				MaxChunkCheckBatch:            heartbeat.Agent.MaxChunkCheckBatch,
				MaxChunkUploadBatchBytes:      heartbeat.Agent.MaxChunkUploadBatchBytes,
				MaxChunkResponseBytes:         heartbeat.Agent.MaxChunkResponseBytes,
				ChunkBatchMaxRetries:          heartbeat.Agent.ChunkBatchMaxRetries,
				ChunkBatchRetryInitialBackoff: heartbeat.Agent.chunkBatchRetryInitialBackoff(),
				ChunkBatchRetryMaxBackoff:     heartbeat.Agent.chunkBatchRetryMaxBackoff(),
				ChunkBatchSplitOn413:          heartbeat.Agent.ChunkBatchSplitOn413,
				ChunkPipelineEnabled:          heartbeat.Agent.ChunkPipelineEnabled,
				MaxChunkCheckInflight:         heartbeat.Agent.MaxChunkCheckInflight,
				MaxChunkUploadInflight:        heartbeat.Agent.MaxChunkUploadInflight,
				MaxChunkPipelineBytes:         heartbeat.Agent.MaxChunkPipelineBytes,
				ChunkBatchUpload:              heartbeat.Agent.ChunkBatchUpload,
				CompactChunkCheckResponse:     heartbeat.Agent.CompactChunkCheckResponse,
				CompactChunkUploadResponse:    heartbeat.Agent.CompactChunkUploadResponse,
				SmallFilePackEnabled:          heartbeat.Agent.SmallFilePack && heartbeat.Agent.SmallFilePackEnabled,
				SmallFilePackMaxFileSize:      heartbeat.Agent.SmallFilePackMaxFileSize,
				SmallFilePackTargetSize:       heartbeat.Agent.SmallFilePackTargetSize,
				ScanParallelEnabled:           heartbeat.Agent.ScanParallelEnabled,
				FileReadWorkers:               heartbeat.Agent.FileReadWorkers,
				FileReadPipelineBytes:         heartbeat.Agent.FileReadPipelineBytes,
				CommandID:                     command.ID,
				Trigger:                       "manual",
				MaxManifestRepairAttempts:     opts.MaxManifestRepairAttempts,
			})
			running = false
			runningRunID = 0
			lastBackupFinished = time.Now().UTC()
			if err != nil {
				lastError = err.Error()
				logger.Error("agent command backup failed", "command", command.ID, "error", err)
				_ = client.ackCommand(command.ID, "failed", err.Error())
			} else {
				_ = client.ackCommand(command.ID, "completed", "")
			}
		}

		scheduleCheckTime := time.Now().UTC()
		if !running && !backupCommandHandled && len(opts.Roots) != 0 && dueByCron(opts.BackupSchedule, lastScheduleChecked, scheduleCheckTime) {
			running = true
			lastBackupStarted = scheduleCheckTime
			err := runBackupWithOptions(client, opts.Roots, logger, opts.ScanOptions, backupRunOptions{
				Catalog:                       catalog,
				RepositoryID:                  heartbeat.Repository.ID,
				ChunkGeneration:               heartbeat.Repository.ChunkGeneration,
				MaxChunkCheckBatch:            heartbeat.Agent.MaxChunkCheckBatch,
				MaxChunkUploadBatchBytes:      heartbeat.Agent.MaxChunkUploadBatchBytes,
				MaxChunkResponseBytes:         heartbeat.Agent.MaxChunkResponseBytes,
				ChunkBatchMaxRetries:          heartbeat.Agent.ChunkBatchMaxRetries,
				ChunkBatchRetryInitialBackoff: heartbeat.Agent.chunkBatchRetryInitialBackoff(),
				ChunkBatchRetryMaxBackoff:     heartbeat.Agent.chunkBatchRetryMaxBackoff(),
				ChunkBatchSplitOn413:          heartbeat.Agent.ChunkBatchSplitOn413,
				ChunkPipelineEnabled:          heartbeat.Agent.ChunkPipelineEnabled,
				MaxChunkCheckInflight:         heartbeat.Agent.MaxChunkCheckInflight,
				MaxChunkUploadInflight:        heartbeat.Agent.MaxChunkUploadInflight,
				MaxChunkPipelineBytes:         heartbeat.Agent.MaxChunkPipelineBytes,
				ChunkBatchUpload:              heartbeat.Agent.ChunkBatchUpload,
				CompactChunkCheckResponse:     heartbeat.Agent.CompactChunkCheckResponse,
				CompactChunkUploadResponse:    heartbeat.Agent.CompactChunkUploadResponse,
				SmallFilePackEnabled:          heartbeat.Agent.SmallFilePack && heartbeat.Agent.SmallFilePackEnabled,
				SmallFilePackMaxFileSize:      heartbeat.Agent.SmallFilePackMaxFileSize,
				SmallFilePackTargetSize:       heartbeat.Agent.SmallFilePackTargetSize,
				ScanParallelEnabled:           heartbeat.Agent.ScanParallelEnabled,
				FileReadWorkers:               heartbeat.Agent.FileReadWorkers,
				FileReadPipelineBytes:         heartbeat.Agent.FileReadPipelineBytes,
				Trigger:                       "schedule",
				MaxManifestRepairAttempts:     opts.MaxManifestRepairAttempts,
			})
			running = false
			runningRunID = 0
			lastBackupFinished = time.Now().UTC()
			if err != nil {
				lastError = err.Error()
				logger.Error("agent scheduled backup failed", "error", err)
			}
			lastScheduleChecked = time.Now().UTC()
		} else {
			lastScheduleChecked = scheduleCheckTime
		}

		time.Sleep(nextPollDelay(opts.PollInterval, opts.PollJitter, heartbeat))
	}
}

func dueByCron(schedule string, lastChecked, now time.Time) bool {
	if strings.TrimSpace(schedule) == "" {
		return false
	}
	end := now.Truncate(time.Minute)
	start := end
	if !lastChecked.IsZero() {
		start = lastChecked.Add(time.Minute).Truncate(time.Minute)
	}
	for cursor := start; !cursor.After(end); cursor = cursor.Add(time.Minute) {
		if cronexpr.Matches(schedule, cursor.Local()) {
			return true
		}
	}
	return false
}

func backupRootsForCommand(command agentCommand, fallback []string) ([]string, error) {
	if roots, ok, err := rootsFromCommandPayload(command.Payload); err != nil {
		return nil, err
	} else if ok {
		return roots, nil
	}
	if len(fallback) == 0 {
		return nil, nil
	}
	return rootset.Normalize(fallback)
}

func rootsFromCommandPayload(payload json.RawMessage) ([]string, bool, error) {
	payload = bytes.TrimSpace(payload)
	if len(payload) == 0 || bytes.Equal(payload, []byte("null")) {
		return nil, false, nil
	}
	var req struct {
		Root  string   `json:"root"`
		Roots []string `json:"roots"`
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, false, fmt.Errorf("parse command payload roots: %w", err)
	}
	if len(req.Roots) > 0 {
		roots, err := rootset.Normalize(req.Roots)
		return roots, true, err
	}
	req.Root = strings.TrimSpace(req.Root)
	if req.Root == "" {
		return nil, false, nil
	}
	roots, err := rootset.Normalize([]string{req.Root})
	return roots, true, err
}

func commandCreatedDuringLastRun(command agentCommand, started, finished time.Time) bool {
	if command.CreatedAt.IsZero() || started.IsZero() || finished.IsZero() {
		return false
	}
	return !command.CreatedAt.Before(started) && !command.CreatedAt.After(finished)
}

func nextPollDelay(localInterval, jitter time.Duration, heartbeat heartbeatResponse) time.Duration {
	interval := localInterval
	if interval <= 0 && heartbeat.Agent.PollIntervalSeconds > 0 {
		interval = time.Duration(heartbeat.Agent.PollIntervalSeconds) * time.Second
	}
	if interval <= 0 {
		interval = 10 * time.Minute
	}
	if jitter > 0 {
		interval += time.Duration(rand.Int63n(int64(jitter)))
	}
	return interval
}

func (c *agentClient) ackCommand(id int64, status, reason string) error {
	if id <= 0 {
		return nil
	}
	var resp map[string]any
	_, err := c.doJSON(http.MethodPost, fmt.Sprintf("/agent/v1/commands/%d/ack", id), map[string]any{
		"status": status,
		"reason": reason,
	}, &resp)
	return err
}

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

type chunkBatchRetryOptions struct {
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	SplitOn413     bool
	Logger         *slog.Logger
	BatchID        int64
	Operation      string
}

func (opts backupRunOptions) chunkBatchRetryOptions(logger *slog.Logger, batchID int64, operation string) chunkBatchRetryOptions {
	retry := chunkBatchRetryOptions{
		MaxRetries:     opts.ChunkBatchMaxRetries,
		InitialBackoff: opts.ChunkBatchRetryInitialBackoff,
		MaxBackoff:     opts.ChunkBatchRetryMaxBackoff,
		SplitOn413:     opts.ChunkBatchSplitOn413,
		Logger:         logger,
		BatchID:        batchID,
		Operation:      operation,
	}
	return retry.normalized()
}

func (o chunkBatchRetryOptions) normalized() chunkBatchRetryOptions {
	if o.MaxRetries <= 0 {
		o.MaxRetries = defaultAgentChunkBatchMaxRetries
	}
	if o.InitialBackoff <= 0 {
		o.InitialBackoff = defaultAgentChunkBatchRetryInitialBackoff
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = defaultAgentChunkBatchRetryMaxBackoff
	}
	if o.MaxBackoff < o.InitialBackoff {
		o.MaxBackoff = o.InitialBackoff
	}
	return o
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

type agentSkipReporter struct {
	logger *slog.Logger
	total  int64
	logged int64
}

func (r *agentSkipReporter) record(event fsfilter.SkipEvent) {
	r.total++
	if r.logger == nil || r.logged >= 20 {
		return
	}
	r.logged++
	r.logger.Warn("agent skipped path", "path", event.Path, "rel", event.Rel, "reason", event.Reason)
}

func agentPathErrorSkipEvent(root, scanPath string, err error) (fsfilter.SkipEvent, bool) {
	if err == nil {
		return fsfilter.SkipEvent{}, false
	}
	isRoot := filepath.Clean(scanPath) == filepath.Clean(root)
	reason := ""
	switch {
	case errors.Is(err, fs.ErrNotExist) || os.IsNotExist(err):
		if isRoot {
			return fsfilter.SkipEvent{}, false
		}
		reason = "path disappeared during scan"
	case errors.Is(err, fs.ErrPermission) || os.IsPermission(err):
		reason = "permission denied during scan"
	case isAgentPathIOError(err):
		reason = "filesystem IO error during scan"
	}
	if reason == "" {
		return fsfilter.SkipEvent{}, false
	}
	rel, relErr := filepath.Rel(root, scanPath)
	if relErr != nil {
		rel = scanPath
	}
	return fsfilter.SkipEvent{
		Path:   scanPath,
		Rel:    cleanManifestPath(rel),
		Reason: reason,
	}, true
}

func isAgentPathIOError(err error) bool {
	for _, target := range []error{
		syscall.EIO,
		syscall.ESTALE,
		syscall.ENXIO,
		syscall.ENODEV,
	} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

type chunkBatchStats struct {
	Uploaded            int64
	Reused              int64
	CheckRequests       int64
	UploadRequests      int64
	UploadRequestBytes  int64
	CheckResponseBytes  int64
	UploadResponseBytes int64
	CheckDuration       time.Duration
	UploadDuration      time.Duration
	PipelineWait        time.Duration
	MaxPipelineBytes    int64
	BatchRetries        int64
	BatchSplits         int64
	PackedFiles         int64
	PackedBytes         int64
	PackCount           int64
}

func (s *chunkBatchStats) Add(other chunkBatchStats) {
	s.Uploaded += other.Uploaded
	s.Reused += other.Reused
	s.CheckRequests += other.CheckRequests
	s.UploadRequests += other.UploadRequests
	s.UploadRequestBytes += other.UploadRequestBytes
	s.CheckResponseBytes += other.CheckResponseBytes
	s.UploadResponseBytes += other.UploadResponseBytes
	s.CheckDuration += other.CheckDuration
	s.UploadDuration += other.UploadDuration
	s.PipelineWait += other.PipelineWait
	if other.MaxPipelineBytes > s.MaxPipelineBytes {
		s.MaxPipelineBytes = other.MaxPipelineBytes
	}
	s.BatchRetries += other.BatchRetries
	s.BatchSplits += other.BatchSplits
	s.PackedFiles += other.PackedFiles
	s.PackedBytes += other.PackedBytes
	s.PackCount += other.PackCount
}

type agentFileCatalogBatcher struct {
	catalog      *agentCatalog
	logger       *slog.Logger
	pending      []agentFileCatalogUpdate
	pendingBytes int64
}

type chunkBatchWaiter struct {
	entry *repository.FileEntry
	index int
}

type pendingBatchChunk struct {
	hash         string
	data         []byte
	originalSize int64
	waiters      []chunkBatchWaiter
}

type pendingPackFile struct {
	entry  repository.FileEntry
	record catalogFileRecord
	file   repository.PackFilePayload
}

type pendingManifestFile struct {
	entry  *repository.FileEntry
	record catalogFileRecord
}

type agentParallelFileJob struct {
	root        string
	catalogPath string
	filePath    string
	entry       repository.FileEntry
	record      catalogFileRecord
}

type agentParallelFileResult struct {
	job    agentParallelFileJob
	chunks [][]byte
	bytes  int64
	err    error
}

type agentSmallFilePackBatcher struct {
	enabled       bool
	maxFileSize   int64
	targetSize    int64
	pending       []pendingPackFile
	pendingBytes  int64
	nextPackIndex int64
}

type agentChunkBatcher struct {
	client           *agentClient
	opts             backupRunOptions
	logger           *slog.Logger
	maxChunks        int
	maxRequestBytes  int64
	maxResponseBytes int64
	pending          []*pendingBatchChunk
	pendingByHash    map[string]*pendingBatchChunk
	pendingByEntry   map[*repository.FileEntry]int
	requestBytes     int64
	nextBatchID      int64
}

type agentChunkUploader interface {
	Add(chunk []byte, entry *repository.FileEntry) (chunkBatchStats, error)
	Flush() (chunkBatchStats, error)
	EntryPending(entry *repository.FileEntry) bool
	Stop()
}

func newAgentChunkUploader(client *agentClient, opts backupRunOptions, logger *slog.Logger) agentChunkUploader {
	if opts.ChunkPipelineEnabled && opts.ChunkBatchUpload {
		return newAgentChunkPipelineBatcher(client, opts, logger)
	}
	return newAgentChunkBatcher(client, opts, logger)
}

func newAgentChunkBatcher(client *agentClient, opts backupRunOptions, logger *slog.Logger) *agentChunkBatcher {
	maxChunks := normalizedAgentChunkCheckBatchLimit(opts.MaxChunkCheckBatch)
	maxRequestBytes := opts.MaxChunkUploadBatchBytes
	if maxRequestBytes <= 0 {
		maxRequestBytes = defaultAgentChunkUploadBatchBytes
	}
	maxResponseBytes := opts.MaxChunkResponseBytes
	if maxResponseBytes <= 0 {
		maxResponseBytes = defaultAgentMaxChunkResponseBytes
	}
	return &agentChunkBatcher{
		client:           client,
		opts:             opts,
		logger:           logger,
		maxChunks:        maxChunks,
		maxRequestBytes:  maxRequestBytes,
		maxResponseBytes: maxResponseBytes,
		pendingByHash:    make(map[string]*pendingBatchChunk),
		pendingByEntry:   make(map[*repository.FileEntry]int),
	}
}

func (b *agentChunkBatcher) Stop() {}

func normalizedAgentChunkCheckBatchLimit(maxChunks int) int {
	if maxChunks <= 0 {
		return 10000
	}
	return maxChunks
}

func newAgentSmallFilePackBatcher(opts backupRunOptions) *agentSmallFilePackBatcher {
	maxFileSize := opts.SmallFilePackMaxFileSize
	if maxFileSize <= 0 {
		maxFileSize = 64 * 1024
	}
	targetSize := opts.SmallFilePackTargetSize
	if targetSize <= 0 {
		targetSize = 8 * 1024 * 1024
	}
	return &agentSmallFilePackBatcher{
		enabled:     opts.SmallFilePackEnabled,
		maxFileSize: maxFileSize,
		targetSize:  targetSize,
	}
}

func (b *agentSmallFilePackBatcher) Eligible(info fs.FileInfo) bool {
	return b != nil &&
		b.enabled &&
		info.Mode().IsRegular() &&
		info.Size() > 0 &&
		info.Size() <= b.maxFileSize
}

func (b *agentSmallFilePackBatcher) Add(file pendingPackFile, flush func() error) error {
	if b == nil || !b.enabled {
		return errors.New("small-file pack is disabled")
	}
	fileBytes := int64(len(file.file.Data))
	if fileBytes <= 0 {
		return fmt.Errorf("pack file %q is empty", file.entry.Path)
	}
	if len(b.pending) > 0 && b.pendingBytes+fileBytes > b.targetSize {
		if err := flush(); err != nil {
			return err
		}
	}
	b.pending = append(b.pending, file)
	b.pendingBytes += fileBytes
	if b.pendingBytes >= b.targetSize {
		return flush()
	}
	return nil
}

func newAgentFileCatalogBatcher(catalog *agentCatalog, logger *slog.Logger) *agentFileCatalogBatcher {
	return &agentFileCatalogBatcher{catalog: catalog, logger: logger}
}

func (b *agentFileCatalogBatcher) Add(record catalogFileRecord, chunks []catalogChunkRecord) {
	if b == nil || b.catalog == nil {
		return
	}
	update := agentFileCatalogUpdate{Record: record}
	if len(chunks) > 0 {
		update.Chunks = append([]catalogChunkRecord(nil), chunks...)
	}
	b.pending = append(b.pending, update)
	b.pendingBytes += estimateFileCatalogUpdateBytes(record, chunks)
	if len(b.pending) >= defaultAgentFileCatalogBatchFiles || b.pendingBytes >= defaultAgentFileCatalogBatchBytes {
		b.Flush()
	}
}

func (b *agentFileCatalogBatcher) Flush() {
	if b == nil || b.catalog == nil || len(b.pending) == 0 {
		return
	}
	updates := b.pending
	b.pending = nil
	b.pendingBytes = 0
	if err := b.catalog.replaceFiles(updates); err != nil && b.logger != nil {
		b.logger.Warn("agent catalog file batch update failed", "files", len(updates), "error", err)
	}
}

func estimateFileCatalogUpdateBytes(record catalogFileRecord, chunks []catalogChunkRecord) int64 {
	return int64(128 + len(record.RootID) + len(record.Path) + len(record.LinkTarget) + len(record.Fingerprint) + len(chunks)*40)
}

func (b *agentChunkBatcher) Add(chunk []byte, entry *repository.FileEntry) (chunkBatchStats, error) {
	if !b.opts.ChunkBatchUpload {
		ref, uploaded, err := b.client.ensureChunk(chunk, b.opts)
		if err != nil {
			return chunkBatchStats{}, err
		}
		entry.Chunks = append(entry.Chunks, ref)
		if uploaded {
			return chunkBatchStats{Uploaded: 1}, nil
		}
		return chunkBatchStats{Reused: 1}, nil
	}

	sum := blake3.Sum256(chunk)
	hash := hex.EncodeToString(sum[:])
	if b.opts.Catalog != nil {
		status, confirmedGeneration, ok, err := b.opts.Catalog.chunkStatus(hash)
		if err != nil {
			return chunkBatchStats{}, err
		}
		if ok && status == "confirmed" && confirmedGeneration >= b.opts.ChunkGeneration {
			entry.Chunks = append(entry.Chunks, repository.ChunkRef{Hash: hash, OriginalSize: int64(len(chunk))})
			return chunkBatchStats{Reused: 1}, nil
		}
	}

	index := len(entry.Chunks)
	entry.Chunks = append(entry.Chunks, repository.ChunkRef{})
	waiter := chunkBatchWaiter{entry: entry, index: index}
	if pending, ok := b.pendingByHash[hash]; ok {
		pending.waiters = append(pending.waiters, waiter)
		b.pendingByEntry[entry]++
		return chunkBatchStats{}, nil
	}

	data := append([]byte(nil), chunk...)
	chunkRequestBytes := int64(32 + 8 + len(data))
	if 12+chunkRequestBytes > b.maxRequestBytes {
		return chunkBatchStats{}, fmt.Errorf("chunk %s size %d exceeds max batch request size %d", hash, len(data), b.maxRequestBytes)
	}
	if len(b.pending) > 0 && b.shouldFlushBeforeAdding(chunkRequestBytes) {
		stats, err := b.Flush()
		if err != nil {
			return chunkBatchStats{}, err
		}
		if b.requestBytes == 0 {
			b.requestBytes = 12
		}
		pending := &pendingBatchChunk{
			hash:         hash,
			data:         data,
			originalSize: int64(len(data)),
			waiters:      []chunkBatchWaiter{waiter},
		}
		b.pending = append(b.pending, pending)
		b.pendingByHash[hash] = pending
		b.pendingByEntry[entry]++
		b.requestBytes += chunkRequestBytes
		return stats, nil
	}
	if b.requestBytes == 0 {
		b.requestBytes = 12
	}
	pending := &pendingBatchChunk{
		hash:         hash,
		data:         data,
		originalSize: int64(len(data)),
		waiters:      []chunkBatchWaiter{waiter},
	}
	b.pending = append(b.pending, pending)
	b.pendingByHash[hash] = pending
	b.pendingByEntry[entry]++
	b.requestBytes += chunkRequestBytes
	if len(b.pending) >= b.maxChunks || b.requestBytes >= b.maxRequestBytes {
		return b.Flush()
	}
	if b.estimatedCheckResponseBytes(len(b.pending)) >= b.maxResponseBytes || b.estimatedUploadResponseBytes(len(b.pending)) >= b.maxResponseBytes {
		return b.Flush()
	}
	return chunkBatchStats{}, nil
}

func (b *agentChunkBatcher) shouldFlushBeforeAdding(nextRequestBytes int64) bool {
	nextCount := len(b.pending) + 1
	return nextCount > b.maxChunks ||
		b.requestBytes+nextRequestBytes > b.maxRequestBytes ||
		b.estimatedCheckResponseBytes(nextCount) > b.maxResponseBytes ||
		b.estimatedUploadResponseBytes(nextCount) > b.maxResponseBytes
}

func (b *agentChunkBatcher) estimatedCheckResponseBytes(chunkCount int) int64 {
	return estimatedChunkBatchResponseBaseBytes + int64(chunkCount)*estimatedChunkCheckResponseBytesPerHash
}

func (b *agentChunkBatcher) estimatedUploadResponseBytes(chunkCount int) int64 {
	perChunk := estimatedFullChunkUploadResponseBytesPerChunk
	if b.opts.CompactChunkUploadResponse {
		perChunk = estimatedCompactChunkUploadResponseBytesPerChunk
	}
	return estimatedChunkBatchResponseBaseBytes + int64(chunkCount)*perChunk
}

func (b *agentChunkBatcher) EntryPending(entry *repository.FileEntry) bool {
	return b.pendingByEntry[entry] > 0
}

func (b *agentChunkBatcher) Flush() (chunkBatchStats, error) {
	if len(b.pending) == 0 {
		return chunkBatchStats{}, nil
	}
	b.nextBatchID++
	batchID := b.nextBatchID
	pending := append([]*pendingBatchChunk(nil), b.pending...)
	hashes := make([]string, 0, len(pending))
	byHash := make(map[string]*pendingBatchChunk, len(pending))
	for _, chunk := range pending {
		hashes = append(hashes, chunk.hash)
		byHash[chunk.hash] = chunk
	}
	checkStarted := time.Now()
	checked, err := b.client.checkChunks(hashes, b.opts.RepositoryID, b.opts.ChunkGeneration, b.opts.CompactChunkCheckResponse, b.opts.chunkBatchRetryOptions(b.logger, batchID, "check"))
	if err != nil {
		return chunkBatchStats{}, err
	}
	if err := validateChunkCheckResponse(hashes, b.opts.RepositoryID, checked, b.opts.CompactChunkCheckResponse); err != nil {
		return chunkBatchStats{}, err
	}
	stats := chunkBatchStats{}
	stats.CheckRequests++
	stats.CheckDuration += time.Since(checkStarted)
	stats.CheckResponseBytes += checked.ResponseBytes
	stats.BatchRetries += checked.RetryCount
	catalogUpdates := make([]agentChunkStatusUpdate, 0, len(pending))
	seen := make(map[string]struct{}, len(pending))
	for _, hash := range existingChunkHashes(hashes, checked, b.opts.CompactChunkCheckResponse) {
		chunk, ok := byHash[hash]
		if !ok {
			return chunkBatchStats{}, fmt.Errorf("server check returned unexpected chunk %s", hash)
		}
		seen[hash] = struct{}{}
		ref := repository.ChunkRef{Hash: hash, OriginalSize: chunk.originalSize}
		b.fillPendingChunk(chunk, ref)
		stats.Reused += int64(len(chunk.waiters))
		if b.opts.Catalog != nil {
			catalogUpdates = append(catalogUpdates, agentChunkStatusUpdate{
				Hash:         hash,
				OriginalSize: chunk.originalSize,
				Status:       "confirmed",
				Generation:   checked.ChunkGeneration,
			})
		}
	}
	missingChunks := make([]*pendingBatchChunk, 0, len(checked.Missing))
	for _, hash := range checked.Missing {
		chunk, ok := byHash[hash]
		if !ok {
			return chunkBatchStats{}, fmt.Errorf("server check returned unexpected missing chunk %s", hash)
		}
		seen[hash] = struct{}{}
		missingChunks = append(missingChunks, chunk)
	}
	for _, chunk := range pending {
		if _, ok := seen[chunk.hash]; !ok {
			return chunkBatchStats{}, fmt.Errorf("server check omitted chunk %s", chunk.hash)
		}
	}
	if len(missingChunks) > 0 {
		uploadStarted := time.Now()
		uploaded, err := b.client.uploadChunksBatch(missingChunks, b.opts.CompactChunkUploadResponse, b.opts.chunkBatchRetryOptions(b.logger, batchID, "upload"))
		if err != nil {
			return chunkBatchStats{}, err
		}
		stats.UploadRequests++
		stats.UploadDuration += time.Since(uploadStarted)
		stats.UploadRequestBytes += uploaded.RequestBytes
		stats.UploadResponseBytes += uploaded.ResponseBytes
		stats.BatchRetries += uploaded.RetryCount
		stats.BatchSplits += uploaded.SplitCount
		uploadedByHash, err := validateChunkUploadResponse(missingChunks, b.opts.RepositoryID, uploaded, b.opts.CompactChunkUploadResponse)
		if err != nil {
			return chunkBatchStats{}, err
		}
		generation := uploaded.ChunkGeneration
		if generation == 0 {
			generation = b.opts.ChunkGeneration
		}
		for _, pendingChunk := range missingChunks {
			resp := uploadedByHash[pendingChunk.hash]
			b.fillPendingChunk(pendingChunk, resp.Ref)
			if resp.Uploaded {
				stats.Uploaded++
				if len(pendingChunk.waiters) > 1 {
					stats.Reused += int64(len(pendingChunk.waiters) - 1)
				}
			} else {
				stats.Reused += int64(len(pendingChunk.waiters))
			}
			if b.opts.Catalog != nil {
				catalogUpdates = append(catalogUpdates, agentChunkStatusUpdate{
					Hash:         pendingChunk.hash,
					OriginalSize: resp.Ref.OriginalSize,
					Status:       "confirmed",
					Generation:   generation,
					Uploaded:     resp.Uploaded,
				})
			}
		}
	}
	if b.opts.Catalog != nil {
		if err := b.opts.Catalog.markChunks(catalogUpdates); err != nil {
			return chunkBatchStats{}, err
		}
	}
	for _, chunk := range pending {
		delete(b.pendingByHash, chunk.hash)
	}
	b.pending = b.pending[:0]
	b.requestBytes = 0
	return stats, nil
}

func (b *agentChunkBatcher) fillPendingChunk(chunk *pendingBatchChunk, ref repository.ChunkRef) {
	for _, waiter := range chunk.waiters {
		waiter.entry.Chunks[waiter.index] = ref
		if count := b.pendingByEntry[waiter.entry]; count <= 1 {
			delete(b.pendingByEntry, waiter.entry)
		} else {
			b.pendingByEntry[waiter.entry] = count - 1
		}
	}
}

type chunkPipelineByteWindow struct {
	limit   int64
	used    int64
	maxUsed int64
	mu      sync.Mutex
}

func newChunkPipelineByteWindow(limit int64) *chunkPipelineByteWindow {
	if limit <= 0 {
		limit = defaultAgentMaxChunkPipelineBytes
	}
	return &chunkPipelineByteWindow{limit: limit}
}

func (w *chunkPipelineByteWindow) tryAcquire(n int64) bool {
	if n <= 0 {
		return true
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.used+n > w.limit {
		return false
	}
	w.used += n
	if w.used > w.maxUsed {
		w.maxUsed = w.used
	}
	return true
}

func (w *chunkPipelineByteWindow) release(n int64) {
	if n <= 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.used -= n
	if w.used < 0 {
		w.used = 0
	}
}

func (w *chunkPipelineByteWindow) max() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.maxUsed
}

type pipelineBatch struct {
	id           int64
	chunks       []*pendingBatchChunk
	requestBytes int64
	createdAt    time.Time
}

type pipelineResult struct {
	batch          *pipelineBatch
	checked        checkChunksResponse
	uploaded       uploadChunksResponse
	uploadedByHash map[string]uploadChunkResponse
	checkDuration  time.Duration
	uploadDuration time.Duration
	err            error
}

type agentChunkPipelineBatcher struct {
	client           *agentClient
	opts             backupRunOptions
	logger           *slog.Logger
	maxChunks        int
	maxRequestBytes  int64
	maxResponseBytes int64
	checkQueue       chan *pipelineBatch
	results          chan pipelineResult
	uploadSlots      chan struct{}
	done             chan struct{}
	stopOnce         sync.Once
	wg               sync.WaitGroup
	bytesWindow      *chunkPipelineByteWindow
	pending          []*pendingBatchChunk
	pendingByHash    map[string]*pendingBatchChunk
	pendingByEntry   map[*repository.FileEntry]int
	requestBytes     int64
	nextBatchID      int64
	err              error
}

func newAgentChunkPipelineBatcher(client *agentClient, opts backupRunOptions, logger *slog.Logger) *agentChunkPipelineBatcher {
	maxChunks := normalizedAgentChunkCheckBatchLimit(opts.MaxChunkCheckBatch)
	maxRequestBytes := opts.MaxChunkUploadBatchBytes
	if maxRequestBytes <= 0 {
		maxRequestBytes = defaultAgentChunkUploadBatchBytes
	}
	maxResponseBytes := opts.MaxChunkResponseBytes
	if maxResponseBytes <= 0 {
		maxResponseBytes = defaultAgentMaxChunkResponseBytes
	}
	checkInflight := opts.MaxChunkCheckInflight
	if checkInflight <= 0 {
		checkInflight = 1
	}
	uploadInflight := opts.MaxChunkUploadInflight
	if uploadInflight <= 0 {
		uploadInflight = 1
	}
	queueSize := checkInflight*2 + uploadInflight + 4
	b := &agentChunkPipelineBatcher{
		client:           client,
		opts:             opts,
		logger:           logger,
		maxChunks:        maxChunks,
		maxRequestBytes:  maxRequestBytes,
		maxResponseBytes: maxResponseBytes,
		checkQueue:       make(chan *pipelineBatch, queueSize),
		results:          make(chan pipelineResult, queueSize),
		uploadSlots:      make(chan struct{}, uploadInflight),
		done:             make(chan struct{}),
		bytesWindow:      newChunkPipelineByteWindow(opts.MaxChunkPipelineBytes),
		pendingByHash:    make(map[string]*pendingBatchChunk),
		pendingByEntry:   make(map[*repository.FileEntry]int),
	}
	for i := 0; i < checkInflight; i++ {
		b.wg.Add(1)
		go b.worker()
	}
	return b
}

func (b *agentChunkPipelineBatcher) Add(chunk []byte, entry *repository.FileEntry) (chunkBatchStats, error) {
	stats, err := b.drainResults()
	if err != nil {
		return stats, err
	}
	if b.err != nil {
		return stats, b.err
	}
	sum := blake3.Sum256(chunk)
	hash := hex.EncodeToString(sum[:])
	if b.opts.Catalog != nil {
		status, confirmedGeneration, ok, err := b.opts.Catalog.chunkStatus(hash)
		if err != nil {
			return stats, err
		}
		if ok && status == "confirmed" && confirmedGeneration >= b.opts.ChunkGeneration {
			entry.Chunks = append(entry.Chunks, repository.ChunkRef{Hash: hash, OriginalSize: int64(len(chunk))})
			stats.Reused++
			return stats, nil
		}
	}

	index := len(entry.Chunks)
	entry.Chunks = append(entry.Chunks, repository.ChunkRef{})
	waiter := chunkBatchWaiter{entry: entry, index: index}
	if pending, ok := b.pendingByHash[hash]; ok {
		pending.waiters = append(pending.waiters, waiter)
		b.pendingByEntry[entry]++
		return stats, nil
	}

	chunkRequestBytes := int64(32 + 8 + len(chunk))
	if 12+chunkRequestBytes > b.maxRequestBytes {
		return stats, fmt.Errorf("chunk %s size %d exceeds max batch request size %d", hash, len(chunk), b.maxRequestBytes)
	}
	if int64(len(chunk)) > b.bytesWindow.limit {
		return stats, fmt.Errorf("chunk %s size %d exceeds max pipeline bytes %d", hash, len(chunk), b.bytesWindow.limit)
	}
	if len(b.pending) > 0 && b.shouldFlushBeforeAdding(chunkRequestBytes) {
		flushed, err := b.submitPending()
		stats.Add(flushed)
		if err != nil {
			return stats, err
		}
	}
	waitStarted := time.Now()
	for !b.bytesWindow.tryAcquire(int64(len(chunk))) {
		if len(b.pending) > 0 {
			flushed, err := b.submitPending()
			stats.Add(flushed)
			if err != nil {
				return stats, err
			}
			continue
		}
		waited, err := b.waitApplyResult()
		stats.Add(waited)
		if err != nil {
			return stats, err
		}
	}
	if waited := time.Since(waitStarted); waited > 0 {
		stats.PipelineWait += waited
	}

	data := append([]byte(nil), chunk...)
	if b.requestBytes == 0 {
		b.requestBytes = 12
	}
	pending := &pendingBatchChunk{
		hash:         hash,
		data:         data,
		originalSize: int64(len(data)),
		waiters:      []chunkBatchWaiter{waiter},
	}
	b.pending = append(b.pending, pending)
	b.pendingByHash[hash] = pending
	b.pendingByEntry[entry]++
	b.requestBytes += chunkRequestBytes
	if len(b.pending) >= b.maxChunks ||
		b.requestBytes >= b.maxRequestBytes ||
		b.estimatedCheckResponseBytes(len(b.pending)) >= b.maxResponseBytes ||
		b.estimatedUploadResponseBytes(len(b.pending)) >= b.maxResponseBytes {
		flushed, err := b.submitPending()
		stats.Add(flushed)
		if err != nil {
			return stats, err
		}
	}
	stats.MaxPipelineBytes = b.bytesWindow.max()
	return stats, nil
}

func (b *agentChunkPipelineBatcher) Flush() (chunkBatchStats, error) {
	stats, err := b.submitPending()
	if err != nil {
		return stats, err
	}
	for {
		drained, err := b.drainResults()
		stats.Add(drained)
		if err != nil {
			return stats, err
		}
		if b.err != nil {
			return stats, b.err
		}
		if len(b.pending) == 0 && len(b.pendingByHash) == 0 {
			stats.MaxPipelineBytes = b.bytesWindow.max()
			return stats, nil
		}
		waited, err := b.waitApplyResult()
		stats.Add(waited)
		if err != nil {
			return stats, err
		}
	}
}

func (b *agentChunkPipelineBatcher) EntryPending(entry *repository.FileEntry) bool {
	_, _ = b.drainResults()
	return b.pendingByEntry[entry] > 0
}

func (b *agentChunkPipelineBatcher) Stop() {
	b.stopOnce.Do(func() {
		close(b.done)
	})
	b.wg.Wait()
}

func (b *agentChunkPipelineBatcher) submitPending() (chunkBatchStats, error) {
	var stats chunkBatchStats
	if len(b.pending) == 0 {
		stats.MaxPipelineBytes = b.bytesWindow.max()
		return stats, b.err
	}
	b.nextBatchID++
	batch := &pipelineBatch{
		id:           b.nextBatchID,
		chunks:       append([]*pendingBatchChunk(nil), b.pending...),
		requestBytes: b.requestBytes,
		createdAt:    time.Now(),
	}
	b.pending = b.pending[:0]
	b.requestBytes = 0
	for {
		if b.err != nil {
			return stats, b.err
		}
		select {
		case b.checkQueue <- batch:
			stats.MaxPipelineBytes = b.bytesWindow.max()
			return stats, nil
		case result := <-b.results:
			applied, err := b.applyResult(result)
			stats.Add(applied)
			if err != nil {
				return stats, err
			}
		case <-b.done:
			if b.err != nil {
				return stats, b.err
			}
			return stats, errors.New("chunk pipeline stopped")
		}
	}
}

func (b *agentChunkPipelineBatcher) drainResults() (chunkBatchStats, error) {
	var stats chunkBatchStats
	for {
		select {
		case result := <-b.results:
			applied, err := b.applyResult(result)
			stats.Add(applied)
			if err != nil {
				return stats, err
			}
		default:
			stats.MaxPipelineBytes = b.bytesWindow.max()
			return stats, b.err
		}
	}
}

func (b *agentChunkPipelineBatcher) waitApplyResult() (chunkBatchStats, error) {
	select {
	case result := <-b.results:
		return b.applyResult(result)
	case <-b.done:
		if b.err != nil {
			return chunkBatchStats{}, b.err
		}
		return chunkBatchStats{}, errors.New("chunk pipeline stopped")
	}
}

func (b *agentChunkPipelineBatcher) applyResult(result pipelineResult) (chunkBatchStats, error) {
	stats := chunkBatchStats{
		CheckDuration:    result.checkDuration,
		UploadDuration:   result.uploadDuration,
		MaxPipelineBytes: b.bytesWindow.max(),
	}
	if result.err != nil {
		b.err = result.err
		return stats, result.err
	}
	if result.batch == nil {
		return stats, nil
	}
	hashes := make([]string, 0, len(result.batch.chunks))
	byHash := make(map[string]*pendingBatchChunk, len(result.batch.chunks))
	for _, chunk := range result.batch.chunks {
		hashes = append(hashes, chunk.hash)
		byHash[chunk.hash] = chunk
	}
	stats.CheckRequests++
	stats.CheckResponseBytes += result.checked.ResponseBytes
	stats.BatchRetries += result.checked.RetryCount
	catalogUpdates := make([]agentChunkStatusUpdate, 0, len(result.batch.chunks))
	checkGeneration := result.checked.ChunkGeneration
	if checkGeneration == 0 {
		checkGeneration = b.opts.ChunkGeneration
	}
	for _, hash := range existingChunkHashes(hashes, result.checked, b.opts.CompactChunkCheckResponse) {
		chunk, ok := byHash[hash]
		if !ok {
			b.err = fmt.Errorf("server check returned unexpected chunk %s", hash)
			return stats, b.err
		}
		ref := repository.ChunkRef{Hash: hash, OriginalSize: chunk.originalSize}
		b.resolveChunk(chunk, ref)
		stats.Reused += int64(len(chunk.waiters))
		if b.opts.Catalog != nil {
			catalogUpdates = append(catalogUpdates, agentChunkStatusUpdate{
				Hash:         hash,
				OriginalSize: chunk.originalSize,
				Status:       "confirmed",
				Generation:   checkGeneration,
			})
		}
	}
	if len(result.uploadedByHash) > 0 {
		stats.UploadRequests++
		stats.UploadRequestBytes += result.uploaded.RequestBytes
		stats.UploadResponseBytes += result.uploaded.ResponseBytes
		stats.BatchRetries += result.uploaded.RetryCount
		stats.BatchSplits += result.uploaded.SplitCount
		uploadGeneration := result.uploaded.ChunkGeneration
		if uploadGeneration == 0 {
			uploadGeneration = b.opts.ChunkGeneration
		}
		for _, hash := range result.checked.Missing {
			chunk, ok := byHash[hash]
			if !ok {
				b.err = fmt.Errorf("server check returned unexpected missing chunk %s", hash)
				return stats, b.err
			}
			resp := result.uploadedByHash[hash]
			b.resolveChunk(chunk, resp.Ref)
			if resp.Uploaded {
				stats.Uploaded++
				if len(chunk.waiters) > 1 {
					stats.Reused += int64(len(chunk.waiters) - 1)
				}
			} else {
				stats.Reused += int64(len(chunk.waiters))
			}
			if b.opts.Catalog != nil {
				catalogUpdates = append(catalogUpdates, agentChunkStatusUpdate{
					Hash:         chunk.hash,
					OriginalSize: resp.Ref.OriginalSize,
					Status:       "confirmed",
					Generation:   uploadGeneration,
					Uploaded:     resp.Uploaded,
				})
			}
		}
	}
	if b.opts.Catalog != nil && len(catalogUpdates) > 0 {
		if err := b.opts.Catalog.markChunks(catalogUpdates); err != nil {
			b.err = err
			return stats, err
		}
	}
	stats.MaxPipelineBytes = b.bytesWindow.max()
	return stats, nil
}

func (b *agentChunkPipelineBatcher) resolveChunk(chunk *pendingBatchChunk, ref repository.ChunkRef) {
	if chunk == nil {
		return
	}
	for _, waiter := range chunk.waiters {
		waiter.entry.Chunks[waiter.index] = ref
		if count := b.pendingByEntry[waiter.entry]; count <= 1 {
			delete(b.pendingByEntry, waiter.entry)
		} else {
			b.pendingByEntry[waiter.entry] = count - 1
		}
	}
	delete(b.pendingByHash, chunk.hash)
	b.bytesWindow.release(chunk.originalSize)
}

func (b *agentChunkPipelineBatcher) worker() {
	defer b.wg.Done()
	for {
		select {
		case <-b.done:
			return
		case batch := <-b.checkQueue:
			if batch == nil {
				continue
			}
			b.publishResult(b.processBatch(batch))
		}
	}
}

func (b *agentChunkPipelineBatcher) processBatch(batch *pipelineBatch) pipelineResult {
	result := pipelineResult{batch: batch}
	hashes := make([]string, 0, len(batch.chunks))
	byHash := make(map[string]*pendingBatchChunk, len(batch.chunks))
	for _, chunk := range batch.chunks {
		hashes = append(hashes, chunk.hash)
		byHash[chunk.hash] = chunk
	}
	checkStarted := time.Now()
	checked, err := b.client.checkChunks(hashes, b.opts.RepositoryID, b.opts.ChunkGeneration, b.opts.CompactChunkCheckResponse, b.opts.chunkBatchRetryOptions(b.logger, batch.id, "check"))
	result.checkDuration = time.Since(checkStarted)
	if err != nil {
		result.err = err
		return result
	}
	if err := validateChunkCheckResponse(hashes, b.opts.RepositoryID, checked, b.opts.CompactChunkCheckResponse); err != nil {
		result.err = err
		return result
	}
	result.checked = checked
	if b.logger != nil {
		b.logger.Debug("agent chunk pipeline check",
			"batch_id", batch.id,
			"hashes", len(hashes),
			"request_bytes", estimateAgentChunkCheckRequestBytes(hashes),
			"response_bytes", checked.ResponseBytes,
			"duration", result.checkDuration.String(),
			"retries", checked.RetryCount,
			"compact_response", b.opts.CompactChunkCheckResponse,
		)
	}
	missingChunks := make([]*pendingBatchChunk, 0, len(checked.Missing))
	for _, hash := range checked.Missing {
		chunk, ok := byHash[hash]
		if !ok {
			result.err = fmt.Errorf("server check returned unexpected missing chunk %s", hash)
			return result
		}
		missingChunks = append(missingChunks, chunk)
	}
	if len(missingChunks) == 0 {
		return result
	}
	if err := b.acquireUploadSlot(); err != nil {
		result.err = err
		return result
	}
	uploadStarted := time.Now()
	uploaded, err := b.client.uploadChunksBatch(missingChunks, b.opts.CompactChunkUploadResponse, b.opts.chunkBatchRetryOptions(b.logger, batch.id, "upload"))
	result.uploadDuration = time.Since(uploadStarted)
	b.releaseUploadSlot()
	if err != nil {
		result.err = err
		return result
	}
	uploadedByHash, err := validateChunkUploadResponse(missingChunks, b.opts.RepositoryID, uploaded, b.opts.CompactChunkUploadResponse)
	if err != nil {
		result.err = err
		return result
	}
	result.uploaded = uploaded
	result.uploadedByHash = uploadedByHash
	if b.logger != nil {
		b.logger.Debug("agent chunk pipeline upload",
			"batch_id", batch.id,
			"chunks", len(missingChunks),
			"request_bytes", uploaded.RequestBytes,
			"response_bytes", uploaded.ResponseBytes,
			"duration", result.uploadDuration.String(),
			"retries", uploaded.RetryCount,
			"split_count", uploaded.SplitCount,
			"compact_response", b.opts.CompactChunkUploadResponse,
		)
	}
	return result
}

func (b *agentChunkPipelineBatcher) acquireUploadSlot() error {
	select {
	case b.uploadSlots <- struct{}{}:
		return nil
	case <-b.done:
		if b.err != nil {
			return b.err
		}
		return errors.New("chunk pipeline stopped")
	}
}

func (b *agentChunkPipelineBatcher) releaseUploadSlot() {
	select {
	case <-b.uploadSlots:
	default:
	}
}

func (b *agentChunkPipelineBatcher) publishResult(result pipelineResult) {
	select {
	case b.results <- result:
	case <-b.done:
	}
}

func (b *agentChunkPipelineBatcher) shouldFlushBeforeAdding(nextRequestBytes int64) bool {
	nextCount := len(b.pending) + 1
	return nextCount > b.maxChunks ||
		b.requestBytes+nextRequestBytes > b.maxRequestBytes ||
		b.estimatedCheckResponseBytes(nextCount) > b.maxResponseBytes ||
		b.estimatedUploadResponseBytes(nextCount) > b.maxResponseBytes
}

func (b *agentChunkPipelineBatcher) estimatedCheckResponseBytes(chunkCount int) int64 {
	return estimatedChunkBatchResponseBaseBytes + int64(chunkCount)*estimatedChunkCheckResponseBytesPerHash
}

func (b *agentChunkPipelineBatcher) estimatedUploadResponseBytes(chunkCount int) int64 {
	perChunk := estimatedFullChunkUploadResponseBytesPerChunk
	if b.opts.CompactChunkUploadResponse {
		perChunk = estimatedCompactChunkUploadResponseBytesPerChunk
	}
	return estimatedChunkBatchResponseBaseBytes + int64(chunkCount)*perChunk
}

func estimateAgentChunkCheckRequestBytes(hashes []string) int64 {
	total := int64(128)
	for _, hash := range hashes {
		total += int64(len(hash) + 4)
	}
	return total
}

type agentFileReadByteWindow struct {
	mu    sync.Mutex
	cond  *sync.Cond
	limit int64
	used  int64
	max   int64
}

func newAgentFileReadByteWindow(limit int64) *agentFileReadByteWindow {
	if limit <= 0 {
		limit = defaultAgentFileReadPipelineBytes
	}
	w := &agentFileReadByteWindow{limit: limit}
	w.cond = sync.NewCond(&w.mu)
	return w
}

func (w *agentFileReadByteWindow) acquire(n int64) {
	if w == nil || n <= 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for w.used > 0 && w.used+n > w.limit {
		w.cond.Wait()
	}
	w.used += n
	if w.used > w.max {
		w.max = w.used
	}
}

func (w *agentFileReadByteWindow) release(n int64) {
	if w == nil || n <= 0 {
		return
	}
	w.mu.Lock()
	w.used -= n
	if w.used < 0 {
		w.used = 0
	}
	w.mu.Unlock()
	w.cond.Broadcast()
}

func (w *agentFileReadByteWindow) maxBytes() int64 {
	if w == nil {
		return 0
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.max
}

func runAgentParallelFileReaders(jobs []agentParallelFileJob, workers int, pipelineBytes int64) (<-chan agentParallelFileResult, *agentFileReadByteWindow) {
	if workers <= 0 {
		workers = 2
	}
	if workers > len(jobs) && len(jobs) > 0 {
		workers = len(jobs)
	}
	window := newAgentFileReadByteWindow(pipelineBytes)
	jobCh := make(chan agentParallelFileJob)
	resultCh := make(chan agentParallelFileResult)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobCh {
				resultCh <- readAgentParallelFile(job, window)
			}
		}()
	}
	go func() {
		for _, job := range jobs {
			jobCh <- job
		}
		close(jobCh)
		wg.Wait()
		close(resultCh)
	}()
	return resultCh, window
}

func readAgentParallelFile(job agentParallelFileJob, window *agentFileReadByteWindow) agentParallelFileResult {
	file, err := agentOpen(job.filePath)
	if err != nil {
		return agentParallelFileResult{job: job, err: fmt.Errorf("open file %q: %w", job.filePath, err)}
	}
	chunker := repository.NewChunker(agentChunkAvgSize)
	var chunks [][]byte
	var acquired int64
	readErr := chunker.Split(file, func(chunk []byte) error {
		data := append([]byte(nil), chunk...)
		size := int64(len(data))
		window.acquire(size)
		acquired += size
		chunks = append(chunks, data)
		return nil
	})
	closeErr := file.Close()
	if readErr != nil {
		window.release(acquired)
		return agentParallelFileResult{job: job, err: fmt.Errorf("chunk file %q: %w", job.filePath, readErr)}
	}
	if closeErr != nil {
		window.release(acquired)
		return agentParallelFileResult{job: job, err: fmt.Errorf("close file %q: %w", job.filePath, closeErr)}
	}
	return agentParallelFileResult{job: job, chunks: chunks, bytes: acquired}
}

func releaseParallelFileChunks(window *agentFileReadByteWindow, chunks [][]byte) {
	for _, chunk := range chunks {
		window.release(int64(len(chunk)))
	}
}

func fileReadWindowMaxBytes(window *agentFileReadByteWindow) int64 {
	if window == nil {
		return 0
	}
	return window.maxBytes()
}

func (c *agentClient) scanAndUpload(runID int64, roots []string, logger *slog.Logger, scanOptions fsfilter.Options, opts backupRunOptions) (*repository.SnapshotManifest, error) {
	manifest := &repository.SnapshotManifest{
		CreatedAt:  time.Now().UTC(),
		SourceType: "agent",
	}
	if len(roots) == 1 {
		manifest.SourceRoot = roots[0]
	} else {
		manifest.SourceRoots = append([]string(nil), roots...)
	}
	fileReadWorkers := opts.FileReadWorkers
	if fileReadWorkers <= 0 {
		fileReadWorkers = 2
	}
	fileReadPipelineBytes := opts.FileReadPipelineBytes
	if fileReadPipelineBytes <= 0 {
		fileReadPipelineBytes = defaultAgentFileReadPipelineBytes
	}
	chunker := repository.NewChunker(agentChunkAvgSize)
	progress := agentProgress{Phase: "scanning", Message: strings.Join(roots, ", ")}
	if err := c.sendProgress(runID, progress); err != nil {
		return nil, err
	}
	lastProgress := time.Now()
	sendProgress := func(force bool) error {
		if !force && time.Since(lastProgress) < 500*time.Millisecond {
			return nil
		}
		lastProgress = time.Now()
		return c.sendProgress(runID, progress)
	}
	skipReporter := &agentSkipReporter{logger: logger}
	chunkBatcher := newAgentChunkUploader(c, opts, logger)
	defer chunkBatcher.Stop()
	packBatcher := newAgentSmallFilePackBatcher(opts)
	fileCatalogBatcher := newAgentFileCatalogBatcher(opts.Catalog, logger)
	var batchStats chunkBatchStats
	pendingFiles := make([]pendingManifestFile, 0)
	parallelJobs := make([]agentParallelFileJob, 0)

	multiRoot := len(roots) > 1
	entryPaths := make(map[string]struct{})
	packIDs := make(map[string]struct{})
	finalizeFileEntry := func(pending pendingManifestFile) error {
		fileCatalogBatcher.Add(pending.record, catalogChunksFromEntry(pending.entry))
		return appendManifestEntry(manifest, entryPaths, *pending.entry)
	}
	appendPackManifest := func(pack repository.PackManifest) {
		if _, ok := packIDs[pack.ID]; ok {
			return
		}
		packIDs[pack.ID] = struct{}{}
		manifest.Packs = append(manifest.Packs, pack)
	}
	var flushPackFiles func() error
	flushPendingFiles := func() error {
		stats, err := chunkBatcher.Flush()
		if err != nil {
			return err
		}
		batchStats.Add(stats)
		progress.UploadedChunks += stats.Uploaded
		progress.ReusedChunks += stats.Reused
		remaining := pendingFiles[:0]
		for _, pending := range pendingFiles {
			if chunkBatcher.EntryPending(pending.entry) {
				remaining = append(remaining, pending)
				continue
			}
			if err := finalizeFileEntry(pending); err != nil {
				return err
			}
		}
		pendingFiles = remaining
		if stats.Uploaded != 0 || stats.Reused != 0 {
			return sendProgress(true)
		}
		return nil
	}
	flushPackFiles = func() error {
		if packBatcher == nil || len(packBatcher.pending) == 0 {
			return nil
		}
		pending := append([]pendingPackFile(nil), packBatcher.pending...)
		packBatcher.pending = packBatcher.pending[:0]
		packBatcher.pendingBytes = 0

		payloads := make([]repository.PackFilePayload, 0, len(pending))
		for _, file := range pending {
			payloads = append(payloads, file.file)
		}
		packData, indexes, err := repository.EncodePack(payloads)
		if err != nil {
			return err
		}
		packHash, err := repository.HashBytes(packData)
		if err != nil {
			return err
		}
		packID := fmt.Sprintf("pack-%d-%s", packBatcher.nextPackIndex, packHash[:16])
		packBatcher.nextPackIndex++

		packEntry := &repository.FileEntry{
			Path:    "__turbk_pack/" + packID,
			Type:    repository.EntryTypeFile,
			Size:    int64(len(packData)),
			Mode:    0o600,
			ModTime: time.Now().UTC(),
		}
		if err := chunker.Split(bytes.NewReader(packData), func(chunk []byte) error {
			stats, err := chunkBatcher.Add(chunk, packEntry)
			if err != nil {
				return err
			}
			progress.UploadedChunks += stats.Uploaded
			progress.ReusedChunks += stats.Reused
			batchStats.Add(stats)
			return nil
		}); err != nil {
			return fmt.Errorf("chunk small-file pack %s: %w", packID, err)
		}
		if err := flushPendingFiles(); err != nil {
			return err
		}
		if len(packEntry.Chunks) == 0 {
			return fmt.Errorf("small-file pack %s produced no chunks", packID)
		}
		appendPackManifest(repository.PackManifest{
			ID:     packID,
			Format: repository.PackFormatTBKPack1,
			Chunks: cloneChunkRefs(packEntry.Chunks),
		})
		for i, file := range pending {
			index := indexes[i]
			entry := file.entry
			record := file.record
			entry.Type = repository.EntryTypePackedFile
			entry.Chunks = nil
			entry.Pack = &repository.PackFileRef{
				ID:     packID,
				Offset: index.Offset,
				Length: index.Length,
			}
			record.Fingerprint = encodePackedFileFingerprint(packID, index.Offset, index.Length, packEntry.Chunks)
			fileCatalogBatcher.Add(record, nil)
			if err := appendManifestEntry(manifest, entryPaths, entry); err != nil {
				return err
			}
		}
		batchStats.PackCount++
		batchStats.PackedFiles += int64(len(pending))
		batchStats.PackedBytes += int64(len(packData))
		return nil
	}
	for _, root := range roots {
		progress.Message = root
		if err := sendProgress(true); err != nil {
			return nil, err
		}
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				if event, skip := agentPathErrorSkipEvent(root, path, walkErr); skip {
					skipReporter.record(event)
					progress.Message = "skipped " + event.Rel
					return sendProgress(false)
				}
				return walkErr
			}
			info, err := agentLstat(path)
			if err != nil {
				if event, skip := agentPathErrorSkipEvent(root, path, err); skip {
					skipReporter.record(event)
					progress.Message = "skipped " + event.Rel
					if progressErr := sendProgress(false); progressErr != nil {
						return progressErr
					}
					if d != nil && d.IsDir() {
						return filepath.SkipDir
					}
					return nil
				}
				return fmt.Errorf("stat %q: %w", path, err)
			}
			if event, skip := fsfilter.ShouldSkip(root, path, info, scanOptions); skip {
				skipReporter.record(event)
				progress.Message = "skipped " + event.Rel
				if err := sendProgress(false); err != nil {
					return err
				}
				if info.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return fmt.Errorf("rel path %q: %w", path, err)
			}
			catalogPath := cleanManifestPath(rel)
			entry := repository.FileEntry{
				Path:    manifestEntryPath(root, catalogPath, multiRoot),
				Size:    info.Size(),
				Mode:    uint32(info.Mode()),
				ModTime: info.ModTime().UTC(),
			}
			entry.UID, entry.GID = fileOwner(info)
			record := catalogRecordFromFile(root, catalogPath, info)

			mode := info.Mode()
			switch {
			case mode.IsDir():
				entry.Type = repository.EntryTypeDir
				record.Type = string(repository.EntryTypeDir)
				fileCatalogBatcher.Add(record, nil)
			case mode&os.ModeSymlink != 0:
				entry.Type = repository.EntryTypeSymlink
				target, err := os.Readlink(path)
				if err != nil {
					if event, skip := agentPathErrorSkipEvent(root, path, err); skip {
						skipReporter.record(event)
						progress.Message = "skipped " + event.Rel
						return sendProgress(false)
					}
					return fmt.Errorf("read symlink %q: %w", path, err)
				}
				entry.LinkTarget = target
				record.Type = string(repository.EntryTypeSymlink)
				record.LinkTarget = target
				fileCatalogBatcher.Add(record, nil)
			case mode.IsRegular():
				entry.Type = repository.EntryTypeFile
				record.Type = string(repository.EntryTypeFile)
				if packBatcher.Eligible(info) {
					if opts.Catalog != nil {
						reused, pack, err := c.tryReuseCatalogPackedFile(opts.Catalog, record, &entry, opts)
						if err != nil {
							logger.Warn("agent packed catalog reuse failed; reading file", "path", entry.Path, "error", err)
						} else if reused {
							appendPackManifest(pack)
							progress.ProcessedFiles++
							progress.ProcessedBytes += entry.Size
							progress.ReusedChunks += int64(len(pack.Chunks))
							progress.Message = entry.Path
							if err := sendProgress(true); err != nil {
								return err
							}
							if err := appendManifestEntry(manifest, entryPaths, entry); err != nil {
								return err
							}
							return nil
						}
					}
					packFile, err := readPackFilePayload(root, catalogPath, path, record, entry)
					if err != nil {
						if event, skip := agentPathErrorSkipEvent(root, path, err); skip {
							skipReporter.record(event)
							progress.Message = "skipped " + event.Rel
							return sendProgress(false)
						}
						return err
					}
					progress.ProcessedFiles++
					progress.ProcessedBytes += entry.Size
					progress.Message = entry.Path
					if err := packBatcher.Add(pendingPackFile{entry: entry, record: record, file: packFile}, flushPackFiles); err != nil {
						return err
					}
					if err := sendProgress(true); err != nil {
						return err
					}
					return nil
				}
				if opts.Catalog != nil {
					reused, err := c.tryReuseCatalogFile(opts.Catalog, record, &entry, opts)
					if err != nil {
						logger.Warn("agent catalog reuse failed; reading file", "path", entry.Path, "error", err)
					} else if reused {
						progress.ProcessedFiles++
						progress.ProcessedBytes += entry.Size
						progress.ReusedChunks += int64(len(entry.Chunks))
						progress.Message = entry.Path
						if err := sendProgress(true); err != nil {
							return err
						}
						if err := appendManifestEntry(manifest, entryPaths, entry); err != nil {
							return err
						}
						return nil
					}
				}
				if opts.ScanParallelEnabled && entry.Size > 0 {
					parallelJobs = append(parallelJobs, agentParallelFileJob{
						root:        root,
						catalogPath: catalogPath,
						filePath:    path,
						entry:       entry,
						record:      record,
					})
					progress.Message = entry.Path
					return sendProgress(false)
				}
				file, err := agentOpen(path)
				if err != nil {
					if event, skip := agentPathErrorSkipEvent(root, path, err); skip {
						skipReporter.record(event)
						progress.Message = "skipped " + event.Rel
						return sendProgress(false)
					}
					return fmt.Errorf("open file %q: %w", path, err)
				}
				entryPtr := &entry
				if err := chunker.Split(file, func(chunk []byte) error {
					stats, err := chunkBatcher.Add(chunk, entryPtr)
					if err != nil {
						_ = file.Close()
						return err
					}
					progress.UploadedChunks += stats.Uploaded
					progress.ReusedChunks += stats.Reused
					batchStats.Add(stats)
					return sendProgress(false)
				}); err != nil {
					_ = file.Close()
					if event, skip := agentPathErrorSkipEvent(root, path, err); skip {
						skipReporter.record(event)
						progress.Message = "skipped " + event.Rel
						return sendProgress(false)
					}
					return fmt.Errorf("chunk file %q: %w", path, err)
				}
				if err := file.Close(); err != nil {
					if event, skip := agentPathErrorSkipEvent(root, path, err); skip {
						skipReporter.record(event)
						progress.Message = "skipped " + event.Rel
						return sendProgress(false)
					}
					return fmt.Errorf("close file %q: %w", path, err)
				}
				progress.ProcessedFiles++
				progress.ProcessedBytes += entry.Size
				progress.Message = entry.Path
				pending := pendingManifestFile{entry: entryPtr, record: record}
				if chunkBatcher.EntryPending(entryPtr) {
					pendingFiles = append(pendingFiles, pending)
				} else if err := finalizeFileEntry(pending); err != nil {
					return err
				}
				if err := sendProgress(true); err != nil {
					return err
				}
				return nil
			default:
				return nil
			}
			return appendManifestEntry(manifest, entryPaths, entry)
		})
		if err != nil {
			return nil, err
		}
		if err := flushPackFiles(); err != nil {
			return nil, err
		}
		if err := flushPendingFiles(); err != nil {
			return nil, err
		}
	}
	var fileReadWindow *agentFileReadByteWindow
	if len(parallelJobs) > 0 {
		progress.Message = "parallel file read"
		if err := sendProgress(true); err != nil {
			return nil, err
		}
		results, window := runAgentParallelFileReaders(parallelJobs, fileReadWorkers, fileReadPipelineBytes)
		fileReadWindow = window
		for result := range results {
			if result.err != nil {
				releaseParallelFileChunks(window, result.chunks)
				if event, skip := agentPathErrorSkipEvent(result.job.root, result.job.filePath, result.err); skip {
					skipReporter.record(event)
					progress.Message = "skipped " + event.Rel
					if err := sendProgress(false); err != nil {
						return nil, err
					}
					continue
				}
				return nil, result.err
			}
			entry := result.job.entry
			entryPtr := &entry
			for i, chunk := range result.chunks {
				stats, err := chunkBatcher.Add(chunk, entryPtr)
				window.release(int64(len(chunk)))
				result.chunks[i] = nil
				if err != nil {
					releaseParallelFileChunks(window, result.chunks[i+1:])
					return nil, err
				}
				progress.UploadedChunks += stats.Uploaded
				progress.ReusedChunks += stats.Reused
				batchStats.Add(stats)
				if err := sendProgress(false); err != nil {
					releaseParallelFileChunks(window, result.chunks[i+1:])
					return nil, err
				}
			}
			progress.ProcessedFiles++
			progress.ProcessedBytes += entry.Size
			progress.Message = entry.Path
			pending := pendingManifestFile{entry: entryPtr, record: result.job.record}
			if chunkBatcher.EntryPending(entryPtr) {
				pendingFiles = append(pendingFiles, pending)
			} else if err := finalizeFileEntry(pending); err != nil {
				return nil, err
			}
			if err := sendProgress(true); err != nil {
				return nil, err
			}
		}
	}
	if err := flushPackFiles(); err != nil {
		return nil, err
	}
	if err := flushPendingFiles(); err != nil {
		return nil, err
	}
	fileCatalogBatcher.Flush()
	progress.Phase = "manifest"
	progress.Message = "manifest ready"
	if err := sendProgress(true); err != nil {
		return nil, err
	}
	if opts.ScanParallelEnabled {
		sort.SliceStable(manifest.Entries, func(i, j int) bool {
			return manifest.Entries[i].Path < manifest.Entries[j].Path
		})
	}
	logger.Info("agent scan complete",
		"files", progress.ProcessedFiles,
		"uploaded_chunks", progress.UploadedChunks,
		"reused_chunks", progress.ReusedChunks,
		"skipped_paths", skipReporter.total,
		"chunk_check_requests", batchStats.CheckRequests,
		"chunk_upload_requests", batchStats.UploadRequests,
		"chunk_upload_request_bytes", batchStats.UploadRequestBytes,
		"chunk_check_response_bytes", batchStats.CheckResponseBytes,
		"chunk_upload_response_bytes", batchStats.UploadResponseBytes,
		"chunk_pipeline_enabled", opts.ChunkPipelineEnabled && opts.ChunkBatchUpload,
		"chunk_check_inflight", opts.MaxChunkCheckInflight,
		"chunk_upload_inflight", opts.MaxChunkUploadInflight,
		"chunk_pipeline_wait", batchStats.PipelineWait.String(),
		"chunk_check_duration", batchStats.CheckDuration.String(),
		"chunk_upload_duration", batchStats.UploadDuration.String(),
		"chunk_pipeline_max_bytes", batchStats.MaxPipelineBytes,
		"chunk_batch_retries", batchStats.BatchRetries,
		"chunk_batch_splits", batchStats.BatchSplits,
		"scan_parallel_enabled", opts.ScanParallelEnabled,
		"file_read_workers", fileReadWorkers,
		"file_read_jobs", len(parallelJobs),
		"file_read_pipeline_bytes", fileReadPipelineBytes,
		"file_read_pipeline_max_bytes", fileReadWindowMaxBytes(fileReadWindow),
		"manifest_bytes", estimateManifestBytes(manifest),
		"packed_files", batchStats.PackedFiles,
		"packed_bytes", batchStats.PackedBytes,
		"pack_count", batchStats.PackCount,
	)
	return manifest, nil
}

func manifestEntryPath(root, catalogPath string, multiRoot bool) string {
	if !multiRoot {
		return catalogPath
	}
	if catalogPath == "." {
		return cleanManifestPath(rootset.ManifestPrefix(root))
	}
	return cleanManifestPath(filepath.Join(rootset.ManifestPrefix(root), catalogPath))
}

func appendManifestEntry(manifest *repository.SnapshotManifest, seen map[string]struct{}, entry repository.FileEntry) error {
	if _, ok := seen[entry.Path]; ok {
		return fmt.Errorf("manifest entry path %q is duplicated", entry.Path)
	}
	seen[entry.Path] = struct{}{}
	manifest.Entries = append(manifest.Entries, entry)
	return nil
}

func readPackFilePayload(root, catalogPath, filePath string, initial catalogFileRecord, entry repository.FileEntry) (repository.PackFilePayload, error) {
	file, err := agentOpen(filePath)
	if err != nil {
		return repository.PackFilePayload{}, fmt.Errorf("open file %q: %w", filePath, err)
	}
	limit := entry.Size + 1
	data, readErr := io.ReadAll(io.LimitReader(file, limit))
	stat, statErr := file.Stat()
	closeErr := file.Close()
	if readErr != nil {
		return repository.PackFilePayload{}, fmt.Errorf("read small file %q: %w", filePath, readErr)
	}
	if statErr != nil {
		return repository.PackFilePayload{}, fmt.Errorf("stat small file %q after read: %w", filePath, statErr)
	}
	if closeErr != nil {
		return repository.PackFilePayload{}, fmt.Errorf("close small file %q: %w", filePath, closeErr)
	}
	if int64(len(data)) != entry.Size {
		return repository.PackFilePayload{}, fmt.Errorf("small file %q changed while reading", filePath)
	}
	current := catalogRecordFromFile(root, catalogPath, stat)
	current.Type = initial.Type
	current.LinkTarget = initial.LinkTarget
	if !catalogFileMatches(current, initial) {
		return repository.PackFilePayload{}, fmt.Errorf("small file %q metadata changed while reading", filePath)
	}
	return repository.PackFilePayload{
		Path:    entry.Path,
		Mode:    entry.Mode,
		ModTime: entry.ModTime,
		Data:    data,
	}, nil
}

func cloneChunkRefs(chunks []repository.ChunkRef) []repository.ChunkRef {
	if len(chunks) == 0 {
		return nil
	}
	cloned := make([]repository.ChunkRef, len(chunks))
	copy(cloned, chunks)
	return cloned
}

func estimateManifestBytes(manifest *repository.SnapshotManifest) int64 {
	data, err := json.Marshal(manifest)
	if err != nil {
		return 0
	}
	return int64(len(data))
}

const packedFileFingerprintType = "turbk-packed-file-v1"

type packedFileFingerprint struct {
	Type   string               `json:"type"`
	PackID string               `json:"pack_id"`
	Offset int64                `json:"offset"`
	Length int64                `json:"length"`
	Chunks []catalogChunkRecord `json:"chunks"`
}

func encodePackedFileFingerprint(packID string, offset, length int64, chunks []repository.ChunkRef) string {
	fingerprint := packedFileFingerprint{
		Type:   packedFileFingerprintType,
		PackID: packID,
		Offset: offset,
		Length: length,
		Chunks: make([]catalogChunkRecord, 0, len(chunks)),
	}
	for _, chunk := range chunks {
		fingerprint.Chunks = append(fingerprint.Chunks, catalogChunkRecord{
			Hash:         chunk.Hash,
			OriginalSize: chunk.OriginalSize,
		})
	}
	data, err := json.Marshal(fingerprint)
	if err != nil {
		return ""
	}
	return string(data)
}

func decodePackedFileFingerprint(value string) (packedFileFingerprint, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return packedFileFingerprint{}, false
	}
	var fingerprint packedFileFingerprint
	if err := json.Unmarshal([]byte(value), &fingerprint); err != nil {
		return packedFileFingerprint{}, false
	}
	if fingerprint.Type != packedFileFingerprintType ||
		strings.TrimSpace(fingerprint.PackID) == "" ||
		fingerprint.Offset < 0 ||
		fingerprint.Length <= 0 ||
		len(fingerprint.Chunks) == 0 {
		return packedFileFingerprint{}, false
	}
	for _, chunk := range fingerprint.Chunks {
		if strings.TrimSpace(chunk.Hash) == "" || chunk.OriginalSize <= 0 {
			return packedFileFingerprint{}, false
		}
	}
	return fingerprint, true
}

func catalogFileMetadataMatches(a, b catalogFileRecord) bool {
	a.Fingerprint = ""
	b.Fingerprint = ""
	return catalogFileMatches(a, b)
}

func packManifestChunksFromCatalog(chunks []catalogChunkRecord) []repository.ChunkRef {
	if len(chunks) == 0 {
		return nil
	}
	refs := make([]repository.ChunkRef, 0, len(chunks))
	for _, chunk := range chunks {
		refs = append(refs, repository.ChunkRef{Hash: chunk.Hash, OriginalSize: chunk.OriginalSize})
	}
	return refs
}

func catalogChunksFromEntry(entry *repository.FileEntry) []catalogChunkRecord {
	if len(entry.Chunks) == 0 {
		return nil
	}
	chunks := make([]catalogChunkRecord, 0, len(entry.Chunks))
	for _, ref := range entry.Chunks {
		chunks = append(chunks, catalogChunkRecord{Hash: ref.Hash, OriginalSize: ref.OriginalSize})
	}
	return chunks
}

func (c *agentClient) sendProgress(runID int64, progress agentProgress) error {
	if runID <= 0 {
		return fmt.Errorf("run id is required for progress")
	}
	var resp map[string]any
	_, err := c.doJSON(http.MethodPost, fmt.Sprintf("/agent/v1/runs/%d/progress", runID), progress, &resp)
	return err
}

func (c *agentClient) failRun(runID int64, message string) error {
	if runID <= 0 {
		return nil
	}
	var resp map[string]any
	_, err := c.doJSON(http.MethodPost, fmt.Sprintf("/agent/v1/runs/%d/finish", runID), map[string]any{
		"status": "failed",
		"error":  message,
	}, &resp)
	return err
}

func (c *agentClient) ensureChunk(chunk []byte, opts backupRunOptions) (repository.ChunkRef, bool, error) {
	sum := blake3.Sum256(chunk)
	hash := hex.EncodeToString(sum[:])
	if opts.Catalog != nil {
		status, confirmedGeneration, ok, err := opts.Catalog.chunkStatus(hash)
		if err != nil {
			return repository.ChunkRef{}, false, err
		}
		if ok && status == "confirmed" && confirmedGeneration >= opts.ChunkGeneration {
			return repository.ChunkRef{Hash: hash, OriginalSize: int64(len(chunk))}, false, nil
		}
	}
	var queried chunkResponse
	if _, err := c.doJSON(http.MethodGet, "/agent/v1/chunks/"+hash, nil, &queried); err != nil {
		return repository.ChunkRef{}, false, err
	}
	if queried.Exists {
		if queried.Ref.Hash == "" {
			return repository.ChunkRef{}, false, fmt.Errorf("server reported existing chunk %s without ref", hash)
		}
		if opts.Catalog != nil {
			_ = opts.Catalog.markChunk(hash, queried.Ref.OriginalSize, "confirmed", opts.ChunkGeneration, false)
		}
		return queried.Ref, false, nil
	}
	var uploaded chunkResponse
	status, err := c.doRaw(http.MethodPut, "/agent/v1/chunks/"+hash, bytes.NewReader(chunk), "application/octet-stream", &uploaded)
	if err != nil {
		return repository.ChunkRef{}, false, err
	}
	if status != http.StatusCreated && status != http.StatusOK {
		return repository.ChunkRef{}, false, fmt.Errorf("unexpected chunk upload status %d", status)
	}
	if uploaded.Ref.Hash == "" {
		return repository.ChunkRef{}, false, fmt.Errorf("server accepted chunk %s without ref", hash)
	}
	if opts.Catalog != nil {
		_ = opts.Catalog.markChunk(hash, uploaded.Ref.OriginalSize, "confirmed", opts.ChunkGeneration, status == http.StatusCreated && uploaded.Uploaded)
	}
	return uploaded.Ref, status == http.StatusCreated && uploaded.Uploaded, nil
}

func (c *agentClient) tryReuseCatalogFile(catalog *agentCatalog, record catalogFileRecord, entry *repository.FileEntry, opts backupRunOptions) (bool, error) {
	existing, chunks, ok, err := catalog.fileRecordWithChunks(record.RootID, record.Path)
	if err != nil || !ok {
		return false, err
	}
	if !catalogFileMatches(existing, record) {
		return false, nil
	}
	if len(chunks) == 0 {
		return entry.Size == 0, nil
	}
	if ok, err := c.ensureCatalogChunksConfirmed(catalog, chunks, opts); err != nil || !ok {
		return false, err
	}
	entry.Chunks = entry.Chunks[:0]
	for _, chunk := range chunks {
		entry.Chunks = append(entry.Chunks, repository.ChunkRef{
			Hash:         chunk.Hash,
			OriginalSize: chunk.OriginalSize,
		})
	}
	return true, nil
}

func (c *agentClient) tryReuseCatalogPackedFile(catalog *agentCatalog, record catalogFileRecord, entry *repository.FileEntry, opts backupRunOptions) (bool, repository.PackManifest, error) {
	existing, _, ok, err := catalog.fileRecordWithChunks(record.RootID, record.Path)
	if err != nil || !ok {
		return false, repository.PackManifest{}, err
	}
	if !catalogFileMetadataMatches(existing, record) {
		return false, repository.PackManifest{}, nil
	}
	fingerprint, ok := decodePackedFileFingerprint(existing.Fingerprint)
	if !ok {
		return false, repository.PackManifest{}, nil
	}
	if fingerprint.Length != entry.Size {
		return false, repository.PackManifest{}, nil
	}
	if ok, err := c.ensureCatalogChunksConfirmed(catalog, fingerprint.Chunks, opts); err != nil || !ok {
		return false, repository.PackManifest{}, err
	}
	entry.Type = repository.EntryTypePackedFile
	entry.Chunks = nil
	entry.Pack = &repository.PackFileRef{
		ID:     fingerprint.PackID,
		Offset: fingerprint.Offset,
		Length: fingerprint.Length,
	}
	return true, repository.PackManifest{
		ID:     fingerprint.PackID,
		Format: repository.PackFormatTBKPack1,
		Chunks: packManifestChunksFromCatalog(fingerprint.Chunks),
	}, nil
}

func (c *agentClient) ensureCatalogChunksConfirmed(catalog *agentCatalog, chunks []catalogChunkRecord, opts backupRunOptions) (bool, error) {
	stale := make([]string, 0)
	originalSizes := make(map[string]int64, len(chunks))
	for _, chunk := range chunks {
		originalSizes[chunk.Hash] = chunk.OriginalSize
		status, generation, ok, err := catalog.chunkStatus(chunk.Hash)
		if err != nil {
			return false, err
		}
		if ok && status == "confirmed" && generation >= opts.ChunkGeneration {
			continue
		}
		stale = append(stale, chunk.Hash)
	}
	if len(stale) == 0 {
		return true, nil
	}
	checked, err := c.checkChunksBatched(stale, opts.RepositoryID, opts.ChunkGeneration, opts.MaxChunkCheckBatch, opts.CompactChunkCheckResponse, opts.chunkBatchRetryOptions(nil, 0, "check"))
	if err != nil {
		return false, err
	}
	if err := validateChunkCheckResponse(stale, opts.RepositoryID, checked, opts.CompactChunkCheckResponse); err != nil {
		return false, err
	}
	missing := make(map[string]struct{}, len(checked.Missing))
	updates := make([]agentChunkStatusUpdate, 0, len(checked.Exists)+len(checked.Missing))
	for _, hash := range existingChunkHashes(stale, checked, opts.CompactChunkCheckResponse) {
		updates = append(updates, agentChunkStatusUpdate{
			Hash:         hash,
			OriginalSize: originalSizes[hash],
			Status:       "confirmed",
			Generation:   checked.ChunkGeneration,
		})
	}
	for _, hash := range checked.Missing {
		missing[hash] = struct{}{}
		updates = append(updates, agentChunkStatusUpdate{
			Hash:         hash,
			OriginalSize: originalSizes[hash],
			Status:       "missing",
		})
	}
	if err := catalog.markChunks(updates); err != nil {
		return false, err
	}
	return len(missing) == 0, nil
}

func (c *agentClient) checkChunksBatched(hashes []string, repositoryID string, baseGeneration int64, maxChunks int, compactResponse bool, retry chunkBatchRetryOptions) (checkChunksResponse, error) {
	maxChunks = normalizedAgentChunkCheckBatchLimit(maxChunks)
	if len(hashes) <= maxChunks {
		return c.checkChunks(hashes, repositoryID, baseGeneration, compactResponse, retry)
	}
	combined := checkChunksResponse{RepositoryID: repositoryID}
	for start := 0; start < len(hashes); start += maxChunks {
		end := start + maxChunks
		if end > len(hashes) {
			end = len(hashes)
		}
		batch := hashes[start:end]
		checked, err := c.checkChunks(batch, repositoryID, baseGeneration, compactResponse, retry)
		if err != nil {
			return checkChunksResponse{}, err
		}
		if err := validateChunkCheckResponse(batch, repositoryID, checked, compactResponse); err != nil {
			return checkChunksResponse{}, err
		}
		if combined.RepositoryID == "" {
			combined.RepositoryID = checked.RepositoryID
		}
		if checked.ChunkGeneration > combined.ChunkGeneration {
			combined.ChunkGeneration = checked.ChunkGeneration
		}
		combined.Exists = append(combined.Exists, existingChunkHashes(batch, checked, compactResponse)...)
		combined.Missing = append(combined.Missing, checked.Missing...)
		combined.ResponseBytes += checked.ResponseBytes
		combined.RetryCount += checked.RetryCount
	}
	return combined, nil
}

func validateChunkCheckResponse(requested []string, repositoryID string, checked checkChunksResponse, allowCompact bool) error {
	if repositoryID != "" && checked.RepositoryID != "" && checked.RepositoryID != repositoryID {
		return fmt.Errorf("server check repository_id = %q, want %q", checked.RepositoryID, repositoryID)
	}
	requestedSet := make(map[string]struct{}, len(requested))
	for _, hash := range requested {
		hash = strings.TrimSpace(hash)
		if hash == "" {
			return fmt.Errorf("server check request contains empty chunk hash")
		}
		requestedSet[hash] = struct{}{}
	}
	seen := make(map[string]string, len(requestedSet))
	for _, hash := range checked.Exists {
		hash = strings.TrimSpace(hash)
		if _, ok := requestedSet[hash]; !ok {
			return fmt.Errorf("server check returned unexpected existing chunk %s", hash)
		}
		if previous, ok := seen[hash]; ok {
			if previous == "existing" {
				return fmt.Errorf("server check returned duplicate existing chunk %s", hash)
			}
			return fmt.Errorf("server check returned chunk %s as both %s and existing", hash, previous)
		}
		seen[hash] = "existing"
	}
	for _, hash := range checked.Missing {
		hash = strings.TrimSpace(hash)
		if _, ok := requestedSet[hash]; !ok {
			return fmt.Errorf("server check returned unexpected missing chunk %s", hash)
		}
		if previous, ok := seen[hash]; ok {
			if previous == "missing" {
				return fmt.Errorf("server check returned duplicate missing chunk %s", hash)
			}
			return fmt.Errorf("server check returned chunk %s as both %s and missing", hash, previous)
		}
		seen[hash] = "missing"
	}
	if !(allowCompact && len(checked.Exists) == 0) {
		for hash := range requestedSet {
			if _, ok := seen[hash]; !ok {
				return fmt.Errorf("server check omitted chunk %s", hash)
			}
		}
	}
	return nil
}

func existingChunkHashes(requested []string, checked checkChunksResponse, allowCompact bool) []string {
	if !(allowCompact && len(checked.Exists) == 0) {
		return checked.Exists
	}
	missing := make(map[string]struct{}, len(checked.Missing))
	for _, hash := range checked.Missing {
		missing[strings.TrimSpace(hash)] = struct{}{}
	}
	exists := make([]string, 0, len(requested)-len(missing))
	seen := make(map[string]struct{}, len(requested))
	for _, hash := range requested {
		hash = strings.TrimSpace(hash)
		if hash == "" {
			continue
		}
		if _, ok := seen[hash]; ok {
			continue
		}
		seen[hash] = struct{}{}
		if _, ok := missing[hash]; !ok {
			exists = append(exists, hash)
		}
	}
	return exists
}

func validateChunkUploadResponse(requested []*pendingBatchChunk, repositoryID string, uploaded uploadChunksResponse, allowCompact bool) (map[string]uploadChunkResponse, error) {
	if repositoryID != "" && uploaded.RepositoryID != "" && uploaded.RepositoryID != repositoryID {
		return nil, fmt.Errorf("server upload repository_id = %q, want %q", uploaded.RepositoryID, repositoryID)
	}
	requestedSet := make(map[string]int64, len(requested))
	for _, chunk := range requested {
		if chunk == nil || strings.TrimSpace(chunk.hash) == "" {
			return nil, fmt.Errorf("upload request contains empty chunk hash")
		}
		requestedSet[chunk.hash] = chunk.originalSize
	}
	uploadedByHash := make(map[string]uploadChunkResponse, len(uploaded.Chunks))
	for _, chunk := range uploaded.Chunks {
		hash := strings.TrimSpace(chunk.Hash)
		originalSize, ok := requestedSet[hash]
		if !ok {
			return nil, fmt.Errorf("server upload returned unexpected chunk %s", hash)
		}
		if _, ok := uploadedByHash[hash]; ok {
			return nil, fmt.Errorf("server upload returned duplicate chunk %s", hash)
		}
		if chunk.Exists != nil && !*chunk.Exists {
			return nil, fmt.Errorf("server upload did not confirm chunk %s", hash)
		}
		if chunk.Exists == nil && !allowCompact {
			return nil, fmt.Errorf("server upload did not confirm chunk %s", hash)
		}
		if chunk.Ref.Hash == "" {
			if !allowCompact {
				return nil, fmt.Errorf("server accepted chunk %s without ref", hash)
			}
			if chunk.OriginalSize <= 0 {
				return nil, fmt.Errorf("server accepted chunk %s without original_size", hash)
			}
			chunk.Ref = repository.ChunkRef{Hash: hash, OriginalSize: chunk.OriginalSize}
		} else if chunk.Ref.Hash != hash {
			return nil, fmt.Errorf("server returned ref %s for uploaded chunk %s", chunk.Ref.Hash, hash)
		}
		if chunk.OriginalSize != 0 && chunk.OriginalSize != originalSize {
			return nil, fmt.Errorf("server returned original_size %d for uploaded chunk %s, want %d", chunk.OriginalSize, hash, originalSize)
		}
		if chunk.Ref.OriginalSize != originalSize {
			return nil, fmt.Errorf("server returned ref original_size %d for uploaded chunk %s, want %d", chunk.Ref.OriginalSize, hash, originalSize)
		}
		uploadedByHash[hash] = chunk
	}
	for hash := range requestedSet {
		if _, ok := uploadedByHash[hash]; !ok {
			return nil, fmt.Errorf("server upload omitted chunk %s", hash)
		}
	}
	return uploadedByHash, nil
}

func catalogRecordFromFile(root, rel string, info fs.FileInfo) catalogFileRecord {
	uid, gid := fileOwner(info)
	record := catalogFileRecord{
		RootID:  filepath.Clean(root),
		Path:    rel,
		Size:    info.Size(),
		Mode:    int64(info.Mode()),
		UID:     int64(uid),
		GID:     int64(gid),
		MTimeNS: info.ModTime().UnixNano(),
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		record.Dev = int64(stat.Dev)
		record.Inode = int64(stat.Ino)
	}
	return record
}

func (c *agentClient) checkChunks(hashes []string, repositoryID string, baseGeneration int64, compactResponse bool, retry chunkBatchRetryOptions) (checkChunksResponse, error) {
	retry = retry.normalized()
	requestValue := map[string]any{
		"repository_id":         repositoryID,
		"base_chunk_generation": baseGeneration,
		"hashes":                hashes,
		"compact_response":      compactResponse,
	}
	for attempt := 1; ; attempt++ {
		var resp checkChunksResponse
		_, err := c.doJSON(http.MethodPost, "/agent/v1/chunks/check", requestValue, &resp)
		if err == nil {
			resp.RetryCount = int64(attempt - 1)
			return resp, nil
		}
		if !retryableChunkBatchError(err) || attempt > retry.MaxRetries {
			return checkChunksResponse{}, err
		}
		backoff := chunkBatchRetryBackoff(retry, attempt, err)
		logChunkBatchRetry(retry, attempt, backoff, err)
		time.Sleep(backoff)
	}
}

func (c *agentClient) uploadChunksBatch(chunks []*pendingBatchChunk, compactResponse bool, retry chunkBatchRetryOptions) (uploadChunksResponse, error) {
	return c.uploadChunksBatchWithSplit(chunks, compactResponse, retry.normalized())
}

func (c *agentClient) uploadChunksBatchWithSplit(chunks []*pendingBatchChunk, compactResponse bool, retry chunkBatchRetryOptions) (uploadChunksResponse, error) {
	resp, err := c.uploadChunksBatchNoSplit(chunks, compactResponse, retry)
	if err == nil {
		return resp, nil
	}
	if retry.SplitOn413 && len(chunks) > 1 && chunkBatchHTTPStatus(err) == http.StatusRequestEntityTooLarge {
		logChunkBatchSplit(retry, len(chunks), err)
		mid := len(chunks) / 2
		left, leftErr := c.uploadChunksBatchWithSplit(chunks[:mid], compactResponse, retry)
		if leftErr != nil {
			return uploadChunksResponse{}, leftErr
		}
		right, rightErr := c.uploadChunksBatchWithSplit(chunks[mid:], compactResponse, retry)
		if rightErr != nil {
			return uploadChunksResponse{}, rightErr
		}
		return combineUploadChunksResponses(left, right)
	}
	return uploadChunksResponse{}, err
}

func (c *agentClient) uploadChunksBatchNoSplit(chunks []*pendingBatchChunk, compactResponse bool, retry chunkBatchRetryOptions) (uploadChunksResponse, error) {
	path := "/agent/v1/chunks/upload"
	if compactResponse {
		path += "?compact_response=1"
	}
	for attempt := 1; ; attempt++ {
		body, err := newAgentChunkBatchBody(chunks)
		if err != nil {
			return uploadChunksResponse{}, err
		}
		var resp uploadChunksResponse
		status, err := c.doRawAllowStatuses(http.MethodPost, path, body, agentChunkBatchContentType, &resp, http.StatusNotFound)
		if err == nil {
			if status == http.StatusNotFound {
				legacy, err := c.uploadChunksLegacy(chunks)
				if err != nil {
					return uploadChunksResponse{}, err
				}
				legacy.RetryCount = int64(attempt - 1)
				return legacy, nil
			}
			if status != http.StatusAccepted && status != http.StatusOK {
				return uploadChunksResponse{}, fmt.Errorf("unexpected chunk batch upload status %d", status)
			}
			resp.RequestBytes = body.Size()
			resp.RetryCount = int64(attempt - 1)
			return resp, nil
		}
		if chunkBatchHTTPStatus(err) == http.StatusRequestEntityTooLarge {
			return uploadChunksResponse{}, err
		}
		if !retryableChunkBatchError(err) || attempt > retry.MaxRetries {
			return uploadChunksResponse{}, err
		}
		backoff := chunkBatchRetryBackoff(retry, attempt, err)
		logChunkBatchRetry(retry, attempt, backoff, err)
		time.Sleep(backoff)
	}
}

func combineUploadChunksResponses(left, right uploadChunksResponse) (uploadChunksResponse, error) {
	combined := left
	if combined.Status == "" {
		combined.Status = right.Status
	}
	if combined.RepositoryID == "" {
		combined.RepositoryID = right.RepositoryID
	} else if right.RepositoryID != "" && combined.RepositoryID != right.RepositoryID {
		return uploadChunksResponse{}, fmt.Errorf("split chunk upload repository_id mismatch: %s != %s", combined.RepositoryID, right.RepositoryID)
	}
	if right.ChunkGeneration > combined.ChunkGeneration {
		combined.ChunkGeneration = right.ChunkGeneration
	}
	combined.Chunks = append(append([]uploadChunkResponse(nil), left.Chunks...), right.Chunks...)
	combined.RequestBytes = left.RequestBytes + right.RequestBytes
	combined.ResponseBytes = left.ResponseBytes + right.ResponseBytes
	combined.RetryCount = left.RetryCount + right.RetryCount
	combined.SplitCount = left.SplitCount + right.SplitCount + 1
	return combined, nil
}

func retryableChunkBatchError(err error) bool {
	var httpErr *agentHTTPError
	if errors.As(err, &httpErr) {
		return httpErr.StatusCode == http.StatusTooManyRequests || (httpErr.StatusCode >= 500 && httpErr.StatusCode <= 599)
	}
	var timeoutErr interface{ Timeout() bool }
	if errors.As(err, &timeoutErr) && timeoutErr.Timeout() {
		return true
	}
	var temporaryErr interface{ Temporary() bool }
	if errors.As(err, &temporaryErr) && temporaryErr.Temporary() {
		return true
	}
	return false
}

func chunkBatchHTTPStatus(err error) int {
	var httpErr *agentHTTPError
	if errors.As(err, &httpErr) {
		return httpErr.StatusCode
	}
	return 0
}

func chunkBatchRetryBackoff(retry chunkBatchRetryOptions, attempt int, err error) time.Duration {
	var httpErr *agentHTTPError
	if errors.As(err, &httpErr) && httpErr.RetryAfterSet {
		return httpErr.RetryAfter
	}
	backoff := retry.InitialBackoff
	for i := 1; i < attempt; i++ {
		if backoff >= retry.MaxBackoff/2 {
			return retry.MaxBackoff
		}
		backoff *= 2
	}
	if backoff > retry.MaxBackoff {
		return retry.MaxBackoff
	}
	return backoff
}

func logChunkBatchRetry(retry chunkBatchRetryOptions, attempt int, backoff time.Duration, err error) {
	if retry.Logger == nil {
		return
	}
	retry.Logger.Warn("agent chunk batch retry",
		"operation", retry.Operation,
		"batch_id", retry.BatchID,
		"attempt", attempt,
		"backoff", backoff.String(),
		"last_error", err,
	)
}

func logChunkBatchSplit(retry chunkBatchRetryOptions, chunks int, err error) {
	if retry.Logger == nil {
		return
	}
	retry.Logger.Warn("agent chunk batch split",
		"operation", retry.Operation,
		"batch_id", retry.BatchID,
		"chunks", chunks,
		"split_count", 1,
		"last_error", err,
	)
}

type agentChunkBatchBody struct {
	io.Reader
	size int64
}

func (b agentChunkBatchBody) Size() int64 {
	return b.size
}

func newAgentChunkBatchBody(chunks []*pendingBatchChunk) (agentChunkBatchBody, error) {
	readers := make([]io.Reader, 0, 2+len(chunks)*3)
	readers = append(readers, bytes.NewReader(agentChunkBatchMagic))
	var count [4]byte
	binary.BigEndian.PutUint32(count[:], uint32(len(chunks)))
	readers = append(readers, bytes.NewReader(count[:]))
	size := int64(len(agentChunkBatchMagic) + len(count))
	for _, chunk := range chunks {
		hashBytes, err := hex.DecodeString(chunk.hash)
		if err != nil || len(hashBytes) != 32 {
			return agentChunkBatchBody{}, fmt.Errorf("invalid chunk hash %q", chunk.hash)
		}
		length := make([]byte, 8)
		binary.BigEndian.PutUint64(length, uint64(len(chunk.data)))
		readers = append(readers, bytes.NewReader(hashBytes), bytes.NewReader(length), bytes.NewReader(chunk.data))
		size += int64(32 + len(length) + len(chunk.data))
	}
	return agentChunkBatchBody{Reader: io.MultiReader(readers...), size: size}, nil
}

func (c *agentClient) uploadChunksLegacy(chunks []*pendingBatchChunk) (uploadChunksResponse, error) {
	resp := uploadChunksResponse{Status: "accepted", Chunks: make([]uploadChunkResponse, 0, len(chunks))}
	for _, chunk := range chunks {
		uploaded, err := c.putSingleChunk(chunk.hash, chunk.data)
		if err != nil {
			return uploadChunksResponse{}, err
		}
		resp.Chunks = append(resp.Chunks, uploaded)
	}
	return resp, nil
}

func (c *agentClient) putSingleChunk(hash string, chunk []byte) (uploadChunkResponse, error) {
	var uploaded chunkResponse
	status, err := c.doRaw(http.MethodPut, "/agent/v1/chunks/"+hash, bytes.NewReader(chunk), "application/octet-stream", &uploaded)
	if err != nil {
		return uploadChunkResponse{}, err
	}
	if status != http.StatusCreated && status != http.StatusOK {
		return uploadChunkResponse{}, fmt.Errorf("unexpected chunk upload status %d", status)
	}
	if uploaded.Ref.Hash == "" {
		return uploadChunkResponse{}, fmt.Errorf("server accepted chunk %s without ref", hash)
	}
	return uploadChunkResponse{
		Hash:     hash,
		Exists:   boolPtr(uploaded.Exists),
		Uploaded: status == http.StatusCreated && uploaded.Uploaded,
		Ref:      uploaded.Ref,
	}, nil
}

func boolPtr(value bool) *bool {
	return &value
}

func (c *agentClient) chunkInvalidations(since int64) (invalidationsResponse, error) {
	var resp invalidationsResponse
	_, err := c.doJSON(http.MethodGet, fmt.Sprintf("/agent/v1/chunks/invalidations?since=%d", since), nil, &resp)
	return resp, err
}

func (c *agentClient) submitManifest(runID int64, manifest *repository.SnapshotManifest) (submitManifestResponse, error) {
	var submitted submitManifestResponse
	status, err := c.doJSONAllowStatuses(http.MethodPost, "/agent/v1/manifests", map[string]any{
		"run_id":   runID,
		"manifest": manifest,
	}, &submitted, http.StatusConflict)
	if err != nil {
		return submitManifestResponse{}, err
	}
	if status == http.StatusConflict && submitted.Status != "missing_chunks" {
		return submitManifestResponse{}, fmt.Errorf("server returned conflict without missing_chunks status")
	}
	return submitted, nil
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

func (c *agentClient) doJSON(method, path string, requestValue any, responseValue any) (int, error) {
	return c.doJSONAllowStatuses(method, path, requestValue, responseValue)
}

func (c *agentClient) doJSONAllowStatuses(method, path string, requestValue any, responseValue any, allowedStatuses ...int) (int, error) {
	var body io.Reader
	if requestValue != nil {
		data, err := json.Marshal(requestValue)
		if err != nil {
			return 0, err
		}
		body = bytes.NewReader(data)
	}
	return c.doRawAllowStatuses(method, path, body, "application/json", responseValue, allowedStatuses...)
}

func (c *agentClient) doRaw(method, path string, body io.Reader, contentType string, responseValue any) (int, error) {
	return c.doRawAllowStatuses(method, path, body, contentType, responseValue)
}

type agentHTTPError struct {
	StatusCode    int
	Status        string
	Body          string
	RetryAfter    time.Duration
	RetryAfterSet bool
}

func (e *agentHTTPError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("server returned %s: %s", e.Status, e.Body)
}

func (c *agentClient) doRawAllowStatuses(method, path string, body io.Reader, contentType string, responseValue any, allowedStatuses ...int) (int, error) {
	req, err := http.NewRequest(method, c.serverURL+path, body)
	if err != nil {
		return 0, err
	}
	if sized, ok := body.(interface{ Size() int64 }); ok {
		req.ContentLength = sized.Size()
	}
	req.Header.Set("Accept", "application/json")
	if contentType != "" && body != nil {
		req.Header.Set("Content-Type", contentType)
	}
	if c.clientID != "" || c.clientSecret != "" {
		req.SetBasicAuth(c.clientID, c.clientSecret)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	respBody, err := readAgentResponseBody(resp.Body)
	if err != nil {
		return resp.StatusCode, err
	}
	allowed := resp.StatusCode >= 200 && resp.StatusCode < 300
	for _, status := range allowedStatuses {
		if resp.StatusCode == status {
			allowed = true
			break
		}
	}
	if !allowed {
		retryAfter, retryAfterSet := parseRetryAfter(resp.Header.Get("Retry-After"))
		return resp.StatusCode, &agentHTTPError{
			StatusCode:    resp.StatusCode,
			Status:        resp.Status,
			Body:          strings.TrimSpace(string(respBody)),
			RetryAfter:    retryAfter,
			RetryAfterSet: retryAfterSet,
		}
	}
	if responseValue != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, responseValue); err != nil {
			return resp.StatusCode, fmt.Errorf("decode response: %w", err)
		}
		if sized, ok := responseValue.(interface{ setResponseBodyBytes(int64) }); ok {
			sized.setResponseBodyBytes(int64(len(respBody)))
		}
	}
	return resp.StatusCode, nil
}

func parseRetryAfter(value string) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds < 0 {
			return 0, false
		}
		return time.Duration(seconds) * time.Second, true
	}
	when, err := http.ParseTime(value)
	if err != nil {
		return 0, false
	}
	delay := time.Until(when)
	if delay < 0 {
		delay = 0
	}
	return delay, true
}

func readAgentResponseBody(body io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(body, defaultAgentResponseBodyLimit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > defaultAgentResponseBodyLimit {
		return nil, fmt.Errorf("response body exceeds %d bytes", defaultAgentResponseBodyLimit)
	}
	return data, nil
}

func fileOwner(info fs.FileInfo) (int, int) {
	if runtime.GOOS == "windows" {
		return 0, 0
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0
	}
	return int(stat.Uid), int(stat.Gid)
}

func cleanManifestPath(path string) string {
	path = filepath.ToSlash(filepath.Clean(path))
	path = strings.TrimPrefix(path, "/")
	if path == "." || path == "" {
		return "."
	}
	return path
}
