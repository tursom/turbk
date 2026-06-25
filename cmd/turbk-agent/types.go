package main

import (
	"encoding/json"
	"os"
	"time"

	"github.com/tursom/turbk/internal/repository"
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

var (
	agentChunkBatchMagic = []byte("TBKCHB1\n")
	agentLstat           = os.Lstat
	agentOpen            = os.Open
)

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
