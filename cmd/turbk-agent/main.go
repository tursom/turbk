package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
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
	ID        int64     `json:"id"`
	Type      string    `json:"type"`
	JobID     int64     `json:"job_id"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

type heartbeatRequest struct {
	Hostname          string `json:"hostname"`
	Version           string `json:"version"`
	Mode              string `json:"mode,omitempty"`
	StateDir          string `json:"state_dir,omitempty"`
	CatalogStatus     string `json:"catalog_status,omitempty"`
	RepositoryID      string `json:"repository_id,omitempty"`
	ChunkGeneration   int64  `json:"chunk_generation,omitempty"`
	ConfigGeneration  int64  `json:"config_generation,omitempty"`
	CommandGeneration int64  `json:"command_generation,omitempty"`
	RunningRunID      int64  `json:"running_run_id,omitempty"`
	LastError         string `json:"last_error,omitempty"`
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
	ConfigGeneration          int64  `json:"config_generation"`
	CommandGeneration         int64  `json:"command_generation"`
	PollIntervalSeconds       int64  `json:"poll_interval_seconds"`
	DefaultPollInterval       string `json:"default_poll_interval"`
	MaxChunkCheckBatch        int    `json:"max_chunk_check_batch"`
	MaxManifestRepairAttempts int    `json:"max_manifest_repair_attempts"`
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
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
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
			Catalog:                   catalog,
			RepositoryID:              heartbeat.Repository.ID,
			ChunkGeneration:           heartbeat.Repository.ChunkGeneration,
			Trigger:                   "once",
			MaxManifestRepairAttempts: maxManifestRepairAttempts,
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
		http:         &http.Client{Timeout: 60 * time.Second},
	}
}

func (c *agentClient) sendHeartbeat() error {
	_, err := c.sendHeartbeatWithState(nil, "", "once", 0, "")
	return err
}

func (c *agentClient) sendHeartbeatWithState(catalog *agentCatalog, stateDir, mode string, runningRunID int64, lastError string) (heartbeatResponse, error) {
	hostname, _ := os.Hostname()
	req := heartbeatRequest{
		Hostname:     hostname,
		Version:      version.Version,
		Mode:         mode,
		StateDir:     stateDir,
		RunningRunID: runningRunID,
		LastError:    lastError,
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
			if len(opts.Roots) == 0 {
				if err := client.ackCommand(command.ID, "dropped", "root_not_configured"); err != nil {
					logger.Warn("agent root-not-configured command drop failed", "command", command.ID, "error", err)
				}
				continue
			}
			running = true
			backupCommandHandled = true
			lastBackupStarted = time.Now().UTC()
			err := runBackupWithOptions(client, opts.Roots, logger, opts.ScanOptions, backupRunOptions{
				Catalog:                   catalog,
				RepositoryID:              heartbeat.Repository.ID,
				ChunkGeneration:           heartbeat.Repository.ChunkGeneration,
				CommandID:                 command.ID,
				Trigger:                   "manual",
				MaxManifestRepairAttempts: opts.MaxManifestRepairAttempts,
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
				Catalog:                   catalog,
				RepositoryID:              heartbeat.Repository.ID,
				ChunkGeneration:           heartbeat.Repository.ChunkGeneration,
				Trigger:                   "schedule",
				MaxManifestRepairAttempts: opts.MaxManifestRepairAttempts,
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
	Catalog                   *agentCatalog
	RepositoryID              string
	ChunkGeneration           int64
	CommandID                 int64
	Trigger                   string
	MaxManifestRepairAttempts int
}

func runBackup(client *agentClient, root string, logger *slog.Logger, scanOptions fsfilter.Options) error {
	return runBackupWithOptions(client, []string{root}, logger, scanOptions, backupRunOptions{Trigger: "once", MaxManifestRepairAttempts: 3})
}

func runBackupWithOptions(client *agentClient, roots []string, logger *slog.Logger, scanOptions fsfilter.Options, opts backupRunOptions) error {
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
		if err := opts.Catalog.recordRunStart(localRunID, created.Run.ID, opts.CommandID, opts.Trigger, time.Now().UTC()); err != nil {
			logger.Warn("agent catalog run start record failed", "run", created.Run.ID, "error", err)
		}
		defer func() {
			if err := opts.Catalog.recordRunFinish(localRunID, runStatus, runMessage, time.Now().UTC()); err != nil {
				logger.Warn("agent catalog run finish record failed", "run", created.Run.ID, "error", err)
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
			for _, missing := range submitted.MissingChunks {
				_ = opts.Catalog.markChunk(missing.Hash, 0, "missing", 0, false)
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

	multiRoot := len(roots) > 1
	entryPaths := make(map[string]struct{})
	for _, root := range roots {
		progress.Message = root
		if err := sendProgress(true); err != nil {
			return nil, err
		}
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			info, err := os.Lstat(path)
			if err != nil {
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
				if opts.Catalog != nil {
					_ = opts.Catalog.replaceFile(record, nil)
				}
			case mode&os.ModeSymlink != 0:
				entry.Type = repository.EntryTypeSymlink
				target, err := os.Readlink(path)
				if err != nil {
					return fmt.Errorf("read symlink %q: %w", path, err)
				}
				entry.LinkTarget = target
				record.Type = string(repository.EntryTypeSymlink)
				record.LinkTarget = target
				if opts.Catalog != nil {
					_ = opts.Catalog.replaceFile(record, nil)
				}
			case mode.IsRegular():
				entry.Type = repository.EntryTypeFile
				record.Type = string(repository.EntryTypeFile)
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
				file, err := os.Open(path)
				if err != nil {
					return fmt.Errorf("open file %q: %w", path, err)
				}
				fileChunks := make([]catalogChunkRecord, 0)
				if err := chunker.Split(file, func(chunk []byte) error {
					ref, uploaded, err := c.ensureChunk(chunk, opts)
					if err != nil {
						_ = file.Close()
						return err
					}
					if uploaded {
						progress.UploadedChunks++
					} else {
						progress.ReusedChunks++
					}
					entry.Chunks = append(entry.Chunks, ref)
					fileChunks = append(fileChunks, catalogChunkRecord{Hash: ref.Hash, OriginalSize: ref.OriginalSize})
					return sendProgress(false)
				}); err != nil {
					_ = file.Close()
					return fmt.Errorf("chunk file %q: %w", path, err)
				}
				if err := file.Close(); err != nil {
					return fmt.Errorf("close file %q: %w", path, err)
				}
				progress.ProcessedFiles++
				progress.ProcessedBytes += entry.Size
				progress.Message = entry.Path
				if opts.Catalog != nil {
					if err := opts.Catalog.replaceFile(record, fileChunks); err != nil {
						logger.Warn("agent catalog file update failed", "path", entry.Path, "error", err)
					}
				}
				if err := sendProgress(true); err != nil {
					return err
				}
			default:
				return nil
			}
			return appendManifestEntry(manifest, entryPaths, entry)
		})
		if err != nil {
			return nil, err
		}
	}
	progress.Phase = "manifest"
	progress.Message = "manifest ready"
	if err := sendProgress(true); err != nil {
		return nil, err
	}
	logger.Info("agent scan complete", "files", progress.ProcessedFiles, "uploaded_chunks", progress.UploadedChunks, "reused_chunks", progress.ReusedChunks, "skipped_paths", skipReporter.total)
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
	existing, ok, err := catalog.fileRecord(record.RootID, record.Path)
	if err != nil || !ok {
		return false, err
	}
	if !catalogFileMatches(existing, record) {
		return false, nil
	}
	chunks, err := catalog.fileChunks(record.RootID, record.Path)
	if err != nil {
		return false, err
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
	checked, err := c.checkChunks(stale, opts.RepositoryID, opts.ChunkGeneration)
	if err != nil {
		return false, err
	}
	missing := make(map[string]struct{}, len(checked.Missing))
	for _, hash := range checked.Exists {
		if err := catalog.markChunk(hash, originalSizes[hash], "confirmed", checked.ChunkGeneration, false); err != nil {
			return false, err
		}
	}
	for _, hash := range checked.Missing {
		missing[hash] = struct{}{}
		if err := catalog.markChunk(hash, originalSizes[hash], "missing", 0, false); err != nil {
			return false, err
		}
	}
	return len(missing) == 0, nil
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

func (c *agentClient) checkChunks(hashes []string, repositoryID string, baseGeneration int64) (checkChunksResponse, error) {
	var resp checkChunksResponse
	_, err := c.doJSON(http.MethodPost, "/agent/v1/chunks/check", map[string]any{
		"repository_id":         repositoryID,
		"base_chunk_generation": baseGeneration,
		"hashes":                hashes,
	}, &resp)
	return resp, err
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

func (c *agentClient) doRawAllowStatuses(method, path string, body io.Reader, contentType string, responseValue any, allowedStatuses ...int) (int, error) {
	req, err := http.NewRequest(method, c.serverURL+path, body)
	if err != nil {
		return 0, err
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
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
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
		return resp.StatusCode, fmt.Errorf("server returned %s: %s", resp.Status, strings.TrimSpace(string(respBody)))
	}
	if responseValue != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, responseValue); err != nil {
			return resp.StatusCode, fmt.Errorf("decode response: %w", err)
		}
	}
	return resp.StatusCode, nil
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
