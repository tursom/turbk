package main

import (
	"encoding/json"
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/tursom/turbk/internal/fsfilter"
	"github.com/tursom/turbk/internal/rootset"
	"github.com/tursom/turbk/internal/version"
)

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
		backupOpts := backupOptionsFromHeartbeat(heartbeat, backupRunOptions{
			Catalog:                   catalog,
			Trigger:                   "once",
			MaxManifestRepairAttempts: maxManifestRepairAttempts,
		})
		if err := runBackupWithOptions(client, roots, logger, scanOptions, backupOpts); err != nil {
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
