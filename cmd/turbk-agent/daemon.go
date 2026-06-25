package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/tursom/turbk/internal/cronexpr"
	"github.com/tursom/turbk/internal/fsfilter"
	"github.com/tursom/turbk/internal/rootset"
)

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
			backupOpts := backupOptionsFromHeartbeat(heartbeat, backupRunOptions{
				Catalog:                   catalog,
				CommandID:                 command.ID,
				Trigger:                   "manual",
				MaxManifestRepairAttempts: opts.MaxManifestRepairAttempts,
			})
			err = runBackupWithOptions(client, commandRoots, logger, opts.ScanOptions, backupOpts)
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
			backupOpts := backupOptionsFromHeartbeat(heartbeat, backupRunOptions{
				Catalog:                   catalog,
				Trigger:                   "schedule",
				MaxManifestRepairAttempts: opts.MaxManifestRepairAttempts,
			})
			err := runBackupWithOptions(client, opts.Roots, logger, opts.ScanOptions, backupOpts)
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
