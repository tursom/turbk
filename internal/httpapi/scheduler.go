package httpapi

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/tursom/turbk/internal/config"
	"github.com/tursom/turbk/internal/state"
)

const schedulerInterval = 30 * time.Second

type scheduledDecision struct {
	due     bool
	retry   bool
	attempt int64
}

func (s *Server) StartScheduler(ctx context.Context) {
	ticker := time.NewTicker(schedulerInterval)
	go func() {
		defer ticker.Stop()
		s.runDueScheduledJobs(ctx, time.Now().UTC())
		s.runDueMaintenance(ctx, time.Now().UTC())
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				s.runDueScheduledJobs(ctx, now.UTC())
				s.runDueMaintenance(ctx, now.UTC())
			}
		}
	}()
}

func (s *Server) runDueScheduledJobs(ctx context.Context, now time.Time) int {
	jobs, err := s.store.ListScheduledJobs(ctx)
	if err != nil {
		s.logger.Error("list scheduled jobs", "error", err)
		return 0
	}
	scheduled := 0
	for _, job := range jobs {
		decision := s.jobDue(ctx, job, now)
		if !decision.due {
			continue
		}
		if !s.tryStartScheduledJob(ctx, job, decision) {
			continue
		}
		scheduled++
	}
	return scheduled
}

func (s *Server) runDueMaintenance(ctx context.Context, now time.Time) int {
	settings := s.currentSettings()
	cfg := settings.Maintenance
	if !cfg.Enabled {
		return 0
	}
	location, err := time.LoadLocation(cfg.Timezone)
	if err != nil {
		s.logger.Warn("invalid maintenance timezone", "timezone", cfg.Timezone, "error", err)
		location, _ = time.LoadLocation(s.cfg.Scheduler.Timezone)
		if location == nil {
			location = time.Local
		}
	}
	scheduled := 0
	if s.maintenanceDue("cleanup", cfg.CleanupSchedule, location, now) {
		scheduled++
		go func() {
			runCtx := context.Background()
			if _, err := s.runStorageMaintenanceAndRecord(runCtx, "retention"); err != nil {
				s.logger.Error("scheduled retention maintenance failed", "error", err)
				return
			}
			if _, err := s.runStorageMaintenanceAndRecord(runCtx, "cleanup-errors"); err != nil {
				s.logger.Error("scheduled cleanup maintenance failed", "error", err)
			}
		}()
	}
	if cfg.CompactEnabled && s.maintenanceDue("compact", cfg.CompactSchedule, location, now) {
		scheduled++
		go s.runScheduledCompact(context.Background(), cfg)
	}
	return scheduled
}

func (s *Server) maintenanceDue(key, schedule string, location *time.Location, now time.Time) bool {
	if !cronMatches(schedule, now.In(location)) {
		return false
	}
	localNow := now.In(location)
	minuteStart := time.Date(localNow.Year(), localNow.Month(), localNow.Day(), localNow.Hour(), localNow.Minute(), 0, 0, location).UTC()
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	if last, ok := s.lastMaintenance[key]; ok && last.Equal(minuteStart) {
		return false
	}
	s.lastMaintenance[key] = minuteStart
	return true
}

func nextCronTime(schedule string, location *time.Location, now time.Time) (time.Time, bool) {
	if location == nil {
		location = time.Local
	}
	next := now.In(location).Truncate(time.Minute).Add(time.Minute)
	for i := 0; i < 366*24*60; i++ {
		if cronMatches(schedule, next) {
			return next.UTC(), true
		}
		next = next.Add(time.Minute)
	}
	return time.Time{}, false
}

func (s *Server) runScheduledCompact(ctx context.Context, cfg config.MaintenanceConfig) {
	if skip, report, err := s.scheduledCompactSkipReport(ctx, cfg); err != nil {
		s.logger.Error("evaluate scheduled compact", "error", err)
	} else if skip {
		s.recordScheduledMaintenanceReport(ctx, "compact", report)
		return
	}
	if _, err := s.runStorageMaintenanceAndRecord(ctx, "compact"); err != nil {
		s.logger.Error("scheduled compact failed", "error", err)
	}
}

func (s *Server) scheduledCompactSkipReport(ctx context.Context, cfg config.MaintenanceConfig) (bool, maintenanceReport, error) {
	activeRuns, err := s.store.CountActiveRuns(ctx)
	if err != nil {
		return false, maintenanceReport{}, err
	}
	if activeRuns > 0 {
		return false, maintenanceReport{}, nil
	}
	started := time.Now().UTC()
	report := maintenanceReport{
		Status:  "completed",
		Mode:    "compact",
		Started: started,
	}
	settings := s.currentSettings()
	report.Retention.Policy = state.RetentionPolicy{
		KeepLast:   settings.Retention.KeepLast,
		KeepDaily:  settings.Retention.KeepDaily,
		KeepWeekly: settings.Retention.KeepWeekly,
	}
	activeSnapshots, err := s.store.ListActiveSnapshots(ctx)
	if err != nil {
		return false, maintenanceReport{}, err
	}
	report.Manifests.Active = len(activeSnapshots)
	counts, err := s.store.SnapshotCounts(ctx)
	if err != nil {
		return false, maintenanceReport{}, err
	}
	report.Retention.ActiveSnapshots = counts.Active
	report.Retention.DeletedSnapshots = counts.Deleted
	stats, err := s.repo.Stats()
	if err != nil {
		return false, maintenanceReport{}, err
	}
	report.Segment.Count = stats.Segments
	report.Segment.Bytes = stats.SegmentBytes
	report.Segment.LogicalBytes = stats.LogicalBytes
	report.Segment.CompressedBytes = stats.CompressedBytes
	report.Segment.Utilization = utilization(stats.CompressedBytes, stats.SegmentBytes)
	report.Chunks.Indexed = stats.Chunks
	referenced, referencedCompressedBytes, manifestErrors := s.referencedChunkStats(activeSnapshots)
	report.Chunks.Referenced = referenced
	if stats.Chunks > referenced {
		report.Chunks.EstimatedOrphans = stats.Chunks - referenced
	}
	report.Manifests.Errors = manifestErrors
	if len(manifestErrors) > 0 {
		report.Compact.SkippedReason = "active manifest errors exist"
		report.Finished = time.Now().UTC()
		return true, report, nil
	}
	minBytes, err := parseByteSize(cfg.CompactMinReclaimBytes)
	if err != nil {
		minBytes = 1 << 30
	}
	reclaimableBytes := stats.SegmentBytes - referencedCompressedBytes
	if reclaimableBytes < 0 {
		reclaimableBytes = 0
	}
	reclaimRatio := utilization(reclaimableBytes, stats.SegmentBytes)
	if reclaimRatio < cfg.CompactMinReclaimRatio && reclaimableBytes < minBytes {
		report.Compact.SkippedReason = "compact reclaim threshold not met"
		report.Finished = time.Now().UTC()
		return true, report, nil
	}
	return false, report, nil
}

func (s *Server) recordScheduledMaintenanceReport(ctx context.Context, mode string, report maintenanceReport) {
	data, _ := json.Marshal(report)
	if len(data) == 0 {
		data = []byte("{}")
	}
	if _, err := s.store.RecordMaintenanceRun(ctx, state.RecordMaintenanceRunInput{
		Mode:          mode,
		Status:        "skipped",
		StartedAt:     report.Started,
		FinishedAt:    report.Finished,
		SkippedReason: report.Compact.SkippedReason,
		ReportJSON:    string(data),
	}); err != nil {
		s.logger.Error("record scheduled maintenance", "mode", mode, "error", err)
	}
}

func (s *Server) jobDue(ctx context.Context, job state.Job, now time.Time) scheduledDecision {
	if !job.Schedule.Valid {
		return scheduledDecision{}
	}
	location, err := time.LoadLocation(job.Timezone)
	if err != nil {
		s.logger.Warn("invalid job timezone", "job", job.ID, "timezone", job.Timezone, "error", err)
		location, _ = time.LoadLocation(s.cfg.Scheduler.Timezone)
		if location == nil {
			location = time.Local
		}
	}
	localNow := now.In(location)
	if !cronMatches(job.Schedule.String, localNow) {
		return scheduledDecision{}
	}
	minuteStart := time.Date(localNow.Year(), localNow.Month(), localNow.Day(), localNow.Hour(), localNow.Minute(), 0, 0, location).UTC()
	summary, err := s.store.RunStatusSummarySince(ctx, job.ID, minuteStart)
	if err != nil {
		s.logger.Error("check scheduled run", "job", job.ID, "error", err)
		return scheduledDecision{}
	}
	if summary.Total == 0 {
		return scheduledDecision{due: true}
	}
	if summary.Active() > 0 || summary.Completed > 0 {
		return scheduledDecision{}
	}
	if summary.Failed > 0 && summary.Failed <= job.RetryAttempts {
		return scheduledDecision{due: true, retry: true, attempt: summary.Failed}
	}
	return scheduledDecision{}
}

func (s *Server) tryStartScheduledJob(ctx context.Context, job state.Job, decision scheduledDecision) bool {
	select {
	case s.schedulerSem <- struct{}{}:
	default:
		s.logger.Warn("scheduler concurrency limit reached", "job", job.ID)
		return false
	}
	releaseRunGate, ok := s.tryEnterBackupWrite()
	if !ok {
		<-s.schedulerSem
		s.logger.Warn("scheduled run skipped because storage maintenance is running", "job", job.ID)
		return false
	}
	go func() {
		defer func() { <-s.schedulerSem }()
		defer releaseRunGate()
		runCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		run, err := s.store.CreateRun(runCtx, job)
		if err != nil {
			s.logger.Warn("create scheduled run skipped", "job", job.ID, "error", err)
			return
		}
		logMessage := "scheduled trigger"
		if decision.retry {
			logMessage = "scheduled retry " + strconv.FormatInt(decision.attempt, 10) + "/" + strconv.FormatInt(job.RetryAttempts, 10)
		}
		_ = s.store.AppendRunLog(runCtx, run.ID, "info", logMessage)
		result, err := s.executeRun(runCtx, job, run)
		if err != nil {
			s.logger.Error("scheduled run failed", "job", job.ID, "run", run.ID, "error", err, "result", result)
			return
		}
		s.logger.Info("scheduled run completed", "job", job.ID, "run", run.ID)
	}()
	return true
}

func cronMatches(expr string, t time.Time) bool {
	expr = strings.TrimSpace(expr)
	switch expr {
	case "":
		return false
	case "@hourly":
		return t.Minute() == 0
	case "@daily", "@midnight":
		return t.Hour() == 0 && t.Minute() == 0
	case "@weekly":
		return t.Weekday() == time.Sunday && t.Hour() == 0 && t.Minute() == 0
	}
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return false
	}
	checks := []struct {
		field string
		value int
		min   int
		max   int
	}{
		{fields[0], t.Minute(), 0, 59},
		{fields[1], t.Hour(), 0, 23},
		{fields[2], t.Day(), 1, 31},
		{fields[3], int(t.Month()), 1, 12},
		{fields[4], int(t.Weekday()), 0, 7},
	}
	for _, check := range checks {
		if !cronFieldMatches(check.field, check.value, check.min, check.max) {
			return false
		}
	}
	return true
}

func validCronExpression(expr string) bool {
	expr = strings.TrimSpace(expr)
	switch expr {
	case "@hourly", "@daily", "@midnight", "@weekly":
		return true
	case "":
		return false
	}
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return false
	}
	ranges := [][2]int{{0, 59}, {0, 23}, {1, 31}, {1, 12}, {0, 7}}
	for i, field := range fields {
		if !cronFieldValid(field, ranges[i][0], ranges[i][1]) {
			return false
		}
	}
	return true
}

func cronFieldMatches(field string, value, minValue, maxValue int) bool {
	for _, part := range strings.Split(field, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			return false
		}
		if cronPartMatches(part, value, minValue, maxValue) {
			return true
		}
	}
	return false
}

func cronFieldValid(field string, minValue, maxValue int) bool {
	for _, part := range strings.Split(field, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			return false
		}
		if _, _, _, ok := parseCronPart(part, minValue, maxValue); !ok {
			return false
		}
	}
	return true
}

func cronPartMatches(part string, value, minValue, maxValue int) bool {
	start, end, step, ok := parseCronPart(part, minValue, maxValue)
	if !ok {
		return false
	}
	if minValue == 0 && maxValue == 7 && value == 0 {
		if start == 7 || end == 7 {
			value = 7
		}
	}
	if value < start || value > end {
		return false
	}
	return (value-start)%step == 0
}

func parseCronPart(part string, minValue, maxValue int) (int, int, int, bool) {
	step := 1
	if before, after, ok := strings.Cut(part, "/"); ok {
		parsedStep, err := strconv.Atoi(after)
		if err != nil || parsedStep <= 0 {
			return 0, 0, 0, false
		}
		step = parsedStep
		part = before
	}
	start, end := minValue, maxValue
	switch {
	case part == "*":
	case strings.Contains(part, "-"):
		left, right, _ := strings.Cut(part, "-")
		parsedStart, err := strconv.Atoi(left)
		if err != nil {
			return 0, 0, 0, false
		}
		parsedEnd, err := strconv.Atoi(right)
		if err != nil {
			return 0, 0, 0, false
		}
		start, end = parsedStart, parsedEnd
	default:
		parsed, err := strconv.Atoi(part)
		if err != nil {
			return 0, 0, 0, false
		}
		start, end = parsed, parsed
	}
	if start < minValue || end > maxValue || start > end {
		return 0, 0, 0, false
	}
	return start, end, step, true
}
