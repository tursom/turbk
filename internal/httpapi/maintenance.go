package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/tursom/turbk/internal/config"
	"github.com/tursom/turbk/internal/repository"
	"github.com/tursom/turbk/internal/state"
)

type maintenanceReport struct {
	Status    string    `json:"status"`
	Mode      string    `json:"mode"`
	Started   time.Time `json:"started_at"`
	Finished  time.Time `json:"finished_at"`
	Retention struct {
		Policy           state.RetentionPolicy `json:"policy"`
		ExpiredSnapshots int64                 `json:"expired_snapshots"`
		ActiveSnapshots  int64                 `json:"active_snapshots"`
		DeletedSnapshots int64                 `json:"deleted_snapshots"`
	} `json:"retention"`
	Segment struct {
		Count           int     `json:"count"`
		Bytes           int64   `json:"bytes"`
		LogicalBytes    int64   `json:"logical_bytes"`
		CompressedBytes int64   `json:"compressed_bytes"`
		Utilization     float64 `json:"utilization"`
	} `json:"segment"`
	Chunks struct {
		Indexed          int64 `json:"indexed"`
		Referenced       int64 `json:"referenced"`
		EstimatedOrphans int64 `json:"estimated_orphans"`
	} `json:"chunks"`
	Manifests struct {
		Active int      `json:"active"`
		Errors []string `json:"errors,omitempty"`
	} `json:"manifests"`
	Verify struct {
		VerifiedChunks int64    `json:"verified_chunks"`
		MissingIndex   int64    `json:"missing_index"`
		CorruptChunks  int64    `json:"corrupt_chunks"`
		Errors         []string `json:"errors,omitempty"`
	} `json:"verify"`
	Compact compactReport `json:"compact"`
	Cleanup cleanupReport `json:"cleanup"`
}

func (s *Server) handleStorageMaintenance(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Mode string `json:"mode"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.Mode == "" {
		req.Mode = "retention"
	}
	report, err := s.runStorageMaintenanceAndRecord(r.Context(), req.Mode)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, errInvalidMaintenanceMode) {
			status = http.StatusBadRequest
		}
		writeError(w, status, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) handleMaintenanceRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := s.store.ListMaintenanceRuns(r.Context(), 50)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": runs})
}

var errInvalidMaintenanceMode = errors.New("invalid maintenance mode")

func (s *Server) runStorageMaintenance(ctx context.Context, mode string) (maintenanceReport, error) {
	switch mode {
	case "retention", "compact-report", "verify", "cleanup-errors", "compact", "full-cleanup":
	default:
		return maintenanceReport{}, fmt.Errorf("%w: %s", errInvalidMaintenanceMode, mode)
	}

	started := time.Now().UTC()
	report := maintenanceReport{
		Status:  "completed",
		Mode:    mode,
		Started: started,
	}
	settings := s.currentSettings()
	retention := settings.Retention
	policy := state.RetentionPolicy{
		KeepLast:   retention.KeepLast,
		KeepDaily:  retention.KeepDaily,
		KeepWeekly: retention.KeepWeekly,
	}
	report.Retention.Policy = policy

	if mode == "compact" || mode == "full-cleanup" {
		releaseCompactGate, ok := s.tryEnterCompactMaintenance()
		if !ok {
			report.Compact.SkippedReason = "backup or chunk upload in progress"
			report.Finished = time.Now().UTC()
			return report, nil
		}
		defer releaseCompactGate()
	}

	activeSnapshots, err := s.store.ListActiveSnapshots(ctx)
	if err != nil {
		return maintenanceReport{}, err
	}
	report.Manifests.Active = len(activeSnapshots)
	if mode == "retention" || mode == "compact-report" || mode == "compact" || mode == "full-cleanup" {
		expiredIDs := state.ExpiredSnapshotIDs(activeSnapshots, policy)
		expired, err := s.store.ExpireSnapshots(ctx, expiredIDs, time.Now().UTC())
		if err != nil {
			return maintenanceReport{}, err
		}
		report.Retention.ExpiredSnapshots = expired
		if expired > 0 {
			activeSnapshots, err = s.store.ListActiveSnapshots(ctx)
			if err != nil {
				return maintenanceReport{}, err
			}
			report.Manifests.Active = len(activeSnapshots)
		}
	}
	if mode == "cleanup-errors" || mode == "compact" || mode == "full-cleanup" {
		cleanup, err := s.cleanupErrorBackupData(ctx, activeSnapshots, time.Now().UTC(), settings.Maintenance)
		if err != nil {
			return maintenanceReport{}, err
		}
		report.Cleanup = cleanup
		activeSnapshots, err = s.store.ListActiveSnapshots(ctx)
		if err != nil {
			return maintenanceReport{}, err
		}
		report.Manifests.Active = len(activeSnapshots)
	}
	if mode == "compact" || mode == "full-cleanup" {
		compact, err := s.compactRepository(ctx, activeSnapshots)
		if err != nil {
			return maintenanceReport{}, err
		}
		report.Compact = compact
		activeSnapshots, err = s.store.ListActiveSnapshots(ctx)
		if err != nil {
			return maintenanceReport{}, err
		}
		report.Manifests.Active = len(activeSnapshots)
	}
	counts, err := s.store.SnapshotCounts(ctx)
	if err != nil {
		return maintenanceReport{}, err
	}
	report.Retention.ActiveSnapshots = counts.Active
	report.Retention.DeletedSnapshots = counts.Deleted

	stats, err := s.repo.Stats()
	if err != nil {
		return maintenanceReport{}, err
	}
	report.Segment.Count = stats.Segments
	report.Segment.Bytes = stats.SegmentBytes
	report.Segment.LogicalBytes = stats.LogicalBytes
	report.Segment.CompressedBytes = stats.CompressedBytes
	report.Segment.Utilization = utilization(stats.CompressedBytes, stats.SegmentBytes)
	report.Chunks.Indexed = stats.Chunks

	referenced, _, manifestErrors := s.referencedChunkStats(activeSnapshots)
	report.Chunks.Referenced = referenced
	if stats.Chunks > referenced {
		report.Chunks.EstimatedOrphans = stats.Chunks - referenced
	}
	report.Manifests.Errors = manifestErrors
	if mode == "verify" {
		verify := s.verifyReferencedChunks(ctx, activeSnapshots)
		report.Verify.VerifiedChunks = verify.VerifiedChunks
		report.Verify.MissingIndex = verify.MissingIndex
		report.Verify.CorruptChunks = verify.CorruptChunks
		report.Verify.Errors = verify.Errors
	}
	report.Finished = time.Now().UTC()
	return report, nil
}

func (s *Server) runStorageMaintenanceAndRecord(ctx context.Context, mode string) (maintenanceReport, error) {
	started := time.Now().UTC()
	report, err := s.runStorageMaintenance(ctx, mode)
	status := "completed"
	errorMessage := ""
	skippedReason := ""
	finished := time.Now().UTC()
	reportJSON := "{}"
	if err != nil {
		status = "failed"
		errorMessage = err.Error()
	} else {
		started = report.Started
		finished = report.Finished
		skippedReason = report.Compact.SkippedReason
		if skippedReason == "" {
			skippedReason = report.Cleanup.SkippedReason
		}
		if skippedReason != "" {
			status = "skipped"
		}
		if data, marshalErr := json.Marshal(report); marshalErr == nil {
			reportJSON = string(data)
		}
	}
	if _, recordErr := s.store.RecordMaintenanceRun(ctx, state.RecordMaintenanceRunInput{
		Mode:          mode,
		Status:        status,
		StartedAt:     started,
		FinishedAt:    finished,
		SkippedReason: skippedReason,
		ReportJSON:    reportJSON,
		ErrorMessage:  errorMessage,
	}); recordErr != nil && err == nil {
		return maintenanceReport{}, recordErr
	}
	return report, err
}

type verifyReport struct {
	VerifiedChunks int64
	MissingIndex   int64
	CorruptChunks  int64
	Errors         []string
}

type compactReport struct {
	RewrittenChunks     int64  `json:"rewritten_chunks"`
	RewrittenBytes      int64  `json:"rewritten_bytes"`
	RemovedChunks       int64  `json:"removed_chunks"`
	RemovedSegments     int    `json:"removed_segments"`
	RemovedSegmentBytes int64  `json:"removed_segment_bytes"`
	SkippedReason       string `json:"skipped_reason,omitempty"`
}

type cleanupReport struct {
	StaleRunsFailed        int64    `json:"stale_runs_failed"`
	RemovedManifests       int      `json:"removed_manifests"`
	RemovedManifestBytes   int64    `json:"removed_manifest_bytes"`
	RemovedChunks          int64    `json:"removed_chunks"`
	RemovedLogicalBytes    int64    `json:"removed_logical_bytes"`
	RemovedCompressedBytes int64    `json:"removed_compressed_bytes"`
	SkippedReason          string   `json:"skipped_reason,omitempty"`
	Errors                 []string `json:"errors,omitempty"`
}

type compactManifest struct {
	snapshot state.Snapshot
	manifest *repository.SnapshotManifest
}

func (s *Server) cleanupErrorBackupData(ctx context.Context, activeSnapshots []state.Snapshot, now time.Time, cfg config.MaintenanceConfig) (cleanupReport, error) {
	var report cleanupReport
	errorGrace, err := time.ParseDuration(cfg.ErrorGracePeriod)
	if err != nil {
		errorGrace = 24 * time.Hour
	}
	staleAfter, err := time.ParseDuration(cfg.StaleRunAfter)
	if err != nil {
		staleAfter = 6 * time.Hour
	}

	staleRuns, err := s.store.MarkStaleRunsFailed(ctx, now.Add(-staleAfter), s.store.StartedAt(), now)
	if err != nil {
		return cleanupReport{}, err
	}
	report.StaleRunsFailed = staleRuns

	liveManifests, liveHashes, _, manifestErrors := s.snapshotLiveSets(activeSnapshots)
	if len(manifestErrors) > 0 {
		report.Errors = manifestErrors
		report.SkippedReason = "active manifest errors exist"
		return report, nil
	}

	keepManifests := make(map[string]struct{}, len(liveManifests))
	for id := range liveManifests {
		keepManifests[id] = struct{}{}
	}
	deletedSnapshots, err := s.store.ListDeletedSnapshots(ctx)
	if err != nil {
		return cleanupReport{}, err
	}
	var deletedMetadataCutoff time.Time
	if cfg.KeepDeletedMetadataDays > 0 {
		deletedMetadataCutoff = now.AddDate(0, 0, -cfg.KeepDeletedMetadataDays)
	}
	for _, snapshot := range deletedSnapshots {
		if snapshot.ManifestRef == "" {
			continue
		}
		if cfg.KeepDeletedMetadataDays == 0 || !snapshot.DeletedAt.Valid || snapshot.DeletedAt.Time.After(deletedMetadataCutoff) {
			keepManifests[snapshot.ManifestRef] = struct{}{}
		}
	}

	cutoff := now.Add(-errorGrace)
	removedManifests, removedManifestBytes, err := s.repo.DeleteManifestsExceptOlderThan(ctx, keepManifests, cutoff)
	if err != nil {
		return cleanupReport{}, err
	}
	report.RemovedManifests = removedManifests
	report.RemovedManifestBytes = removedManifestBytes

	chunkCleanup, err := s.repo.DeleteUnreferencedChunksOlderThan(ctx, liveHashes, cutoff)
	if err != nil {
		return cleanupReport{}, err
	}
	report.RemovedChunks = chunkCleanup.Count
	report.RemovedLogicalBytes = chunkCleanup.LogicalBytes
	report.RemovedCompressedBytes = chunkCleanup.CompressedBytes
	return report, nil
}

func (s *Server) compactRepository(ctx context.Context, snapshots []state.Snapshot) (compactReport, error) {
	var report compactReport
	activeRuns, err := s.store.CountActiveRuns(ctx)
	if err != nil {
		return compactReport{}, err
	}
	if activeRuns > 0 {
		report.SkippedReason = "active runs exist"
		return report, nil
	}

	manifests := make([]compactManifest, 0, len(snapshots))
	refsByHash := make(map[string]repository.ChunkRef)
	keepHashes := make(map[string]struct{})
	for _, snapshot := range snapshots {
		if err := ctx.Err(); err != nil {
			return compactReport{}, err
		}
		manifest, err := s.repo.ReadManifest(snapshot.ManifestRef)
		if err != nil {
			return compactReport{}, fmt.Errorf("read active snapshot %d manifest %q before compact: %w", snapshot.ID, snapshot.ManifestRef, err)
		}
		manifests = append(manifests, compactManifest{snapshot: snapshot, manifest: manifest})
		for _, entry := range manifest.Entries {
			if entry.Type != repository.EntryTypeFile {
				continue
			}
			for _, ref := range entry.Chunks {
				if ref.Hash == "" {
					continue
				}
				keepHashes[ref.Hash] = struct{}{}
				if _, exists := refsByHash[ref.Hash]; !exists {
					refsByHash[ref.Hash] = ref
				}
			}
		}
	}

	if _, err := s.repo.RotateSegmentForMaintenance(ctx); err != nil {
		return compactReport{}, err
	}
	hashes := make([]string, 0, len(refsByHash))
	for hash := range refsByHash {
		hashes = append(hashes, hash)
	}
	sort.Strings(hashes)
	newRefs := make(map[string]repository.ChunkRef, len(refsByHash))
	for _, hash := range hashes {
		if err := ctx.Err(); err != nil {
			return compactReport{}, err
		}
		oldRef := refsByHash[hash]
		newRef, err := s.repo.RewriteChunkRef(ctx, oldRef)
		if err != nil {
			return compactReport{}, fmt.Errorf("rewrite chunk %s: %w", hash, err)
		}
		newRefs[hash] = newRef
		report.RewrittenChunks++
		report.RewrittenBytes += oldRef.OriginalSize
	}

	for _, item := range manifests {
		changed := false
		for entryIndex := range item.manifest.Entries {
			entry := &item.manifest.Entries[entryIndex]
			for chunkIndex := range entry.Chunks {
				oldRef := entry.Chunks[chunkIndex]
				newRef, ok := newRefs[oldRef.Hash]
				if !ok {
					continue
				}
				if !sameChunkRefLocation(oldRef, newRef) {
					entry.Chunks[chunkIndex] = newRef
					changed = true
				}
			}
		}
		if changed {
			if err := s.repo.WriteManifest(item.manifest); err != nil {
				return compactReport{}, fmt.Errorf("write compacted manifest for snapshot %d: %w", item.snapshot.ID, err)
			}
		}
	}

	removedChunks, err := s.repo.DeleteUnreferencedChunks(ctx, keepHashes)
	if err != nil {
		return compactReport{}, err
	}
	report.RemovedChunks = removedChunks

	keepSegments := make(map[int64]struct{})
	for _, ref := range newRefs {
		keepSegments[ref.SegmentID] = struct{}{}
	}
	removedSegments, removedBytes, err := s.repo.DeleteSegmentsExcept(ctx, keepSegments)
	if err != nil {
		return compactReport{}, err
	}
	report.RemovedSegments = removedSegments
	report.RemovedSegmentBytes = removedBytes
	return report, nil
}

func (s *Server) verifyReferencedChunks(ctx context.Context, snapshots []state.Snapshot) verifyReport {
	const maxVerifyErrors = 200
	report := verifyReport{}
	seen := make(map[string]struct{})
	hashErrors := make(map[string]string)
	addError := func(format string, args ...any) {
		if len(report.Errors) < maxVerifyErrors {
			report.Errors = append(report.Errors, fmt.Sprintf(format, args...))
		}
	}

	for _, snapshot := range snapshots {
		if err := ctx.Err(); err != nil {
			addError("verification canceled: %v", err)
			return report
		}
		var snapshotErrors []string
		addSnapshotError := func(format string, args ...any) {
			message := fmt.Sprintf(format, args...)
			if len(snapshotErrors) < 5 {
				snapshotErrors = append(snapshotErrors, message)
			}
			addError("%s", message)
		}
		manifest, err := s.repo.ReadManifest(snapshot.ManifestRef)
		if err != nil {
			addSnapshotError("snapshot %d manifest %q: %v", snapshot.ID, snapshot.ManifestRef, err)
			_ = s.store.UpdateSnapshotHealth(ctx, snapshot.ID, "corrupt", strings.Join(snapshotErrors, "; "), time.Now().UTC())
			continue
		}
		for _, entry := range manifest.Entries {
			if entry.Type != repository.EntryTypeFile {
				continue
			}
			var chunkBytes int64
			for _, manifestRef := range entry.Chunks {
				if manifestRef.Hash == "" {
					report.CorruptChunks++
					addSnapshotError("snapshot %d file %q references an empty chunk hash", snapshot.ID, entry.Path)
					continue
				}
				chunkBytes += manifestRef.OriginalSize
				if _, ok := seen[manifestRef.Hash]; ok {
					if message := hashErrors[manifestRef.Hash]; message != "" {
						addSnapshotError("snapshot %d file %q references corrupt chunk %s: %s", snapshot.ID, entry.Path, manifestRef.Hash, message)
					}
					continue
				}
				seen[manifestRef.Hash] = struct{}{}
				indexRef, exists, err := s.repo.GetChunkRef(ctx, manifestRef.Hash)
				if err != nil {
					report.CorruptChunks++
					message := fmt.Sprintf("chunk %s index read failed: %v", manifestRef.Hash, err)
					hashErrors[manifestRef.Hash] = message
					addSnapshotError("%s", message)
					continue
				}
				if !exists {
					report.MissingIndex++
					message := fmt.Sprintf("chunk %s missing from index", manifestRef.Hash)
					hashErrors[manifestRef.Hash] = message
					addSnapshotError("%s", message)
					continue
				}
				if !sameChunkRefLocation(manifestRef, indexRef) {
					report.CorruptChunks++
					message := fmt.Sprintf("chunk %s manifest ref does not match index ref", manifestRef.Hash)
					hashErrors[manifestRef.Hash] = message
					addSnapshotError("%s", message)
					continue
				}
				if _, err := s.repo.ReadChunkRef(ctx, indexRef); err != nil {
					report.CorruptChunks++
					message := fmt.Sprintf("chunk %s read failed: %v", manifestRef.Hash, err)
					hashErrors[manifestRef.Hash] = message
					addSnapshotError("%s", message)
					continue
				}
				report.VerifiedChunks++
			}
			if chunkBytes != entry.Size {
				report.CorruptChunks++
				addSnapshotError("snapshot %d file %q size %d does not match chunk bytes %d", snapshot.ID, entry.Path, entry.Size, chunkBytes)
			}
		}
		health := "healthy"
		message := ""
		if len(snapshotErrors) > 0 {
			health = "corrupt"
			message = strings.Join(snapshotErrors, "; ")
		}
		if len(message) > 1000 {
			message = message[:1000]
		}
		_ = s.store.UpdateSnapshotHealth(ctx, snapshot.ID, health, message, time.Now().UTC())
	}
	return report
}

func sameChunkRefLocation(a, b repository.ChunkRef) bool {
	return a.Hash == b.Hash &&
		a.SegmentID == b.SegmentID &&
		a.Offset == b.Offset &&
		a.Length == b.Length &&
		a.OriginalSize == b.OriginalSize &&
		a.CompressedSize == b.CompressedSize
}

func (s *Server) referencedChunkStats(snapshots []state.Snapshot) (int64, int64, []string) {
	_, _, stats, errors := s.snapshotLiveSets(snapshots)
	return stats.Count, stats.CompressedBytes, errors
}

func (s *Server) snapshotLiveSets(snapshots []state.Snapshot) (map[string]struct{}, map[string]struct{}, repository.CleanupStats, []string) {
	manifestIDs := make(map[string]struct{})
	hashes := make(map[string]struct{})
	refs := make(map[string]repository.ChunkRef)
	var manifestErrors []string
	for _, snapshot := range snapshots {
		manifest, err := s.repo.ReadManifest(snapshot.ManifestRef)
		if err != nil {
			manifestErrors = append(manifestErrors, err.Error())
			continue
		}
		manifestIDs[snapshot.ManifestRef] = struct{}{}
		for _, entry := range manifest.Entries {
			for _, chunk := range entry.Chunks {
				if chunk.Hash != "" {
					hashes[chunk.Hash] = struct{}{}
					if _, ok := refs[chunk.Hash]; !ok {
						refs[chunk.Hash] = chunk
					}
				}
			}
		}
	}
	var stats repository.CleanupStats
	for _, ref := range refs {
		stats.Count++
		stats.LogicalBytes += ref.OriginalSize
		stats.CompressedBytes += ref.CompressedSize
	}
	return manifestIDs, hashes, stats, manifestErrors
}

func utilization(used, total int64) float64 {
	if total <= 0 {
		return 0
	}
	value := float64(used) / float64(total)
	return math.Round(value*10000) / 10000
}
