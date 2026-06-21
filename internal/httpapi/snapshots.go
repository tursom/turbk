package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/tursom/turbk/internal/state"
)

type deleteSnapshotResult struct {
	ID           int64           `json:"id"`
	Status       string          `json:"status"`
	Deleted      bool            `json:"deleted"`
	Snapshot     *state.Snapshot `json:"snapshot,omitempty"`
	ErrorMessage string          `json:"error,omitempty"`
}

func (s *Server) handleDeleteSnapshot(w http.ResponseWriter, r *http.Request) {
	snapshotID, err := parsePathID(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	snapshot, deleted, err := s.store.DeleteSnapshot(r.Context(), state.DeleteSnapshotInput{
		ID:        snapshotID,
		Reason:    "manual",
		DeletedBy: "admin",
		Now:       time.Now().UTC(),
	})
	if err != nil {
		status := http.StatusNotFound
		if errors.Is(err, state.ErrSnapshotInUse) {
			status = http.StatusConflict
		}
		writeError(w, status, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":   "deleted",
		"deleted":  deleted,
		"snapshot": snapshot,
		"space_reclaim": map[string]any{
			"requires_compact": true,
			"message":          "space will be reclaimed by scheduled compact",
		},
	})
}

func (s *Server) handleDeleteSnapshots(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SnapshotIDs []int64 `json:"snapshot_ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if len(req.SnapshotIDs) == 0 {
		writeError(w, http.StatusBadRequest, errors.New("snapshot_ids is required"))
		return
	}
	results := make([]deleteSnapshotResult, 0, len(req.SnapshotIDs))
	for _, id := range req.SnapshotIDs {
		result := deleteSnapshotResult{ID: id}
		if id <= 0 {
			result.Status = "error"
			result.ErrorMessage = "snapshot id is required"
			results = append(results, result)
			continue
		}
		snapshot, deleted, err := s.store.DeleteSnapshot(r.Context(), state.DeleteSnapshotInput{
			ID:        id,
			Reason:    "manual",
			DeletedBy: "admin",
			Now:       time.Now().UTC(),
		})
		if err != nil {
			result.Status = "error"
			result.ErrorMessage = err.Error()
			results = append(results, result)
			continue
		}
		result.Status = "deleted"
		result.Deleted = deleted
		result.Snapshot = &snapshot
		results = append(results, result)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "completed",
		"results": results,
		"space_reclaim": map[string]any{
			"requires_compact": true,
			"message":          "space will be reclaimed by scheduled compact",
		},
	})
}
