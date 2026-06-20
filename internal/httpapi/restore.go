package httpapi

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"mime"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/tursom/turbk/internal/repository"
	"github.com/tursom/turbk/internal/state"
)

type snapshotTreeEntry struct {
	Path       string               `json:"path"`
	Name       string               `json:"name"`
	Type       repository.EntryType `json:"type"`
	Size       int64                `json:"size"`
	Mode       uint32               `json:"mode"`
	ModTime    time.Time            `json:"mod_time"`
	LinkTarget string               `json:"link_target,omitempty"`
	Synthetic  bool                 `json:"synthetic,omitempty"`
}

func (s *Server) handleSnapshotTree(w http.ResponseWriter, r *http.Request) {
	snapshot, manifest, ok := s.loadActiveSnapshotManifest(w, r)
	if !ok {
		return
	}
	queryPath, err := cleanEntryPath(r.URL.Query().Get("path"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	current, exists := manifest.Find(queryPath)
	if !exists && hasManifestDescendant(manifest, queryPath) {
		current = repository.FileEntry{Path: queryPath, Type: repository.EntryTypeDir}
		exists = true
	}
	if !exists {
		writeError(w, http.StatusNotFound, fmt.Errorf("manifest entry %q not found", queryPath))
		return
	}
	if current.Type != repository.EntryTypeDir {
		writeError(w, http.StatusBadRequest, fmt.Errorf("manifest entry %q is not a directory", queryPath))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"snapshot": snapshot,
		"manifest": map[string]any{
			"id":          manifest.ID,
			"source_type": manifest.SourceType,
			"source_root": manifest.SourceRoot,
			"created_at":  manifest.CreatedAt,
		},
		"path":    queryPath,
		"entries": childEntries(manifest, queryPath),
	})
}

func (s *Server) handleSnapshotDownload(w http.ResponseWriter, r *http.Request) {
	_, manifest, ok := s.loadActiveSnapshotManifest(w, r)
	if !ok {
		return
	}
	rawPath := r.PathValue("path")
	if rawPath == "" {
		rawPath = r.URL.Query().Get("path")
	}
	entryPath, err := cleanEntryPath(rawPath)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	entry, exists := manifest.Find(entryPath)
	if !exists {
		writeError(w, http.StatusNotFound, fmt.Errorf("manifest entry %q not found", entryPath))
		return
	}
	if entry.Type == repository.EntryTypeDir {
		filename := safeDownloadName(entryPath, "snapshot") + ".tar.gz"
		w.Header().Set("Content-Type", "application/gzip")
		w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": filename}))
		if err := s.writeDirectoryTarGz(r.Context(), w, manifest, entryPath); err != nil {
			s.logger.Error("write directory tar.gz", "snapshot", manifest.ID, "path", entryPath, "error", err)
		}
		return
	}
	if entry.Type == repository.EntryTypeSymlink {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": safeDownloadName(entryPath, "link") + ".symlink"}))
		_, _ = io.WriteString(w, entry.LinkTarget)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", entry.Size))
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": safeDownloadName(entryPath, "file")}))
	if err := s.writeFileEntry(r.Context(), w, entry); err != nil {
		s.logger.Error("write snapshot file", "snapshot", manifest.ID, "path", entryPath, "error", err)
	}
}

func (s *Server) handleRestore(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SnapshotID int64  `json:"snapshot_id"`
		Path       string `json:"path"`
		TargetPath string `json:"target_path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.SnapshotID <= 0 {
		writeError(w, http.StatusBadRequest, errors.New("snapshot_id is required"))
		return
	}
	entryPath, err := cleanEntryPath(req.Path)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	targetPath, restoreRoot, err := s.resolveRestoreTarget(req.TargetPath)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	snapshot, manifest, err := s.activeSnapshotManifestByID(r.Context(), req.SnapshotID)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	entry, exists := manifest.Find(entryPath)
	if !exists {
		writeError(w, http.StatusNotFound, fmt.Errorf("manifest entry %q not found", entryPath))
		return
	}
	task, err := s.store.CreateRestoreTask(r.Context(), state.CreateRestoreTaskInput{
		SnapshotID: snapshot.ID,
		TargetPath: targetPath,
		Status:     "running",
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if err := s.restoreEntry(r.Context(), manifest, entry, entryPath, targetPath, restoreRoot); err != nil {
		task, _ = s.store.UpdateRestoreTaskStatus(r.Context(), task.ID, "failed")
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"status": "error",
			"error":  err.Error(),
			"task":   task,
		})
		return
	}
	task, err = s.store.UpdateRestoreTaskStatus(r.Context(), task.ID, "completed")
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"status":   "completed",
		"task":     task,
		"snapshot": snapshot,
		"path":     entryPath,
	})
}

func (s *Server) handleRestoreTasks(w http.ResponseWriter, r *http.Request) {
	tasks, err := s.store.ListRestoreTasks(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tasks": tasks})
}

func (s *Server) loadActiveSnapshotManifest(w http.ResponseWriter, r *http.Request) (state.Snapshot, *repository.SnapshotManifest, bool) {
	snapshotID, err := parsePathID(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return state.Snapshot{}, nil, false
	}
	snapshot, manifest, err := s.activeSnapshotManifestByID(r.Context(), snapshotID)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return state.Snapshot{}, nil, false
	}
	return snapshot, manifest, true
}

func (s *Server) activeSnapshotManifestByID(ctx context.Context, snapshotID int64) (state.Snapshot, *repository.SnapshotManifest, error) {
	snapshot, err := s.store.GetSnapshot(ctx, snapshotID)
	if err != nil {
		return state.Snapshot{}, nil, err
	}
	if snapshot.DeletedAt.Valid {
		return state.Snapshot{}, nil, fmt.Errorf("snapshot %d not found", snapshotID)
	}
	manifest, err := s.repo.ReadManifest(snapshot.ManifestRef)
	if err != nil {
		return state.Snapshot{}, nil, err
	}
	return snapshot, manifest, nil
}

func childEntries(manifest *repository.SnapshotManifest, dir string) []snapshotTreeEntry {
	children := make(map[string]snapshotTreeEntry)
	for _, entry := range manifest.Entries {
		if entry.Path == dir {
			continue
		}
		rel, ok := manifestRelative(dir, entry.Path)
		if !ok || rel == "" {
			continue
		}
		name := rel
		if idx := strings.Index(name, "/"); idx >= 0 {
			name = name[:idx]
		}
		childPath := name
		if dir != "." {
			childPath = dir + "/" + name
		}
		if existing, ok := children[childPath]; ok && !existing.Synthetic {
			continue
		}
		if entry.Path == childPath {
			children[childPath] = treeEntryFromManifest(entry, name, false)
			continue
		}
		children[childPath] = snapshotTreeEntry{
			Path:      childPath,
			Name:      name,
			Type:      repository.EntryTypeDir,
			Synthetic: true,
		}
	}
	out := make([]snapshotTreeEntry, 0, len(children))
	for _, entry := range children {
		out = append(out, entry)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Type != out[j].Type {
			return out[i].Type == repository.EntryTypeDir
		}
		return out[i].Path < out[j].Path
	})
	return out
}

func treeEntryFromManifest(entry repository.FileEntry, name string, synthetic bool) snapshotTreeEntry {
	if name == "" {
		name = path.Base(entry.Path)
	}
	if entry.Path == "." {
		name = "."
	}
	return snapshotTreeEntry{
		Path:       entry.Path,
		Name:       name,
		Type:       entry.Type,
		Size:       entry.Size,
		Mode:       entry.Mode,
		ModTime:    entry.ModTime,
		LinkTarget: entry.LinkTarget,
		Synthetic:  synthetic,
	}
}

func hasManifestDescendant(manifest *repository.SnapshotManifest, dir string) bool {
	for _, entry := range manifest.Entries {
		if _, ok := manifestRelative(dir, entry.Path); ok && entry.Path != dir {
			return true
		}
	}
	return false
}

func manifestRelative(dir, entryPath string) (string, bool) {
	if dir == "." {
		if entryPath == "." {
			return "", true
		}
		return entryPath, true
	}
	prefix := dir + "/"
	if !strings.HasPrefix(entryPath, prefix) {
		return "", false
	}
	return strings.TrimPrefix(entryPath, prefix), true
}

func (s *Server) writeDirectoryTarGz(ctx context.Context, dst io.Writer, manifest *repository.SnapshotManifest, dir string) error {
	gz := gzip.NewWriter(dst)
	defer gz.Close()
	tw := tar.NewWriter(gz)
	defer tw.Close()
	for _, entry := range manifest.Entries {
		if entry.Path == "." && dir == "." {
			continue
		}
		if entry.Path != dir {
			if _, ok := manifestRelative(dir, entry.Path); !ok {
				continue
			}
		}
		if err := s.writeTarEntry(ctx, tw, entry); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) writeTarEntry(ctx context.Context, tw *tar.Writer, entry repository.FileEntry) error {
	name := entry.Path
	if name == "." {
		name = "snapshot"
	}
	header := &tar.Header{
		Name:    name,
		Mode:    int64(entry.Mode & 0o777),
		Size:    entry.Size,
		ModTime: entry.ModTime,
		Uid:     entry.UID,
		Gid:     entry.GID,
	}
	switch entry.Type {
	case repository.EntryTypeDir:
		header.Typeflag = tar.TypeDir
		header.Size = 0
	case repository.EntryTypeSymlink:
		header.Typeflag = tar.TypeSymlink
		header.Size = 0
		header.Linkname = entry.LinkTarget
	case repository.EntryTypeFile:
		header.Typeflag = tar.TypeReg
	default:
		return fmt.Errorf("unsupported manifest entry type %q", entry.Type)
	}
	if err := tw.WriteHeader(header); err != nil {
		return err
	}
	if entry.Type != repository.EntryTypeFile {
		return nil
	}
	return s.writeFileEntry(ctx, tw, entry)
}

func (s *Server) writeFileEntry(ctx context.Context, dst io.Writer, entry repository.FileEntry) error {
	for _, ref := range entry.Chunks {
		if err := ctx.Err(); err != nil {
			return err
		}
		data, err := s.repo.ReadChunkRef(ctx, ref)
		if err != nil {
			return err
		}
		if _, err := dst.Write(data); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) restoreEntry(ctx context.Context, manifest *repository.SnapshotManifest, entry repository.FileEntry, entryPath, targetPath, restoreRoot string) error {
	if entry.Type != repository.EntryTypeDir {
		return s.restoreSingleEntry(ctx, entry, targetPath, restoreRoot)
	}
	for _, current := range manifest.Entries {
		if current.Path != entryPath {
			if _, ok := manifestRelative(entryPath, current.Path); !ok {
				continue
			}
		}
		rel, _ := manifestRelative(entryPath, current.Path)
		target := targetPath
		if rel != "" {
			target = filepath.Join(targetPath, filepath.FromSlash(rel))
		}
		if err := s.restoreSingleEntry(ctx, current, target, restoreRoot); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) restoreSingleEntry(ctx context.Context, entry repository.FileEntry, targetPath, restoreRoot string) error {
	if err := ensureRestoreParentSafe(restoreRoot, targetPath); err != nil {
		return err
	}
	switch entry.Type {
	case repository.EntryTypeDir:
		if err := ensureRestoreTargetNotSymlink(targetPath); err != nil {
			return err
		}
		if err := os.MkdirAll(targetPath, fs.FileMode(entry.Mode)&0o777); err != nil {
			return fmt.Errorf("create restore directory %q: %w", targetPath, err)
		}
		return os.Chtimes(targetPath, entry.ModTime, entry.ModTime)
	case repository.EntryTypeSymlink:
		_ = os.Remove(targetPath)
		if err := os.Symlink(entry.LinkTarget, targetPath); err != nil {
			return fmt.Errorf("create restore symlink %q: %w", targetPath, err)
		}
		return nil
	case repository.EntryTypeFile:
		if err := ensureRestoreTargetNotDir(targetPath); err != nil {
			return err
		}
		_ = os.Remove(targetPath)
		file, err := os.OpenFile(targetPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, fs.FileMode(entry.Mode)&0o777)
		if err != nil {
			return fmt.Errorf("create restore file %q: %w", targetPath, err)
		}
		if err := s.writeFileEntry(ctx, file, entry); err != nil {
			_ = file.Close()
			return fmt.Errorf("write restore file %q: %w", targetPath, err)
		}
		if err := file.Close(); err != nil {
			return fmt.Errorf("close restore file %q: %w", targetPath, err)
		}
		return os.Chtimes(targetPath, entry.ModTime, entry.ModTime)
	default:
		return fmt.Errorf("unsupported manifest entry type %q", entry.Type)
	}
}

func (s *Server) resolveRestoreTarget(raw string) (string, string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", errors.New("target_path is required")
	}
	target, err := filepath.Abs(filepath.Clean(raw))
	if err != nil {
		return "", "", fmt.Errorf("resolve target_path: %w", err)
	}
	for _, root := range s.cfg.Paths.RestoreRoots {
		rootAbs, err := filepath.Abs(filepath.Clean(root))
		if err != nil {
			return "", "", fmt.Errorf("resolve restore root %q: %w", root, err)
		}
		rel, err := filepath.Rel(rootAbs, target)
		if err != nil {
			continue
		}
		if rel == "." || (!strings.HasPrefix(rel, ".."+string(os.PathSeparator)) && rel != "..") {
			if err := os.MkdirAll(rootAbs, 0o750); err != nil {
				return "", "", fmt.Errorf("create restore root %q: %w", rootAbs, err)
			}
			return target, rootAbs, nil
		}
	}
	return "", "", fmt.Errorf("target_path %q is outside configured restore roots", raw)
}

func ensureRestoreParentSafe(root, target string) error {
	parent := filepath.Dir(target)
	if err := ensureNoSymlinkParents(root, parent); err != nil {
		return err
	}
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return fmt.Errorf("create restore parent %q: %w", parent, err)
	}
	return nil
}

func ensureNoSymlinkParents(root, targetParent string) error {
	rel, err := filepath.Rel(root, targetParent)
	if err != nil {
		return err
	}
	if rel == "." {
		return nil
	}
	current := root
	for _, part := range strings.Split(rel, string(os.PathSeparator)) {
		if part == "" || part == "." {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("stat restore parent %q: %w", current, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("restore parent %q is a symlink", current)
		}
		if !info.IsDir() {
			return fmt.Errorf("restore parent %q is not a directory", current)
		}
	}
	return nil
}

func ensureRestoreTargetNotSymlink(target string) error {
	info, err := os.Lstat(target)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat restore target %q: %w", target, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("restore target %q is a symlink", target)
	}
	return nil
}

func ensureRestoreTargetNotDir(target string) error {
	info, err := os.Lstat(target)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat restore target %q: %w", target, err)
	}
	if info.IsDir() {
		return fmt.Errorf("restore target %q is a directory", target)
	}
	return nil
}

func cleanEntryPath(raw string) (string, error) {
	raw = strings.TrimSpace(filepath.ToSlash(raw))
	if raw == "" || raw == "." || raw == "/" {
		return ".", nil
	}
	raw = strings.TrimPrefix(raw, "/")
	for _, part := range strings.Split(raw, "/") {
		if part == ".." {
			return "", fmt.Errorf("path %q must not contain ..", raw)
		}
	}
	cleaned := path.Clean(raw)
	if cleaned == "." || cleaned == "" {
		return ".", nil
	}
	return cleaned, nil
}

func safeDownloadName(entryPath, fallback string) string {
	if entryPath == "." || entryPath == "" {
		return fallback
	}
	base := path.Base(entryPath)
	base = strings.Map(func(r rune) rune {
		switch r {
		case '/', '\\', ':', 0:
			return '-'
		default:
			return r
		}
	}, base)
	if base == "." || base == "" {
		return fallback
	}
	return base
}
