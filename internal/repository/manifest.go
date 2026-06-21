package repository

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type EntryType string

const (
	EntryTypeFile    EntryType = "file"
	EntryTypeDir     EntryType = "dir"
	EntryTypeSymlink EntryType = "symlink"
)

type SnapshotManifest struct {
	ID         string      `json:"id"`
	CreatedAt  time.Time   `json:"created_at"`
	SourceType string      `json:"source_type"`
	SourceRoot string      `json:"source_root"`
	Entries    []FileEntry `json:"entries"`
}

type FileEntry struct {
	Path       string     `json:"path"`
	Type       EntryType  `json:"type"`
	Size       int64      `json:"size"`
	Mode       uint32     `json:"mode"`
	UID        int        `json:"uid"`
	GID        int        `json:"gid"`
	ModTime    time.Time  `json:"mod_time"`
	LinkTarget string     `json:"link_target,omitempty"`
	Chunks     []ChunkRef `json:"chunks,omitempty"`
}

func (r *Repository) WriteManifest(manifest *SnapshotManifest) error {
	if manifest.ID == "" {
		manifest.ID = newManifestID()
	}
	if err := validateManifestID(manifest.ID); err != nil {
		return err
	}
	if manifest.CreatedAt.IsZero() {
		manifest.CreatedAt = time.Now().UTC()
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("encode manifest: %w", err)
	}
	dir := manifestsDir(r.opts.StateDir)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("create manifests dir: %w", err)
	}
	temp, err := os.CreateTemp(dir, ".manifest-*.tmp")
	if err != nil {
		return fmt.Errorf("create manifest temp file: %w", err)
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)

	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		return fmt.Errorf("write manifest temp file: %w", err)
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return fmt.Errorf("sync manifest temp file: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close manifest temp file: %w", err)
	}
	if err := os.Rename(tempPath, manifestPath(r.opts.StateDir, manifest.ID)); err != nil {
		return fmt.Errorf("commit manifest: %w", err)
	}
	return nil
}

func (r *Repository) ReadManifest(id string) (*SnapshotManifest, error) {
	if err := validateManifestID(id); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(manifestPath(r.opts.StateDir, id))
	if err != nil {
		return nil, fmt.Errorf("read manifest %q: %w", id, err)
	}
	var manifest SnapshotManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("decode manifest %q: %w", id, err)
	}
	return &manifest, nil
}

func (m *SnapshotManifest) Find(path string) (FileEntry, bool) {
	path = cleanManifestPath(path)
	for _, entry := range m.Entries {
		if entry.Path == path {
			return entry, true
		}
	}
	return FileEntry{}, false
}

func newManifestID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UTC().UnixNano())
	}
	return hex.EncodeToString(raw[:])
}

func validateManifestID(id string) error {
	if id == "" || strings.Contains(id, "/") || strings.Contains(id, "\\") || strings.Contains(id, "..") {
		return fmt.Errorf("invalid manifest id %q", id)
	}
	return nil
}

func manifestPath(stateDir, id string) string {
	return filepath.Join(manifestsDir(stateDir), id+".json")
}

func manifestsDir(stateDir string) string {
	return filepath.Join(stateDir, "manifests")
}

func countManifests(stateDir string) (int, error) {
	entries, err := os.ReadDir(manifestsDir(stateDir))
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("list manifests: %w", err)
	}
	count := 0
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			count++
		}
	}
	return count, nil
}

func (r *Repository) DeleteManifestsExceptOlderThan(ctx context.Context, keep map[string]struct{}, cutoff time.Time) (int, int64, error) {
	entries, err := os.ReadDir(manifestsDir(r.opts.StateDir))
	if os.IsNotExist(err) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, fmt.Errorf("list manifests: %w", err)
	}
	var removed int
	var removedBytes int64
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return removed, removedBytes, err
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		id := strings.TrimSuffix(entry.Name(), ".json")
		if _, ok := keep[id]; ok {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return removed, removedBytes, fmt.Errorf("stat manifest %q: %w", entry.Name(), err)
		}
		if !cutoff.IsZero() && info.ModTime().After(cutoff) {
			continue
		}
		path := filepath.Join(manifestsDir(r.opts.StateDir), entry.Name())
		if err := os.Remove(path); err != nil {
			return removed, removedBytes, fmt.Errorf("delete manifest %q: %w", id, err)
		}
		removed++
		removedBytes += info.Size()
	}
	return removed, removedBytes, nil
}

func cleanManifestPath(path string) string {
	path = filepath.ToSlash(filepath.Clean(path))
	path = strings.TrimPrefix(path, "/")
	if path == "." || path == "" {
		return "."
	}
	return path
}
