package repository

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/tursom/turbk/internal/fsfilter"
	"github.com/tursom/turbk/internal/source"
)

type BackupProgress struct {
	Phase          string
	ProcessedFiles int64
	ProcessedBytes int64
	UploadedChunks int64
	ReusedChunks   int64
	Message        string
}

type BackupProgressFunc func(BackupProgress)

func (r *Repository) BackupLocalTree(ctx context.Context, snapshotID, sourceType, root string) (*SnapshotManifest, error) {
	return r.BackupLocalTreeIncremental(ctx, snapshotID, sourceType, root, nil, nil)
}

func (r *Repository) BackupLocalTreeIncremental(ctx context.Context, snapshotID, sourceType, root string, previous *SnapshotManifest, progressFn BackupProgressFunc) (*SnapshotManifest, error) {
	root = filepath.Clean(root)
	if fsName, ok, err := fsfilter.PseudoFilesystemName(root); err == nil && ok {
		return nil, fmt.Errorf("root %q is on unsupported pseudo filesystem %s", root, fsName)
	}
	manifest := &SnapshotManifest{
		ID:         snapshotID,
		CreatedAt:  time.Now().UTC(),
		SourceType: sourceType,
		SourceRoot: root,
	}
	chunker := r.Chunker()
	previousEntries := manifestEntryMap(previous)
	var progress BackupProgress
	progress.Phase = "scanning"
	reportBackupProgress(progressFn, progress)

	scanOptions := fsfilter.Options{SkipPseudoFilesystems: true}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		info, err := os.Lstat(path)
		if err != nil {
			return fmt.Errorf("stat %q: %w", path, err)
		}
		if event, skip := fsfilter.ShouldSkip(root, path, info, scanOptions); skip {
			progress.Message = "skipped " + event.Rel + ": " + event.Reason
			reportBackupProgress(progressFn, progress)
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return fmt.Errorf("rel path %q: %w", path, err)
		}
		entry := FileEntry{
			Path:    cleanManifestPath(rel),
			Size:    info.Size(),
			Mode:    uint32(info.Mode()),
			ModTime: info.ModTime().UTC(),
		}
		entry.UID, entry.GID = fileOwner(info)

		mode := info.Mode()
		switch {
		case mode.IsDir():
			entry.Type = EntryTypeDir
		case mode&os.ModeSymlink != 0:
			entry.Type = EntryTypeSymlink
			target, err := os.Readlink(path)
			if err != nil {
				return fmt.Errorf("read symlink %q: %w", path, err)
			}
			entry.LinkTarget = target
		case mode.IsRegular():
			entry.Type = EntryTypeFile
			if previousEntry, ok := previousEntries[entry.Path]; ok && localFileEntryReusable(entry, previousEntry) {
				entry.Chunks = cloneChunkRefs(previousEntry.Chunks)
				progress.ProcessedFiles++
				progress.ProcessedBytes += entry.Size
				progress.ReusedChunks += int64(len(entry.Chunks))
				progress.Message = entry.Path
				reportBackupProgress(progressFn, progress)
				manifest.Entries = append(manifest.Entries, entry)
				return nil
			}
			file, err := os.Open(path)
			if err != nil {
				return fmt.Errorf("open file %q: %w", path, err)
			}
			if err := chunker.Split(file, func(chunk []byte) error {
				ref, existed, err := r.PutChunk(ctx, chunk)
				if err != nil {
					_ = file.Close()
					return err
				}
				if existed {
					progress.ReusedChunks++
				} else {
					progress.UploadedChunks++
				}
				entry.Chunks = append(entry.Chunks, ref)
				return nil
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
			reportBackupProgress(progressFn, progress)
		default:
			return nil
		}
		manifest.Entries = append(manifest.Entries, entry)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := r.WriteManifest(manifest); err != nil {
		return nil, err
	}
	progress.Phase = "manifest"
	progress.Message = manifest.ID
	reportBackupProgress(progressFn, progress)
	return manifest, nil
}

func (r *Repository) BackupFromSource(ctx context.Context, snapshotID, sourceType, root string, connector source.Connector) (*SnapshotManifest, error) {
	return r.BackupFromSourceIncremental(ctx, snapshotID, sourceType, root, connector, nil, nil)
}

func (r *Repository) BackupFromSourceIncremental(ctx context.Context, snapshotID, sourceType, root string, connector source.Connector, previous *SnapshotManifest, progressFn BackupProgressFunc) (*SnapshotManifest, error) {
	root = cleanSourceRoot(root)
	manifest := &SnapshotManifest{
		ID:         snapshotID,
		CreatedAt:  time.Now().UTC(),
		SourceType: sourceType,
		SourceRoot: root,
	}
	chunker := r.Chunker()
	previousEntries := manifestEntryMap(previous)
	var progress BackupProgress
	progress.Phase = "scanning"
	reportBackupProgress(progressFn, progress)
	err := connector.Walk(ctx, root, func(sourceEntry source.Entry) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		entry := FileEntry{
			Path:       cleanManifestPath(sourceRel(root, sourceEntry.Path)),
			Size:       sourceEntry.Size,
			Mode:       uint32(sourceEntry.Mode),
			ModTime:    sourceEntry.ModTime.UTC(),
			LinkTarget: sourceEntry.LinkTarget,
		}
		switch sourceEntry.Type {
		case source.EntryDir:
			entry.Type = EntryTypeDir
		case source.EntrySymlink:
			entry.Type = EntryTypeSymlink
		case source.EntryFile:
			entry.Type = EntryTypeFile
			if previousEntry, ok := previousEntries[entry.Path]; ok && sourceFileEntryReusable(entry, previousEntry) {
				entry.Chunks = cloneChunkRefs(previousEntry.Chunks)
				progress.ProcessedFiles++
				progress.ProcessedBytes += entry.Size
				progress.ReusedChunks += int64(len(entry.Chunks))
				progress.Message = entry.Path
				reportBackupProgress(progressFn, progress)
				manifest.Entries = append(manifest.Entries, entry)
				return nil
			}
			reader, err := connector.Open(ctx, sourceEntry.Path)
			if err != nil {
				return fmt.Errorf("open source file %q: %w", sourceEntry.Path, err)
			}
			if err := chunker.Split(reader, func(chunk []byte) error {
				ref, existed, err := r.PutChunk(ctx, chunk)
				if err != nil {
					_ = reader.Close()
					return err
				}
				if existed {
					progress.ReusedChunks++
				} else {
					progress.UploadedChunks++
				}
				entry.Chunks = append(entry.Chunks, ref)
				return nil
			}); err != nil {
				_ = reader.Close()
				return fmt.Errorf("chunk source file %q: %w", sourceEntry.Path, err)
			}
			if err := reader.Close(); err != nil {
				return fmt.Errorf("close source file %q: %w", sourceEntry.Path, err)
			}
			progress.ProcessedFiles++
			progress.ProcessedBytes += entry.Size
			progress.Message = entry.Path
			reportBackupProgress(progressFn, progress)
		default:
			return nil
		}
		manifest.Entries = append(manifest.Entries, entry)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := r.WriteManifest(manifest); err != nil {
		return nil, err
	}
	progress.Phase = "manifest"
	progress.Message = manifest.ID
	reportBackupProgress(progressFn, progress)
	return manifest, nil
}

func reportBackupProgress(fn BackupProgressFunc, progress BackupProgress) {
	if fn != nil {
		fn(progress)
	}
}

func manifestEntryMap(manifest *SnapshotManifest) map[string]FileEntry {
	if manifest == nil {
		return nil
	}
	entries := make(map[string]FileEntry, len(manifest.Entries))
	for _, entry := range manifest.Entries {
		entries[entry.Path] = entry
	}
	return entries
}

func localFileEntryReusable(current, previous FileEntry) bool {
	if !sourceFileEntryReusable(current, previous) {
		return false
	}
	return current.UID == previous.UID && current.GID == previous.GID
}

func sourceFileEntryReusable(current, previous FileEntry) bool {
	if current.Type != EntryTypeFile || previous.Type != EntryTypeFile {
		return false
	}
	if current.Size != previous.Size || current.Mode != previous.Mode {
		return false
	}
	if !current.ModTime.Equal(previous.ModTime) {
		return false
	}
	if current.Size > 0 && len(previous.Chunks) == 0 {
		return false
	}
	return true
}

func cloneChunkRefs(chunks []ChunkRef) []ChunkRef {
	if len(chunks) == 0 {
		return nil
	}
	cloned := make([]ChunkRef, len(chunks))
	copy(cloned, chunks)
	return cloned
}

func (r *Repository) RestoreFile(ctx context.Context, snapshotID, manifestEntryPath, targetPath string) error {
	manifest, err := r.ReadManifest(snapshotID)
	if err != nil {
		return err
	}
	entry, ok := manifest.Find(manifestEntryPath)
	if !ok {
		return fmt.Errorf("manifest entry %q not found", manifestEntryPath)
	}
	if entry.Type == EntryTypeDir {
		return fmt.Errorf("manifest entry %q is a directory", manifestEntryPath)
	}
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o750); err != nil {
		return fmt.Errorf("create restore parent: %w", err)
	}
	if entry.Type == EntryTypeSymlink {
		_ = os.Remove(targetPath)
		return os.Symlink(entry.LinkTarget, targetPath)
	}
	file, err := os.OpenFile(targetPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, fs.FileMode(entry.Mode)&0o777)
	if err != nil {
		return fmt.Errorf("create restore target: %w", err)
	}
	for _, ref := range entry.Chunks {
		if err := ctx.Err(); err != nil {
			_ = file.Close()
			return err
		}
		data, err := r.ReadChunkRef(ctx, ref)
		if err != nil {
			_ = file.Close()
			return err
		}
		if _, err := file.Write(data); err != nil {
			_ = file.Close()
			return fmt.Errorf("write restore target: %w", err)
		}
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close restore target: %w", err)
	}
	_ = os.Chtimes(targetPath, entry.ModTime, entry.ModTime)
	return nil
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

func cleanSourceRoot(root string) string {
	root = strings.TrimSpace(root)
	if root == "" {
		return "."
	}
	if strings.Contains(root, "\\") {
		return filepath.ToSlash(filepath.Clean(root))
	}
	return path.Clean(root)
}

func sourceRel(root, entryPath string) string {
	root = cleanSourceRoot(root)
	entryPath = cleanSourceRoot(entryPath)
	if entryPath == root {
		return "."
	}
	prefix := strings.TrimRight(root, "/") + "/"
	if strings.HasPrefix(entryPath, prefix) {
		return strings.TrimPrefix(entryPath, prefix)
	}
	return strings.TrimPrefix(entryPath, "/")
}
