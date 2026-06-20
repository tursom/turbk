package source

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

type Local struct{}

func NewLocal() *Local {
	return &Local{}
}

func (l *Local) Walk(ctx context.Context, root string, fn func(Entry) error) error {
	root = filepath.Clean(root)
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		info, err := os.Lstat(path)
		if err != nil {
			return fmt.Errorf("stat local path %q: %w", path, err)
		}
		entry := Entry{
			Path:    filepath.ToSlash(filepath.Clean(path)),
			Size:    info.Size(),
			Mode:    info.Mode(),
			ModTime: info.ModTime(),
		}
		switch mode := info.Mode(); {
		case mode.IsDir():
			entry.Type = EntryDir
		case mode&os.ModeSymlink != 0:
			entry.Type = EntrySymlink
			target, err := os.Readlink(path)
			if err != nil {
				return fmt.Errorf("read local symlink %q: %w", path, err)
			}
			entry.LinkTarget = target
		case mode.IsRegular():
			entry.Type = EntryFile
		default:
			return nil
		}
		return fn(entry)
	})
}

func (l *Local) Open(_ context.Context, path string) (io.ReadCloser, error) {
	return os.Open(path)
}

func (l *Local) Close() error {
	return nil
}
