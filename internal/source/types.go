package source

import (
	"context"
	"io"
	"io/fs"
	"time"
)

type EntryType string

const (
	EntryFile    EntryType = "file"
	EntryDir     EntryType = "dir"
	EntrySymlink EntryType = "symlink"
)

type Entry struct {
	Path       string
	Type       EntryType
	Size       int64
	Mode       fs.FileMode
	ModTime    time.Time
	LinkTarget string
}

type Connector interface {
	Walk(ctx context.Context, root string, fn func(Entry) error) error
	Open(ctx context.Context, path string) (io.ReadCloser, error)
	Close() error
}
