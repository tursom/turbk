package source

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/jlaffaye/ftp"
)

type FTPConfig struct {
	Address  string
	Username string
	Password string
	Timeout  time.Duration
	TLS      bool
	Explicit bool
	// SkipTLSVerify is intended for self-hosted FTPS servers with private or
	// self-signed certificates. Leave it false when the server has a trusted cert.
	SkipTLSVerify bool
}

type FTP struct {
	conn *ftp.ServerConn
}

func NewFTP(cfg FTPConfig) (*FTP, error) {
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}
	opts := []ftp.DialOption{ftp.DialWithTimeout(cfg.Timeout)}
	if cfg.TLS {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: cfg.SkipTLSVerify}
		if cfg.Explicit {
			opts = append(opts, ftp.DialWithExplicitTLS(tlsConfig))
		} else {
			opts = append(opts, ftp.DialWithTLS(tlsConfig))
		}
	}
	conn, err := ftp.Dial(cfg.Address, opts...)
	if err != nil {
		return nil, fmt.Errorf("dial ftp: %w", err)
	}
	if err := conn.Login(cfg.Username, cfg.Password); err != nil {
		_ = conn.Quit()
		return nil, fmt.Errorf("login ftp: %w", err)
	}
	return &FTP{conn: conn}, nil
}

func (f *FTP) Walk(ctx context.Context, root string, fn func(Entry) error) error {
	return f.walk(ctx, cleanRemotePath(root), fn)
}

func (f *FTP) walk(ctx context.Context, dir string, fn func(Entry) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	entries, err := f.conn.List(dir)
	if err != nil {
		return fmt.Errorf("list ftp %q: %w", dir, err)
	}
	for _, item := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		remotePath := path.Join(dir, item.Name)
		entry := Entry{
			Path:    remotePath,
			Size:    int64(item.Size),
			ModTime: item.Time,
		}
		switch item.Type {
		case ftp.EntryTypeFolder:
			entry.Type = EntryDir
		case ftp.EntryTypeFile:
			entry.Type = EntryFile
		default:
			continue
		}
		if err := fn(entry); err != nil {
			return err
		}
		if item.Type == ftp.EntryTypeFolder {
			if err := f.walk(ctx, remotePath, fn); err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *FTP) Open(_ context.Context, remotePath string) (io.ReadCloser, error) {
	return f.conn.Retr(cleanRemotePath(remotePath))
}

func (f *FTP) Close() error {
	if f == nil || f.conn == nil {
		return nil
	}
	return f.conn.Quit()
}

func cleanRemotePath(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || value == "." {
		return "/"
	}
	if !strings.HasPrefix(value, "/") {
		value = "/" + value
	}
	return path.Clean(value)
}
