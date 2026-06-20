package source

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type SFTPConfig struct {
	Address    string
	Username   string
	Password   string
	PrivateKey []byte
	Timeout    time.Duration
}

type SFTP struct {
	sshClient  *ssh.Client
	sftpClient *sftp.Client
}

func NewSFTP(cfg SFTPConfig) (*SFTP, error) {
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}
	auth := make([]ssh.AuthMethod, 0, 2)
	if cfg.Password != "" {
		auth = append(auth, ssh.Password(cfg.Password))
	}
	if len(cfg.PrivateKey) > 0 {
		signer, err := ssh.ParsePrivateKey(cfg.PrivateKey)
		if err != nil {
			return nil, fmt.Errorf("parse private key: %w", err)
		}
		auth = append(auth, ssh.PublicKeys(signer))
	}
	if len(auth) == 0 {
		return nil, fmt.Errorf("sftp auth is required")
	}
	sshConfig := &ssh.ClientConfig{
		User:            cfg.Username,
		Auth:            auth,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         cfg.Timeout,
	}
	dialer := net.Dialer{Timeout: cfg.Timeout}
	conn, err := dialer.Dial("tcp", cfg.Address)
	if err != nil {
		return nil, fmt.Errorf("dial sftp ssh: %w", err)
	}
	sshConn, chans, reqs, err := ssh.NewClientConn(conn, cfg.Address, sshConfig)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("open sftp ssh connection: %w", err)
	}
	sshClient := ssh.NewClient(sshConn, chans, reqs)
	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		_ = sshClient.Close()
		return nil, fmt.Errorf("open sftp client: %w", err)
	}
	return &SFTP{sshClient: sshClient, sftpClient: sftpClient}, nil
}

func (s *SFTP) Walk(ctx context.Context, root string, fn func(Entry) error) error {
	walker := s.sftpClient.Walk(root)
	for walker.Step() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := walker.Err(); err != nil {
			return err
		}
		info := walker.Stat()
		entry := Entry{
			Path:    filepath.ToSlash(walker.Path()),
			Size:    info.Size(),
			Mode:    info.Mode(),
			ModTime: info.ModTime(),
		}
		switch mode := info.Mode(); {
		case mode.IsDir():
			entry.Type = EntryDir
		case mode&os.ModeSymlink != 0:
			entry.Type = EntrySymlink
			if target, err := s.sftpClient.ReadLink(walker.Path()); err == nil {
				entry.LinkTarget = target
			}
		case mode.IsRegular():
			entry.Type = EntryFile
		default:
			continue
		}
		if err := fn(entry); err != nil {
			return err
		}
	}
	return nil
}

func (s *SFTP) Open(_ context.Context, path string) (io.ReadCloser, error) {
	return s.sftpClient.Open(path)
}

func (s *SFTP) Close() error {
	var err error
	if s.sftpClient != nil {
		err = s.sftpClient.Close()
	}
	if s.sshClient != nil {
		if closeErr := s.sshClient.Close(); err == nil {
			err = closeErr
		}
	}
	return err
}
