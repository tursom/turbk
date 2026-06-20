package source

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	pkgsftp "github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

func TestSFTPWalkAndOpen(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	if err := os.MkdirAll(filepath.Join(dataDir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "file.txt"), []byte("sftp file body"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "sub", "nested.txt"), []byte("nested sftp body"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("file.txt", filepath.Join(dataDir, "file-link")); err != nil {
		t.Fatal(err)
	}

	address, stop := startSFTPTestServer(t, root, "backup", "secret")
	defer stop()

	connector, err := NewSFTP(SFTPConfig{
		Address:  address,
		Username: "backup",
		Password: "secret",
		Timeout:  5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close()

	var entries []Entry
	if err := connector.Walk(context.Background(), "data", func(entry Entry) error {
		entries = append(entries, entry)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	want := map[string]EntryType{
		"data":                EntryDir,
		"data/file-link":      EntrySymlink,
		"data/file.txt":       EntryFile,
		"data/sub":            EntryDir,
		"data/sub/nested.txt": EntryFile,
	}
	if len(entries) != len(want) {
		t.Fatalf("entries = %+v, want %d entries", entries, len(want))
	}
	for _, entry := range entries {
		if want[entry.Path] != entry.Type {
			t.Fatalf("unexpected entry %+v, want type %q", entry, want[entry.Path])
		}
		if entry.Path == "data/file-link" && entry.LinkTarget != "file.txt" {
			t.Fatalf("symlink target = %q, want file.txt", entry.LinkTarget)
		}
	}

	body, err := readConnectorFile(connector, "data/sub/nested.txt")
	if err != nil {
		t.Fatal(err)
	}
	if body != "nested sftp body" {
		t.Fatalf("nested body = %q", body)
	}
}

func startSFTPTestServer(t *testing.T, root, username, password string) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hostKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(hostKey)
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	serverConfig := &ssh.ServerConfig{
		PasswordCallback: func(conn ssh.ConnMetadata, pass []byte) (*ssh.Permissions, error) {
			if conn.User() == username && string(pass) == password {
				return nil, nil
			}
			return nil, fmt.Errorf("unauthorized")
		},
	}
	serverConfig.AddHostKey(signer)
	errCh := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				if !isClosedNetworkError(err) {
					errCh <- err
				}
				return
			}
			go serveSFTPSSHConn(conn, serverConfig, root, errCh)
		}
	}()
	stop := func() {
		_ = listener.Close()
		<-done
		select {
		case err := <-errCh:
			t.Fatal(err)
		default:
		}
	}
	return listener.Addr().String(), stop
}

func serveSFTPSSHConn(conn net.Conn, config *ssh.ServerConfig, root string, errCh chan<- error) {
	sshConn, chans, reqs, err := ssh.NewServerConn(conn, config)
	if err != nil {
		_ = conn.Close()
		return
	}
	defer sshConn.Close()
	go ssh.DiscardRequests(reqs)
	for channel := range chans {
		if channel.ChannelType() != "session" {
			_ = channel.Reject(ssh.UnknownChannelType, "session required")
			continue
		}
		stream, requests, err := channel.Accept()
		if err != nil {
			errCh <- err
			return
		}
		go func() {
			accepted := false
			for req := range requests {
				ok := false
				if req.Type == "subsystem" && len(req.Payload) >= 4 && string(req.Payload[4:]) == "sftp" {
					ok = true
					accepted = true
				}
				_ = req.Reply(ok, nil)
			}
			if !accepted {
				_ = stream.Close()
			}
		}()
		server, err := pkgsftp.NewServer(stream, pkgsftp.ReadOnly(), pkgsftp.WithServerWorkingDirectory(root))
		if err != nil {
			errCh <- err
			_ = stream.Close()
			continue
		}
		if err := server.Serve(); err != nil && err != io.EOF {
			errCh <- err
		}
		_ = server.Close()
	}
}

func isClosedNetworkError(err error) bool {
	return err != nil && (errors.Is(err, net.ErrClosed) || os.IsTimeout(err) || strings.Contains(err.Error(), "use of closed network connection"))
}
