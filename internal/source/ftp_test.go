package source

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/textproto"
	"path"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestFTPWalkAndOpen(t *testing.T) {
	files := map[string]string{
		"/root/file.txt":       "ftp file body",
		"/root/sub/nested.txt": "nested ftp body",
	}
	server := startFTPTestServer(t, files)
	defer server.close()

	connector, err := NewFTP(FTPConfig{
		Address:  server.address(),
		Username: "backup",
		Password: "secret",
		Timeout:  5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close()

	var entries []Entry
	if err := connector.Walk(context.Background(), "/root", func(entry Entry) error {
		entries = append(entries, entry)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	want := map[string]EntryType{
		"/root/file.txt":       EntryFile,
		"/root/sub":            EntryDir,
		"/root/sub/nested.txt": EntryFile,
	}
	if len(entries) != len(want) {
		t.Fatalf("entries = %+v, want %d entries", entries, len(want))
	}
	for _, entry := range entries {
		if want[entry.Path] != entry.Type {
			t.Fatalf("unexpected entry %+v, want type %q", entry, want[entry.Path])
		}
	}

	body, err := readConnectorFile(connector, "/root/sub/nested.txt")
	if err != nil {
		t.Fatal(err)
	}
	if body != "nested ftp body" {
		t.Fatalf("nested body = %q", body)
	}
}

func TestExplicitFTPSWalkAndOpenWithSelfSignedCertificate(t *testing.T) {
	files := map[string]string{
		"/secure/file.txt": "explicit ftps body",
	}
	server := startFTPSTestServer(t, files)
	defer server.close()

	connector, err := NewFTP(FTPConfig{
		Address:       server.address(),
		Username:      "backup",
		Password:      "secret",
		Timeout:       5 * time.Second,
		TLS:           true,
		Explicit:      true,
		SkipTLSVerify: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close()

	var entries []Entry
	if err := connector.Walk(context.Background(), "/secure", func(entry Entry) error {
		entries = append(entries, entry)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Path != "/secure/file.txt" || entries[0].Type != EntryFile {
		t.Fatalf("unexpected FTPS entries: %+v", entries)
	}
	body, err := readConnectorFile(connector, "/secure/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if body != "explicit ftps body" {
		t.Fatalf("FTPS body = %q", body)
	}
}

type ftpTestServer struct {
	t        *testing.T
	listener net.Listener
	files    map[string]string
	tls      *tls.Config
	errCh    chan error
	done     chan struct{}
}

func startFTPTestServer(t *testing.T, files map[string]string) *ftpTestServer {
	t.Helper()
	return startFTPTestServerWithTLS(t, files, nil)
}

func startFTPTestServerWithTLS(t *testing.T, files map[string]string, tlsConfig *tls.Config) *ftpTestServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &ftpTestServer{
		t:        t,
		listener: listener,
		files:    files,
		tls:      tlsConfig,
		errCh:    make(chan error, 1),
		done:     make(chan struct{}),
	}
	go server.serve()
	return server
}

func startFTPSTestServer(t *testing.T, files map[string]string) *ftpTestServer {
	t.Helper()
	return startFTPTestServerWithTLS(t, files, newFTPTestTLSConfig(t))
}

func (s *ftpTestServer) address() string {
	return s.listener.Addr().String()
}

func (s *ftpTestServer) close() {
	_ = s.listener.Close()
	<-s.done
	select {
	case err := <-s.errCh:
		s.t.Fatal(err)
	default:
	}
}

func (s *ftpTestServer) serve() {
	defer close(s.done)
	conn, err := s.listener.Accept()
	if err != nil {
		if !isClosedNetworkError(err) {
			s.errCh <- err
		}
		return
	}
	defer conn.Close()
	proto := textproto.NewConn(conn)
	defer proto.Close()
	if err := proto.PrintfLine("220 Turbk test FTP ready"); err != nil {
		s.errCh <- err
		return
	}
	var dataListener net.Listener
	var dataTLS bool
	defer func() {
		if dataListener != nil {
			_ = dataListener.Close()
		}
	}()
	for {
		line, err := proto.ReadLine()
		if err != nil {
			if err != io.EOF && !isClosedNetworkError(err) {
				s.errCh <- err
			}
			return
		}
		command, argument := splitFTPCommand(line)
		switch command {
		case "AUTH":
			if argument != "TLS" || s.tls == nil {
				_ = proto.PrintfLine("502 unsupported auth")
				continue
			}
			if err := proto.PrintfLine("234 ready for TLS"); err != nil {
				s.errCh <- err
				return
			}
			tlsConn := tls.Server(conn, s.tls)
			if err := tlsConn.Handshake(); err != nil {
				s.errCh <- err
				return
			}
			proto = textproto.NewConn(tlsConn)
		case "USER":
			if argument != "backup" {
				_ = proto.PrintfLine("530 invalid user")
				continue
			}
			_ = proto.PrintfLine("331 password required")
		case "PASS":
			if argument != "secret" {
				_ = proto.PrintfLine("530 invalid password")
				continue
			}
			_ = proto.PrintfLine("230 logged in")
		case "FEAT":
			_ = proto.PrintfLine("211-Features:\r\n EPSV\r\n PASV\r\n MLST type*;size*;modify*;\r\n UTF8\r\n211 End")
		case "TYPE", "OPTS":
			_ = proto.PrintfLine("200 ok")
		case "PBSZ":
			_ = proto.PrintfLine("200 ok")
		case "PROT":
			dataTLS = argument == "P"
			_ = proto.PrintfLine("200 ok")
		case "EPSV":
			next, port, err := listenFTPData()
			if err != nil {
				_ = proto.PrintfLine("425 %s", err)
				continue
			}
			if dataListener != nil {
				_ = dataListener.Close()
			}
			dataListener = next
			_ = proto.PrintfLine("229 Entering Extended Passive Mode (|||%d|)", port)
		case "PASV":
			next, port, err := listenFTPData()
			if err != nil {
				_ = proto.PrintfLine("425 %s", err)
				continue
			}
			if dataListener != nil {
				_ = dataListener.Close()
			}
			dataListener = next
			_ = proto.PrintfLine("227 Entering Passive Mode (127,0,0,1,%d,%d)", port/256, port%256)
		case "MLSD":
			if err := s.sendData(proto, dataListener, s.mlsd(argument), dataTLS); err != nil {
				s.errCh <- err
				return
			}
			dataListener = nil
		case "LIST":
			if err := s.sendData(proto, dataListener, s.list(argument), dataTLS); err != nil {
				s.errCh <- err
				return
			}
			dataListener = nil
		case "RETR":
			if err := s.sendData(proto, dataListener, []byte(s.files[cleanRemotePath(argument)]), dataTLS); err != nil {
				s.errCh <- err
				return
			}
			dataListener = nil
		case "QUIT":
			_ = proto.PrintfLine("221 goodbye")
			return
		default:
			_ = proto.PrintfLine("502 unsupported command")
		}
	}
}

func (s *ftpTestServer) sendData(proto *textproto.Conn, listener net.Listener, data []byte, dataTLS bool) error {
	if listener == nil {
		_ = proto.PrintfLine("425 data connection missing")
		return nil
	}
	if err := proto.PrintfLine("150 opening data connection"); err != nil {
		return err
	}
	conn, err := listener.Accept()
	if err != nil {
		return err
	}
	if dataTLS {
		tlsConn := tls.Server(conn, s.tls)
		if err := tlsConn.Handshake(); err != nil {
			_ = conn.Close()
			return err
		}
		conn = tlsConn
	}
	if _, err := conn.Write(data); err != nil {
		_ = conn.Close()
		return err
	}
	if err := conn.Close(); err != nil {
		return err
	}
	if err := listener.Close(); err != nil && !isClosedNetworkError(err) {
		return err
	}
	return proto.PrintfLine("226 transfer complete")
}

func (s *ftpTestServer) mlsd(dir string) []byte {
	dir = cleanRemotePath(dir)
	entries := s.children(dir)
	var b strings.Builder
	for _, entry := range entries {
		if entry.kind == EntryDir {
			fmt.Fprintf(&b, "type=dir;size=0;modify=20260620100000; %s\r\n", entry.name)
		} else {
			fmt.Fprintf(&b, "type=file;size=%d;modify=20260620100000; %s\r\n", len(s.files[path.Join(dir, entry.name)]), entry.name)
		}
	}
	return []byte(b.String())
}

func (s *ftpTestServer) list(dir string) []byte {
	dir = cleanRemotePath(dir)
	entries := s.children(dir)
	var b strings.Builder
	for _, entry := range entries {
		if entry.kind == EntryDir {
			fmt.Fprintf(&b, "drwxr-xr-x 1 owner group 0 Jun 20 10:00 %s\r\n", entry.name)
		} else {
			fmt.Fprintf(&b, "-rw-r--r-- 1 owner group %d Jun 20 10:00 %s\r\n", len(s.files[path.Join(dir, entry.name)]), entry.name)
		}
	}
	return []byte(b.String())
}

func (s *ftpTestServer) children(dir string) []ftpTestEntry {
	seenDirs := make(map[string]struct{})
	var entries []ftpTestEntry
	prefix := strings.TrimRight(dir, "/") + "/"
	for filePath := range s.files {
		if !strings.HasPrefix(filePath, prefix) {
			continue
		}
		rest := strings.TrimPrefix(filePath, prefix)
		name, _, nested := strings.Cut(rest, "/")
		if nested {
			if _, ok := seenDirs[name]; ok {
				continue
			}
			seenDirs[name] = struct{}{}
			entries = append(entries, ftpTestEntry{name: name, kind: EntryDir})
			continue
		}
		entries = append(entries, ftpTestEntry{name: name, kind: EntryFile})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })
	return entries
}

type ftpTestEntry struct {
	name string
	kind EntryType
}

func listenFTPData() (net.Listener, int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, 0, err
	}
	_, portString, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		_ = listener.Close()
		return nil, 0, err
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		_ = listener.Close()
		return nil, 0, err
	}
	return listener, port, nil
}

func splitFTPCommand(line string) (string, string) {
	line = strings.TrimSpace(line)
	command, argument, ok := strings.Cut(line, " ")
	if !ok {
		return strings.ToUpper(command), ""
	}
	return strings.ToUpper(command), strings.TrimSpace(argument)
}

func newFTPTestTLSConfig(t *testing.T) *tls.Config {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "turbk-ftps-test",
		},
		NotBefore: time.Now().Add(-time.Hour),
		NotAfter:  time.Now().Add(time.Hour),
		KeyUsage:  x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
		},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{{Certificate: [][]byte{certDER}, PrivateKey: key}},
	}
}
