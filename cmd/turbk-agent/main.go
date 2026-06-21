package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/tursom/turbk/internal/repository"
	"github.com/tursom/turbk/internal/version"
	"github.com/zeebo/blake3"
)

const agentChunkAvgSize = 1024 * 1024

type agentClient struct {
	serverURL    string
	clientID     string
	clientSecret string
	http         *http.Client
}

type runRef struct {
	ID int64 `json:"id"`
}

type createRunResponse struct {
	Status string `json:"status"`
	Run    runRef `json:"run"`
}

type chunkResponse struct {
	Exists   bool                `json:"exists"`
	Uploaded bool                `json:"uploaded"`
	Ref      repository.ChunkRef `json:"ref"`
}

type submitManifestResponse struct {
	Status string `json:"status"`
	Run    runRef `json:"run"`
}

type agentProgress struct {
	Phase          string `json:"phase"`
	TotalFiles     int64  `json:"total_files"`
	ProcessedFiles int64  `json:"processed_files"`
	TotalBytes     int64  `json:"total_bytes"`
	ProcessedBytes int64  `json:"processed_bytes"`
	UploadedChunks int64  `json:"uploaded_chunks"`
	ReusedChunks   int64  `json:"reused_chunks"`
	Message        string `json:"message"`
}

func main() {
	var serverURL string
	var clientID string
	var clientSecret string
	var root string
	var once bool
	flag.StringVar(&serverURL, "server", os.Getenv("TURBK_SERVER_URL"), "Turbk server URL")
	flag.StringVar(&clientID, "client-id", os.Getenv("TURBK_AGENT_ID"), "Agent client ID")
	flag.StringVar(&clientSecret, "client-secret", os.Getenv("TURBK_AGENT_SECRET"), "Agent client secret")
	flag.StringVar(&root, "root", os.Getenv("TURBK_AGENT_ROOT"), "Root directory to back up")
	flag.BoolVar(&once, "once", false, "Send one heartbeat or run one backup and exit")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if serverURL == "" {
		printReady()
		return
	}
	if clientID == "" || clientSecret == "" {
		logger.Error("agent client credentials are required", "client_id_set", clientID != "", "client_secret_set", clientSecret != "")
		os.Exit(1)
	}
	client := newAgentClient(serverURL, clientID, clientSecret)
	if root != "" {
		if err := client.sendHeartbeat(); err != nil {
			logger.Error("agent heartbeat failed", "error", err)
			os.Exit(1)
		}
		if err := runBackup(client, root, logger); err != nil {
			logger.Error("agent backup failed", "error", err)
			os.Exit(1)
		}
		return
	}
	if err := client.sendHeartbeat(); err != nil {
		logger.Error("agent heartbeat failed", "error", err)
		os.Exit(1)
	}
	logger.Info("agent heartbeat accepted", "server", serverURL)
	if once {
		return
	}
	logger.Info("agent idle; pass -root to run a backup")
}

func printReady() {
	hostname, _ := os.Hostname()
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
		"name":     "turbk-agent",
		"version":  version.Version,
		"hostname": hostname,
		"status":   "ready",
	})
}

func newAgentClient(serverURL, clientID, clientSecret string) *agentClient {
	return &agentClient{
		serverURL:    strings.TrimRight(serverURL, "/"),
		clientID:     clientID,
		clientSecret: clientSecret,
		http:         &http.Client{Timeout: 60 * time.Second},
	}
}

func (c *agentClient) sendHeartbeat() error {
	hostname, _ := os.Hostname()
	var resp map[string]any
	_, err := c.doJSON(http.MethodPost, "/agent/v1/heartbeat", map[string]any{
		"hostname": hostname,
		"version":  version.Version,
	}, &resp)
	return err
}

func runBackup(client *agentClient, root string, logger *slog.Logger) error {
	root = filepath.Clean(root)
	info, err := os.Lstat(root)
	if err != nil {
		return fmt.Errorf("stat root %q: %w", root, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("root %q is not a directory", root)
	}
	hostname, _ := os.Hostname()
	var created createRunResponse
	if _, err := client.doJSON(http.MethodPost, "/agent/v1/runs", map[string]any{
		"hostname": hostname,
		"root":     root,
	}, &created); err != nil {
		return err
	}
	if created.Run.ID <= 0 {
		return fmt.Errorf("server did not return a run id")
	}
	logger.Info("agent run started", "run", created.Run.ID, "root", root)

	manifest, err := client.scanAndUpload(created.Run.ID, root, logger)
	if err != nil {
		return err
	}
	var submitted submitManifestResponse
	if _, err := client.doJSON(http.MethodPost, "/agent/v1/manifests", map[string]any{
		"run_id":   created.Run.ID,
		"manifest": manifest,
	}, &submitted); err != nil {
		return err
	}
	if _, err := client.doJSON(http.MethodPost, fmt.Sprintf("/agent/v1/runs/%d/finish", created.Run.ID), nil, nil); err != nil {
		return err
	}
	logger.Info("agent backup completed", "run", created.Run.ID, "entries", len(manifest.Entries), "status", submitted.Status)
	return nil
}

func (c *agentClient) scanAndUpload(runID int64, root string, logger *slog.Logger) (*repository.SnapshotManifest, error) {
	manifest := &repository.SnapshotManifest{
		CreatedAt:  time.Now().UTC(),
		SourceType: "agent",
		SourceRoot: root,
	}
	chunker := repository.NewChunker(agentChunkAvgSize)
	progress := agentProgress{Phase: "scanning", Message: root}
	if err := c.sendProgress(runID, progress); err != nil {
		return nil, err
	}
	lastProgress := time.Now()
	sendProgress := func(force bool) error {
		if !force && time.Since(lastProgress) < 500*time.Millisecond {
			return nil
		}
		lastProgress = time.Now()
		return c.sendProgress(runID, progress)
	}

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := os.Lstat(path)
		if err != nil {
			return fmt.Errorf("stat %q: %w", path, err)
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return fmt.Errorf("rel path %q: %w", path, err)
		}
		entry := repository.FileEntry{
			Path:    cleanManifestPath(rel),
			Size:    info.Size(),
			Mode:    uint32(info.Mode()),
			ModTime: info.ModTime().UTC(),
		}
		entry.UID, entry.GID = fileOwner(info)

		mode := info.Mode()
		switch {
		case mode.IsDir():
			entry.Type = repository.EntryTypeDir
		case mode&os.ModeSymlink != 0:
			entry.Type = repository.EntryTypeSymlink
			target, err := os.Readlink(path)
			if err != nil {
				return fmt.Errorf("read symlink %q: %w", path, err)
			}
			entry.LinkTarget = target
		case mode.IsRegular():
			entry.Type = repository.EntryTypeFile
			file, err := os.Open(path)
			if err != nil {
				return fmt.Errorf("open file %q: %w", path, err)
			}
			if err := chunker.Split(file, func(chunk []byte) error {
				ref, uploaded, err := c.ensureChunk(chunk)
				if err != nil {
					_ = file.Close()
					return err
				}
				if uploaded {
					progress.UploadedChunks++
				} else {
					progress.ReusedChunks++
				}
				entry.Chunks = append(entry.Chunks, ref)
				return sendProgress(false)
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
			if err := sendProgress(true); err != nil {
				return err
			}
		default:
			return nil
		}
		manifest.Entries = append(manifest.Entries, entry)
		return nil
	})
	if err != nil {
		return nil, err
	}
	progress.Phase = "manifest"
	progress.Message = "manifest ready"
	if err := sendProgress(true); err != nil {
		return nil, err
	}
	logger.Info("agent scan complete", "files", progress.ProcessedFiles, "uploaded_chunks", progress.UploadedChunks, "reused_chunks", progress.ReusedChunks)
	return manifest, nil
}

func (c *agentClient) sendProgress(runID int64, progress agentProgress) error {
	if runID <= 0 {
		return fmt.Errorf("run id is required for progress")
	}
	var resp map[string]any
	_, err := c.doJSON(http.MethodPost, fmt.Sprintf("/agent/v1/runs/%d/progress", runID), progress, &resp)
	return err
}

func (c *agentClient) ensureChunk(chunk []byte) (repository.ChunkRef, bool, error) {
	sum := blake3.Sum256(chunk)
	hash := hex.EncodeToString(sum[:])
	var queried chunkResponse
	if _, err := c.doJSON(http.MethodGet, "/agent/v1/chunks/"+hash, nil, &queried); err != nil {
		return repository.ChunkRef{}, false, err
	}
	if queried.Exists {
		if queried.Ref.Hash == "" {
			return repository.ChunkRef{}, false, fmt.Errorf("server reported existing chunk %s without ref", hash)
		}
		return queried.Ref, false, nil
	}
	var uploaded chunkResponse
	status, err := c.doRaw(http.MethodPut, "/agent/v1/chunks/"+hash, bytes.NewReader(chunk), "application/octet-stream", &uploaded)
	if err != nil {
		return repository.ChunkRef{}, false, err
	}
	if status != http.StatusCreated && status != http.StatusOK {
		return repository.ChunkRef{}, false, fmt.Errorf("unexpected chunk upload status %d", status)
	}
	if uploaded.Ref.Hash == "" {
		return repository.ChunkRef{}, false, fmt.Errorf("server accepted chunk %s without ref", hash)
	}
	return uploaded.Ref, status == http.StatusCreated && uploaded.Uploaded, nil
}

func (c *agentClient) doJSON(method, path string, requestValue any, responseValue any) (int, error) {
	var body io.Reader
	if requestValue != nil {
		data, err := json.Marshal(requestValue)
		if err != nil {
			return 0, err
		}
		body = bytes.NewReader(data)
	}
	return c.doRaw(method, path, body, "application/json", responseValue)
}

func (c *agentClient) doRaw(method, path string, body io.Reader, contentType string, responseValue any) (int, error) {
	req, err := http.NewRequest(method, c.serverURL+path, body)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Accept", "application/json")
	if contentType != "" && body != nil {
		req.Header.Set("Content-Type", contentType)
	}
	if c.clientID != "" || c.clientSecret != "" {
		req.SetBasicAuth(c.clientID, c.clientSecret)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
	if err != nil {
		return resp.StatusCode, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf("server returned %s: %s", resp.Status, strings.TrimSpace(string(respBody)))
	}
	if responseValue != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, responseValue); err != nil {
			return resp.StatusCode, fmt.Errorf("decode response: %w", err)
		}
	}
	return resp.StatusCode, nil
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

func cleanManifestPath(path string) string {
	path = filepath.ToSlash(filepath.Clean(path))
	path = strings.TrimPrefix(path, "/")
	if path == "." || path == "" {
		return "."
	}
	return path
}
