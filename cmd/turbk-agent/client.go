package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tursom/turbk/internal/repository"
	"github.com/tursom/turbk/internal/version"
)

type agentClient struct {
	serverURL    string
	clientID     string
	clientSecret string
	http         *http.Client
}

func newAgentClient(serverURL, clientID, clientSecret string) *agentClient {
	return &agentClient{
		serverURL:    strings.TrimRight(serverURL, "/"),
		clientID:     clientID,
		clientSecret: clientSecret,
		http:         newAgentHTTPClient(),
	}
}

func newAgentHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ForceAttemptHTTP2 = true
	transport.MaxIdleConns = envInt("TURBK_AGENT_HTTP_MAX_IDLE_CONNS", 100)
	transport.MaxIdleConnsPerHost = envInt("TURBK_AGENT_HTTP_MAX_IDLE_CONNS_PER_HOST", 16)
	transport.MaxConnsPerHost = envInt("TURBK_AGENT_HTTP_MAX_CONNS_PER_HOST", 0)
	transport.IdleConnTimeout = parseDurationOrDefault(envString("TURBK_AGENT_HTTP_IDLE_CONN_TIMEOUT", "90s"), 90*time.Second)
	transport.TLSHandshakeTimeout = parseDurationOrDefault(envString("TURBK_AGENT_HTTP_TLS_HANDSHAKE_TIMEOUT", "10s"), 10*time.Second)
	transport.ResponseHeaderTimeout = parseDurationOrDefault(os.Getenv("TURBK_AGENT_HTTP_RESPONSE_HEADER_TIMEOUT"), 0)
	transport.ExpectContinueTimeout = parseDurationOrDefault(envString("TURBK_AGENT_HTTP_EXPECT_CONTINUE_TIMEOUT", "1s"), time.Second)
	return &http.Client{
		Timeout:   parseDurationOrDefault(envString("TURBK_AGENT_HTTP_TIMEOUT", "60s"), 60*time.Second),
		Transport: transport,
	}
}

func (c *agentClient) sendHeartbeat() error {
	_, err := c.sendHeartbeatWithState(nil, "", "once", 0, "")
	return err
}

func (c *agentClient) sendHeartbeatWithState(catalog *agentCatalog, stateDir, mode string, runningRunID int64, lastError string) (heartbeatResponse, error) {
	hostname, _ := os.Hostname()
	req := heartbeatRequest{
		Hostname:                   hostname,
		Version:                    version.Version,
		Mode:                       mode,
		StateDir:                   stateDir,
		RunningRunID:               runningRunID,
		LastError:                  lastError,
		CompactChunkCheckResponse:  true,
		CompactChunkUploadResponse: true,
		SmallFilePack:              true,
		ChunkPipeline:              true,
		ScanParallel:               true,
	}
	if catalog != nil {
		req.CatalogStatus = "ok"
		if state, ok, err := catalog.serverState(c.serverURL, c.clientID); err == nil && ok {
			req.RepositoryID = state.RepositoryID
			req.ChunkGeneration = state.ChunkGeneration
			req.ConfigGeneration = state.ConfigGeneration
			req.CommandGeneration = state.CommandGeneration
		} else if err != nil {
			req.CatalogStatus = "error: " + err.Error()
		}
	}
	var resp heartbeatResponse
	_, err := c.doJSON(http.MethodPost, "/agent/v1/heartbeat", req, &resp)
	return resp, err
}

func (c *agentClient) sendProgress(runID int64, progress agentProgress) error {
	if runID <= 0 {
		return fmt.Errorf("run id is required for progress")
	}
	var resp map[string]any
	_, err := c.doJSON(http.MethodPost, fmt.Sprintf("/agent/v1/runs/%d/progress", runID), progress, &resp)
	return err
}

func (c *agentClient) failRun(runID int64, message string) error {
	if runID <= 0 {
		return nil
	}
	var resp map[string]any
	_, err := c.doJSON(http.MethodPost, fmt.Sprintf("/agent/v1/runs/%d/finish", runID), map[string]any{
		"status": "failed",
		"error":  message,
	}, &resp)
	return err
}

func (c *agentClient) chunkInvalidations(since int64) (invalidationsResponse, error) {
	var resp invalidationsResponse
	_, err := c.doJSON(http.MethodGet, fmt.Sprintf("/agent/v1/chunks/invalidations?since=%d", since), nil, &resp)
	return resp, err
}

func (c *agentClient) submitManifest(runID int64, manifest *repository.SnapshotManifest) (submitManifestResponse, error) {
	var submitted submitManifestResponse
	status, err := c.doJSONAllowStatuses(http.MethodPost, "/agent/v1/manifests", map[string]any{
		"run_id":   runID,
		"manifest": manifest,
	}, &submitted, http.StatusConflict)
	if err != nil {
		return submitManifestResponse{}, err
	}
	if status == http.StatusConflict && submitted.Status != "missing_chunks" {
		return submitManifestResponse{}, fmt.Errorf("server returned conflict without missing_chunks status")
	}
	return submitted, nil
}

func (c *agentClient) doJSON(method, path string, requestValue any, responseValue any) (int, error) {
	return c.doJSONAllowStatuses(method, path, requestValue, responseValue)
}

func (c *agentClient) doJSONAllowStatuses(method, path string, requestValue any, responseValue any, allowedStatuses ...int) (int, error) {
	var body io.Reader
	if requestValue != nil {
		data, err := json.Marshal(requestValue)
		if err != nil {
			return 0, err
		}
		body = bytes.NewReader(data)
	}
	return c.doRawAllowStatuses(method, path, body, "application/json", responseValue, allowedStatuses...)
}

func (c *agentClient) doRaw(method, path string, body io.Reader, contentType string, responseValue any) (int, error) {
	return c.doRawAllowStatuses(method, path, body, contentType, responseValue)
}

type agentHTTPError struct {
	StatusCode    int
	Status        string
	Body          string
	RetryAfter    time.Duration
	RetryAfterSet bool
}

func (e *agentHTTPError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("server returned %s: %s", e.Status, e.Body)
}

func (c *agentClient) doRawAllowStatuses(method, path string, body io.Reader, contentType string, responseValue any, allowedStatuses ...int) (int, error) {
	req, err := http.NewRequest(method, c.serverURL+path, body)
	if err != nil {
		return 0, err
	}
	if sized, ok := body.(interface{ Size() int64 }); ok {
		req.ContentLength = sized.Size()
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
	respBody, err := readAgentResponseBody(resp.Body)
	if err != nil {
		return resp.StatusCode, err
	}
	allowed := resp.StatusCode >= 200 && resp.StatusCode < 300
	for _, status := range allowedStatuses {
		if resp.StatusCode == status {
			allowed = true
			break
		}
	}
	if !allowed {
		retryAfter, retryAfterSet := parseRetryAfter(resp.Header.Get("Retry-After"))
		return resp.StatusCode, &agentHTTPError{
			StatusCode:    resp.StatusCode,
			Status:        resp.Status,
			Body:          strings.TrimSpace(string(respBody)),
			RetryAfter:    retryAfter,
			RetryAfterSet: retryAfterSet,
		}
	}
	if responseValue != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, responseValue); err != nil {
			return resp.StatusCode, fmt.Errorf("decode response: %w", err)
		}
		if sized, ok := responseValue.(interface{ setResponseBodyBytes(int64) }); ok {
			sized.setResponseBodyBytes(int64(len(respBody)))
		}
	}
	return resp.StatusCode, nil
}

func parseRetryAfter(value string) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds < 0 {
			return 0, false
		}
		return time.Duration(seconds) * time.Second, true
	}
	when, err := http.ParseTime(value)
	if err != nil {
		return 0, false
	}
	delay := time.Until(when)
	if delay < 0 {
		delay = 0
	}
	return delay, true
}

func readAgentResponseBody(body io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(body, defaultAgentResponseBodyLimit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > defaultAgentResponseBodyLimit {
		return nil, fmt.Errorf("response body exceeds %d bytes", defaultAgentResponseBodyLimit)
	}
	return data, nil
}
