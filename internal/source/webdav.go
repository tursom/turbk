package source

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"
)

type WebDAVConfig struct {
	BaseURL     string
	Username    string
	Password    string
	BearerToken string
	Timeout     time.Duration
}

type WebDAV struct {
	base   *url.URL
	client *http.Client
	cfg    WebDAVConfig
}

func NewWebDAV(cfg WebDAVConfig) (*WebDAV, error) {
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}
	base, err := url.Parse(cfg.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse webdav base url: %w", err)
	}
	if base.Scheme == "" || base.Host == "" {
		return nil, fmt.Errorf("webdav base url must include scheme and host")
	}
	return &WebDAV{
		base:   base,
		client: &http.Client{Timeout: cfg.Timeout},
		cfg:    cfg,
	}, nil
}

func (w *WebDAV) Walk(ctx context.Context, root string, fn func(Entry) error) error {
	seen := map[string]bool{}
	var walk func(string) error
	walk = func(dir string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		entries, err := w.propfind(ctx, dir)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if entry.Path == cleanRemotePath(dir) {
				continue
			}
			if seen[entry.Path] {
				continue
			}
			seen[entry.Path] = true
			if err := fn(entry); err != nil {
				return err
			}
			if entry.Type == EntryDir {
				if err := walk(entry.Path); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return walk(root)
}

func (w *WebDAV) Open(ctx context.Context, remotePath string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, w.urlFor(remotePath), nil)
	if err != nil {
		return nil, err
	}
	w.applyAuth(req)
	resp, err := w.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("open webdav %q: %w", remotePath, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		return nil, fmt.Errorf("open webdav %q: %s", remotePath, resp.Status)
	}
	return resp.Body, nil
}

func (w *WebDAV) Close() error {
	return nil
}

func (w *WebDAV) propfind(ctx context.Context, remotePath string) ([]Entry, error) {
	body := bytes.NewBufferString(`<?xml version="1.0" encoding="utf-8" ?><D:propfind xmlns:D="DAV:"><D:prop><D:resourcetype/><D:getcontentlength/><D:getlastmodified/></D:prop></D:propfind>`)
	req, err := http.NewRequestWithContext(ctx, "PROPFIND", w.urlFor(remotePath), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Depth", "1")
	req.Header.Set("Content-Type", "application/xml; charset=utf-8")
	w.applyAuth(req)
	resp, err := w.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("propfind webdav %q: %w", remotePath, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMultiStatus {
		return nil, fmt.Errorf("propfind webdav %q: %s", remotePath, resp.Status)
	}
	var multi davMultiStatus
	if err := xml.NewDecoder(resp.Body).Decode(&multi); err != nil {
		return nil, fmt.Errorf("decode webdav multistatus: %w", err)
	}
	basePath := cleanRemotePath(w.base.EscapedPath())
	out := make([]Entry, 0, len(multi.Responses))
	for _, response := range multi.Responses {
		entry, ok := response.toEntry(basePath)
		if ok {
			out = append(out, entry)
		}
	}
	return out, nil
}

func (w *WebDAV) applyAuth(req *http.Request) {
	if w.cfg.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+w.cfg.BearerToken)
		return
	}
	if w.cfg.Username != "" {
		req.SetBasicAuth(w.cfg.Username, w.cfg.Password)
	}
}

func (w *WebDAV) urlFor(remotePath string) string {
	u := *w.base
	basePath := strings.TrimRight(w.base.Path, "/")
	u.Path = path.Join(basePath, cleanRemotePath(remotePath))
	if strings.HasSuffix(remotePath, "/") && !strings.HasSuffix(u.Path, "/") {
		u.Path += "/"
	}
	return u.String()
}

type davMultiStatus struct {
	Responses []davResponse `xml:"response"`
}

type davResponse struct {
	Href     string        `xml:"href"`
	PropStat []davPropStat `xml:"propstat"`
}

type davPropStat struct {
	Prop davProp `xml:"prop"`
}

type davProp struct {
	ResourceType     davResourceType `xml:"resourcetype"`
	GetContentLength string          `xml:"getcontentlength"`
	GetLastModified  string          `xml:"getlastmodified"`
}

type davResourceType struct {
	Collection *struct{} `xml:"collection"`
}

func (r davResponse) toEntry(basePath string) (Entry, bool) {
	if r.Href == "" || len(r.PropStat) == 0 {
		return Entry{}, false
	}
	href, err := url.PathUnescape(r.Href)
	if err != nil {
		href = r.Href
	}
	parsed, err := url.Parse(href)
	if err == nil && parsed.Path != "" {
		href = parsed.Path
	}
	remotePath := cleanRemotePath(strings.TrimPrefix(href, basePath))
	prop := r.PropStat[0].Prop
	entry := Entry{Path: remotePath}
	if prop.ResourceType.Collection != nil || strings.HasSuffix(href, "/") {
		entry.Type = EntryDir
	} else {
		entry.Type = EntryFile
	}
	if prop.GetContentLength != "" {
		if size, err := strconv.ParseInt(strings.TrimSpace(prop.GetContentLength), 10, 64); err == nil {
			entry.Size = size
		}
	}
	if prop.GetLastModified != "" {
		if modTime, err := http.ParseTime(prop.GetLastModified); err == nil {
			entry.ModTime = modTime
		}
	}
	return entry, true
}
