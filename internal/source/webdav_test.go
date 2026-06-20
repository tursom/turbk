package source

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"testing"
	"time"
)

func TestWebDAVWalkAndOpen(t *testing.T) {
	const token = "webdav-token"
	fileBody := "webdav file body"
	nestedBody := "nested webdav body"
	modTime := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch r.Method {
		case "PROPFIND":
			if r.Header.Get("Depth") != "1" {
				http.Error(w, "bad depth", http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/xml; charset=utf-8")
			w.WriteHeader(http.StatusMultiStatus)
			switch r.URL.Path {
			case "/dav/root":
				_, _ = io.WriteString(w, davMultiStatusXML(
					davTestResource{href: "/dav/root/", collection: true, modified: modTime},
					davTestResource{href: "/dav/root/file.txt", size: int64(len(fileBody)), modified: modTime},
					davTestResource{href: "/dav/root/sub/", collection: true, modified: modTime},
				))
			case "/dav/root/sub":
				_, _ = io.WriteString(w, davMultiStatusXML(
					davTestResource{href: "/dav/root/sub/", collection: true, modified: modTime},
					davTestResource{href: "/dav/root/sub/nested.txt", size: int64(len(nestedBody)), modified: modTime},
				))
			default:
				http.Error(w, "not found", http.StatusNotFound)
			}
		case http.MethodGet:
			switch r.URL.Path {
			case "/dav/root/file.txt":
				_, _ = io.WriteString(w, fileBody)
			case "/dav/root/sub/nested.txt":
				_, _ = io.WriteString(w, nestedBody)
			default:
				http.Error(w, "not found", http.StatusNotFound)
			}
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	connector, err := NewWebDAV(WebDAVConfig{BaseURL: server.URL + "/dav", BearerToken: token})
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
	if len(entries) != 3 {
		t.Fatalf("entries = %+v, want 3 entries", entries)
	}
	want := map[string]EntryType{
		"/root/file.txt":       EntryFile,
		"/root/sub":            EntryDir,
		"/root/sub/nested.txt": EntryFile,
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
	if body != nestedBody {
		t.Fatalf("nested body = %q, want %q", body, nestedBody)
	}
}

type davTestResource struct {
	href       string
	collection bool
	size       int64
	modified   time.Time
}

func davMultiStatusXML(resources ...davTestResource) string {
	out := `<?xml version="1.0" encoding="utf-8"?><D:multistatus xmlns:D="DAV:">`
	for _, resource := range resources {
		out += `<D:response><D:href>` + resource.href + `</D:href><D:propstat><D:prop>`
		if resource.collection {
			out += `<D:resourcetype><D:collection/></D:resourcetype>`
		} else {
			out += `<D:resourcetype/><D:getcontentlength>` + strconv.FormatInt(resource.size, 10) + `</D:getcontentlength>`
		}
		out += `<D:getlastmodified>` + resource.modified.Format(http.TimeFormat) + `</D:getlastmodified>`
		out += `</D:prop></D:propstat></D:response>`
	}
	out += `</D:multistatus>`
	return out
}

func readConnectorFile(connector Connector, path string) (string, error) {
	reader, err := connector.Open(context.Background(), path)
	if err != nil {
		return "", err
	}
	defer reader.Close()
	body, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}
	return string(body), nil
}
