package main

import (
	"encoding/hex"
	"path/filepath"
	"testing"

	"github.com/tursom/turbk/internal/repository"
)

func TestPebbleChunkStatusKeyIsBinary(t *testing.T) {
	hash := testChunkHash("binary key")
	key, err := encodePebbleChunkStatusKey(hash)
	if err != nil {
		t.Fatal(err)
	}
	if len(key) != 33 {
		t.Fatalf("key length = %d, want 33", len(key))
	}
	if key[0] != agentPebbleRecordChunkStatus {
		t.Fatalf("key prefix = 0x%x, want 0x%x", key[0], agentPebbleRecordChunkStatus)
	}
	if got := hex.EncodeToString(key[1:]); got != hash {
		t.Fatalf("key hash = %s, want %s", got, hash)
	}
}

func TestPebbleChunkCatalogMarkChunksInvalidationAndReset(t *testing.T) {
	catalog, err := openPebbleChunkCatalog(filepath.Join(t.TempDir(), "catalog.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Close()

	hashA := testChunkHash("chunk-a")
	hashB := testChunkHash("chunk-b")
	if err := catalog.markChunks([]agentChunkStatusUpdate{
		{Hash: hashA, OriginalSize: 123, Status: "confirmed", Generation: 7, Uploaded: true},
		{Hash: hashB, OriginalSize: 456, Status: "missing"},
	}); err != nil {
		t.Fatal(err)
	}
	status, generation, ok, err := catalog.chunkStatus(hashA)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || status != "confirmed" || generation != 7 {
		t.Fatalf("hashA status=%q generation=%d ok=%v, want confirmed generation 7", status, generation, ok)
	}
	status, generation, ok, err = catalog.chunkStatus(hashB)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || status != "missing" || generation != 0 {
		t.Fatalf("hashB status=%q generation=%d ok=%v, want missing generation 0", status, generation, ok)
	}

	if err := catalog.applyInvalidations([]string{hashA}, 9); err != nil {
		t.Fatal(err)
	}
	status, generation, ok, err = catalog.chunkStatus(hashA)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || status != "unknown" || generation != 0 {
		t.Fatalf("invalidated hashA status=%q generation=%d ok=%v, want unknown generation 0", status, generation, ok)
	}

	if err := catalog.markChunk(hashB, 456, "confirmed", 7, false); err != nil {
		t.Fatal(err)
	}
	if err := catalog.applyInvalidations(nil, 11); err != nil {
		t.Fatal(err)
	}
	status, generation, ok, err = catalog.chunkStatus(hashB)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || status != "confirmed" || generation != 11 {
		t.Fatalf("advanced hashB status=%q generation=%d ok=%v, want confirmed generation 11", status, generation, ok)
	}

	if err := catalog.resetServerChunks(); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := catalog.chunkStatus(hashB); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("chunk status still present after reset")
	}
}

func TestPebbleFileKeyAndRecordRoundTrip(t *testing.T) {
	rootID := "/data/app"
	path := "dir/文件 name.txt"
	key := encodePebbleFileKey(rootID, path)
	decodedRootID, decodedPath, ok := decodePebbleFileKey(key)
	if !ok {
		t.Fatal("decodePebbleFileKey returned not ok")
	}
	if decodedRootID != rootID || decodedPath != path {
		t.Fatalf("decoded key root=%q path=%q, want root=%q path=%q", decodedRootID, decodedPath, rootID, path)
	}

	catalog, err := openPebbleChunkCatalog(filepath.Join(t.TempDir(), "catalog.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Close()

	chunkA := testChunkHash("file-a")
	chunkB := testChunkHash("file-b")
	record := catalogFileRecord{
		RootID:      rootID,
		Path:        path,
		Type:        string(repository.EntryTypeFile),
		Size:        579,
		Mode:        0o100644,
		UID:         1000,
		GID:         1001,
		MTimeNS:     123456789,
		Dev:         42,
		Inode:       84,
		Fingerprint: "fp",
	}
	chunks := []catalogChunkRecord{
		{Hash: chunkA, OriginalSize: 123},
		{Hash: chunkB, OriginalSize: 456},
	}
	if err := catalog.replaceFile(record, chunks); err != nil {
		t.Fatal(err)
	}
	gotRecord, gotChunks, ok, err := catalog.fileRecordWithChunks(rootID, path)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("file record missing")
	}
	if gotRecord != record {
		t.Fatalf("record = %#v, want %#v", gotRecord, record)
	}
	if len(gotChunks) != len(chunks) {
		t.Fatalf("chunks = %#v, want %#v", gotChunks, chunks)
	}
	for i := range chunks {
		if gotChunks[i] != chunks[i] {
			t.Fatalf("chunk %d = %#v, want %#v", i, gotChunks[i], chunks[i])
		}
	}
}

func TestOpenAgentCatalogSQLiteBackendUsesSQLiteChunks(t *testing.T) {
	t.Setenv("TURBK_AGENT_CATALOG_BACKEND", "sqlite")
	catalog, err := openAgentCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Close()

	hash := testChunkHash("sqlite backend")
	if err := catalog.markChunk(hash, 321, "confirmed", 4, false); err != nil {
		t.Fatal(err)
	}
	status, generation, ok, err := catalog.chunkStatus(hash)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || status != "confirmed" || generation != 4 {
		t.Fatalf("chunk status=%q generation=%d ok=%v, want confirmed generation 4", status, generation, ok)
	}
	var sqliteChunkRows int
	if err := catalog.db.QueryRow(`SELECT COUNT(*) FROM server_chunks`).Scan(&sqliteChunkRows); err != nil {
		t.Fatal(err)
	}
	if sqliteChunkRows != 1 {
		t.Fatalf("sqlite server_chunks rows = %d, want 1", sqliteChunkRows)
	}
}
