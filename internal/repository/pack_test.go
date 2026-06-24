package repository

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
)

func TestEncodeDecodePackIndexesFiles(t *testing.T) {
	mtime := time.Unix(1710000000, 123)
	data, indexes, err := EncodePack([]PackFilePayload{
		{Path: "dir/a.txt", Mode: 0o644, ModTime: mtime, Data: []byte("alpha")},
		{Path: "dir/b.txt", Mode: 0o600, ModTime: mtime.Add(time.Second), Data: []byte("bravo-data")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(indexes) != 2 {
		t.Fatalf("indexes = %d, want 2", len(indexes))
	}
	decoded, err := DecodePackIndex(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 2 {
		t.Fatalf("decoded indexes = %d, want 2", len(decoded))
	}
	for i := range indexes {
		if decoded[i] != indexes[i] {
			t.Fatalf("decoded index %d = %+v, want %+v", i, decoded[i], indexes[i])
		}
		fileData, err := PackFileBytes(data, PackFileRef{ID: "pack-test", Offset: decoded[i].Offset, Length: decoded[i].Length})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(fileData, []byte([]string{"alpha", "bravo-data"}[i])) {
			t.Fatalf("file data %d = %q", i, fileData)
		}
	}
}

func TestDecodePackRejectsInvalidRanges(t *testing.T) {
	data, _, err := EncodePack([]PackFilePayload{
		{Path: "a.txt", Mode: 0o644, ModTime: time.Now(), Data: []byte("alpha")},
	})
	if err != nil {
		t.Fatal(err)
	}
	offsetField := len(PackMagicTBKPack1) + 4 + 4 + len("a.txt") + 8
	corrupt := append([]byte(nil), data...)
	binary.BigEndian.PutUint64(corrupt[offsetField:], uint64(len(corrupt)+1))
	if _, err := DecodePackIndex(corrupt); err == nil {
		t.Fatal("DecodePackIndex accepted out-of-bounds file offset")
	}

	corrupt = append([]byte(nil), data...)
	binary.BigEndian.PutUint64(corrupt[offsetField:], 1)
	if _, err := DecodePackIndex(corrupt); err == nil {
		t.Fatal("DecodePackIndex accepted header file offset")
	}
}

func TestEncodePackRejectsEmptyAndInvalidFiles(t *testing.T) {
	if _, _, err := EncodePack(nil); err == nil {
		t.Fatal("EncodePack accepted empty pack")
	}
	if _, _, err := EncodePack([]PackFilePayload{{Path: "/abs.txt", Data: []byte("x")}}); err == nil {
		t.Fatal("EncodePack accepted absolute path")
	}
	if _, _, err := EncodePack([]PackFilePayload{{Path: "empty.txt"}}); err == nil {
		t.Fatal("EncodePack accepted empty file")
	}
}
