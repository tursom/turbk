package repository

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/cockroachdb/pebble"
)

type chunkIndex struct {
	db *pebble.DB
}

func openChunkIndex(stateDir string) (*chunkIndex, error) {
	path := filepath.Join(stateDir, "chunk-index.pebble")
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("open chunk index: %w", err)
	}
	return &chunkIndex{db: db}, nil
}

func (i *chunkIndex) Close() error {
	if i == nil || i.db == nil {
		return nil
	}
	return i.db.Close()
}

func (i *chunkIndex) Get(hash string) (ChunkRef, bool, error) {
	value, closer, err := i.db.Get(indexKey(hash))
	if err == pebble.ErrNotFound {
		return ChunkRef{}, false, nil
	}
	if err != nil {
		return ChunkRef{}, false, fmt.Errorf("get chunk index: %w", err)
	}
	defer closer.Close()

	var ref ChunkRef
	if err := json.Unmarshal(value, &ref); err != nil {
		return ChunkRef{}, false, fmt.Errorf("decode chunk index value: %w", err)
	}
	return ref, true, nil
}

func (i *chunkIndex) Put(ref ChunkRef) error {
	data, err := json.Marshal(ref)
	if err != nil {
		return fmt.Errorf("encode chunk index value: %w", err)
	}
	if err := i.db.Set(indexKey(ref.Hash), data, pebble.Sync); err != nil {
		return fmt.Errorf("put chunk index: %w", err)
	}
	return nil
}

func (i *chunkIndex) DeleteUnreferenced(keep map[string]struct{}) (int64, error) {
	iter, err := i.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("chunk:"),
		UpperBound: []byte("chunk;"),
	})
	if err != nil {
		return 0, fmt.Errorf("create chunk index iterator: %w", err)
	}
	defer iter.Close()

	batch := i.db.NewBatch()
	defer batch.Close()
	var deleted int64
	for valid := iter.First(); valid; valid = iter.Next() {
		if !bytes.HasPrefix(iter.Key(), []byte("chunk:")) {
			continue
		}
		hash := string(bytes.TrimPrefix(iter.Key(), []byte("chunk:")))
		if _, ok := keep[hash]; ok {
			continue
		}
		key := append([]byte(nil), iter.Key()...)
		if err := batch.Delete(key, nil); err != nil {
			return 0, fmt.Errorf("delete unreferenced chunk index %q: %w", hash, err)
		}
		deleted++
	}
	if err := iter.Error(); err != nil {
		return 0, fmt.Errorf("iterate chunk index: %w", err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return 0, fmt.Errorf("commit unreferenced chunk index deletes: %w", err)
	}
	return deleted, nil
}

func (i *chunkIndex) Stats() (chunks int64, logicalBytes int64, compressedBytes int64, err error) {
	iter, err := i.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("chunk:"),
		UpperBound: []byte("chunk;"),
	})
	if err != nil {
		return 0, 0, 0, fmt.Errorf("create chunk index iterator: %w", err)
	}
	defer iter.Close()

	for valid := iter.First(); valid; valid = iter.Next() {
		if !bytes.HasPrefix(iter.Key(), []byte("chunk:")) {
			continue
		}
		var ref ChunkRef
		if err := json.Unmarshal(iter.Value(), &ref); err != nil {
			return 0, 0, 0, fmt.Errorf("decode chunk index value: %w", err)
		}
		chunks++
		logicalBytes += ref.OriginalSize
		compressedBytes += ref.CompressedSize
	}
	if err := iter.Error(); err != nil {
		return 0, 0, 0, fmt.Errorf("iterate chunk index: %w", err)
	}
	return chunks, logicalBytes, compressedBytes, nil
}

func indexKey(hash string) []byte {
	return []byte("chunk:" + hash)
}
