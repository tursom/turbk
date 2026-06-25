package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"strings"

	"github.com/tursom/turbk/internal/repository"
)

type pendingPackFile struct {
	entry  repository.FileEntry
	record catalogFileRecord
	file   repository.PackFilePayload
}

type agentSmallFilePackBatcher struct {
	enabled       bool
	maxFileSize   int64
	targetSize    int64
	pending       []pendingPackFile
	pendingBytes  int64
	nextPackIndex int64
}

func newAgentSmallFilePackBatcher(opts backupRunOptions) *agentSmallFilePackBatcher {
	maxFileSize := opts.SmallFilePackMaxFileSize
	if maxFileSize <= 0 {
		maxFileSize = 64 * 1024
	}
	targetSize := opts.SmallFilePackTargetSize
	if targetSize <= 0 {
		targetSize = 8 * 1024 * 1024
	}
	return &agentSmallFilePackBatcher{
		enabled:     opts.SmallFilePackEnabled,
		maxFileSize: maxFileSize,
		targetSize:  targetSize,
	}
}

func (b *agentSmallFilePackBatcher) Eligible(info fs.FileInfo) bool {
	return b != nil &&
		b.enabled &&
		info.Mode().IsRegular() &&
		info.Size() > 0 &&
		info.Size() <= b.maxFileSize
}

func (b *agentSmallFilePackBatcher) Add(file pendingPackFile, flush func() error) error {
	if b == nil || !b.enabled {
		return errors.New("small-file pack is disabled")
	}
	fileBytes := int64(len(file.file.Data))
	if fileBytes <= 0 {
		return fmt.Errorf("pack file %q is empty", file.entry.Path)
	}
	if len(b.pending) > 0 && b.pendingBytes+fileBytes > b.targetSize {
		if err := flush(); err != nil {
			return err
		}
	}
	b.pending = append(b.pending, file)
	b.pendingBytes += fileBytes
	if b.pendingBytes >= b.targetSize {
		return flush()
	}
	return nil
}

const packedFileFingerprintType = "turbk-packed-file-v1"

type packedFileFingerprint struct {
	Type   string               `json:"type"`
	PackID string               `json:"pack_id"`
	Offset int64                `json:"offset"`
	Length int64                `json:"length"`
	Chunks []catalogChunkRecord `json:"chunks"`
}

func encodePackedFileFingerprint(packID string, offset, length int64, chunks []repository.ChunkRef) string {
	fingerprint := packedFileFingerprint{
		Type:   packedFileFingerprintType,
		PackID: packID,
		Offset: offset,
		Length: length,
		Chunks: make([]catalogChunkRecord, 0, len(chunks)),
	}
	for _, chunk := range chunks {
		fingerprint.Chunks = append(fingerprint.Chunks, catalogChunkRecord{
			Hash:         chunk.Hash,
			OriginalSize: chunk.OriginalSize,
		})
	}
	data, err := json.Marshal(fingerprint)
	if err != nil {
		return ""
	}
	return string(data)
}

func decodePackedFileFingerprint(value string) (packedFileFingerprint, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return packedFileFingerprint{}, false
	}
	var fingerprint packedFileFingerprint
	if err := json.Unmarshal([]byte(value), &fingerprint); err != nil {
		return packedFileFingerprint{}, false
	}
	if fingerprint.Type != packedFileFingerprintType ||
		strings.TrimSpace(fingerprint.PackID) == "" ||
		fingerprint.Offset < 0 ||
		fingerprint.Length <= 0 ||
		len(fingerprint.Chunks) == 0 {
		return packedFileFingerprint{}, false
	}
	for _, chunk := range fingerprint.Chunks {
		if strings.TrimSpace(chunk.Hash) == "" || chunk.OriginalSize <= 0 {
			return packedFileFingerprint{}, false
		}
	}
	return fingerprint, true
}

func catalogFileMetadataMatches(a, b catalogFileRecord) bool {
	a.Fingerprint = ""
	b.Fingerprint = ""
	return catalogFileMatches(a, b)
}

func packManifestChunksFromCatalog(chunks []catalogChunkRecord) []repository.ChunkRef {
	if len(chunks) == 0 {
		return nil
	}
	refs := make([]repository.ChunkRef, 0, len(chunks))
	for _, chunk := range chunks {
		refs = append(refs, repository.ChunkRef{Hash: chunk.Hash, OriginalSize: chunk.OriginalSize})
	}
	return refs
}

func catalogChunksFromEntry(entry *repository.FileEntry) []catalogChunkRecord {
	if len(entry.Chunks) == 0 {
		return nil
	}
	chunks := make([]catalogChunkRecord, 0, len(entry.Chunks))
	for _, ref := range entry.Chunks {
		chunks = append(chunks, catalogChunkRecord{Hash: ref.Hash, OriginalSize: ref.OriginalSize})
	}
	return chunks
}
