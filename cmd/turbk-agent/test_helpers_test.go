package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"io"
	"testing"

	"github.com/zeebo/blake3"
)

type decodedAgentChunkBatch struct {
	hash string
	data []byte
}

func testChunkHash(value string) string {
	sum := blake3.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func pendingChunkForTest(data []byte) *pendingBatchChunk {
	sum := blake3.Sum256(data)
	return &pendingBatchChunk{
		hash:         hex.EncodeToString(sum[:]),
		data:         append([]byte(nil), data...),
		originalSize: int64(len(data)),
	}
}

func decodeAgentChunkBatchRequest(t *testing.T, body io.Reader) []decodedAgentChunkBatch {
	t.Helper()
	data, err := io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(data, agentChunkBatchMagic) {
		t.Fatalf("chunk batch magic mismatch: %q", data[:min(len(data), len(agentChunkBatchMagic))])
	}
	offset := len(agentChunkBatchMagic)
	if len(data) < offset+4 {
		t.Fatalf("chunk batch missing count")
	}
	count := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	chunks := make([]decodedAgentChunkBatch, 0, count)
	for i := uint32(0); i < count; i++ {
		if len(data) < offset+32+8 {
			t.Fatalf("chunk %d missing header", i)
		}
		hashBytes := data[offset : offset+32]
		offset += 32
		length := binary.BigEndian.Uint64(data[offset : offset+8])
		offset += 8
		if length > uint64(len(data)-offset) {
			t.Fatalf("chunk %d length %d exceeds remaining body %d", i, length, len(data)-offset)
		}
		chunk := data[offset : offset+int(length)]
		offset += int(length)
		sum := blake3.Sum256(chunk)
		if !bytes.Equal(sum[:], hashBytes) {
			t.Fatalf("chunk %d hash mismatch", i)
		}
		chunks = append(chunks, decodedAgentChunkBatch{
			hash: hex.EncodeToString(hashBytes),
			data: append([]byte(nil), chunk...),
		})
	}
	if offset != len(data) {
		t.Fatalf("chunk batch has %d trailing bytes", len(data)-offset)
	}
	return chunks
}
