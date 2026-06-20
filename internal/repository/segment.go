package repository

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/zeebo/blake3"
)

const (
	recordMagic            = "TBKREC1\n"
	recordVersion   uint16 = 1
	recordHeaderLen        = 116
	recordPrefixLen        = 84
	recordFlagZstd  uint32 = 1 << 0
	recordFlagAES   uint32 = 1 << 1
)

type segmentWriter struct {
	dir       string
	maxSize   int64
	id        int64
	file      *os.File
	offset    int64
	encryptor cipher.AEAD
	encoder   *zstd.Encoder
}

func openSegmentWriter(repoDir string, maxSize int64, encryptor cipher.AEAD, encoder *zstd.Encoder) (*segmentWriter, error) {
	dir := filepath.Join(repoDir, "segments")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create segments dir: %w", err)
	}
	ids, err := listSegmentIDs(dir)
	if err != nil {
		return nil, err
	}
	id := int64(1)
	if len(ids) > 0 {
		id = ids[len(ids)-1]
	}
	w := &segmentWriter{
		dir:       dir,
		maxSize:   maxSize,
		encryptor: encryptor,
		encoder:   encoder,
	}
	if err := w.open(id); err != nil {
		return nil, err
	}
	if w.offset >= maxSize && w.offset > 0 {
		if err := w.rotate(); err != nil {
			_ = w.Close()
			return nil, err
		}
	}
	return w, nil
}

func (w *segmentWriter) Close() error {
	if w == nil || w.file == nil {
		return nil
	}
	return w.file.Close()
}

func (w *segmentWriter) WriteChunk(hash [32]byte, data []byte) (ChunkRef, error) {
	record, compressedSize, err := w.encodeRecord(hash, data)
	if err != nil {
		return ChunkRef{}, err
	}
	if w.offset > 0 && w.offset+int64(len(record)) > w.maxSize {
		if err := w.rotate(); err != nil {
			return ChunkRef{}, err
		}
	}
	offset := w.offset
	if _, err := w.file.Write(record); err != nil {
		return ChunkRef{}, fmt.Errorf("write segment record: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return ChunkRef{}, fmt.Errorf("sync segment record: %w", err)
	}
	w.offset += int64(len(record))
	return ChunkRef{
		Hash:           hex.EncodeToString(hash[:]),
		SegmentID:      w.id,
		Offset:         offset,
		Length:         int64(len(record)),
		OriginalSize:   int64(len(data)),
		CompressedSize: int64(compressedSize),
		CreatedAt:      time.Now().UTC(),
	}, nil
}

func (w *segmentWriter) encodeRecord(hash [32]byte, data []byte) ([]byte, int, error) {
	compressed := w.encoder.EncodeAll(data, make([]byte, 0, len(data)))
	nonce := make([]byte, w.encryptor.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, 0, fmt.Errorf("generate record nonce: %w", err)
	}
	payload := w.encryptor.Seal(nil, nonce, compressed, hash[:])
	prefix := make([]byte, recordPrefixLen)
	copy(prefix[0:8], []byte(recordMagic))
	binary.LittleEndian.PutUint16(prefix[8:10], recordHeaderLen)
	binary.LittleEndian.PutUint16(prefix[10:12], recordVersion)
	binary.LittleEndian.PutUint32(prefix[12:16], recordFlagZstd|recordFlagAES)
	copy(prefix[16:48], hash[:])
	binary.LittleEndian.PutUint64(prefix[48:56], uint64(len(data)))
	binary.LittleEndian.PutUint64(prefix[56:64], uint64(len(compressed)))
	binary.LittleEndian.PutUint64(prefix[64:72], uint64(len(payload)))
	copy(prefix[72:84], nonce)

	sum := blake3.Sum256(append(copyBytes(prefix), payload...))
	record := make([]byte, 0, recordHeaderLen+len(payload))
	record = append(record, prefix...)
	record = append(record, sum[:]...)
	record = append(record, payload...)
	return record, len(compressed), nil
}

func (w *segmentWriter) open(id int64) error {
	path := segmentPath(w.dir, id)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o640)
	if err != nil {
		return fmt.Errorf("open segment %d: %w", id, err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return fmt.Errorf("stat segment %d: %w", id, err)
	}
	w.id = id
	w.file = file
	w.offset = info.Size()
	return nil
}

func (w *segmentWriter) rotate() error {
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			return fmt.Errorf("close segment: %w", err)
		}
	}
	return w.open(w.id + 1)
}

type segmentReader struct {
	dir       string
	decryptor cipher.AEAD
	decoder   *zstd.Decoder
}

func newSegmentReader(repoDir string, decryptor cipher.AEAD, decoder *zstd.Decoder) segmentReader {
	return segmentReader{
		dir:       filepath.Join(repoDir, "segments"),
		decryptor: decryptor,
		decoder:   decoder,
	}
}

func (r segmentReader) Read(ref ChunkRef) ([]byte, error) {
	file, err := os.Open(segmentPath(r.dir, ref.SegmentID))
	if err != nil {
		return nil, fmt.Errorf("open segment %d: %w", ref.SegmentID, err)
	}
	defer file.Close()

	record := make([]byte, ref.Length)
	if _, err := file.ReadAt(record, ref.Offset); err != nil {
		return nil, fmt.Errorf("read segment record: %w", err)
	}
	if len(record) < recordHeaderLen {
		return nil, errors.New("segment record is shorter than header")
	}
	prefix := record[:recordPrefixLen]
	storedChecksum := record[recordPrefixLen:recordHeaderLen]
	payload := record[recordHeaderLen:]
	if string(prefix[:8]) != recordMagic {
		return nil, errors.New("invalid segment record magic")
	}
	if headerLen := binary.LittleEndian.Uint16(prefix[8:10]); headerLen != recordHeaderLen {
		return nil, fmt.Errorf("unsupported record header length %d", headerLen)
	}
	if version := binary.LittleEndian.Uint16(prefix[10:12]); version != recordVersion {
		return nil, fmt.Errorf("unsupported record version %d", version)
	}
	flags := binary.LittleEndian.Uint32(prefix[12:16])
	if flags&(recordFlagZstd|recordFlagAES) != recordFlagZstd|recordFlagAES {
		return nil, fmt.Errorf("unsupported record flags %d", flags)
	}
	var hash [32]byte
	copy(hash[:], prefix[16:48])
	if hex.EncodeToString(hash[:]) != ref.Hash {
		return nil, errors.New("chunk hash does not match segment record")
	}
	originalSize := binary.LittleEndian.Uint64(prefix[48:56])
	compressedSize := binary.LittleEndian.Uint64(prefix[56:64])
	payloadSize := binary.LittleEndian.Uint64(prefix[64:72])
	if int(payloadSize) != len(payload) {
		return nil, fmt.Errorf("payload size mismatch: header=%d actual=%d", payloadSize, len(payload))
	}
	checksum := blake3.Sum256(append(copyBytes(prefix), payload...))
	if !bytes.Equal(checksum[:], storedChecksum) {
		return nil, errors.New("segment record checksum mismatch")
	}
	nonce := prefix[72:84]
	compressed, err := r.decryptor.Open(nil, nonce, payload, hash[:])
	if err != nil {
		return nil, fmt.Errorf("decrypt segment record: %w", err)
	}
	if uint64(len(compressed)) != compressedSize {
		return nil, fmt.Errorf("compressed size mismatch: header=%d actual=%d", compressedSize, len(compressed))
	}
	data, err := r.decoder.DecodeAll(compressed, make([]byte, 0, originalSize))
	if err != nil {
		return nil, fmt.Errorf("decompress segment record: %w", err)
	}
	if uint64(len(data)) != originalSize {
		return nil, fmt.Errorf("original size mismatch: header=%d actual=%d", originalSize, len(data))
	}
	actualHash := blake3.Sum256(data)
	if actualHash != hash {
		return nil, errors.New("decompressed chunk hash mismatch")
	}
	return data, nil
}

func newAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create AES cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create AES-GCM: %w", err)
	}
	return aead, nil
}

func listSegmentIDs(dir string) ([]int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("list segments: %w", err)
	}
	ids := make([]int64, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".seg") {
			continue
		}
		raw := strings.TrimSuffix(entry.Name(), ".seg")
		id, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

func segmentPath(dir string, id int64) string {
	return filepath.Join(dir, fmt.Sprintf("%016d.seg", id))
}

func segmentStats(repoDir string) (count int, bytes int64, err error) {
	dir := filepath.Join(repoDir, "segments")
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, fmt.Errorf("list segments: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".seg") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return 0, 0, fmt.Errorf("stat segment %q: %w", entry.Name(), err)
		}
		count++
		bytes += info.Size()
	}
	return count, bytes, nil
}
