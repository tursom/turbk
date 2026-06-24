package repository

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"
)

const PackFormatTBKPack1 = "TBKPACK1"

var PackMagicTBKPack1 = []byte("TBKPACK1\n")

type PackFilePayload struct {
	Path    string
	Mode    uint32
	ModTime time.Time
	Data    []byte
}

type PackFileIndex struct {
	Path         string
	OriginalSize int64
	Offset       int64
	Length       int64
	Mode         uint32
	MTimeNS      int64
}

func EncodePack(files []PackFilePayload) ([]byte, []PackFileIndex, error) {
	if len(files) == 0 {
		return nil, nil, errors.New("pack has no files")
	}
	if len(files) > int(^uint32(0)) {
		return nil, nil, errors.New("pack file count exceeds u32")
	}
	headerLen := int64(len(PackMagicTBKPack1) + 4)
	indexes := make([]PackFileIndex, 0, len(files))
	for _, file := range files {
		path := cleanManifestPath(file.Path)
		if path == "." || path != file.Path {
			return nil, nil, fmt.Errorf("invalid pack file path %q", file.Path)
		}
		if len(file.Data) == 0 {
			return nil, nil, fmt.Errorf("pack file %q is empty", file.Path)
		}
		if len(path) > int(^uint32(0)) {
			return nil, nil, fmt.Errorf("pack file path %q is too long", file.Path)
		}
		headerLen += int64(4 + len(path) + 8 + 8 + 8 + 4 + 8)
		indexes = append(indexes, PackFileIndex{
			Path:         path,
			OriginalSize: int64(len(file.Data)),
			Length:       int64(len(file.Data)),
			Mode:         file.Mode,
			MTimeNS:      file.ModTime.UnixNano(),
		})
	}
	offset := headerLen
	for i := range indexes {
		indexes[i].Offset = offset
		offset += indexes[i].Length
	}

	var buf bytes.Buffer
	buf.Grow(int(offset))
	buf.Write(PackMagicTBKPack1)
	if err := binary.Write(&buf, binary.BigEndian, uint32(len(files))); err != nil {
		return nil, nil, err
	}
	for i, index := range indexes {
		if err := binary.Write(&buf, binary.BigEndian, uint32(len(index.Path))); err != nil {
			return nil, nil, err
		}
		buf.WriteString(index.Path)
		for _, value := range []uint64{
			uint64(index.OriginalSize),
			uint64(index.Offset),
			uint64(index.Length),
		} {
			if err := binary.Write(&buf, binary.BigEndian, value); err != nil {
				return nil, nil, err
			}
		}
		if err := binary.Write(&buf, binary.BigEndian, index.Mode); err != nil {
			return nil, nil, err
		}
		if err := binary.Write(&buf, binary.BigEndian, index.MTimeNS); err != nil {
			return nil, nil, err
		}
		if files[i].Path != index.Path {
			return nil, nil, fmt.Errorf("pack file path changed during encode: %q", files[i].Path)
		}
	}
	for _, file := range files {
		buf.Write(file.Data)
	}
	return buf.Bytes(), indexes, nil
}

func DecodePackIndex(data []byte) ([]PackFileIndex, error) {
	reader := bytes.NewReader(data)
	magic := make([]byte, len(PackMagicTBKPack1))
	if _, err := io.ReadFull(reader, magic); err != nil {
		return nil, fmt.Errorf("read pack magic: %w", err)
	}
	if !bytes.Equal(magic, PackMagicTBKPack1) {
		return nil, errors.New("pack magic is invalid")
	}
	var count uint32
	if err := binary.Read(reader, binary.BigEndian, &count); err != nil {
		return nil, fmt.Errorf("read pack file count: %w", err)
	}
	if count == 0 {
		return nil, errors.New("pack has no files")
	}
	indexes := make([]PackFileIndex, 0, int(count))
	for i := uint32(0); i < count; i++ {
		var pathLen uint32
		if err := binary.Read(reader, binary.BigEndian, &pathLen); err != nil {
			return nil, fmt.Errorf("read pack file %d path length: %w", i, err)
		}
		if pathLen == 0 || uint64(pathLen) > uint64(reader.Len()) {
			return nil, fmt.Errorf("pack file %d path length is invalid", i)
		}
		pathBytes := make([]byte, int(pathLen))
		if _, err := io.ReadFull(reader, pathBytes); err != nil {
			return nil, fmt.Errorf("read pack file %d path: %w", i, err)
		}
		path := string(pathBytes)
		if cleanManifestPath(path) != path || path == "." {
			return nil, fmt.Errorf("pack file %d path %q is invalid", i, path)
		}
		var originalSize uint64
		var offset uint64
		var length uint64
		var mode uint32
		var mtime int64
		for _, item := range []struct {
			name string
			dst  *uint64
		}{
			{"original size", &originalSize},
			{"offset", &offset},
			{"length", &length},
		} {
			if err := binary.Read(reader, binary.BigEndian, item.dst); err != nil {
				return nil, fmt.Errorf("read pack file %d %s: %w", i, item.name, err)
			}
		}
		if err := binary.Read(reader, binary.BigEndian, &mode); err != nil {
			return nil, fmt.Errorf("read pack file %d mode: %w", i, err)
		}
		if err := binary.Read(reader, binary.BigEndian, &mtime); err != nil {
			return nil, fmt.Errorf("read pack file %d mtime: %w", i, err)
		}
		if originalSize != length {
			return nil, fmt.Errorf("pack file %q original size %d does not match data length %d", path, originalSize, length)
		}
		if offset > uint64(len(data)) || length > uint64(len(data)) || offset+length > uint64(len(data)) || offset+length < offset {
			return nil, fmt.Errorf("pack file %q data range is out of bounds", path)
		}
		if offset > uint64(^uint(0)>>1) || length > uint64(^uint(0)>>1) {
			return nil, fmt.Errorf("pack file %q data range is too large", path)
		}
		indexes = append(indexes, PackFileIndex{
			Path:         path,
			OriginalSize: int64(originalSize),
			Offset:       int64(offset),
			Length:       int64(length),
			Mode:         mode,
			MTimeNS:      mtime,
		})
	}
	dataStart := int64(len(data) - reader.Len())
	for _, index := range indexes {
		if index.Offset < dataStart {
			return nil, fmt.Errorf("pack file %q data offset points into header", index.Path)
		}
	}
	return indexes, nil
}

func PackFileBytes(data []byte, ref PackFileRef) ([]byte, error) {
	if ref.Offset < 0 || ref.Length < 0 || ref.Offset+ref.Length < ref.Offset || ref.Offset+ref.Length > int64(len(data)) {
		return nil, fmt.Errorf("pack file %q range is out of bounds", ref.ID)
	}
	out := data[ref.Offset : ref.Offset+ref.Length]
	return out, nil
}

func (m *SnapshotManifest) FindPack(id string) (PackManifest, bool) {
	if m == nil {
		return PackManifest{}, false
	}
	for _, pack := range m.Packs {
		if pack.ID == id {
			return pack, true
		}
	}
	return PackManifest{}, false
}

func (r *Repository) ReadPackData(ctx context.Context, pack PackManifest) ([]byte, error) {
	var data []byte
	for _, ref := range pack.Chunks {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		chunk, err := r.ReadChunkRef(ctx, ref)
		if err != nil {
			return nil, err
		}
		data = append(data, chunk...)
	}
	return data, nil
}

func (r *Repository) ReadPackedFile(ctx context.Context, manifest *SnapshotManifest, entry FileEntry) ([]byte, error) {
	if entry.Type != EntryTypePackedFile {
		return nil, fmt.Errorf("manifest entry %q is not a packed file", entry.Path)
	}
	if entry.Pack == nil {
		return nil, fmt.Errorf("packed file %q has no pack reference", entry.Path)
	}
	pack, ok := manifest.FindPack(entry.Pack.ID)
	if !ok {
		return nil, fmt.Errorf("packed file %q references missing pack %q", entry.Path, entry.Pack.ID)
	}
	data, err := r.ReadPackData(ctx, pack)
	if err != nil {
		return nil, err
	}
	indexes, err := DecodePackIndex(data)
	if err != nil {
		return nil, err
	}
	found := false
	for _, index := range indexes {
		if index.Path != entry.Path {
			continue
		}
		found = true
		if index.Offset != entry.Pack.Offset || index.Length != entry.Pack.Length {
			return nil, fmt.Errorf("packed file %q range does not match pack index", entry.Path)
		}
		break
	}
	if !found {
		return nil, fmt.Errorf("packed file %q not found in pack %q", entry.Path, entry.Pack.ID)
	}
	fileData, err := PackFileBytes(data, *entry.Pack)
	if err != nil {
		return nil, err
	}
	if int64(len(fileData)) != entry.Size {
		return nil, fmt.Errorf("packed file %q size %d does not match bytes %d", entry.Path, entry.Size, len(fileData))
	}
	return fileData, nil
}
