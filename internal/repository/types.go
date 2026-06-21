package repository

import "time"

const (
	defaultSegmentSize  = int64(512 * 1024 * 1024)
	defaultChunkAvgSize = 1 * 1024 * 1024
)

type Options struct {
	StateDir     string
	RepoDir      string
	SegmentSize  int64
	ChunkAvgSize int
}

type ChunkRef struct {
	Hash           string    `json:"hash"`
	SegmentID      int64     `json:"segment_id"`
	Offset         int64     `json:"offset"`
	Length         int64     `json:"length"`
	OriginalSize   int64     `json:"original_size"`
	CompressedSize int64     `json:"compressed_size"`
	CreatedAt      time.Time `json:"created_at"`
}

type Stats struct {
	Segments          int   `json:"segments"`
	SegmentBytes      int64 `json:"segment_bytes"`
	Chunks            int64 `json:"chunks"`
	LogicalBytes      int64 `json:"logical_bytes"`
	CompressedBytes   int64 `json:"compressed_bytes"`
	SegmentSize       int64 `json:"segment_size"`
	ChunkAvgSize      int   `json:"chunk_avg_size"`
	ManifestCount     int   `json:"manifest_count"`
	AppendOnlyRecords bool  `json:"append_only_records"`
}

type CleanupStats struct {
	Count           int64 `json:"count"`
	LogicalBytes    int64 `json:"logical_bytes"`
	CompressedBytes int64 `json:"compressed_bytes"`
}
