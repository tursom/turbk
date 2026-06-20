package repository

import (
	"bytes"
	"fmt"
	"io"
)

type Chunker struct {
	MinSize int
	AvgSize int
	MaxSize int
	Mask    uint64
	table   [256]uint64
}

func NewChunker(avgSize int) Chunker {
	if avgSize <= 0 {
		avgSize = defaultChunkAvgSize
	}
	minSize := avgSize / 4
	if minSize < 64*1024 {
		minSize = min(avgSize, 64*1024)
	}
	maxSize := avgSize * 4
	if maxSize < avgSize {
		maxSize = avgSize
	}
	c := Chunker{
		MinSize: minSize,
		AvgSize: avgSize,
		MaxSize: maxSize,
		Mask:    uint64(nextPowerOfTwo(avgSize) - 1),
		table:   gearTable(),
	}
	if c.Mask == 0 {
		c.Mask = uint64(avgSize - 1)
	}
	return c
}

func (c Chunker) Split(r io.Reader, emit func([]byte) error) error {
	buf := make([]byte, 32*1024)
	chunk := bytes.NewBuffer(make([]byte, 0, c.AvgSize))
	var fingerprint uint64

	for {
		n, readErr := r.Read(buf)
		for _, b := range buf[:n] {
			chunk.WriteByte(b)
			fingerprint = (fingerprint << 1) + c.table[b]
			size := chunk.Len()
			if size >= c.MaxSize || (size >= c.MinSize && fingerprint&c.Mask == 0) {
				if err := emit(copyBytes(chunk.Bytes())); err != nil {
					return err
				}
				chunk.Reset()
				fingerprint = 0
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read chunk source: %w", readErr)
		}
	}
	if chunk.Len() > 0 {
		if err := emit(copyBytes(chunk.Bytes())); err != nil {
			return err
		}
	}
	return nil
}

func copyBytes(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func nextPowerOfTwo(value int) int {
	out := 1
	for out < value {
		out <<= 1
	}
	return out
}

func gearTable() [256]uint64 {
	var table [256]uint64
	var x uint64 = 0x9e3779b97f4a7c15
	for i := range table {
		x ^= x >> 12
		x ^= x << 25
		x ^= x >> 27
		table[i] = x * 0x2545f4914f6cdd1d
	}
	return table
}
