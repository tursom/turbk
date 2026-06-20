package repository

import (
	"encoding/hex"

	"github.com/zeebo/blake3"
)

func HashBytes(data []byte) (string, error) {
	sum := blake3.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}
