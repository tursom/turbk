package repository

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
)

const repositoryKeySize = 32

func loadOrCreateMasterKey(stateDir string) ([]byte, error) {
	keyDir := filepath.Join(stateDir, "keys")
	keyPath := filepath.Join(keyDir, "repository.key")
	if err := os.MkdirAll(keyDir, 0o700); err != nil {
		return nil, fmt.Errorf("create key dir: %w", err)
	}
	if data, err := os.ReadFile(keyPath); err == nil {
		if len(data) != repositoryKeySize {
			return nil, fmt.Errorf("repository key %q has invalid size %d", keyPath, len(data))
		}
		return data, nil
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read repository key: %w", err)
	}

	key := make([]byte, repositoryKeySize)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("generate repository key: %w", err)
	}
	if err := os.WriteFile(keyPath, key, 0o600); err != nil {
		return nil, fmt.Errorf("write repository key: %w", err)
	}
	return key, nil
}
