package state

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const credentialKeySize = 32

var ErrAgentCredentialNotFound = errors.New("agent credential not found")

type CreateCredentialInput struct {
	Name    string          `json:"name"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type credentialEnvelope struct {
	Version    int    `json:"version"`
	Nonce      []byte `json:"nonce"`
	Ciphertext []byte `json:"ciphertext"`
}

type agentCredentialPayload struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret,omitempty"`
	SecretHash   string `json:"secret_hash"`
	Subject      string `json:"subject"`
}

func HashAgentSecret(clientID, clientSecret string) string {
	sum := sha256.Sum256([]byte(clientID + "\x00" + clientSecret))
	return hex.EncodeToString(sum[:])
}

func (s *Store) CreateCredential(ctx context.Context, input CreateCredentialInput) (Credential, error) {
	if input.Name == "" {
		return Credential{}, errors.New("credential name is required")
	}
	if input.Type == "" {
		return Credential{}, errors.New("credential type is required")
	}
	if len(input.Payload) == 0 || !json.Valid(input.Payload) {
		return Credential{}, errors.New("credential payload must be valid JSON")
	}
	encrypted, err := s.encryptCredentialPayload(input.Payload)
	if err != nil {
		return Credential{}, err
	}
	result, err := s.db.ExecContext(ctx, `
		INSERT INTO credentials (name, type, encrypted_payload)
		VALUES (?, ?, ?)`, input.Name, input.Type, encrypted)
	if err != nil {
		return Credential{}, fmt.Errorf("create credential: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return Credential{}, fmt.Errorf("get created credential id: %w", err)
	}
	return s.GetCredential(ctx, id)
}

func (s *Store) ListCredentials(ctx context.Context) ([]Credential, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, name, type, encrypted_payload, created_at, updated_at
		FROM credentials
		ORDER BY created_at DESC, id DESC
		LIMIT 200`)
	if err != nil {
		return nil, fmt.Errorf("list credentials: %w", err)
	}
	defer rows.Close()

	var credentials []Credential
	for rows.Next() {
		var credential Credential
		var encrypted []byte
		if err := rows.Scan(&credential.ID, &credential.Name, &credential.Type, &encrypted, &credential.CreatedAt, &credential.UpdatedAt); err != nil {
			return nil, err
		}
		if credential.Type == "agent" {
			payload, err := s.decryptCredentialPayload(encrypted)
			if err != nil {
				return nil, err
			}
			var cfg agentCredentialPayload
			if err := json.Unmarshal(payload, &cfg); err != nil {
				return nil, fmt.Errorf("decode agent credential %d: %w", credential.ID, err)
			}
			credential.ClientID = cfg.ClientID
			credential.ClientSecret = cfg.ClientSecret
		}
		credentials = append(credentials, credential)
	}
	return credentials, rows.Err()
}

func (s *Store) GetCredential(ctx context.Context, id int64) (Credential, error) {
	var credential Credential
	err := s.db.QueryRowContext(ctx, `
		SELECT id, name, type, created_at, updated_at
		FROM credentials
		WHERE id = ?`, id).
		Scan(&credential.ID, &credential.Name, &credential.Type, &credential.CreatedAt, &credential.UpdatedAt)
	if err == sql.ErrNoRows {
		return Credential{}, fmt.Errorf("credential %d not found", id)
	}
	if err != nil {
		return Credential{}, fmt.Errorf("get credential %d: %w", id, err)
	}
	return credential, nil
}

func (s *Store) GetCredentialPayload(ctx context.Context, id int64) (Credential, json.RawMessage, error) {
	var credential Credential
	var encrypted []byte
	err := s.db.QueryRowContext(ctx, `
		SELECT id, name, type, encrypted_payload, created_at, updated_at
		FROM credentials
		WHERE id = ?`, id).
		Scan(&credential.ID, &credential.Name, &credential.Type, &encrypted, &credential.CreatedAt, &credential.UpdatedAt)
	if err == sql.ErrNoRows {
		return Credential{}, nil, fmt.Errorf("credential %d not found", id)
	}
	if err != nil {
		return Credential{}, nil, fmt.Errorf("get credential payload %d: %w", id, err)
	}
	payload, err := s.decryptCredentialPayload(encrypted)
	if err != nil {
		return Credential{}, nil, err
	}
	return credential, payload, nil
}

func (s *Store) FindAgentCredentialByClientSecret(ctx context.Context, clientID, clientSecret string) (Credential, string, error) {
	if clientID == "" || clientSecret == "" {
		return Credential{}, "", ErrAgentCredentialNotFound
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, name, type, encrypted_payload, created_at, updated_at
		FROM credentials
		WHERE type = 'agent'
		ORDER BY id ASC`)
	if err != nil {
		return Credential{}, "", fmt.Errorf("list agent credentials: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var credential Credential
		var encrypted []byte
		if err := rows.Scan(&credential.ID, &credential.Name, &credential.Type, &encrypted, &credential.CreatedAt, &credential.UpdatedAt); err != nil {
			return Credential{}, "", err
		}
		payload, err := s.decryptCredentialPayload(encrypted)
		if err != nil {
			return Credential{}, "", err
		}
		var cfg agentCredentialPayload
		if err := json.Unmarshal(payload, &cfg); err != nil {
			return Credential{}, "", fmt.Errorf("decode agent credential %d: %w", credential.ID, err)
		}
		if cfg.ClientID == "" || cfg.SecretHash == "" {
			continue
		}
		if subtle.ConstantTimeCompare([]byte(clientID), []byte(cfg.ClientID)) != 1 {
			continue
		}
		if subtle.ConstantTimeCompare([]byte(HashAgentSecret(clientID, clientSecret)), []byte(cfg.SecretHash)) != 1 {
			continue
		}
		credential.ClientID = cfg.ClientID
		credential.ClientSecret = cfg.ClientSecret
		subject := cfg.Subject
		if subject == "" {
			subject = credential.Name
		}
		return credential, subject, nil
	}
	if err := rows.Err(); err != nil {
		return Credential{}, "", err
	}
	return Credential{}, "", ErrAgentCredentialNotFound
}

func (s *Store) encryptCredentialPayload(payload []byte) ([]byte, error) {
	aead, err := s.credentialAEAD()
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("generate credential nonce: %w", err)
	}
	envelope := credentialEnvelope{
		Version:    1,
		Nonce:      nonce,
		Ciphertext: aead.Seal(nil, nonce, payload, []byte("turbk-credential-v1")),
	}
	data, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("encode credential envelope: %w", err)
	}
	return data, nil
}

func (s *Store) decryptCredentialPayload(encrypted []byte) (json.RawMessage, error) {
	var envelope credentialEnvelope
	if err := json.Unmarshal(encrypted, &envelope); err != nil {
		return nil, fmt.Errorf("decode credential envelope: %w", err)
	}
	if envelope.Version != 1 {
		return nil, fmt.Errorf("unsupported credential envelope version %d", envelope.Version)
	}
	aead, err := s.credentialAEAD()
	if err != nil {
		return nil, err
	}
	payload, err := aead.Open(nil, envelope.Nonce, envelope.Ciphertext, []byte("turbk-credential-v1"))
	if err != nil {
		return nil, fmt.Errorf("decrypt credential payload: %w", err)
	}
	return json.RawMessage(payload), nil
}

func (s *Store) credentialAEAD() (cipher.AEAD, error) {
	key, err := s.loadOrCreateCredentialKey()
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create credential AES cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create credential AES-GCM: %w", err)
	}
	return aead, nil
}

func (s *Store) loadOrCreateCredentialKey() ([]byte, error) {
	keyDir := filepath.Join(s.stateDir, "keys")
	keyPath := filepath.Join(keyDir, "credentials.key")
	if err := os.MkdirAll(keyDir, 0o700); err != nil {
		return nil, fmt.Errorf("create credential key dir: %w", err)
	}
	if data, err := os.ReadFile(keyPath); err == nil {
		if len(data) != credentialKeySize {
			return nil, fmt.Errorf("credential key %q has invalid size %d", keyPath, len(data))
		}
		return data, nil
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read credential key: %w", err)
	}
	key := make([]byte, credentialKeySize)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("generate credential key: %w", err)
	}
	if err := os.WriteFile(keyPath, key, 0o600); err != nil {
		return nil, fmt.Errorf("write credential key: %w", err)
	}
	return key, nil
}

type Credential struct {
	ID           int64     `json:"id"`
	Name         string    `json:"name"`
	Type         string    `json:"type"`
	ClientID     string    `json:"client_id,omitempty"`
	ClientSecret string    `json:"client_secret,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}
