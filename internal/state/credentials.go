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
	"strings"
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

type CreateAgentHostInput struct {
	Name         string
	Payload      json.RawMessage
	ClientID     string
	ClientSecret string
	SecretHash   string
	Subject      string
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
		SELECT id, name, type, created_at, updated_at
		FROM credentials
		WHERE type != 'agent'
		ORDER BY created_at DESC, id DESC
		LIMIT 200`)
	if err != nil {
		return nil, fmt.Errorf("list credentials: %w", err)
	}
	defer rows.Close()

	credentials := make([]Credential, 0)
	for rows.Next() {
		var credential Credential
		if err := rows.Scan(&credential.ID, &credential.Name, &credential.Type, &credential.CreatedAt, &credential.UpdatedAt); err != nil {
			return nil, err
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

func (s *Store) CreateAgentHost(ctx context.Context, input CreateAgentHostInput) (Host, Credential, error) {
	input.Name = strings.TrimSpace(input.Name)
	input.ClientID = strings.TrimSpace(input.ClientID)
	input.SecretHash = strings.TrimSpace(input.SecretHash)
	input.Subject = strings.TrimSpace(input.Subject)
	if input.Name == "" {
		return Host{}, Credential{}, errors.New("host name is required")
	}
	if input.Subject == "" {
		input.Subject = input.Name
	}
	if input.ClientID == "" {
		return Host{}, Credential{}, errors.New("agent client_id is required")
	}
	if input.ClientSecret == "" {
		return Host{}, Credential{}, errors.New("agent client_secret is required")
	}
	if input.SecretHash == "" {
		return Host{}, Credential{}, errors.New("agent secret_hash is required")
	}
	if len(input.Payload) == 0 || !json.Valid(input.Payload) {
		return Host{}, Credential{}, errors.New("agent credential payload must be valid JSON")
	}
	encrypted, err := s.encryptCredentialPayload(input.Payload)
	if err != nil {
		return Host{}, Credential{}, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Host{}, Credential{}, fmt.Errorf("begin agent host transaction: %w", err)
	}
	defer tx.Rollback()

	credentialResult, err := tx.ExecContext(ctx, `
		INSERT INTO credentials (name, type, encrypted_payload)
		VALUES (?, 'agent', ?)`, input.Name, encrypted)
	if err != nil {
		return Host{}, Credential{}, fmt.Errorf("create agent credential: %w", err)
	}
	credentialID, err := credentialResult.LastInsertId()
	if err != nil {
		return Host{}, Credential{}, fmt.Errorf("get created agent credential id: %w", err)
	}
	hostResult, err := tx.ExecContext(ctx, `
		INSERT INTO hosts (name, source_type, credential_id, status)
		VALUES (?, 'agent', ?, 'unknown')`, input.Name, credentialID)
	if err != nil {
		return Host{}, Credential{}, fmt.Errorf("create agent host: %w", err)
	}
	hostID, err := hostResult.LastInsertId()
	if err != nil {
		return Host{}, Credential{}, fmt.Errorf("get created agent host id: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agent_credentials (credential_id, host_id, client_id, secret_hash, subject)
		VALUES (?, ?, ?, ?, ?)`,
		credentialID, hostID, input.ClientID, input.SecretHash, input.Subject); err != nil {
		return Host{}, Credential{}, fmt.Errorf("index agent credential: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Host{}, Credential{}, fmt.Errorf("commit agent host transaction: %w", err)
	}

	host, err := s.GetHost(ctx, hostID)
	if err != nil {
		return Host{}, Credential{}, err
	}
	credential, err := s.GetCredential(ctx, credentialID)
	if err != nil {
		return Host{}, Credential{}, err
	}
	if _, err := s.BumpConfigGeneration(ctx); err != nil {
		return Host{}, Credential{}, err
	}
	credential.ClientID = input.ClientID
	credential.ClientSecret = input.ClientSecret
	credential.Subject = input.Subject
	return host, credential, nil
}

func (s *Store) GetAgentCredentialForHost(ctx context.Context, hostID int64) (AgentCredential, bool, error) {
	var agent AgentCredential
	var encrypted []byte
	err := s.db.QueryRowContext(ctx, `
		SELECT ac.credential_id, ac.host_id, ac.client_id, ac.subject, ac.created_at, ac.updated_at, ac.last_used_at, ac.revoked_at,
			c.encrypted_payload
		FROM agent_credentials ac
		JOIN credentials c ON c.id = ac.credential_id
		WHERE ac.host_id = ?`, hostID).
		Scan(&agent.CredentialID, &agent.HostID, &agent.ClientID, &agent.Subject, &agent.CreatedAt, &agent.UpdatedAt, &agent.LastUsedAt, &agent.RevokedAt, &encrypted)
	if err == sql.ErrNoRows {
		return AgentCredential{}, false, nil
	}
	if err != nil {
		return AgentCredential{}, false, fmt.Errorf("get agent credential for host %d: %w", hostID, err)
	}
	payload, err := s.decryptCredentialPayload(encrypted)
	if err != nil {
		return AgentCredential{}, false, err
	}
	var cfg agentCredentialPayload
	if err := json.Unmarshal(payload, &cfg); err != nil {
		return AgentCredential{}, false, fmt.Errorf("decode agent credential %d: %w", agent.CredentialID, err)
	}
	agent.ClientSecret = cfg.ClientSecret
	if agent.Subject == "" {
		agent.Subject = cfg.Subject
	}
	return agent, true, nil
}

func (s *Store) FindAgentCredentialByClientSecret(ctx context.Context, clientID, clientSecret string) (AgentCredentialAuth, error) {
	if clientID == "" || clientSecret == "" {
		return AgentCredentialAuth{}, ErrAgentCredentialNotFound
	}
	var auth AgentCredentialAuth
	var secretHash string
	err := s.db.QueryRowContext(ctx, `
		SELECT c.id, c.name, c.type, c.created_at, c.updated_at,
			ac.host_id, ac.client_id, ac.secret_hash, ac.subject, ac.revoked_at
		FROM agent_credentials ac
		JOIN credentials c ON c.id = ac.credential_id
		WHERE ac.client_id = ? AND c.type = 'agent'`, clientID).
		Scan(&auth.Credential.ID, &auth.Credential.Name, &auth.Credential.Type, &auth.Credential.CreatedAt, &auth.Credential.UpdatedAt,
			&auth.HostID, &auth.ClientID, &secretHash, &auth.Subject, &auth.RevokedAt)
	if err == sql.ErrNoRows {
		return AgentCredentialAuth{}, ErrAgentCredentialNotFound
	}
	if err != nil {
		return AgentCredentialAuth{}, fmt.Errorf("find agent credential: %w", err)
	}
	if auth.RevokedAt.Valid {
		return AgentCredentialAuth{}, ErrAgentCredentialNotFound
	}
	if subtle.ConstantTimeCompare([]byte(HashAgentSecret(clientID, clientSecret)), []byte(secretHash)) != 1 {
		return AgentCredentialAuth{}, ErrAgentCredentialNotFound
	}
	auth.Credential.ClientID = auth.ClientID
	auth.Credential.Subject = auth.Subject
	if auth.Subject == "" {
		auth.Subject = auth.Credential.Name
		auth.Credential.Subject = auth.Subject
	}
	if _, err := s.db.ExecContext(ctx, `
		UPDATE agent_credentials
		SET last_used_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
		WHERE credential_id = ?`, auth.Credential.ID); err != nil {
		return AgentCredentialAuth{}, fmt.Errorf("update agent credential usage: %w", err)
	}
	return auth, nil
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
	Subject      string    `json:"subject,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type CredentialSummary struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
	Type string `json:"type"`
}

type AgentCredential struct {
	CredentialID int64        `json:"credential_id"`
	HostID       int64        `json:"host_id"`
	ClientID     string       `json:"client_id"`
	ClientSecret string       `json:"client_secret,omitempty"`
	Subject      string       `json:"subject,omitempty"`
	CreatedAt    time.Time    `json:"created_at"`
	UpdatedAt    time.Time    `json:"updated_at"`
	LastUsedAt   sql.NullTime `json:"last_used_at"`
	RevokedAt    sql.NullTime `json:"revoked_at"`
}

type AgentCredentialAuth struct {
	Credential Credential
	HostID     int64
	ClientID   string
	Subject    string
	RevokedAt  sql.NullTime
}
