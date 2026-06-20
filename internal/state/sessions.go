package state

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"
)

type WebSession struct {
	ID        int64     `json:"id"`
	Username  string    `json:"username"`
	ExpiresAt time.Time `json:"expires_at"`
	CreatedAt time.Time `json:"created_at"`
}

func (s *Store) CreateWebSession(ctx context.Context, token, username string, expiresAt time.Time) (WebSession, error) {
	if token == "" {
		return WebSession{}, fmt.Errorf("session token is required")
	}
	if username == "" {
		return WebSession{}, fmt.Errorf("session username is required")
	}
	hash := sessionTokenHash(token)
	result, err := s.db.ExecContext(ctx, `
		INSERT INTO web_sessions (token_hash, username, expires_at)
		VALUES (?, ?, ?)`, hash, username, expiresAt.UTC())
	if err != nil {
		return WebSession{}, fmt.Errorf("create web session: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return WebSession{}, fmt.Errorf("get created web session id: %w", err)
	}
	return s.GetWebSessionByID(ctx, id)
}

func (s *Store) GetWebSession(ctx context.Context, token string, now time.Time) (WebSession, bool, error) {
	if token == "" {
		return WebSession{}, false, nil
	}
	hash := sessionTokenHash(token)
	var session WebSession
	err := s.db.QueryRowContext(ctx, `
		SELECT id, username, expires_at, created_at
		FROM web_sessions
		WHERE token_hash = ? AND expires_at > ?`, hash, now.UTC()).
		Scan(&session.ID, &session.Username, &session.ExpiresAt, &session.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return WebSession{}, false, nil
		}
		return WebSession{}, false, fmt.Errorf("get web session: %w", err)
	}
	return session, true, nil
}

func (s *Store) GetWebSessionByID(ctx context.Context, id int64) (WebSession, error) {
	var session WebSession
	err := s.db.QueryRowContext(ctx, `
		SELECT id, username, expires_at, created_at
		FROM web_sessions
		WHERE id = ?`, id).
		Scan(&session.ID, &session.Username, &session.ExpiresAt, &session.CreatedAt)
	if err != nil {
		return WebSession{}, fmt.Errorf("get web session %d: %w", id, err)
	}
	return session, nil
}

func (s *Store) DeleteWebSession(ctx context.Context, token string) error {
	if token == "" {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM web_sessions WHERE token_hash = ?`, sessionTokenHash(token))
	if err != nil {
		return fmt.Errorf("delete web session: %w", err)
	}
	return nil
}

func (s *Store) DeleteExpiredWebSessions(ctx context.Context, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM web_sessions WHERE expires_at <= ?`, now.UTC())
	if err != nil {
		return fmt.Errorf("delete expired web sessions: %w", err)
	}
	return nil
}

func sessionTokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
