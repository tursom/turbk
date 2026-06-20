package httpapi

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
)

const sessionCookieName = "turbk_session"

func (s *Server) handleAuthLogin(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if !s.verifyAdminCredentials(req.Username, req.Password) {
		writeError(w, http.StatusUnauthorized, errors.New("invalid username or password"))
		return
	}
	settings := s.currentSettings()
	token, err := newSessionToken()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	expiresAt := time.Now().UTC().Add(time.Duration(settings.SessionTTLHours) * time.Hour)
	session, err := s.store.CreateWebSession(r.Context(), token, settings.AuthUsername, expiresAt)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	_ = s.store.DeleteExpiredWebSessions(r.Context(), time.Now().UTC())
	http.SetCookie(w, s.sessionCookie(token, expiresAt))
	writeJSON(w, http.StatusOK, map[string]any{
		"status":     "authenticated",
		"user":       session.Username,
		"expires_at": session.ExpiresAt,
	})
}

func (s *Server) handleAuthLogout(w http.ResponseWriter, r *http.Request) {
	if cookie, err := r.Cookie(sessionCookieName); err == nil {
		_ = s.store.DeleteWebSession(r.Context(), cookie.Value)
	}
	http.SetCookie(w, s.expiredSessionCookie())
	writeJSON(w, http.StatusOK, map[string]any{"status": "logged_out"})
}

func (s *Server) handleAuthSession(w http.ResponseWriter, r *http.Request) {
	session, ok := s.webSession(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, errors.New("authentication required"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":     "authenticated",
		"user":       session.Username,
		"expires_at": session.ExpiresAt,
	})
}

func (s *Server) withManagementAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !requiresManagementAuth(r) {
			next.ServeHTTP(w, r)
			return
		}
		if _, ok := s.webSession(r); !ok {
			writeError(w, http.StatusUnauthorized, errors.New("authentication required"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func requiresManagementAuth(r *http.Request) bool {
	path := r.URL.Path
	if strings.HasPrefix(path, "/agent/") {
		return false
	}
	if !strings.HasPrefix(path, "/api/") {
		return false
	}
	if r.Method == http.MethodGet && path == "/api/v1/health" {
		return false
	}
	if path == "/api/v1/auth/login" || path == "/api/v1/auth/logout" || path == "/api/v1/auth/session" {
		return false
	}
	return true
}

func (s *Server) webSession(r *http.Request) (webSessionView, bool) {
	cookie, err := r.Cookie(sessionCookieName)
	if err != nil || cookie.Value == "" {
		return webSessionView{}, false
	}
	session, ok, err := s.store.GetWebSession(r.Context(), cookie.Value, time.Now().UTC())
	if err != nil || !ok {
		return webSessionView{}, false
	}
	return webSessionView{Username: session.Username, ExpiresAt: session.ExpiresAt}, true
}

type webSessionView struct {
	Username  string
	ExpiresAt time.Time
}

func (s *Server) sessionCookie(token string, expiresAt time.Time) *http.Cookie {
	return &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		Expires:  expiresAt,
		MaxAge:   int(time.Until(expiresAt).Seconds()),
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   strings.HasPrefix(strings.ToLower(s.cfg.Server.PublicURL), "https://"),
	}
}

func (s *Server) expiredSessionCookie() *http.Cookie {
	return &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		Expires:  time.Unix(0, 0),
		MaxAge:   -1,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   strings.HasPrefix(strings.ToLower(s.cfg.Server.PublicURL), "https://"),
	}
}

func newSessionToken() (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}
