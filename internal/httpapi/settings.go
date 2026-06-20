package httpapi

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/turbk/turbk/internal/config"
	"github.com/turbk/turbk/internal/state"
	"golang.org/x/crypto/bcrypt"
)

const (
	settingAuthUsername        = "auth.username"
	settingAuthPasswordHash    = "auth.password_hash"
	settingAuthSessionTTLHours = "auth.session_ttl_hours"
	settingRetentionKeepLast   = "retention.keep_last"
	settingRetentionKeepDaily  = "retention.keep_daily"
	settingRetentionKeepWeekly = "retention.keep_weekly"
)

type runtimeSettings struct {
	AuthUsername     string
	AuthPassword     string
	AuthPasswordHash string
	SessionTTLHours  int
	Retention        config.RetentionConfig
}

func defaultRuntimeSettings(cfg config.Config) runtimeSettings {
	return runtimeSettings{
		AuthUsername:    cfg.Auth.Username,
		AuthPassword:    cfg.Auth.Password,
		SessionTTLHours: cfg.Auth.SessionTTLHours,
		Retention:       cfg.Retention,
	}
}

func loadRuntimeSettings(ctx context.Context, cfg config.Config, store *state.Store, logger *slog.Logger) runtimeSettings {
	settings := defaultRuntimeSettings(cfg)
	if store == nil {
		return settings
	}
	values, err := store.LoadSettings(ctx)
	if err != nil {
		if logger != nil {
			logger.Warn("load runtime settings", "error", err)
		}
		return settings
	}
	applyRuntimeSettingsMap(&settings, values)
	return settings
}

func applyRuntimeSettingsMap(settings *runtimeSettings, values map[string]string) {
	if value := strings.TrimSpace(values[settingAuthUsername]); value != "" {
		settings.AuthUsername = value
	}
	if value := strings.TrimSpace(values[settingAuthPasswordHash]); value != "" {
		settings.AuthPasswordHash = value
	}
	applyPositiveInt(values, settingAuthSessionTTLHours, &settings.SessionTTLHours)
	applyPositiveInt(values, settingRetentionKeepLast, &settings.Retention.KeepLast)
	applyNonNegativeInt(values, settingRetentionKeepDaily, &settings.Retention.KeepDaily)
	applyNonNegativeInt(values, settingRetentionKeepWeekly, &settings.Retention.KeepWeekly)
}

func applyPositiveInt(values map[string]string, key string, dst *int) {
	if value := strings.TrimSpace(values[key]); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			*dst = parsed
		}
	}
}

func applyNonNegativeInt(values map[string]string, key string, dst *int) {
	if value := strings.TrimSpace(values[key]); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed >= 0 {
			*dst = parsed
		}
	}
}

func (s *Server) currentSettings() runtimeSettings {
	s.settingsMu.RLock()
	defer s.settingsMu.RUnlock()
	return s.settings
}

func (s *Server) replaceSettings(settings runtimeSettings) {
	s.settingsMu.Lock()
	defer s.settingsMu.Unlock()
	s.settings = settings
}

func (s *Server) verifyAdminCredentials(username, password string) bool {
	settings := s.currentSettings()
	if settings.AuthUsername == "" || subtleCompare(username, settings.AuthUsername) != 1 {
		return false
	}
	if settings.AuthPasswordHash != "" {
		return bcrypt.CompareHashAndPassword([]byte(settings.AuthPasswordHash), []byte(password)) == nil
	}
	return subtleCompare(password, settings.AuthPassword) == 1
}

func subtleCompare(left, right string) int {
	return subtle.ConstantTimeCompare([]byte(left), []byte(right))
}

func (s *Server) handleSettings(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"settings": s.settingsView()})
}

func (s *Server) settingsView() map[string]any {
	settings := s.currentSettings()
	return map[string]any{
		"auth": map[string]any{
			"username":          settings.AuthUsername,
			"session_ttl_hours": settings.SessionTTLHours,
		},
		"retention": settings.Retention,
	}
}

func (s *Server) handleUpdateSettings(w http.ResponseWriter, r *http.Request) {
	var req struct {
		AdminUsername   *string `json:"admin_username"`
		CurrentPassword string  `json:"current_password"`
		AdminPassword   *string `json:"admin_password"`
		SessionTTLHours *int    `json:"session_ttl_hours"`
		Retention       *struct {
			KeepLast   *int `json:"keep_last"`
			KeepDaily  *int `json:"keep_daily"`
			KeepWeekly *int `json:"keep_weekly"`
		} `json:"retention"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	next := s.currentSettings()
	updates := make(map[string]string)
	if req.AdminUsername != nil {
		username := strings.TrimSpace(*req.AdminUsername)
		if username == "" {
			writeError(w, http.StatusBadRequest, errors.New("admin_username is required"))
			return
		}
		next.AuthUsername = username
		updates[settingAuthUsername] = username
	}
	if req.SessionTTLHours != nil {
		if *req.SessionTTLHours <= 0 {
			writeError(w, http.StatusBadRequest, errors.New("session_ttl_hours must be positive"))
			return
		}
		next.SessionTTLHours = *req.SessionTTLHours
		updates[settingAuthSessionTTLHours] = strconv.Itoa(*req.SessionTTLHours)
	}
	if req.AdminPassword != nil {
		password := *req.AdminPassword
		if len(password) < 8 {
			writeError(w, http.StatusBadRequest, errors.New("admin_password must be at least 8 characters"))
			return
		}
		if !s.verifyAdminCredentials(next.AuthUsername, req.CurrentPassword) && !s.verifyAdminCredentials(s.currentSettings().AuthUsername, req.CurrentPassword) {
			writeError(w, http.StatusUnauthorized, errors.New("current_password is invalid"))
			return
		}
		hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Errorf("hash admin password: %w", err))
			return
		}
		next.AuthPassword = ""
		next.AuthPasswordHash = string(hash)
		updates[settingAuthPasswordHash] = next.AuthPasswordHash
	}
	if req.Retention != nil {
		if req.Retention.KeepLast != nil {
			if *req.Retention.KeepLast <= 0 {
				writeError(w, http.StatusBadRequest, errors.New("retention.keep_last must be positive"))
				return
			}
			next.Retention.KeepLast = *req.Retention.KeepLast
			updates[settingRetentionKeepLast] = strconv.Itoa(*req.Retention.KeepLast)
		}
		if req.Retention.KeepDaily != nil {
			if *req.Retention.KeepDaily < 0 {
				writeError(w, http.StatusBadRequest, errors.New("retention.keep_daily must be non-negative"))
				return
			}
			next.Retention.KeepDaily = *req.Retention.KeepDaily
			updates[settingRetentionKeepDaily] = strconv.Itoa(*req.Retention.KeepDaily)
		}
		if req.Retention.KeepWeekly != nil {
			if *req.Retention.KeepWeekly < 0 {
				writeError(w, http.StatusBadRequest, errors.New("retention.keep_weekly must be non-negative"))
				return
			}
			next.Retention.KeepWeekly = *req.Retention.KeepWeekly
			updates[settingRetentionKeepWeekly] = strconv.Itoa(*req.Retention.KeepWeekly)
		}
	}
	if err := s.store.UpsertSettings(r.Context(), updates); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	s.replaceSettings(next)
	writeJSON(w, http.StatusOK, map[string]any{"settings": s.settingsView()})
}
