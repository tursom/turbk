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
	"time"

	"github.com/tursom/turbk/internal/config"
	"github.com/tursom/turbk/internal/state"
	"golang.org/x/crypto/bcrypt"
)

const (
	settingAuthUsername                       = "auth.username"
	settingAuthPasswordHash                   = "auth.password_hash"
	settingAuthSessionTTLHours                = "auth.session_ttl_hours"
	settingRetentionKeepLast                  = "retention.keep_last"
	settingRetentionKeepDaily                 = "retention.keep_daily"
	settingRetentionKeepWeekly                = "retention.keep_weekly"
	settingMaintenanceEnabled                 = "maintenance.enabled"
	settingMaintenanceTimezone                = "maintenance.timezone"
	settingMaintenanceCleanupSchedule         = "maintenance.cleanup_schedule"
	settingMaintenanceCompactEnabled          = "maintenance.compact_enabled"
	settingMaintenanceCompactSchedule         = "maintenance.compact_schedule"
	settingMaintenanceErrorGracePeriod        = "maintenance.error_grace_period"
	settingMaintenanceStaleRunAfter           = "maintenance.stale_run_after"
	settingMaintenanceKeepDeletedMetadataDays = "maintenance.keep_deleted_metadata_days"
	settingMaintenanceCompactMinReclaimRatio  = "maintenance.compact_min_reclaim_ratio"
	settingMaintenanceCompactMinReclaimBytes  = "maintenance.compact_min_reclaim_bytes"
)

type runtimeSettings struct {
	AuthUsername     string
	AuthPassword     string
	AuthPasswordHash string
	SessionTTLHours  int
	Retention        config.RetentionConfig
	Maintenance      config.MaintenanceConfig
}

func defaultRuntimeSettings(cfg config.Config) runtimeSettings {
	return runtimeSettings{
		AuthUsername:    cfg.Auth.Username,
		AuthPassword:    cfg.Auth.Password,
		SessionTTLHours: cfg.Auth.SessionTTLHours,
		Retention:       cfg.Retention,
		Maintenance:     cfg.Maintenance,
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
	applyBoolSetting(values, settingMaintenanceEnabled, &settings.Maintenance.Enabled)
	if value := strings.TrimSpace(values[settingMaintenanceTimezone]); value != "" {
		settings.Maintenance.Timezone = value
	}
	if value := strings.TrimSpace(values[settingMaintenanceCleanupSchedule]); value != "" {
		settings.Maintenance.CleanupSchedule = value
	}
	applyBoolSetting(values, settingMaintenanceCompactEnabled, &settings.Maintenance.CompactEnabled)
	if value := strings.TrimSpace(values[settingMaintenanceCompactSchedule]); value != "" {
		settings.Maintenance.CompactSchedule = value
	}
	if value := strings.TrimSpace(values[settingMaintenanceErrorGracePeriod]); value != "" {
		settings.Maintenance.ErrorGracePeriod = value
	}
	if value := strings.TrimSpace(values[settingMaintenanceStaleRunAfter]); value != "" {
		settings.Maintenance.StaleRunAfter = value
	}
	applyNonNegativeInt(values, settingMaintenanceKeepDeletedMetadataDays, &settings.Maintenance.KeepDeletedMetadataDays)
	applyNonNegativeFloat(values, settingMaintenanceCompactMinReclaimRatio, &settings.Maintenance.CompactMinReclaimRatio)
	if value := strings.TrimSpace(values[settingMaintenanceCompactMinReclaimBytes]); value != "" {
		settings.Maintenance.CompactMinReclaimBytes = value
	}
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

func applyBoolSetting(values map[string]string, key string, dst *bool) {
	if value := strings.TrimSpace(values[key]); value != "" {
		if parsed, err := strconv.ParseBool(value); err == nil {
			*dst = parsed
		}
	}
}

func applyNonNegativeFloat(values map[string]string, key string, dst *float64) {
	if value := strings.TrimSpace(values[key]); value != "" {
		if parsed, err := strconv.ParseFloat(value, 64); err == nil && parsed >= 0 {
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
		"retention":   settings.Retention,
		"maintenance": settings.Maintenance,
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
		Maintenance *struct {
			Enabled                 *bool    `json:"enabled"`
			Timezone                *string  `json:"timezone"`
			CleanupSchedule         *string  `json:"cleanup_schedule"`
			CompactEnabled          *bool    `json:"compact_enabled"`
			CompactSchedule         *string  `json:"compact_schedule"`
			ErrorGracePeriod        *string  `json:"error_grace_period"`
			StaleRunAfter           *string  `json:"stale_run_after"`
			KeepDeletedMetadataDays *int     `json:"keep_deleted_metadata_days"`
			CompactMinReclaimRatio  *float64 `json:"compact_min_reclaim_ratio"`
			CompactMinReclaimBytes  *string  `json:"compact_min_reclaim_bytes"`
		} `json:"maintenance"`
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
	if req.Maintenance != nil {
		if req.Maintenance.Enabled != nil {
			next.Maintenance.Enabled = *req.Maintenance.Enabled
			updates[settingMaintenanceEnabled] = strconv.FormatBool(*req.Maintenance.Enabled)
		}
		if req.Maintenance.Timezone != nil {
			timezone := strings.TrimSpace(*req.Maintenance.Timezone)
			if timezone == "" {
				writeError(w, http.StatusBadRequest, errors.New("maintenance.timezone is required"))
				return
			}
			if _, err := time.LoadLocation(timezone); err != nil {
				writeError(w, http.StatusBadRequest, fmt.Errorf("maintenance.timezone is invalid: %w", err))
				return
			}
			next.Maintenance.Timezone = timezone
			updates[settingMaintenanceTimezone] = timezone
		}
		if req.Maintenance.CleanupSchedule != nil {
			schedule := strings.TrimSpace(*req.Maintenance.CleanupSchedule)
			if !validCronExpression(schedule) {
				writeError(w, http.StatusBadRequest, fmt.Errorf("invalid maintenance.cleanup_schedule %q", schedule))
				return
			}
			next.Maintenance.CleanupSchedule = schedule
			updates[settingMaintenanceCleanupSchedule] = schedule
		}
		if req.Maintenance.CompactEnabled != nil {
			next.Maintenance.CompactEnabled = *req.Maintenance.CompactEnabled
			updates[settingMaintenanceCompactEnabled] = strconv.FormatBool(*req.Maintenance.CompactEnabled)
		}
		if req.Maintenance.CompactSchedule != nil {
			schedule := strings.TrimSpace(*req.Maintenance.CompactSchedule)
			if !validCronExpression(schedule) {
				writeError(w, http.StatusBadRequest, fmt.Errorf("invalid maintenance.compact_schedule %q", schedule))
				return
			}
			next.Maintenance.CompactSchedule = schedule
			updates[settingMaintenanceCompactSchedule] = schedule
		}
		if req.Maintenance.ErrorGracePeriod != nil {
			duration := strings.TrimSpace(*req.Maintenance.ErrorGracePeriod)
			parsed, err := time.ParseDuration(duration)
			if err != nil {
				writeError(w, http.StatusBadRequest, fmt.Errorf("maintenance.error_grace_period must be a duration: %w", err))
				return
			}
			if parsed < 0 {
				writeError(w, http.StatusBadRequest, errors.New("maintenance.error_grace_period must be non-negative"))
				return
			}
			next.Maintenance.ErrorGracePeriod = duration
			updates[settingMaintenanceErrorGracePeriod] = duration
		}
		if req.Maintenance.StaleRunAfter != nil {
			duration := strings.TrimSpace(*req.Maintenance.StaleRunAfter)
			parsed, err := time.ParseDuration(duration)
			if err != nil {
				writeError(w, http.StatusBadRequest, fmt.Errorf("maintenance.stale_run_after must be a duration: %w", err))
				return
			}
			if parsed < 0 {
				writeError(w, http.StatusBadRequest, errors.New("maintenance.stale_run_after must be non-negative"))
				return
			}
			next.Maintenance.StaleRunAfter = duration
			updates[settingMaintenanceStaleRunAfter] = duration
		}
		if req.Maintenance.KeepDeletedMetadataDays != nil {
			if *req.Maintenance.KeepDeletedMetadataDays < 0 {
				writeError(w, http.StatusBadRequest, errors.New("maintenance.keep_deleted_metadata_days must be non-negative"))
				return
			}
			next.Maintenance.KeepDeletedMetadataDays = *req.Maintenance.KeepDeletedMetadataDays
			updates[settingMaintenanceKeepDeletedMetadataDays] = strconv.Itoa(*req.Maintenance.KeepDeletedMetadataDays)
		}
		if req.Maintenance.CompactMinReclaimRatio != nil {
			if *req.Maintenance.CompactMinReclaimRatio < 0 {
				writeError(w, http.StatusBadRequest, errors.New("maintenance.compact_min_reclaim_ratio must be non-negative"))
				return
			}
			next.Maintenance.CompactMinReclaimRatio = *req.Maintenance.CompactMinReclaimRatio
			updates[settingMaintenanceCompactMinReclaimRatio] = strconv.FormatFloat(*req.Maintenance.CompactMinReclaimRatio, 'f', -1, 64)
		}
		if req.Maintenance.CompactMinReclaimBytes != nil {
			value := strings.TrimSpace(*req.Maintenance.CompactMinReclaimBytes)
			if value == "" {
				writeError(w, http.StatusBadRequest, errors.New("maintenance.compact_min_reclaim_bytes is required"))
				return
			}
			if _, err := parseByteSize(value); err != nil {
				writeError(w, http.StatusBadRequest, fmt.Errorf("maintenance.compact_min_reclaim_bytes is invalid: %w", err))
				return
			}
			next.Maintenance.CompactMinReclaimBytes = value
			updates[settingMaintenanceCompactMinReclaimBytes] = value
		}
	}
	if err := s.store.UpsertSettings(r.Context(), updates); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	s.replaceSettings(next)
	writeJSON(w, http.StatusOK, map[string]any{"settings": s.settingsView()})
}

func parseByteSize(value string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, errors.New("size is required")
	}
	units := []struct {
		suffix string
		mult   float64
	}{
		{"tib", 1024 * 1024 * 1024 * 1024},
		{"tb", 1000 * 1000 * 1000 * 1000},
		{"gib", 1024 * 1024 * 1024},
		{"gb", 1000 * 1000 * 1000},
		{"mib", 1024 * 1024},
		{"mb", 1000 * 1000},
		{"kib", 1024},
		{"kb", 1000},
		{"b", 1},
	}
	lower := strings.ToLower(value)
	multiplier := float64(1)
	number := lower
	for _, unit := range units {
		if strings.HasSuffix(lower, unit.suffix) {
			multiplier = unit.mult
			number = strings.TrimSpace(strings.TrimSuffix(lower, unit.suffix))
			break
		}
	}
	parsed, err := strconv.ParseFloat(number, 64)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("invalid size %q", value)
	}
	return int64(parsed * multiplier), nil
}
