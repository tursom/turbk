package repository

import (
	"fmt"
	"strconv"
	"strings"
)

func parseSize(value string, fallback int64) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback, nil
	}
	unit := int64(1)
	number := value
	for suffix, multiplier := range map[string]int64{
		"kib": 1024,
		"kb":  1000,
		"mib": 1024 * 1024,
		"mb":  1000 * 1000,
		"gib": 1024 * 1024 * 1024,
		"gb":  1000 * 1000 * 1000,
	} {
		if strings.HasSuffix(strings.ToLower(value), suffix) {
			unit = multiplier
			number = strings.TrimSpace(value[:len(value)-len(suffix)])
			break
		}
	}
	parsed, err := strconv.ParseFloat(number, 64)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("invalid size %q", value)
	}
	return int64(parsed * float64(unit)), nil
}
