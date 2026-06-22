package cronexpr

import (
	"strconv"
	"strings"
	"time"
)

func Matches(expr string, t time.Time) bool {
	expr = strings.TrimSpace(expr)
	switch expr {
	case "":
		return false
	case "@hourly":
		return t.Minute() == 0
	case "@daily", "@midnight":
		return t.Hour() == 0 && t.Minute() == 0
	case "@weekly":
		return t.Weekday() == time.Sunday && t.Hour() == 0 && t.Minute() == 0
	}
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return false
	}
	checks := []struct {
		field string
		value int
		min   int
		max   int
	}{
		{fields[0], t.Minute(), 0, 59},
		{fields[1], t.Hour(), 0, 23},
		{fields[2], t.Day(), 1, 31},
		{fields[3], int(t.Month()), 1, 12},
		{fields[4], int(t.Weekday()), 0, 7},
	}
	for _, check := range checks {
		if !cronFieldMatches(check.field, check.value, check.min, check.max) {
			return false
		}
	}
	return true
}

func Valid(expr string) bool {
	expr = strings.TrimSpace(expr)
	switch expr {
	case "@hourly", "@daily", "@midnight", "@weekly":
		return true
	case "":
		return false
	}
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return false
	}
	ranges := [][2]int{{0, 59}, {0, 23}, {1, 31}, {1, 12}, {0, 7}}
	for i, field := range fields {
		if !cronFieldValid(field, ranges[i][0], ranges[i][1]) {
			return false
		}
	}
	return true
}

func cronFieldMatches(field string, value, minValue, maxValue int) bool {
	for _, part := range strings.Split(field, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			return false
		}
		if cronPartMatches(part, value, minValue, maxValue) {
			return true
		}
	}
	return false
}

func cronFieldValid(field string, minValue, maxValue int) bool {
	for _, part := range strings.Split(field, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			return false
		}
		if _, _, _, ok := parseCronPart(part, minValue, maxValue); !ok {
			return false
		}
	}
	return true
}

func cronPartMatches(part string, value, minValue, maxValue int) bool {
	start, end, step, ok := parseCronPart(part, minValue, maxValue)
	if !ok {
		return false
	}
	if minValue == 0 && maxValue == 7 && value == 0 {
		if start == 7 || end == 7 {
			value = 7
		}
	}
	if value < start || value > end {
		return false
	}
	return (value-start)%step == 0
}

func parseCronPart(part string, minValue, maxValue int) (int, int, int, bool) {
	step := 1
	if before, after, ok := strings.Cut(part, "/"); ok {
		parsedStep, err := strconv.Atoi(after)
		if err != nil || parsedStep <= 0 {
			return 0, 0, 0, false
		}
		step = parsedStep
		part = before
	}
	start, end := minValue, maxValue
	switch {
	case part == "*":
	case strings.Contains(part, "-"):
		left, right, _ := strings.Cut(part, "-")
		parsedStart, err := strconv.Atoi(left)
		if err != nil {
			return 0, 0, 0, false
		}
		parsedEnd, err := strconv.Atoi(right)
		if err != nil {
			return 0, 0, 0, false
		}
		start, end = parsedStart, parsedEnd
	default:
		parsed, err := strconv.Atoi(part)
		if err != nil {
			return 0, 0, 0, false
		}
		start, end = parsed, parsed
	}
	if start < minValue || end > maxValue || start > end {
		return 0, 0, 0, false
	}
	return start, end, step, true
}
