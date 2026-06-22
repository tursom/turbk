package rootset

import (
	"fmt"
	"path/filepath"
	"strings"
)

// SplitList parses comma/newline separated root lists used by env vars.
func SplitList(value string) []string {
	fields := strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == '\n' || r == '\r'
	})
	roots := make([]string, 0, len(fields))
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field != "" {
			roots = append(roots, field)
		}
	}
	return roots
}

// Normalize cleans and validates absolute backup roots while preserving input order.
func Normalize(roots []string) ([]string, error) {
	normalized := make([]string, 0, len(roots))
	seen := make(map[string]struct{}, len(roots))
	for _, root := range roots {
		raw := strings.TrimSpace(root)
		if raw == "" {
			continue
		}
		cleaned := filepath.Clean(raw)
		if !filepath.IsAbs(cleaned) {
			return nil, fmt.Errorf("root %q must be absolute", root)
		}
		if _, ok := seen[cleaned]; ok {
			return nil, fmt.Errorf("root %q is duplicated", cleaned)
		}
		seen[cleaned] = struct{}{}
		normalized = append(normalized, cleaned)
	}
	if len(normalized) == 0 {
		return nil, fmt.Errorf("at least one root is required")
	}
	for i := range normalized {
		for j := range normalized {
			if i == j {
				continue
			}
			if isNestedRoot(normalized[i], normalized[j]) {
				return nil, fmt.Errorf("root %q is nested under %q", normalized[j], normalized[i])
			}
		}
	}
	return normalized, nil
}

// ManifestPrefix converts an absolute root into the manifest path prefix used by multi-root snapshots.
func ManifestPrefix(root string) string {
	root = filepath.ToSlash(filepath.Clean(root))
	root = strings.TrimPrefix(root, "/")
	if root == "" || root == "." {
		return "."
	}
	return root
}

func isNestedRoot(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	if err != nil || rel == "." || rel == ".." {
		return false
	}
	if filepath.IsAbs(rel) {
		return false
	}
	return !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
