package fsfilter

import (
	"fmt"
	"io/fs"
	"path"
	"path/filepath"
	"strings"
)

type Options struct {
	ExcludePatterns       []string
	SkipPseudoFilesystems bool
}

type SkipEvent struct {
	Path   string
	Rel    string
	Reason string
}

func ShouldSkip(root, current string, info fs.FileInfo, opts Options) (SkipEvent, bool) {
	rel, err := filepath.Rel(root, current)
	if err != nil {
		rel = current
	}
	rel = cleanRel(rel)
	for _, pattern := range opts.ExcludePatterns {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		if Match(pattern, rel) {
			return SkipEvent{
				Path:   current,
				Rel:    rel,
				Reason: fmt.Sprintf("excluded by pattern %q", pattern),
			}, true
		}
	}
	if opts.SkipPseudoFilesystems && info != nil && info.Mode()&fs.ModeSymlink == 0 {
		if fsName, ok, err := PseudoFilesystemName(current); err == nil && ok {
			return SkipEvent{
				Path:   current,
				Rel:    rel,
				Reason: "pseudo filesystem " + fsName,
			}, true
		}
	}
	return SkipEvent{}, false
}

func Match(pattern, rel string) bool {
	pattern = cleanPattern(pattern)
	rel = cleanRel(rel)
	if pattern == "" || rel == "" {
		return false
	}
	if pattern == rel {
		return true
	}
	patternParts := splitPath(pattern)
	relParts := splitPath(rel)
	if len(patternParts) == 1 {
		for _, part := range relParts {
			if ok, err := path.Match(patternParts[0], part); err == nil && ok {
				return true
			}
		}
	}
	return matchParts(patternParts, relParts)
}

func cleanPattern(pattern string) string {
	pattern = strings.TrimSpace(filepath.ToSlash(pattern))
	pattern = strings.TrimPrefix(pattern, "/")
	pattern = path.Clean(pattern)
	if pattern == "." {
		return ""
	}
	return pattern
}

func cleanRel(rel string) string {
	rel = strings.TrimSpace(filepath.ToSlash(rel))
	rel = strings.TrimPrefix(rel, "/")
	rel = path.Clean(rel)
	if rel == "." {
		return "."
	}
	return rel
}

func splitPath(value string) []string {
	if value == "." {
		return []string{"."}
	}
	return strings.Split(value, "/")
}

func matchParts(patternParts, relParts []string) bool {
	if len(patternParts) == 0 {
		return len(relParts) == 0
	}
	if patternParts[0] == "**" {
		if matchParts(patternParts[1:], relParts) {
			return true
		}
		for len(relParts) > 0 {
			relParts = relParts[1:]
			if matchParts(patternParts[1:], relParts) {
				return true
			}
		}
		return false
	}
	if len(relParts) == 0 {
		return false
	}
	ok, err := path.Match(patternParts[0], relParts[0])
	if err != nil || !ok {
		return false
	}
	return matchParts(patternParts[1:], relParts[1:])
}

func SplitPatterns(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	fields := strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == '\n' || r == '\r'
	})
	patterns := make([]string, 0, len(fields))
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field != "" {
			patterns = append(patterns, field)
		}
	}
	return patterns
}
