package fsfilter

import (
	"runtime"
	"testing"
)

func TestMatch(t *testing.T) {
	tests := []struct {
		pattern string
		rel     string
		want    bool
	}{
		{"proc", "proc", true},
		{"proc", "overlay2/id/merged/proc", true},
		{"overlay2/*/merged/proc/**", "overlay2/id/merged/proc/1/attr/current", true},
		{"overlay2/*/merged/proc/**", "overlay2/id/diff/proc/1/attr/current", false},
		{"*.sock", "run/docker.sock", true},
		{"cache/**", "cache/a/b", true},
		{"cache/**", "cached/a/b", false},
	}
	for _, tt := range tests {
		t.Run(tt.pattern+" "+tt.rel, func(t *testing.T) {
			if got := Match(tt.pattern, tt.rel); got != tt.want {
				t.Fatalf("Match(%q, %q) = %v, want %v", tt.pattern, tt.rel, got, tt.want)
			}
		})
	}
}

func TestSplitPatterns(t *testing.T) {
	got := SplitPatterns("proc/**, sys/**\nrun/*.sock")
	want := []string{"proc/**", "sys/**", "run/*.sock"}
	if len(got) != len(want) {
		t.Fatalf("patterns = %#v, want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("patterns = %#v, want %#v", got, want)
		}
	}
}

func TestPseudoFilesystemNameProc(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("linux-only pseudo filesystem check")
	}
	name, ok, err := PseudoFilesystemName("/proc")
	if err != nil {
		t.Fatalf("PseudoFilesystemName(/proc): %v", err)
	}
	if !ok || name != "proc" {
		t.Fatalf("/proc pseudo filesystem = (%q, %v), want proc true", name, ok)
	}
}
