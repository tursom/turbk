package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/tursom/turbk/internal/rootset"
)

func TestRootFlagReadsRootEnvironment(t *testing.T) {
	t.Setenv("TURBK_AGENT_ROOT", "/legacy/root")
	t.Setenv("TURBK_AGENT_ROOTS", "/data/app,/var/log/myapp")

	flag := newRootFlagFromEnv()
	roots, err := rootset.Normalize(flag.Values())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"/data/app", "/var/log/myapp"}
	if len(roots) != len(want) || roots[0] != want[0] || roots[1] != want[1] {
		t.Fatalf("roots = %#v, want %#v", roots, want)
	}
}

func TestRootFlagReadsLegacyRootEnvironment(t *testing.T) {
	t.Setenv("TURBK_AGENT_ROOT", "/legacy/root")
	t.Setenv("TURBK_AGENT_ROOTS", "")

	flag := newRootFlagFromEnv()
	roots, err := rootset.Normalize(flag.Values())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"/legacy/root"}
	if len(roots) != len(want) || roots[0] != want[0] {
		t.Fatalf("roots = %#v, want %#v", roots, want)
	}
}

func TestRootFlagCommandLineOverridesEnvironment(t *testing.T) {
	t.Setenv("TURBK_AGENT_ROOTS", "/env/root")
	flag := newRootFlagFromEnv()
	if err := flag.Set("/cli/one"); err != nil {
		t.Fatal(err)
	}
	if err := flag.Set("/cli/two"); err != nil {
		t.Fatal(err)
	}

	roots, err := rootset.Normalize(flag.Values())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"/cli/one", "/cli/two"}
	if len(roots) != len(want) || roots[0] != want[0] || roots[1] != want[1] {
		t.Fatalf("roots = %#v, want %#v", roots, want)
	}
}

func TestBackupRootsForCommandUsesPayloadRoots(t *testing.T) {
	roots, err := backupRootsForCommand(agentCommand{
		Payload: json.RawMessage(`{"roots":["/server/root","/server/logs"]}`),
	}, []string{"/local/root"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"/server/root", "/server/logs"}
	if len(roots) != len(want) || roots[0] != want[0] || roots[1] != want[1] {
		t.Fatalf("roots = %#v, want %#v", roots, want)
	}
}

func TestBackupRootsForCommandFallsBackToLocalRoots(t *testing.T) {
	roots, err := backupRootsForCommand(agentCommand{Payload: json.RawMessage(`{"job_id":7}`)}, []string{"/local/root"})
	if err != nil {
		t.Fatal(err)
	}
	if len(roots) != 1 || roots[0] != "/local/root" {
		t.Fatalf("roots = %#v, want local fallback", roots)
	}
}

func TestParseBackupScheduleOrDefault(t *testing.T) {
	if got := parseBackupScheduleOrDefault("*/15 * * * *", defaultAgentBackupSchedule); got != "*/15 * * * *" {
		t.Fatalf("valid schedule = %q", got)
	}
	if got := parseBackupScheduleOrDefault("24h", defaultAgentBackupSchedule); got != defaultAgentBackupSchedule {
		t.Fatalf("invalid duration schedule = %q, want default", got)
	}
}

func TestDueByCronChecksWindowWithoutDuplicateMinute(t *testing.T) {
	now := time.Date(2026, 6, 22, 10, 0, 30, 0, time.UTC)
	if !dueByCron("@hourly", time.Time{}, now) {
		t.Fatal("expected first check inside matching minute to be due")
	}
	if dueByCron("@hourly", time.Time{}, now.Add(time.Minute)) {
		t.Fatal("did not expect first check outside matching minute to be due")
	}
	if !dueByCron("@hourly", now.Add(-10*time.Minute), now.Add(5*time.Minute)) {
		t.Fatal("expected missed matching minute inside check window to be due")
	}
	if dueByCron("@hourly", now, now.Add(20*time.Second)) {
		t.Fatal("did not expect same matching minute to trigger twice")
	}
}
