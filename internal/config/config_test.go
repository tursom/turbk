package config

import "testing"

func TestDefaultConfigNormalizes(t *testing.T) {
	cfg := Default()
	if err := cfg.Normalize(); err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if cfg.Server.Listen == "" {
		t.Fatal("expected listen address")
	}
	if cfg.Paths.StateDir == "" || cfg.Paths.RepoDir == "" {
		t.Fatal("expected runtime directories")
	}
}
