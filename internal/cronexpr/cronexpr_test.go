package cronexpr

import (
	"testing"
	"time"
)

func TestValid(t *testing.T) {
	for _, expr := range []string{"0 2 * * *", "*/15 1-5 * * 1,3,5", "@hourly", "@daily", "@midnight", "@weekly"} {
		if !Valid(expr) {
			t.Fatalf("Valid(%q) = false", expr)
		}
	}
	for _, expr := range []string{"", "24h", "* * * *", "60 * * * *", "*/0 * * * *", "1--2 * * * *"} {
		if Valid(expr) {
			t.Fatalf("Valid(%q) = true", expr)
		}
	}
}

func TestMatches(t *testing.T) {
	timestamp := time.Date(2026, 6, 22, 2, 15, 0, 0, time.UTC)
	for _, expr := range []string{"15 2 * * *", "*/5 2 * * 1"} {
		if !Matches(expr, timestamp) {
			t.Fatalf("Matches(%q) = false", expr)
		}
	}
	if !Matches("@daily", time.Date(2026, 6, 22, 0, 0, 0, 0, time.UTC)) {
		t.Fatal("Matches(@daily) = false")
	}
	if Matches("16 2 * * *", timestamp) {
		t.Fatal("unexpected minute match")
	}
}
