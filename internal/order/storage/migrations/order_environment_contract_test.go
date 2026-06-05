package migrations_test

import (
	"os"
	"regexp"
	"testing"
)

func TestFreshOrderMigrationsDoNotCreateLegacyAccountEnvironmentColumns(t *testing.T) {
	files := []string{
		"0006_order_venue_hard_cut.sql",
	}
	legacyAccountRoutingColumn := regexp.MustCompile(`(?i)\b[m]ode\b`)
	for _, path := range files {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if legacyAccountRoutingColumn.Match(raw) {
			t.Fatalf("%s must use environment, not the legacy account-routing column", path)
		}
	}
}
