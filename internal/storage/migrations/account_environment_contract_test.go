package migrations_test

import (
	"os"
	"regexp"
	"testing"
)

func TestFreshAccountMigrationsDoNotCreateLegacyAccountEnvironmentColumns(t *testing.T) {
	files := []string{
		"0001_create_accounts.sql",
		"0002_create_account_snapshots.sql",
		"0004_create_strategy_sessions.sql",
		"0007_create_reconciliation_runs.sql",
		"0017_strategy_runtime_debug_metadata.sql",
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
