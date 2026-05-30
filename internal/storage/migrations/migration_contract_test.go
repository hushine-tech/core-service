package migrations_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPortfolioVenueHardCutDropsStaleStrategyMounts(t *testing.T) {
	raw, err := os.ReadFile("0019_portfolio_venue_hard_cut.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	sql := strings.ToLower(string(raw))
	if !strings.Contains(sql, "drop table if exists account_strategies") {
		t.Fatal("hard-cut account migration must drop account_strategies so stale mounts cannot attach to recreated account IDs")
	}
	if !strings.Contains(sql, "create table account_strategies") {
		t.Fatal("hard-cut migration must recreate account_strategies after dropping stale mounts")
	}
	if !strings.Contains(sql, "uidx_account_strategies_active") {
		t.Fatal("hard-cut migration must recreate the active-strategy uniqueness guard")
	}
}

func TestBacktestAccountsGetDefaultSimulatedVenueBackfill(t *testing.T) {
	matches, err := filepath.Glob("*.sql")
	if err != nil {
		t.Fatalf("glob migrations: %v", err)
	}
	var combined strings.Builder
	for _, path := range matches {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read migration %s: %v", path, err)
		}
		combined.Write(raw)
		combined.WriteByte('\n')
	}
	sql := strings.ToLower(combined.String())
	for _, required := range []string{
		"insert into venues",
		"from accounts",
		"environment = 0",
		"default simulated venue",
		"not exists",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("migrations must backfill default simulated venues for existing backtest accounts; missing %q", required)
		}
	}
}
