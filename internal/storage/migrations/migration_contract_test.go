package migrations_test

import (
	"os"
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
}
