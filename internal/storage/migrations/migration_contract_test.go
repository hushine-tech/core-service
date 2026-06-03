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
	if !strings.Contains(sql, "create table account_strategies") {
		t.Fatal("hard-cut migration must recreate account_strategies after dropping stale mounts")
	}
	if !strings.Contains(sql, "uidx_account_strategies_active") {
		t.Fatal("hard-cut migration must recreate the active-strategy uniqueness guard")
	}
}

func TestBacktestAccountsDoNotKeepCompatibilityBackfills(t *testing.T) {
	for _, path := range []string{
		"0022_backfill_backtest_simulated_venues.sql",
		"0024_backfill_backtest_simulated_spot_venues.sql",
	} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read migration %s: %v", path, err)
		}
		sql := strings.ToLower(string(raw))
		for _, forbidden := range []string{
			"insert into venues",
			"from accounts",
			"sim_btv_",
			"simulated binance perpetual futures",
			"default simulated venue",
		} {
			if strings.Contains(sql, forbidden) {
				t.Fatalf("%s must not backfill old account rows into venues; found %q", path, forbidden)
			}
		}
	}

	raw, err := os.ReadFile("0025_remove_unsupported_backtest_simulated_spot_venues.sql")
	if err != nil {
		t.Fatalf("read 0025 migration: %v", err)
	}
	sql := strings.ToLower(string(raw))
	if !strings.Contains(sql, "delete from venues") || !strings.Contains(sql, "market = 1") {
		t.Fatal("migrations should still remove unsupported empty-key backtest spot venues when present")
	}
}

func TestVenueSchemaRequiresSyntheticBacktestKeys(t *testing.T) {
	raw, err := os.ReadFile("0019_portfolio_venue_hard_cut.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	sql := strings.ToLower(string(raw))
	for _, forbidden := range []string{
		"api_key text not null default ''",
		"where api_key <> ''",
	} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("venue schema must not contain %q", forbidden)
		}
	}
	for _, required := range []string{
		"api_key text not null",
		"credential_info = ''",
		"credential_key_version = 'synthetic'",
		"credential_fingerprint = ''",
		"api_key ~ '^sim_btv_[0-9a-f]{32}$'",
		"environment in (1, 2)",
		"credential_info <> ''",
		"credential_key_version <> 'synthetic'",
		"create unique index uidx_venues_api_key_scope",
		"on venues(exchange, environment, market, api_key)",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("venue schema missing synthetic-key invariant %q", required)
		}
	}
}

func TestVenueWalletStateSchemaExists(t *testing.T) {
	for _, path := range []string{
		"0019_portfolio_venue_hard_cut.sql",
		"0026_create_venue_wallet_states.sql",
	} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read migration %s: %v", path, err)
		}
		sql := strings.ToLower(string(raw))
		for _, required := range []string{
			"venue_wallet_states",
			"venue_id bigint primary key references venues(venue_id) on delete cascade",
			"account_id bigint references accounts(account_id) on delete set null",
			"snapshot_json jsonb not null default '{}'::jsonb",
			"idx_venue_wallet_states_account",
		} {
			if !strings.Contains(sql, required) {
				t.Fatalf("%s missing venue wallet schema invariant %q", path, required)
			}
		}
	}
	raw, err := os.ReadFile("0027_allow_unbound_venue_wallet_states.sql")
	if err != nil {
		t.Fatalf("read 0027 migration: %v", err)
	}
	sql := strings.ToLower(string(raw))
	for _, required := range []string{
		"alter column account_id drop not null",
		"on delete set null",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("0027 missing unbound venue wallet invariant %q", required)
		}
	}
}

func TestBacktestFuturesVenueBackfillIsHardCutNoop(t *testing.T) {
	raw, err := os.ReadFile("0022_backfill_backtest_simulated_venues.sql")
	if err != nil {
		t.Fatalf("read 0022 migration: %v", err)
	}
	sql := strings.ToLower(string(raw))
	for _, forbidden := range []string{
		"insert into venues",
		"from accounts",
		"sim_btv_",
		"default simulated venue",
	} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("0022 must remain a hard-cut no-op, found %q", forbidden)
		}
	}

	raw, err = os.ReadFile("0024_backfill_backtest_simulated_spot_venues.sql")
	if err != nil {
		t.Fatalf("read 0024 migration: %v", err)
	}
	sql = strings.ToLower(string(raw))
	if strings.Contains(sql, "insert into venues") {
		t.Fatal("obsolete spot venue backfill must not insert simulated venues")
	}
}
