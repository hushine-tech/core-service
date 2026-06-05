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
	raw, err := os.ReadFile("0019_portfolio_venue_hard_cut.sql")
	if err != nil {
		t.Fatalf("read 0019 migration: %v", err)
	}
	sql027 := strings.ToLower(string(raw))
	for _, required := range []string{
		"venue_wallet_states",
		"venue_id bigint primary key references venues(venue_id) on delete cascade",
		"account_id bigint references accounts(account_id) on delete set null",
		"snapshot_json jsonb not null default '{}'::jsonb",
		"idx_venue_wallet_states_account",
	} {
		if !strings.Contains(sql027, required) {
			t.Fatalf("0019 missing venue wallet schema invariant %q", required)
		}
	}
	raw, err = os.ReadFile("0027_allow_unbound_venue_wallet_states.sql")
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
