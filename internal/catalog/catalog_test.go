package catalog

import (
	"context"
	"testing"
	"time"
)

func TestCatalogListMocked(t *testing.T) {
	c := NewWithFetchers(time.Hour,
		func(ctx context.Context) ([]string, error) {
			return []string{"AAAUSDT", "BBBUSDT", "BTCUSDT"}, nil
		},
		func(ctx context.Context) ([]string, error) {
			return []string{"BTCUSDT", "ETHUSDT"}, nil
		},
	)
	ctx := context.Background()
	syms, stale, err := c.List(ctx, MarketSpot, "BTC", 10)
	if err != nil {
		t.Fatal(err)
	}
	if stale {
		t.Fatal("unexpected stale")
	}
	if len(syms) != 1 || syms[0] != "BTCUSDT" {
		t.Fatalf("spot: %v", syms)
	}
	syms2, _, err := c.List(ctx, MarketUSDMFutures, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(syms2) != 2 {
		t.Fatalf("fut: %v", syms2)
	}
}

func TestParseMarket(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want Market
	}{
		{"spot", MarketSpot},
		{"USDM_FUTURES", MarketUSDMFutures},
		{"futures", MarketUSDMFutures},
	} {
		m, err := ParseMarket(tc.in)
		if err != nil || m != tc.want {
			t.Fatalf("%q: got %v %v want %v", tc.in, m, err, tc.want)
		}
	}
	if _, err := ParseMarket("fx"); err == nil {
		t.Fatal("expected error")
	}
}
