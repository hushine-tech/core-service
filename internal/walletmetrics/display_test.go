package walletmetrics

import (
	"testing"

	"github.com/hushine-tech/core-service/internal/domain"
)

func TestSpotEstimatedExchangeAligned_pricedAsset(t *testing.T) {
	p := 2.0
	sw := domain.SpotWallet{Free: 10, Locked: 1, Assets: []domain.SpotAsset{
		{Symbol: "BTC", Qty: 0.5, Locked: 0.1, Price: &p},
	}}
	got := SpotEstimatedExchangeAligned(sw)
	want := 11 + (0.5+0.1)*2 // 12.2
	if got != want {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestComputeDisplay_binanceLive_matchesTotal(t *testing.T) {
	info := domain.OnlineAccountInfo{
		Mode:       domain.AccountModeBinanceLive,
		TotalValue: 9020,
		Spot:       domain.SpotWallet{Free: 100, Locked: 10},
		Futures: domain.FuturesWallet{
			WalletBalance: 8910,
		},
	}
	b := ComputeDisplay(info)
	if b.SpotEstimated != 110 {
		t.Fatalf("spot EV got %v want 110", b.SpotEstimated)
	}
	if b.FuturesEquity != 8910 {
		t.Fatalf("futures got %v want 8910", b.FuturesEquity)
	}
	if !b.Authoritative {
		t.Fatal("expected authoritative")
	}
}

func TestComputeDisplay_isolatedFlat(t *testing.T) {
	info := domain.OnlineAccountInfo{
		Mode:       domain.AccountModeBacktest,
		TotalValue: 2000,
		Spot:       domain.SpotWallet{Free: 1000, Locked: 0},
		Futures: domain.FuturesWallet{
			MarginMode:   "isolated",
			PositionMode: "one_way",
			Positions: []domain.FuturesPosition{
				{Symbol: "BTCUSDT", InitialBalance: 1000, Leverage: 10, FeeRate: 0.0004},
			},
		},
	}
	b := ComputeDisplay(info)
	if !b.Authoritative {
		t.Fatalf("expected authoritative se=%v fe=%v total=%v", b.SpotEstimated, b.FuturesEquity, info.TotalValue)
	}
	if b.PositionDisplay == nil || len(b.PositionDisplay) != 1 || b.PositionDisplay[0] == nil || *b.PositionDisplay[0] != 1000 {
		t.Fatalf("position display %+v", b.PositionDisplay)
	}
}

func TestComputeDisplay_isolatedOpen(t *testing.T) {
	info := domain.OnlineAccountInfo{
		Mode:       domain.AccountModeBacktest,
		TotalValue: 1500,
		Spot:       domain.SpotWallet{},
		Futures: domain.FuturesWallet{
			MarginMode:   "isolated",
			PositionMode: "one_way",
			Positions: []domain.FuturesPosition{
				{
					Symbol: "BTCUSDT", Direction: 1, InitialBalance: 1000, Leverage: 10, FeeRate: 0.0004,
					Qty: 0.1, EntryPrice: 50000, MarkPrice: 50000, UnrealizedPnl: 500,
				},
			},
		},
	}
	b := ComputeDisplay(info)
	// IM = 0.1 * 50000 / 10 = 500; row = 500 + 1000 + 500 = 2000 — may not match TotalValue 1500
	// fe2 fallback: 1500 - 0 = 1500
	if b.FuturesEquity != 1500 {
		t.Fatalf("futures equity got %v want 1500 (fallback to total-se)", b.FuturesEquity)
	}
	if !b.Authoritative {
		t.Fatal("expected authoritative via fallback")
	}
}

func TestComputeDisplay_crossWithPositions(t *testing.T) {
	info := domain.OnlineAccountInfo{
		Mode:       domain.AccountModeBacktest,
		TotalValue: 10050,
		Spot:       domain.SpotWallet{Free: 50},
		Futures: domain.FuturesWallet{
			MarginMode:         "cross",
			PositionMode:       "one_way",
			InitialBalance:     10000,
			WalletBalance:      10000,
			TotalUnrealizedPnl: 0,
			Positions: []domain.FuturesPosition{
				{Symbol: "BTCUSDT", Qty: 0.01, Leverage: 10, MarkPrice: 50000, EntryPrice: 50000},
			},
		},
	}
	b := ComputeDisplay(info)
	// IM = 0.01 * 50000 / 10 = 50; futures eq = 10000 + 0 + 50 = 10050
	if b.FuturesEquity < 10000 {
		t.Fatalf("futures equity unexpectedly low: %v", b.FuturesEquity)
	}
	if !b.Authoritative {
		t.Fatalf("not authoritative se=%v fe=%v", b.SpotEstimated, b.FuturesEquity)
	}
	if b.PositionDisplay != nil {
		t.Fatal("cross mode should not set per-position display")
	}
}
