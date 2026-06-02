package service

import (
	"testing"

	"github.com/hushine-tech/core-service/internal/domain"
)

func TestToProtoAccountWalletState_liveDisplayMetrics(t *testing.T) {
	info := domain.OnlineAccountInfo{
		Environment:      domain.EnvironmentLive,
		TotalValue:       9040,
		WalletBalance:    8910,
		AvailableBalance: 8000,
		Spot:             domain.SpotWallet{Free: 100, Locked: 10},
		Futures: domain.FuturesWallet{
			WalletBalance:           8910,
			TotalMarginBalance:      8930,
			TotalUnrealizedPnl:      20,
			MultiAssetsMode:         true,
			PortfolioMargin:         false,
			DisplayWalletBalanceUsd: 10761.5682,
			DisplayMarginBalanceUsd: 10781.5682,
			DisplayUnrealizedPnlUsd: 20,
		},
	}
	w := toProtoAccountWalletState(info)
	if w.GetSpotEstimatedValue() != 110 {
		t.Fatalf("spot %v", w.GetSpotEstimatedValue())
	}
	if w.GetFuturesPositionEquity() != 8930 {
		t.Fatalf("futures %v", w.GetFuturesPositionEquity())
	}
	if !w.GetMetricsAuthoritative() {
		t.Fatal("want authoritative")
	}
	if w.GetFutures().GetMarginBalance() != 8930 {
		t.Fatalf("margin_balance %v", w.GetFutures().GetMarginBalance())
	}
	if w.GetFutures().GetUnrealizedPnl() != 20 {
		t.Fatalf("unrealized_pnl %v", w.GetFutures().GetUnrealizedPnl())
	}
	if w.GetFutures().GetDisplayWalletBalanceUsd() != 10761.5682 {
		t.Fatalf("display_wallet_balance_usd %v", w.GetFutures().GetDisplayWalletBalanceUsd())
	}
	if w.GetFutures().GetDisplayMarginBalanceUsd() != 10781.5682 {
		t.Fatalf("display_margin_balance_usd %v", w.GetFutures().GetDisplayMarginBalanceUsd())
	}
	if w.GetFutures().GetDisplayUnrealizedPnlUsd() != 20 {
		t.Fatalf("display_unrealized_pnl_usd %v", w.GetFutures().GetDisplayUnrealizedPnlUsd())
	}
	if !w.GetFutures().GetMultiAssetsMode() {
		t.Fatal("want multi_assets_mode")
	}
	if w.GetFutures().GetPortfolioMargin() {
		t.Fatal("want portfolio_margin=false")
	}
}

func TestToProtoAccountWalletState_DefaultsEmptyFuturesMarginMode(t *testing.T) {
	info := domain.OnlineAccountInfo{
		Environment: domain.EnvironmentDemo,
		TotalValue:  5000,
		Futures: domain.FuturesWallet{
			WalletBalance:    5000,
			MarginBalance:    5000,
			PositionMode:     "",
			MarginMode:       "",
			AvailableBalance: 4990,
			Positions: []domain.FuturesPosition{
				{
					Symbol:       "ETHUSDT",
					PositionSide: "BOTH",
					PositionQty:  -0.021,
					Qty:          -0.021,
					EntryPrice:   2328.08,
					Leverage:     20,
					MarginMode:   "",
					MarginType:   "",
				},
			},
		},
	}

	w := toProtoAccountWalletState(info)

	if got := w.GetFutures().GetMarginMode(); got != "cross" {
		t.Fatalf("futures.margin_mode = %q, want cross", got)
	}
	if got := w.GetFutures().GetPositionMode(); got != "one_way" {
		t.Fatalf("futures.position_mode = %q, want one_way", got)
	}
	if len(w.GetFutures().GetPositions()) != 1 {
		t.Fatalf("positions = %d, want 1", len(w.GetFutures().GetPositions()))
	}
	if got := w.GetFutures().GetPositions()[0].GetMarginMode(); got != "cross" {
		t.Fatalf("position.margin_mode = %q, want cross", got)
	}
}
