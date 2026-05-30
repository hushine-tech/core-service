package lifecycle

import (
	"testing"
	"time"
)

func TestBacktestBuyLimitFillsWhenLowTouchesPrice(t *testing.T) {
	at := time.Date(2026, 5, 30, 1, 2, 3, 0, time.UTC)
	result := MatchBacktestLimitGTC(BacktestLimitOrder{
		OrderID:         "order-1",
		ExchangeOrderID: "exchange-1",
		Symbol:          "ETHUSDT",
		Side:            "BUY",
		Qty:             0.2,
		LimitPrice:      3000,
		FeeRate:         0.001,
	}, BacktestBar{Symbol: "ETHUSDT", Time: at, High: 3010, Low: 2999})

	if !result.Filled || result.Event == nil {
		t.Fatal("expected buy limit to fill when low touches limit price")
	}
	if result.State.Status != "FILLED" || result.State.AvgPrice != 3000 {
		t.Fatalf("state = %+v, want filled at limit", result.State)
	}
	if result.Event.FillDelta.Fee != 0.6 {
		t.Fatalf("fee = %v, want 0.6", result.Event.FillDelta.Fee)
	}
}

func TestBacktestSellLimitFillsWhenHighTouchesPrice(t *testing.T) {
	result := MatchBacktestLimitGTC(BacktestLimitOrder{
		OrderID:         "order-1",
		ExchangeOrderID: "exchange-1",
		Symbol:          "ETHUSDT",
		Side:            "SELL",
		Qty:             0.2,
		LimitPrice:      3000,
	}, BacktestBar{Symbol: "ETHUSDT", High: 3000, Low: 2990})

	if !result.Filled || result.State.Status != "FILLED" || result.State.AvgPrice != 3000 {
		t.Fatalf("result = %+v, want sell limit filled at 3000", result)
	}
}

func TestBacktestLimitNotFilledWhenPriceNotTouched(t *testing.T) {
	result := MatchBacktestLimitGTC(BacktestLimitOrder{
		OrderID:         "order-1",
		ExchangeOrderID: "exchange-1",
		Symbol:          "ETHUSDT",
		Side:            "BUY",
		Qty:             0.2,
		LimitPrice:      3000,
	}, BacktestBar{Symbol: "ETHUSDT", High: 3010, Low: 3001})

	if result.Filled || result.Event != nil {
		t.Fatalf("result = %+v, want no fill", result)
	}
	if result.State.Status != "NEW" || result.State.RemainingQty != 0.2 {
		t.Fatalf("state = %+v, want open order", result.State)
	}
}

func TestBacktestSessionEndCancelsOpenLimitOrders(t *testing.T) {
	at := time.Date(2026, 5, 30, 1, 2, 3, 0, time.UTC)
	event := CancelBacktestLimitOrder(BacktestLimitOrder{
		OrderID:         "order-1",
		ExchangeOrderID: "exchange-1",
		Symbol:          "ETHUSDT",
		Side:            "BUY",
		Qty:             0.2,
		LimitPrice:      3000,
	}, at)

	if event.EventType != "canceled" || event.OrderStatus != "CANCELED" {
		t.Fatalf("event = %+v, want canceled", event)
	}
	if event.OrderState.RemainingQty != 0.2 || !event.OccurredAt.Equal(at) {
		t.Fatalf("event state = %+v at %s, want remaining qty and timestamp", event.OrderState, event.OccurredAt)
	}
}
