package lifecycle

import (
	"math"
	"strings"
	"time"
)

type BacktestBar struct {
	Symbol string
	Time   time.Time
	Open   float64
	High   float64
	Low    float64
	Close  float64
}

type BacktestLimitOrder struct {
	SessionID       string
	AccountID       int64
	VenueID         int64
	IntentID        string
	AttemptID       string
	OrderID         string
	ExchangeOrderID string
	ClientOrderID   string
	Symbol          string
	Side            string
	Qty             float64
	LimitPrice      float64
	FeeRate         float64
}

type BacktestMatchResult struct {
	State  OrderState
	Event  *Event
	Filled bool
}

func MatchBacktestLimitGTC(order BacktestLimitOrder, bar BacktestBar) BacktestMatchResult {
	state := OrderState{
		ExchangeOrderID: order.ExchangeOrderID,
		ClientOrderID:   order.ClientOrderID,
		Symbol:          firstNonEmpty(order.Symbol, bar.Symbol),
		Status:          "NEW",
		OrigQty:         math.Abs(order.Qty),
		ExecutedQty:     0,
		RemainingQty:    math.Abs(order.Qty),
		UpdatedAt:       bar.Time,
	}
	if order.LimitPrice <= 0 || order.Qty == 0 || !limitTouched(order.Side, order.LimitPrice, bar) {
		return BacktestMatchResult{State: state}
	}

	qty := math.Abs(order.Qty)
	fee := qty * order.LimitPrice * order.FeeRate
	fill := FillDelta{
		ExchangeOrderID: order.ExchangeOrderID,
		Symbol:          state.Symbol,
		Qty:             qty,
		FillPrice:       order.LimitPrice,
		Fee:             fee,
		TradeTime:       bar.Time,
	}
	state.Status = "FILLED"
	state.ExecutedQty = qty
	state.RemainingQty = 0
	state.AvgPrice = order.LimitPrice
	return BacktestMatchResult{
		State: state,
		Event: &Event{
			SessionID:       order.SessionID,
			AccountID:       order.AccountID,
			VenueID:         order.VenueID,
			IntentID:        order.IntentID,
			AttemptID:       order.AttemptID,
			OrderID:         order.OrderID,
			ExchangeOrderID: order.ExchangeOrderID,
			EventType:       "fill",
			OrderStatus:     state.Status,
			FillDelta:       fill,
			OrderState:      state,
			OccurredAt:      bar.Time,
		},
		Filled: true,
	}
}

func CancelBacktestLimitOrder(order BacktestLimitOrder, at time.Time) Event {
	state := OrderState{
		ExchangeOrderID: order.ExchangeOrderID,
		ClientOrderID:   order.ClientOrderID,
		Symbol:          order.Symbol,
		Status:          "CANCELED",
		OrigQty:         math.Abs(order.Qty),
		RemainingQty:    math.Abs(order.Qty),
		UpdatedAt:       at,
	}
	return Event{
		SessionID:       order.SessionID,
		AccountID:       order.AccountID,
		VenueID:         order.VenueID,
		IntentID:        order.IntentID,
		AttemptID:       order.AttemptID,
		OrderID:         order.OrderID,
		ExchangeOrderID: order.ExchangeOrderID,
		EventType:       "canceled",
		OrderStatus:     "CANCELED",
		OrderState:      state,
		OccurredAt:      at,
	}
}

func limitTouched(side string, price float64, bar BacktestBar) bool {
	switch strings.ToUpper(strings.TrimSpace(side)) {
	case "BUY", "LONG":
		return bar.Low <= price
	case "SELL", "SHORT":
		return bar.High >= price
	default:
		return false
	}
}
