package repository

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/internal/order/lifecycle"
)

func lifecycleTestRepo(t *testing.T) (*TimescaleRepository, context.Context) {
	t.Helper()
	dsn := os.Getenv("TIMESCALEDB_DSN")
	if dsn == "" {
		t.Skip("skip: TIMESCALEDB_DSN is required for order lifecycle repository tests")
	}
	repo, err := NewTimescaleRepository(dsn, nil)
	if err != nil {
		t.Skipf("skip: cannot connect to TimescaleDB (%v). Set TIMESCALEDB_DSN or ensure DB is up.", err)
	}
	t.Cleanup(func() { _ = repo.Close() })
	return repo, context.Background()
}

func lifecycleTestEvent(sessionID string, seq int) lifecycle.Event {
	now := time.Now().UTC().Add(time.Duration(seq) * time.Millisecond)
	return lifecycle.Event{
		SessionID:       sessionID,
		AccountID:       100 + int64(seq),
		VenueID:         200,
		Environment:     2,
		Exchange:        1,
		Market:          2,
		PositionSide:    0,
		Side:            "BUY",
		IntentID:        fmt.Sprintf("intent-%s-%d", sessionID, seq),
		AttemptID:       fmt.Sprintf("attempt-%s-%d", sessionID, seq),
		OrderID:         fmt.Sprintf("order-%s-%d", sessionID, seq),
		ExchangeOrderID: fmt.Sprintf("exchange-order-%s-%d", sessionID, seq),
		ExchangeTradeID: fmt.Sprintf("trade-%s-%d", sessionID, seq),
		EventType:       "fill",
		EventSource:     lifecycle.EventSourceRESTRecovery,
		OrderStatus:     "FILLED",
		FillDelta: lifecycle.FillDelta{
			ExchangeTradeID: fmt.Sprintf("trade-%s-%d", sessionID, seq),
			ExchangeOrderID: fmt.Sprintf("exchange-order-%s-%d", sessionID, seq),
			Symbol:          "ETHUSDT",
			Qty:             0.1,
			FillPrice:       3000 + float64(seq),
			Fee:             0.12,
			FeeAsset:        "USDT",
			TradeTime:       now,
		},
		OrderState: lifecycle.OrderState{
			ExchangeOrderID: fmt.Sprintf("exchange-order-%s-%d", sessionID, seq),
			Symbol:          "ETHUSDT",
			Status:          "FILLED",
			OrigQty:         0.1,
			ExecutedQty:     0.1,
			RemainingQty:    0,
			AvgPrice:        3000 + float64(seq),
			UpdatedAt:       now,
		},
		OccurredAt: now,
	}
}

func lifecycleTestStateEvent(sessionID string, seq int) lifecycle.Event {
	now := time.Now().UTC().Truncate(time.Microsecond).Add(time.Duration(seq) * time.Millisecond)
	return lifecycle.Event{
		SessionID:       sessionID,
		AccountID:       100 + int64(seq),
		VenueID:         200,
		Environment:     2,
		Exchange:        1,
		Market:          2,
		PositionSide:    0,
		Side:            "BUY",
		IntentID:        fmt.Sprintf("intent-state-%s-%d", sessionID, seq),
		AttemptID:       fmt.Sprintf("attempt-state-%s-%d", sessionID, seq),
		OrderID:         fmt.Sprintf("order-state-%s-%d", sessionID, seq),
		ExchangeOrderID: fmt.Sprintf("exchange-order-state-%s", sessionID),
		EventType:       "terminal",
		EventSource:     lifecycle.EventSourceWebsocket,
		OrderStatus:     "CANCELED",
		OrderState: lifecycle.OrderState{
			ExchangeOrderID: fmt.Sprintf("exchange-order-state-%s", sessionID),
			Symbol:          "ETHUSDT",
			Status:          "CANCELED",
			OrigQty:         0.3,
			ExecutedQty:     0.1,
			RemainingQty:    0.2,
			UpdatedAt:       now,
		},
		OccurredAt: now,
	}
}

func TestSaveLifecycleEventPersistsFillDelta(t *testing.T) {
	repo, ctx := lifecycleTestRepo(t)
	sessionID := fmt.Sprintf("life-persist-%d", time.Now().UnixNano())

	saved, err := repo.SaveLifecycleEvent(ctx, lifecycleTestEvent(sessionID, 1))
	if err != nil {
		t.Fatalf("SaveLifecycleEvent: %v", err)
	}
	if saved.EventID == 0 || saved.CreatedAt.IsZero() {
		t.Fatalf("saved event missing generated fields: %+v", saved)
	}

	events, err := repo.ListLifecycleEvents(ctx, sessionID, 0, 10)
	if err != nil {
		t.Fatalf("ListLifecycleEvents: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("events len = %d, want 1", len(events))
	}
	got := events[0]
	if got.FillDelta.ExchangeTradeID != saved.ExchangeTradeID || got.FillDelta.FillPrice == 0 || got.OrderState.Status != "FILLED" {
		t.Fatalf("event payload not persisted: %+v", got)
	}
	if got.Environment != 2 || got.Exchange != 1 || got.Market != 2 || got.PositionSide != 0 || got.Side != "BUY" {
		t.Fatalf("route facts not persisted: %+v", got)
	}
	if got.EventSource != lifecycle.EventSourceRESTRecovery {
		t.Fatalf("event_source = %q, want %s", got.EventSource, lifecycle.EventSourceRESTRecovery)
	}
}

func TestSaveLifecycleEventDeduplicatesExchangeTrade(t *testing.T) {
	repo, ctx := lifecycleTestRepo(t)
	sessionID := fmt.Sprintf("life-dedupe-%d", time.Now().UnixNano())
	event := lifecycleTestEvent(sessionID, 1)

	first, err := repo.SaveLifecycleEvent(ctx, event)
	if err != nil {
		t.Fatalf("first SaveLifecycleEvent: %v", err)
	}
	event.OrderStatus = "PARTIALLY_FILLED"
	event.FillDelta.FeeMissing = true
	second, err := repo.SaveLifecycleEvent(ctx, event)
	if err != nil {
		t.Fatalf("second SaveLifecycleEvent: %v", err)
	}
	if second.EventID != first.EventID {
		t.Fatalf("duplicate trade produced new event_id: first=%d second=%d", first.EventID, second.EventID)
	}

	events, err := repo.ListLifecycleEvents(ctx, sessionID, 0, 10)
	if err != nil {
		t.Fatalf("ListLifecycleEvents: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("events len = %d, want 1", len(events))
	}
	if !events[0].FillDelta.FeeMissing || events[0].OrderStatus != "PARTIALLY_FILLED" {
		t.Fatalf("duplicate event did not update lifecycle payload: %+v", events[0])
	}
}

func TestSaveLifecycleEventBackfillsOrderFillForRecoveryEvent(t *testing.T) {
	repo, ctx := lifecycleTestRepo(t)
	seed := time.Now().UnixNano()
	sessionID := fmt.Sprintf("life-fill-backfill-%d", seed)
	intentID := fmt.Sprintf("intent-%d", seed)
	attemptID := fmt.Sprintf("attempt-%d", seed)
	orderID := fmt.Sprintf("order-%d", seed)
	exchangeOrderID := fmt.Sprintf("exchange-%d", seed)
	tradeID := fmt.Sprintf("trade-%d", seed)
	baseTime := time.Date(2026, 6, 13, 12, 30, 0, 0, time.UTC)

	intent := OrderIntent{
		IntentID:       intentID,
		Time:           baseTime,
		AccountID:      1001,
		VenueID:        2001,
		UserID:         3001,
		StrategyID:     4001,
		SessionID:      sessionID,
		Environment:    2,
		Exchange:       1,
		Market:         2,
		PositionSide:   0,
		OrderType:      2,
		Symbol:         "ETHUSDT",
		Side:           "BUY",
		RequestedQty:   0.5,
		RequestedPrice: 3000,
		Status:         "REQUESTED",
	}
	if err := repo.UpsertOrderIntent(ctx, intent); err != nil {
		t.Fatalf("UpsertOrderIntent: %v", err)
	}

	attempt := OrderAttempt{
		AttemptID:      attemptID,
		IntentID:       intentID,
		Time:           baseTime.Add(time.Second),
		AccountID:      intent.AccountID,
		VenueID:        intent.VenueID,
		UserID:         intent.UserID,
		StrategyID:     intent.StrategyID,
		SessionID:      sessionID,
		Environment:    intent.Environment,
		Exchange:       intent.Exchange,
		Market:         intent.Market,
		PositionSide:   intent.PositionSide,
		OrderType:      intent.OrderType,
		Symbol:         intent.Symbol,
		Side:           intent.Side,
		RequestedQty:   intent.RequestedQty,
		RequestedPrice: intent.RequestedPrice,
		MarkPrice:      3000,
		Status:         "PENDING",
		ClientOrderID:  fmt.Sprintf("client-%d", seed),
	}
	if err := repo.CreateOrderAttempt(ctx, attempt); err != nil {
		t.Fatalf("CreateOrderAttempt: %v", err)
	}

	attempt.Status = "ACCEPTED"
	attempt.OrderID = orderID
	attempt.ExchangeOrderID = exchangeOrderID
	order := &Order{
		OrderID:         orderID,
		ExchangeOrderID: exchangeOrderID,
		ClientOrderID:   attempt.ClientOrderID,
		AttemptID:       attemptID,
		IntentID:        intentID,
		Time:            baseTime.Add(2 * time.Second),
		AccountID:       intent.AccountID,
		VenueID:         intent.VenueID,
		UserID:          intent.UserID,
		StrategyID:      intent.StrategyID,
		SessionID:       sessionID,
		Environment:     intent.Environment,
		Exchange:        intent.Exchange,
		Market:          intent.Market,
		PositionSide:    intent.PositionSide,
		Symbol:          intent.Symbol,
		Side:            intent.Side,
		OrigQty:         0.5,
		ExecutedQty:     0.2,
		RemainingQty:    0.3,
		AvgPrice:        3000,
		Price:           3000,
		Status:          "PARTIALLY_FILLED",
		RecoveryStatus:  "PARTIALLY_FILLED",
	}
	if err := repo.FinalizeOrderAttempt(ctx, attempt, order, nil); err != nil {
		t.Fatalf("FinalizeOrderAttempt: %v", err)
	}

	event := lifecycle.Event{
		SessionID:       sessionID,
		AccountID:       intent.AccountID,
		VenueID:         intent.VenueID,
		Environment:     intent.Environment,
		Exchange:        intent.Exchange,
		Market:          intent.Market,
		PositionSide:    intent.PositionSide,
		Side:            intent.Side,
		IntentID:        intentID,
		AttemptID:       attemptID,
		OrderID:         orderID,
		ExchangeOrderID: exchangeOrderID,
		ExchangeTradeID: tradeID,
		EventType:       "fill",
		EventSource:     lifecycle.EventSourceRESTRecovery,
		OrderStatus:     "FILLED",
		FillDelta: lifecycle.FillDelta{
			ExchangeTradeID: tradeID,
			ExchangeOrderID: exchangeOrderID,
			Symbol:          intent.Symbol,
			Qty:             0.3,
			FillPrice:       3010,
			Fee:             0.3612,
			FeeAsset:        "USDT",
			TradeTime:       baseTime.Add(3 * time.Second),
		},
		OrderState: lifecycle.OrderState{
			ExchangeOrderID: exchangeOrderID,
			ClientOrderID:   attempt.ClientOrderID,
			Symbol:          intent.Symbol,
			Status:          "FILLED",
			OrigQty:         0.5,
			ExecutedQty:     0.5,
			RemainingQty:    0,
			AvgPrice:        3006,
			UpdatedAt:       baseTime.Add(3 * time.Second),
		},
		OccurredAt: baseTime.Add(3 * time.Second),
	}
	if _, err := repo.SaveLifecycleEvent(ctx, event); err != nil {
		t.Fatalf("SaveLifecycleEvent: %v", err)
	}

	fills, total, err := repo.QueryOrderFillsPaginated(ctx, intent.UserID, intent.AccountID, intent.StrategyID, sessionID, intentID, attemptID, orderID, 10, 0)
	if err != nil {
		t.Fatalf("QueryOrderFillsPaginated: %v", err)
	}
	if total != 1 || len(fills) != 1 {
		t.Fatalf("fills total=%d len=%d, want 1/1", total, len(fills))
	}
	if fills[0].AttemptID != attemptID || fills[0].OrderID != orderID || fills[0].ExchangeTradeID != tradeID || fills[0].Qty != 0.3 {
		t.Fatalf("backfilled fill = %+v, want recovered trade on original attempt/order", fills[0])
	}

	event.FillDelta.Fee = 0.42
	event.OrderStatus = "FILLED"
	if _, err := repo.SaveLifecycleEvent(ctx, event); err != nil {
		t.Fatalf("second SaveLifecycleEvent: %v", err)
	}
	fills, total, err = repo.QueryOrderFillsPaginated(ctx, intent.UserID, intent.AccountID, intent.StrategyID, sessionID, intentID, attemptID, orderID, 10, 0)
	if err != nil {
		t.Fatalf("QueryOrderFillsPaginated after duplicate: %v", err)
	}
	if total != 1 || len(fills) != 1 || fills[0].Fee != 0.42 {
		t.Fatalf("duplicate lifecycle fill should update one order_fill, total=%d fills=%+v", total, fills)
	}
}

func TestSaveLifecycleEventDeduplicatesOrderStateWithoutTradeID(t *testing.T) {
	repo, ctx := lifecycleTestRepo(t)
	sessionID := fmt.Sprintf("life-state-dedupe-%d", time.Now().UnixNano())
	event := lifecycleTestStateEvent(sessionID, 1)

	first, err := repo.SaveLifecycleEvent(ctx, event)
	if err != nil {
		t.Fatalf("first SaveLifecycleEvent: %v", err)
	}
	event.EventSource = lifecycle.EventSourceRESTRecovery
	event.OrderState.RemainingQty = 0.15
	second, err := repo.SaveLifecycleEvent(ctx, event)
	if err != nil {
		t.Fatalf("second SaveLifecycleEvent: %v", err)
	}
	if second.EventID != first.EventID {
		t.Fatalf("duplicate state event produced new event_id: first=%d second=%d", first.EventID, second.EventID)
	}

	events, err := repo.ListLifecycleEvents(ctx, sessionID, 0, 10)
	if err != nil {
		t.Fatalf("ListLifecycleEvents: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("events len = %d, want 1", len(events))
	}
	if events[0].EventSource != lifecycle.EventSourceRESTRecovery || events[0].OrderState.RemainingQty != 0.15 {
		t.Fatalf("duplicate state event did not update lifecycle payload: %+v", events[0])
	}
}

func TestListLifecycleEventsAfterCursor(t *testing.T) {
	repo, ctx := lifecycleTestRepo(t)
	sessionID := fmt.Sprintf("life-cursor-%d", time.Now().UnixNano())

	first, err := repo.SaveLifecycleEvent(ctx, lifecycleTestEvent(sessionID, 1))
	if err != nil {
		t.Fatalf("first SaveLifecycleEvent: %v", err)
	}
	second, err := repo.SaveLifecycleEvent(ctx, lifecycleTestEvent(sessionID, 2))
	if err != nil {
		t.Fatalf("second SaveLifecycleEvent: %v", err)
	}

	events, err := repo.ListLifecycleEvents(ctx, sessionID, first.EventID, 10)
	if err != nil {
		t.Fatalf("ListLifecycleEvents: %v", err)
	}
	if len(events) != 1 || events[0].EventID != second.EventID {
		t.Fatalf("cursor events = %+v, want only event_id=%d", events, second.EventID)
	}
}
