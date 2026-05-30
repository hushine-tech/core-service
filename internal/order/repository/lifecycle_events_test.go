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
