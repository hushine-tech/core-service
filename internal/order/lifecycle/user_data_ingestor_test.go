package lifecycle

import (
	"context"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/internal/exchange/adapter"
)

type userDataStore struct {
	open  OpenOrder
	saved []Event
}

func (s *userDataStore) ResolveOpenOrderByExchangeRef(_ context.Context, venueID int64, exchangeOrderID, clientOrderID string) (OpenOrder, error) {
	if s.open.VenueID == venueID && (s.open.ExchangeOrderID == exchangeOrderID || s.open.ClientOrderID == clientOrderID) {
		return s.open, nil
	}
	return OpenOrder{}, ErrOpenOrderNotFound
}

func (s *userDataStore) SaveLifecycleEvent(_ context.Context, event Event) (Event, error) {
	event.EventID = int64(len(s.saved) + 1)
	s.saved = append(s.saved, event)
	return event, nil
}

func TestUserDataIngestorWritesPartialFillEvent(t *testing.T) {
	store := &userDataStore{open: OpenOrder{
		SessionID:       "sess-1",
		AccountID:       1,
		VenueID:         2,
		Environment:     2,
		Exchange:        1,
		Market:          2,
		PositionSide:    1,
		Side:            "BUY",
		IntentID:        "intent-1",
		AttemptID:       "attempt-1",
		OrderID:         "order-1",
		ExchangeOrderID: "1001",
		ClientOrderID:   "cid-1",
		Symbol:          "ETHUSDT",
	}}
	ingestor := NewUserDataIngestor(store)
	err := ingestor.Ingest(context.Background(), 2, adapter.UserDataOrderEvent{
		EventSource:          "websocket",
		EventTime:            time.UnixMilli(1700000000000).UTC(),
		Symbol:               "ETHUSDT",
		ClientOrderID:        "cid-1",
		ExchangeOrderID:      "1001",
		ExchangeTradeID:      "9001",
		Side:                 "BUY",
		PositionSide:         "LONG",
		OrderStatus:          "PARTIALLY_FILLED",
		ExecutionType:        "TRADE",
		LastFilledQty:        0.2,
		LastFilledPrice:      2000,
		AccumulatedFilledQty: 0.2,
		Fee:                  0.08,
		FeeAsset:             "USDT",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(store.saved) != 1 {
		t.Fatalf("saved events = %d, want 1", len(store.saved))
	}
	got := store.saved[0]
	if got.EventType != "fill" || got.EventSource != EventSourceWebsocket || got.FillDelta.ExchangeTradeID != "9001" {
		t.Fatalf("unexpected event: %+v", got)
	}
	if got.FillDelta.Qty != 0.2 || got.FillDelta.FillPrice != 2000 || got.FillDelta.Fee != 0.08 {
		t.Fatalf("unexpected fill delta: %+v", got.FillDelta)
	}
	if got.OrderState.Status != "PARTIALLY_FILLED" || got.OrderState.ExecutedQty != 0.2 {
		t.Fatalf("unexpected order state: %+v", got.OrderState)
	}
}

func TestUserDataIngestorIgnoresNonTradeNewState(t *testing.T) {
	store := &userDataStore{open: OpenOrder{VenueID: 2, ExchangeOrderID: "1001", ClientOrderID: "cid-1"}}
	ingestor := NewUserDataIngestor(store)
	err := ingestor.Ingest(context.Background(), 2, adapter.UserDataOrderEvent{
		EventSource:     "websocket",
		EventTime:       time.Now().UTC(),
		ExchangeOrderID: "1001",
		ClientOrderID:   "cid-1",
		OrderStatus:     "NEW",
		ExecutionType:   "NEW",
		Symbol:          "ETHUSDT",
		Side:            "BUY",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(store.saved) != 0 {
		t.Fatalf("saved events = %d, want 0", len(store.saved))
	}
}
