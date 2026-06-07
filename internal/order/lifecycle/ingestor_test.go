package lifecycle

import (
	"context"
	"testing"
	"time"
)

type ingestorStoreStub struct {
	events []Event
	calls  int
}

func (s *ingestorStoreStub) SaveLifecycleEvent(_ context.Context, event Event) (Event, error) {
	s.calls++
	event.EventID = int64(len(s.events) + 1)
	s.events = append(s.events, event)
	return event, nil
}

func TestIngestorPassesDuplicateTradeToStore(t *testing.T) {
	store := &ingestorStoreStub{}
	ingestor := NewIngestor(store)
	event := Event{
		SessionID:       "sess-1",
		AccountID:       1,
		VenueID:         10,
		Environment:     2,
		Exchange:        1,
		Market:          2,
		PositionSide:    0,
		Side:            "BUY",
		EventType:       "fill",
		EventSource:     EventSourceWebsocket,
		OrderStatus:     "PARTIALLY_FILLED",
		ExchangeOrderID: "ex-1",
		ExchangeTradeID: "trade-1",
		FillDelta: FillDelta{
			ExchangeOrderID: "ex-1",
			ExchangeTradeID: "trade-1",
			Symbol:          "ETHUSDT",
			Qty:             0.1,
			FillPrice:       3000,
			TradeTime:       time.Now().UTC(),
		},
		OccurredAt: time.Now().UTC(),
	}

	first, err := ingestor.Ingest(context.Background(), event)
	if err != nil {
		t.Fatalf("first Ingest: %v", err)
	}
	restEvent := event
	restEvent.EventSource = EventSourceRESTRecovery
	restEvent.OrderStatus = "FILLED"
	restEvent.FillDelta.FeeMissing = true
	second, err := ingestor.Ingest(context.Background(), restEvent)
	if err != nil {
		t.Fatalf("second Ingest: %v", err)
	}
	if first.EventID == second.EventID {
		t.Fatalf("stub should allocate a new id when ingestor calls store twice: first=%d second=%d", first.EventID, second.EventID)
	}
	if store.calls != 2 {
		t.Fatalf("store calls = %d, want 2 so repository upsert can refresh duplicate payloads", store.calls)
	}
}
