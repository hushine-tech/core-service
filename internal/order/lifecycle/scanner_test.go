package lifecycle

import (
	"context"
	"errors"
	"testing"
	"time"
)

type scannerStoreStub struct {
	orders []OpenOrder
	events []Event
}

func (s *scannerStoreStub) ListOpenOrders(_ context.Context, _ int) ([]OpenOrder, error) {
	return s.orders, nil
}

func (s *scannerStoreStub) SaveLifecycleEvent(_ context.Context, event Event) (Event, error) {
	event.EventID = int64(len(s.events) + 1)
	s.events = append(s.events, event)
	return event, nil
}

type stateReaderStub struct {
	state     OrderState
	trades    []FillDelta
	queryErr  error
	tradeErr  error
	queryHits int
}

func (r *stateReaderStub) QueryOrder(_ context.Context, _ OpenOrder) (OrderState, error) {
	r.queryHits++
	return r.state, r.queryErr
}

func (r *stateReaderStub) QueryTrades(_ context.Context, _ OpenOrder) ([]FillDelta, error) {
	return r.trades, r.tradeErr
}

func TestScannerWritesFillLifecycleEvent(t *testing.T) {
	now := time.Now().UTC()
	store := &scannerStoreStub{orders: []OpenOrder{{
		SessionID:       "sess-1",
		AccountID:       1,
		VenueID:         10,
		IntentID:        "intent-1",
		AttemptID:       "attempt-1",
		OrderID:         "order-1",
		ExchangeOrderID: "ex-1",
		Symbol:          "ETHUSDT",
	}}}
	reader := &stateReaderStub{
		state:  OrderState{ExchangeOrderID: "ex-1", Status: "FILLED", ExecutedQty: 0.2},
		trades: []FillDelta{{ExchangeOrderID: "ex-1", ExchangeTradeID: "trade-1", Symbol: "ETHUSDT", Qty: 0.2, FillPrice: 3000, TradeTime: now}},
	}

	written, err := NewScanner(store, reader, ScannerConfig{}).ScanOnce(context.Background(), now)
	if err != nil {
		t.Fatalf("ScanOnce: %v", err)
	}
	if written != 1 || len(store.events) != 1 {
		t.Fatalf("written=%d events=%d, want 1", written, len(store.events))
	}
	event := store.events[0]
	if event.EventType != "fill" || event.ExchangeTradeID != "trade-1" || event.OrderStatus != "FILLED" {
		t.Fatalf("event = %+v, want fill lifecycle event", event)
	}
}

func TestScannerBackoffAfterVenueFailure(t *testing.T) {
	now := time.Now().UTC()
	store := &scannerStoreStub{orders: []OpenOrder{{VenueID: 10, OrderID: "order-1"}}}
	reader := &stateReaderStub{queryErr: errors.New("venue down")}
	scanner := NewScanner(store, reader, ScannerConfig{InitialBackoff: time.Minute})

	if _, err := scanner.ScanOnce(context.Background(), now); err != nil {
		t.Fatalf("first ScanOnce: %v", err)
	}
	if _, err := scanner.ScanOnce(context.Background(), now.Add(10*time.Second)); err != nil {
		t.Fatalf("second ScanOnce: %v", err)
	}
	if reader.queryHits != 1 {
		t.Fatalf("query hits = %d, want 1 due to venue backoff", reader.queryHits)
	}
}
