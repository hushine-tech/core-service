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
		Environment:     0,
		Exchange:        1,
		Market:          2,
		PositionSide:    0,
		Side:            "BUY",
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
	if event.EventSource != EventSourceRESTRecovery {
		t.Fatalf("event_source = %q, want %s", event.EventSource, EventSourceRESTRecovery)
	}
	if event.PositionSide != 0 {
		t.Fatalf("position_side = %d, want BOTH/0 to be valid", event.PositionSide)
	}
}

func TestValidateEventRouteFactsAllowsPositionSideBoth(t *testing.T) {
	err := ValidateEventRouteFacts(Event{
		SessionID:    "sess-1",
		AccountID:    1,
		VenueID:      10,
		Environment:  0,
		Exchange:     1,
		Market:       2,
		PositionSide: 0,
		Side:         "BUY",
		EventType:    "fill",
		EventSource:  EventSourceRESTRecovery,
		OrderStatus:  "FILLED",
		FillDelta:    FillDelta{Symbol: "ETHUSDT", Qty: 0.2, FillPrice: 3000},
		OrderState:   OrderState{Symbol: "ETHUSDT", Status: "FILLED"},
		OccurredAt:   time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("ValidateEventRouteFacts: %v", err)
	}
}

func TestScannerRejectsLifecycleEventWithoutRouteFacts(t *testing.T) {
	now := time.Now().UTC()
	store := &scannerStoreStub{orders: []OpenOrder{{
		SessionID:       "sess-1",
		AccountID:       1,
		VenueID:         10,
		ExchangeOrderID: "ex-1",
		Symbol:          "ETHUSDT",
	}}}
	reader := &stateReaderStub{
		state:  OrderState{ExchangeOrderID: "ex-1", Symbol: "ETHUSDT", Status: "FILLED", ExecutedQty: 0.2},
		trades: []FillDelta{{ExchangeOrderID: "ex-1", ExchangeTradeID: "trade-1", Symbol: "ETHUSDT", Qty: 0.2, FillPrice: 3000, TradeTime: now}},
	}

	written, err := NewScanner(store, reader, ScannerConfig{}).ScanOnce(context.Background(), now)
	if err == nil {
		t.Fatal("expected missing route facts to fail closed")
	}
	if written != 0 || len(store.events) != 0 {
		t.Fatalf("written=%d events=%d, want no ambiguous lifecycle event", written, len(store.events))
	}
}

func TestValidateEventRouteFactsRejectsMissingSymbol(t *testing.T) {
	err := ValidateEventRouteFacts(Event{
		SessionID:    "sess-1",
		AccountID:    1,
		VenueID:      10,
		Environment:  0,
		Exchange:     1,
		Market:       2,
		PositionSide: 0,
		Side:         "BUY",
		EventType:    "fill",
		EventSource:  EventSourceRESTRecovery,
		OrderStatus:  "FILLED",
		OccurredAt:   time.Now().UTC(),
	})
	if err == nil {
		t.Fatal("expected missing symbol to be rejected")
	}
}

func TestValidateEventRouteFactsRejectsUnsupportedEventSource(t *testing.T) {
	err := ValidateEventRouteFacts(Event{
		SessionID:    "sess-1",
		AccountID:    1,
		VenueID:      10,
		Environment:  0,
		Exchange:     1,
		Market:       2,
		PositionSide: 0,
		Side:         "BUY",
		EventType:    "fill",
		EventSource:  "unknown",
		OrderStatus:  "FILLED",
		FillDelta:    FillDelta{Symbol: "ETHUSDT", Qty: 0.2, FillPrice: 3000},
		OccurredAt:   time.Now().UTC(),
	})
	if err == nil {
		t.Fatal("expected unsupported event source to be rejected")
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
