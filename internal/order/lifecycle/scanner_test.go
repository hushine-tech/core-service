package lifecycle

import (
	"context"
	"errors"
	"testing"
	"time"
)

type scannerStoreStub struct {
	orders        []OpenOrder
	events        []Event
	markedOrderID string
	markedAt      time.Time
}

func (s *scannerStoreStub) ListDueOpenOrders(_ context.Context, _ int) ([]OpenOrder, error) {
	return s.orders, nil
}

func (s *scannerStoreStub) SaveLifecycleEvent(_ context.Context, event Event) (Event, error) {
	event.EventID = int64(len(s.events) + 1)
	s.events = append(s.events, event)
	return event, nil
}

func (s *scannerStoreStub) MarkRecoveryExpired(_ context.Context, orderID string, forceClosedAt time.Time, _ string) error {
	s.markedOrderID = orderID
	s.markedAt = forceClosedAt
	return nil
}

type stateReaderStub struct {
	state     OrderState
	trades    []FillDelta
	queryErr  error
	tradeErr  error
	cancelErr error

	queryHits  int
	cancelHits int
	tradeOrder OpenOrder
}

func (r *stateReaderStub) QueryOrder(_ context.Context, _ OpenOrder) (OrderState, error) {
	r.queryHits++
	return r.state, r.queryErr
}

func (r *stateReaderStub) QueryTrades(_ context.Context, order OpenOrder) ([]FillDelta, error) {
	r.tradeOrder = order
	return r.trades, r.tradeErr
}

func (r *stateReaderStub) CancelOrder(_ context.Context, _ OpenOrder) (CancelResult, error) {
	r.cancelHits++
	return CancelResult{ExchangeOrderID: r.state.ExchangeOrderID, Status: "CANCELED"}, r.cancelErr
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

func TestScannerQueriesTradesWithRecoveredExchangeOrderID(t *testing.T) {
	now := time.Now().UTC()
	store := &scannerStoreStub{orders: []OpenOrder{{
		SessionID:     "sess-1",
		AccountID:     1,
		VenueID:       10,
		Environment:   2,
		Exchange:      1,
		Market:        1,
		PositionSide:  0,
		Side:          "BUY",
		IntentID:      "intent-1",
		AttemptID:     "attempt-1",
		OrderID:       "order-1",
		ClientOrderID: "client-1",
		Symbol:        "ETHUSDT",
	}}}
	reader := &stateReaderStub{
		state:  OrderState{ExchangeOrderID: "ex-from-query", Symbol: "ETHUSDT", Status: "FILLED", ExecutedQty: 0.2},
		trades: []FillDelta{{ExchangeOrderID: "ex-from-query", ExchangeTradeID: "trade-1", Symbol: "ETHUSDT", Qty: 0.2, FillPrice: 3000, TradeTime: now}},
	}

	if _, err := NewScanner(store, reader, ScannerConfig{}).ScanOnce(context.Background(), now); err != nil {
		t.Fatalf("ScanOnce: %v", err)
	}
	if reader.tradeOrder.ExchangeOrderID != "ex-from-query" {
		t.Fatalf("trade query exchange_order_id = %q, want recovered exchange id", reader.tradeOrder.ExchangeOrderID)
	}
}

func TestScannerForceClosesExpiredRecoveryOrder(t *testing.T) {
	now := time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)
	store := &scannerStoreStub{orders: []OpenOrder{{
		SessionID:          "sess-1",
		AccountID:          1,
		VenueID:            10,
		Environment:        2,
		Exchange:           1,
		Market:             2,
		PositionSide:       0,
		Side:               "BUY",
		IntentID:           "intent-1",
		AttemptID:          "attempt-1",
		OrderID:            "order-1",
		ExchangeOrderID:    "ex-1",
		ClientOrderID:      "client-1",
		Symbol:             "ETHUSDT",
		RecoveryStatus:     "PARTIALLY_FILLED",
		RecoveryDeadlineAt: now.Add(-time.Second),
	}}}
	reader := &stateReaderStub{
		state: OrderState{
			ExchangeOrderID: "ex-1",
			Symbol:          "ETHUSDT",
			Status:          "CANCELED",
			OrigQty:         0.5,
			ExecutedQty:     0.2,
			RemainingQty:    0.3,
			UpdatedAt:       now,
		},
		trades: []FillDelta{{
			ExchangeOrderID: "ex-1",
			ExchangeTradeID: "trade-1",
			Symbol:          "ETHUSDT",
			Qty:             0.2,
			FillPrice:       3000,
			TradeTime:       now.Add(-time.Minute),
		}},
	}

	written, err := NewScanner(store, reader, ScannerConfig{}).ScanOnce(context.Background(), now)
	if err != nil {
		t.Fatalf("ScanOnce: %v", err)
	}
	if reader.cancelHits != 1 || reader.queryHits != 1 {
		t.Fatalf("cancel_hits=%d query_hits=%d, want 1/1", reader.cancelHits, reader.queryHits)
	}
	if written != 2 || len(store.events) != 2 {
		t.Fatalf("written=%d events=%d, want fill + terminal", written, len(store.events))
	}
	if store.events[0].EventType != "fill" || store.events[0].EventSource != EventSourceForceClose {
		t.Fatalf("fill event = %+v, want force_close fill", store.events[0])
	}
	terminal := store.events[1]
	if terminal.EventType != "terminal" || terminal.EventSource != EventSourceForceClose || terminal.OrderStatus != "RECOVERY_EXPIRED" {
		t.Fatalf("terminal event = %+v, want RECOVERY_EXPIRED force_close terminal", terminal)
	}
	if !terminal.OccurredAt.Equal(now) {
		t.Fatalf("terminal occurred_at = %s, want %s", terminal.OccurredAt, now)
	}
	if store.markedOrderID != "order-1" || !store.markedAt.Equal(now) {
		t.Fatalf("marked recovery expired order_id=%q at=%s, want order-1/%s", store.markedOrderID, store.markedAt, now)
	}
}

func TestScannerQueriesFinalStateWhenForceCloseCancelFails(t *testing.T) {
	now := time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)
	store := &scannerStoreStub{orders: []OpenOrder{{
		SessionID:          "sess-1",
		AccountID:          1,
		VenueID:            10,
		Environment:        2,
		Exchange:           1,
		Market:             2,
		PositionSide:       0,
		Side:               "BUY",
		IntentID:           "intent-1",
		AttemptID:          "attempt-1",
		OrderID:            "order-1",
		ExchangeOrderID:    "ex-1",
		Symbol:             "ETHUSDT",
		RecoveryStatus:     "OPEN",
		RecoveryDeadlineAt: now.Add(-time.Second),
	}}}
	reader := &stateReaderStub{
		state:     OrderState{ExchangeOrderID: "ex-1", Symbol: "ETHUSDT", Status: "CANCELED", UpdatedAt: now},
		cancelErr: errors.New("cancel failed"),
	}

	written, err := NewScanner(store, reader, ScannerConfig{}).ScanOnce(context.Background(), now)
	if err != nil {
		t.Fatalf("ScanOnce: %v", err)
	}
	if written != 1 || reader.cancelHits != 1 || reader.queryHits != 1 {
		t.Fatalf("written=%d cancel_hits=%d query_hits=%d, want terminal after failed cancel", written, reader.cancelHits, reader.queryHits)
	}
	if store.events[0].EventType != "terminal" || store.events[0].OrderStatus != "RECOVERY_EXPIRED" {
		t.Fatalf("terminal event = %+v", store.events[0])
	}
	if store.markedOrderID != "order-1" {
		t.Fatalf("marked recovery expired order_id=%q, want order-1", store.markedOrderID)
	}
}

func TestScannerBackoffsWhenForceCloseCancelFailsAndOrderStillOpen(t *testing.T) {
	now := time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)
	store := &scannerStoreStub{orders: []OpenOrder{{
		SessionID:          "sess-1",
		AccountID:          1,
		VenueID:            10,
		Environment:        2,
		Exchange:           1,
		Market:             2,
		PositionSide:       0,
		Side:               "BUY",
		IntentID:           "intent-1",
		AttemptID:          "attempt-1",
		OrderID:            "order-1",
		ExchangeOrderID:    "ex-1",
		Symbol:             "ETHUSDT",
		RecoveryStatus:     "OPEN",
		RecoveryDeadlineAt: now.Add(-time.Second),
	}}}
	reader := &stateReaderStub{
		state:     OrderState{ExchangeOrderID: "ex-1", Symbol: "ETHUSDT", Status: "NEW", RemainingQty: 0.3, UpdatedAt: now},
		cancelErr: errors.New("cancel failed"),
	}
	scanner := NewScanner(store, reader, ScannerConfig{InitialBackoff: time.Minute})

	written, err := scanner.ScanOnce(context.Background(), now)
	if err != nil {
		t.Fatalf("first ScanOnce: %v", err)
	}
	if written != 0 || len(store.events) != 0 || store.markedOrderID != "" {
		t.Fatalf("force-close should not finalize open order after cancel failure: written=%d events=%d marked=%q", written, len(store.events), store.markedOrderID)
	}
	if _, err := scanner.ScanOnce(context.Background(), now.Add(10*time.Second)); err != nil {
		t.Fatalf("second ScanOnce: %v", err)
	}
	if reader.cancelHits != 1 {
		t.Fatalf("cancel hits = %d, want venue backoff to skip retry", reader.cancelHits)
	}
}

func TestScannerBackoffsWhenForceCloseCancelAndQueryFail(t *testing.T) {
	now := time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)
	store := &scannerStoreStub{orders: []OpenOrder{{
		SessionID:          "sess-1",
		AccountID:          1,
		VenueID:            10,
		Environment:        2,
		Exchange:           1,
		Market:             2,
		PositionSide:       0,
		Side:               "BUY",
		IntentID:           "intent-1",
		AttemptID:          "attempt-1",
		OrderID:            "order-1",
		ExchangeOrderID:    "ex-1",
		Symbol:             "ETHUSDT",
		RecoveryStatus:     "OPEN",
		RecoveryDeadlineAt: now.Add(-time.Second),
	}}}
	reader := &stateReaderStub{
		cancelErr: errors.New("cancel failed"),
		queryErr:  errors.New("query failed"),
	}
	scanner := NewScanner(store, reader, ScannerConfig{InitialBackoff: time.Minute})

	written, err := scanner.ScanOnce(context.Background(), now)
	if err != nil {
		t.Fatalf("first ScanOnce: %v", err)
	}
	if written != 0 || reader.cancelHits != 1 || reader.queryHits != 1 {
		t.Fatalf("written=%d cancel_hits=%d query_hits=%d, want failed force-close attempt", written, reader.cancelHits, reader.queryHits)
	}
	if _, err := scanner.ScanOnce(context.Background(), now.Add(10*time.Second)); err != nil {
		t.Fatalf("second ScanOnce: %v", err)
	}
	if reader.cancelHits != 1 {
		t.Fatalf("cancel hits = %d, want venue backoff to skip retry", reader.cancelHits)
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
