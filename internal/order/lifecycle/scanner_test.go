package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"
)

type scannerStoreStub struct {
	orders          []OpenOrder
	events          []Event
	markedOrderID   string
	markedAt        time.Time
	resolvedOrderID string
	resolvedState   OrderState
	resolvedAt      time.Time
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

func (s *scannerStoreStub) MarkRecoveryResolved(_ context.Context, orderID string, state OrderState, resolvedAt time.Time) error {
	s.resolvedOrderID = orderID
	s.resolvedState = state
	s.resolvedAt = resolvedAt
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
	if written != 2 || len(store.events) != 2 {
		t.Fatalf("written=%d events=%d, want fill + terminal", written, len(store.events))
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
	terminal := store.events[1]
	if terminal.EventType != "terminal" || terminal.EventSource != EventSourceRESTRecovery || terminal.OrderStatus != "FILLED" {
		t.Fatalf("terminal event = %+v, want rest_recovery FILLED terminal", terminal)
	}
	if store.resolvedOrderID != "order-1" {
		t.Fatalf("resolved order_id=%q, want order-1", store.resolvedOrderID)
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

func TestScannerMarksTerminalRecoveredOrderResolved(t *testing.T) {
	now := time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)
	store := &scannerStoreStub{orders: []OpenOrder{{
		SessionID:       "sess-1",
		AccountID:       1,
		VenueID:         10,
		Environment:     2,
		Exchange:        1,
		Market:          2,
		PositionSide:    0,
		Side:            "BUY",
		IntentID:        "intent-1",
		AttemptID:       "attempt-1",
		OrderID:         "order-1",
		ExchangeOrderID: "ex-1",
		Symbol:          "ETHUSDT",
		RecoveryStatus:  "PARTIALLY_FILLED",
	}}}
	reader := &stateReaderStub{
		state: OrderState{
			ExchangeOrderID: "ex-1",
			Symbol:          "ETHUSDT",
			Status:          "FILLED",
			OrigQty:         0.5,
			ExecutedQty:     0.5,
			RemainingQty:    0,
			AvgPrice:        3000,
			UpdatedAt:       now,
		},
		trades: []FillDelta{
			{ExchangeOrderID: "ex-1", ExchangeTradeID: "trade-1", Symbol: "ETHUSDT", Qty: 0.2, FillPrice: 2990, TradeTime: now.Add(-time.Minute)},
			{ExchangeOrderID: "ex-1", ExchangeTradeID: "trade-2", Symbol: "ETHUSDT", Qty: 0.3, FillPrice: 3006.6667, TradeTime: now},
		},
	}

	written, err := NewScanner(store, reader, ScannerConfig{}).ScanOnce(context.Background(), now)
	if err != nil {
		t.Fatalf("ScanOnce: %v", err)
	}
	if written != 3 || len(store.events) != 3 {
		t.Fatalf("written=%d events=%d, want 2 fill events + terminal event", written, len(store.events))
	}
	terminal := store.events[2]
	if terminal.EventType != "terminal" || terminal.EventSource != EventSourceRESTRecovery || terminal.OrderStatus != "FILLED" {
		t.Fatalf("terminal event = %+v, want rest_recovery FILLED terminal", terminal)
	}
	if store.resolvedOrderID != "order-1" || store.resolvedState.Status != "FILLED" || !store.resolvedAt.Equal(now) {
		t.Fatalf("resolved order_id=%q state=%+v at=%s, want order-1 FILLED at %s", store.resolvedOrderID, store.resolvedState, store.resolvedAt, now)
	}
}

func TestScannerKeepsTerminalOrderRecoverableWhenTradesIncomplete(t *testing.T) {
	now := time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)
	store := &scannerStoreStub{orders: []OpenOrder{{
		SessionID:       "sess-1",
		AccountID:       1,
		VenueID:         10,
		Environment:     2,
		Exchange:        1,
		Market:          2,
		PositionSide:    0,
		Side:            "BUY",
		IntentID:        "intent-1",
		AttemptID:       "attempt-1",
		OrderID:         "order-1",
		ExchangeOrderID: "ex-1",
		Symbol:          "ETHUSDT",
		RecoveryStatus:  "PARTIALLY_FILLED",
	}}}
	reader := &stateReaderStub{
		state: OrderState{
			ExchangeOrderID: "ex-1",
			Symbol:          "ETHUSDT",
			Status:          "FILLED",
			OrigQty:         0.5,
			ExecutedQty:     0.5,
			RemainingQty:    0,
			AvgPrice:        3000,
			UpdatedAt:       now,
		},
		trades: []FillDelta{
			{ExchangeOrderID: "ex-1", ExchangeTradeID: "trade-1", Symbol: "ETHUSDT", Qty: 0.2, FillPrice: 2990, TradeTime: now.Add(-time.Minute)},
		},
	}

	written, err := NewScanner(store, reader, ScannerConfig{}).ScanOnce(context.Background(), now)
	if err != nil {
		t.Fatalf("ScanOnce: %v", err)
	}
	if written != 0 || len(store.events) != 0 {
		t.Fatalf("written=%d events=%d, want no events until trade qty covers executed_qty", written, len(store.events))
	}
	if store.resolvedOrderID != "" {
		t.Fatalf("resolved order_id=%q, want order to remain recoverable", store.resolvedOrderID)
	}
}

func TestScannerRestRecoveryCompletesPartialOrderWithMockBinance(t *testing.T) {
	now := time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/fapi/v1/order":
			if r.URL.Query().Get("symbol") != "ETHUSDT" {
				t.Fatalf("symbol = %q, want ETHUSDT", r.URL.Query().Get("symbol"))
			}
			if r.URL.Query().Get("origClientOrderId") != "client-1" {
				t.Fatalf("origClientOrderId = %q, want client-1", r.URL.Query().Get("origClientOrderId"))
			}
			_, _ = w.Write([]byte(fmt.Sprintf(`{
				"orderId":9001,
				"clientOrderId":"client-1",
				"symbol":"ETHUSDT",
				"status":"FILLED",
				"origQty":"0.5",
				"executedQty":"0.5",
				"avgPrice":"3006",
				"updateTime":%d
			}`, now.UnixMilli())))
		case "/fapi/v1/userTrades":
			if r.URL.Query().Get("orderId") != "9001" {
				t.Fatalf("orderId = %q, want 9001", r.URL.Query().Get("orderId"))
			}
			_, _ = w.Write([]byte(fmt.Sprintf(`[
				{"id":7001,"orderId":9001,"symbol":"ETHUSDT","price":"2990","qty":"0.2","commission":"0.1196","commissionAsset":"USDT","time":%d},
				{"id":7002,"orderId":9001,"symbol":"ETHUSDT","price":"3016.66666667","qty":"0.3","commission":"0.1810","commissionAsset":"USDT","time":%d}
			]`, now.Add(-time.Minute).UnixMilli(), now.UnixMilli())))
		default:
			t.Fatalf("unexpected mock Binance path: %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	store := &scannerStoreStub{orders: []OpenOrder{{
		SessionID:       "sess-1",
		AccountID:       1,
		VenueID:         10,
		Environment:     2,
		Exchange:        1,
		Market:          2,
		PositionSide:    0,
		Side:            "BUY",
		IntentID:        "intent-1",
		AttemptID:       "attempt-1",
		OrderID:         "order-1",
		ClientOrderID:   "client-1",
		ExchangeOrderID: "9001",
		Symbol:          "ETHUSDT",
		RecoveryStatus:  "PARTIALLY_FILLED",
	}}}
	reader := &mockBinanceRESTReader{baseURL: srv.URL, client: srv.Client()}

	written, err := NewScanner(store, reader, ScannerConfig{}).ScanOnce(context.Background(), now)
	if err != nil {
		t.Fatalf("ScanOnce: %v", err)
	}
	if reader.orderHits != 1 || reader.tradeHits != 1 {
		t.Fatalf("mock Binance hits order=%d trades=%d, want 1/1", reader.orderHits, reader.tradeHits)
	}
	if written != 3 || len(store.events) != 3 {
		t.Fatalf("written=%d events=%d, want 2 fill events + terminal", written, len(store.events))
	}
	if store.events[0].ExchangeTradeID != "7001" || store.events[1].ExchangeTradeID != "7002" {
		t.Fatalf("fill events = %+v, want two mock Binance trades", store.events[:2])
	}
	terminal := store.events[2]
	if terminal.EventType != "terminal" || terminal.OrderStatus != "FILLED" || terminal.OrderState.ExecutedQty != 0.5 {
		t.Fatalf("terminal event = %+v, want final FILLED state", terminal)
	}
	if store.resolvedOrderID != "order-1" || store.resolvedState.ExchangeOrderID != "9001" || !store.resolvedAt.Equal(now) {
		t.Fatalf("resolved order_id=%q state=%+v at=%s, want recovered mock Binance terminal", store.resolvedOrderID, store.resolvedState, store.resolvedAt)
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

type mockBinanceRESTReader struct {
	baseURL string
	client  *http.Client

	orderHits int
	tradeHits int
}

func (r *mockBinanceRESTReader) QueryOrder(ctx context.Context, order OpenOrder) (OrderState, error) {
	r.orderHits++
	params := url.Values{}
	params.Set("symbol", order.Symbol)
	if order.ClientOrderID != "" {
		params.Set("origClientOrderId", order.ClientOrderID)
	} else {
		params.Set("orderId", order.ExchangeOrderID)
	}
	var raw struct {
		OrderID       int64  `json:"orderId"`
		ClientOrderID string `json:"clientOrderId"`
		Symbol        string `json:"symbol"`
		Status        string `json:"status"`
		OrigQty       string `json:"origQty"`
		ExecutedQty   string `json:"executedQty"`
		AvgPrice      string `json:"avgPrice"`
		UpdateTime    int64  `json:"updateTime"`
	}
	if err := r.getJSON(ctx, "/fapi/v1/order", params, &raw); err != nil {
		return OrderState{}, err
	}
	origQty, err := parseMockBinanceFloat(raw.OrigQty)
	if err != nil {
		return OrderState{}, fmt.Errorf("parse origQty: %w", err)
	}
	executedQty, err := parseMockBinanceFloat(raw.ExecutedQty)
	if err != nil {
		return OrderState{}, fmt.Errorf("parse executedQty: %w", err)
	}
	avgPrice, err := parseMockBinanceFloat(raw.AvgPrice)
	if err != nil {
		return OrderState{}, fmt.Errorf("parse avgPrice: %w", err)
	}
	return OrderState{
		ExchangeOrderID: strconv.FormatInt(raw.OrderID, 10),
		ClientOrderID:   raw.ClientOrderID,
		Symbol:          raw.Symbol,
		Status:          raw.Status,
		OrigQty:         origQty,
		ExecutedQty:     executedQty,
		RemainingQty:    maxFloat(origQty-executedQty, 0),
		AvgPrice:        avgPrice,
		UpdatedAt:       time.UnixMilli(raw.UpdateTime).UTC(),
	}, nil
}

func (r *mockBinanceRESTReader) QueryTrades(ctx context.Context, order OpenOrder) ([]FillDelta, error) {
	r.tradeHits++
	params := url.Values{}
	params.Set("symbol", order.Symbol)
	params.Set("orderId", order.ExchangeOrderID)
	var raw []struct {
		ID              int64  `json:"id"`
		OrderID         int64  `json:"orderId"`
		Symbol          string `json:"symbol"`
		Price           string `json:"price"`
		Qty             string `json:"qty"`
		Commission      string `json:"commission"`
		CommissionAsset string `json:"commissionAsset"`
		Time            int64  `json:"time"`
	}
	if err := r.getJSON(ctx, "/fapi/v1/userTrades", params, &raw); err != nil {
		return nil, err
	}
	out := make([]FillDelta, 0, len(raw))
	for _, item := range raw {
		qty, err := parseMockBinanceFloat(item.Qty)
		if err != nil {
			return nil, fmt.Errorf("parse trade qty: %w", err)
		}
		price, err := parseMockBinanceFloat(item.Price)
		if err != nil {
			return nil, fmt.Errorf("parse trade price: %w", err)
		}
		fee, err := parseMockBinanceFloat(item.Commission)
		if err != nil {
			return nil, fmt.Errorf("parse trade commission: %w", err)
		}
		out = append(out, FillDelta{
			ExchangeTradeID: strconv.FormatInt(item.ID, 10),
			ExchangeOrderID: strconv.FormatInt(item.OrderID, 10),
			Symbol:          item.Symbol,
			Qty:             qty,
			FillPrice:       price,
			Fee:             fee,
			FeeAsset:        item.CommissionAsset,
			TradeTime:       time.UnixMilli(item.Time).UTC(),
		})
	}
	return out, nil
}

func (r *mockBinanceRESTReader) getJSON(ctx context.Context, path string, params url.Values, out any) error {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, r.baseURL+path+"?"+params.Encode(), nil)
	if err != nil {
		return err
	}
	resp, err := r.client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func parseMockBinanceFloat(value string) (float64, error) {
	return strconv.ParseFloat(value, 64)
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
