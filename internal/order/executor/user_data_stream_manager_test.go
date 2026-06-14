package executor

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
	exchangeadapter "github.com/hushine-tech/core-service/internal/exchange/adapter"
	"github.com/hushine-tech/core-service/internal/order/accountmeta"
	"github.com/hushine-tech/core-service/internal/order/lifecycle"
)

type streamManagerStore struct {
	openOrders []lifecycle.OpenOrder
	saved      []lifecycle.Event
	savedCh    chan lifecycle.Event
	mu         sync.Mutex
}

func (s *streamManagerStore) ListOpenOrders(context.Context, int) ([]lifecycle.OpenOrder, error) {
	return append([]lifecycle.OpenOrder(nil), s.openOrders...), nil
}

func (s *streamManagerStore) ResolveOpenOrderByExchangeRef(_ context.Context, venueID int64, exchangeOrderID, clientOrderID string) (lifecycle.OpenOrder, error) {
	for _, order := range s.openOrders {
		if order.VenueID != venueID {
			continue
		}
		if exchangeOrderID != "" && order.ExchangeOrderID == exchangeOrderID {
			return order, nil
		}
		if clientOrderID != "" && order.ClientOrderID == clientOrderID {
			return order, nil
		}
	}
	return lifecycle.OpenOrder{}, lifecycle.ErrOpenOrderNotFound
}

func (s *streamManagerStore) SaveLifecycleEvent(_ context.Context, event lifecycle.Event) (lifecycle.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	event.EventID = int64(len(s.saved) + 1)
	s.saved = append(s.saved, event)
	if s.savedCh != nil {
		select {
		case s.savedCh <- event:
		default:
		}
	}
	return event, nil
}

type streamManagerMetaGetter struct {
	meta accountmeta.Meta
	err  error
}

func (g streamManagerMetaGetter) Get(context.Context, int64, int32, int32) (accountmeta.Meta, error) {
	return g.meta, g.err
}

type captureUserDataStream struct {
	started  chan exchangeadapter.UserDataStreamRequest
	handlers chan func(context.Context, exchangeadapter.UserDataOrderEvent) error
	mu       sync.Mutex
	calls    int
}

func newCaptureUserDataStream() *captureUserDataStream {
	return &captureUserDataStream{
		started:  make(chan exchangeadapter.UserDataStreamRequest, 8),
		handlers: make(chan func(context.Context, exchangeadapter.UserDataOrderEvent) error, 8),
	}
}

func (s *captureUserDataStream) Listen(ctx context.Context, req exchangeadapter.UserDataStreamRequest, handle func(context.Context, exchangeadapter.UserDataOrderEvent) error) error {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	s.started <- req
	s.handlers <- handle
	<-ctx.Done()
	return ctx.Err()
}

func (s *captureUserDataStream) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

type streamManagerFactory struct {
	stream *captureUserDataStream
}

func (f streamManagerFactory) CredentialValidator() (exchangeadapter.CredentialValidator, error) {
	return streamManagerCredentialValidator{}, nil
}

func (f streamManagerFactory) AccountSnapshotReader() (exchangeadapter.AccountSnapshotReader, error) {
	return nil, exchangeadapter.CapabilityUnsupported("account_snapshot_reader")
}

func (f streamManagerFactory) SymbolRulesReader() (exchangeadapter.SymbolRulesReader, error) {
	return nil, exchangeadapter.CapabilityUnsupported("symbol_rules_reader")
}

func (f streamManagerFactory) OrderExecutor() (exchangeadapter.OrderExecutor, error) {
	return nil, exchangeadapter.CapabilityUnsupported("order_executor")
}

func (f streamManagerFactory) OrderCapabilityProvider() (exchangeadapter.OrderCapabilityProvider, error) {
	return nil, exchangeadapter.CapabilityUnsupported("order_capability_provider")
}

func (f streamManagerFactory) OrderStateReader() (exchangeadapter.OrderStateReader, error) {
	return nil, exchangeadapter.CapabilityUnsupported("order_state_reader")
}

func (f streamManagerFactory) OrderCanceller() (exchangeadapter.OrderCanceller, error) {
	return nil, exchangeadapter.CapabilityUnsupported("order_canceller")
}

func (f streamManagerFactory) UserDataStream() (exchangeadapter.UserDataStream, error) {
	if f.stream == nil {
		return nil, exchangeadapter.CapabilityUnsupported("user_data_stream")
	}
	return f.stream, nil
}

type streamManagerCredentialValidator struct{}

func (streamManagerCredentialValidator) ValidateCredential(_ context.Context, raw json.RawMessage) (exchangeadapter.ParsedCredential, error) {
	var payload map[string]string
	if err := json.Unmarshal(raw, &payload); err != nil {
		return exchangeadapter.ParsedCredential{}, err
	}
	if payload["api_key"] == "" || payload["api_secret"] == "" {
		return exchangeadapter.ParsedCredential{}, errors.New("missing credential")
	}
	return exchangeadapter.ParsedCredential{Raw: raw, Metadata: payload}, nil
}

func TestUserDataStreamManagerStartsStreamAndIngestsPartialFill(t *testing.T) {
	stream := newCaptureUserDataStream()
	registry := exchangeadapter.NewRegistry()
	route := exchangeadapter.Route{
		Exchange:    domain.ExchangeBinance,
		Environment: domain.EnvironmentDemo,
		Market:      domain.MarketPerpetualFutures,
	}
	registry.Register(route, streamManagerFactory{stream: stream})
	store := &streamManagerStore{
		savedCh: make(chan lifecycle.Event, 1),
		openOrders: []lifecycle.OpenOrder{{
			SessionID:       "sess-1",
			AccountID:       10,
			VenueID:         20,
			Environment:     int32(domain.EnvironmentDemo),
			Exchange:        int32(domain.ExchangeBinance),
			Market:          int32(domain.MarketPerpetualFutures),
			PositionSide:    1,
			Side:            "BUY",
			IntentID:        "intent-1",
			AttemptID:       "attempt-1",
			OrderID:         "order-1",
			ExchangeOrderID: "1001",
			ClientOrderID:   "cid-1",
			Symbol:          "ETHUSDT",
		}},
	}
	manager := NewUserDataStreamManager(registry, streamManagerMetaGetter{meta: accountmeta.Meta{
		AccountID:      10,
		VenueID:        20,
		UserID:         30,
		Environment:    int32(domain.EnvironmentDemo),
		Exchange:       int32(domain.ExchangeBinance),
		Market:         int32(domain.MarketPerpetualFutures),
		APIKey:         "key",
		APISecret:      "secret",
		CredentialJSON: `{"api_secret":"secret"}`,
	}}, store, UserDataStreamManagerConfig{BatchSize: 50})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, err := manager.SyncOnce(ctx)
	if err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	if started != 1 {
		t.Fatalf("started streams = %d, want 1", started)
	}
	req := <-stream.started
	if req.AccountID != 10 || req.VenueID != 20 || req.Credential.Metadata["api_key"] != "key" {
		t.Fatalf("unexpected stream request: %+v", req)
	}
	handle := <-stream.handlers
	if err := handle(ctx, exchangeadapter.UserDataOrderEvent{
		EventSource:          "websocket",
		EventTime:            time.UnixMilli(1700000000000).UTC(),
		Symbol:               "ETHUSDT",
		ClientOrderID:        "cid-1",
		ExchangeOrderID:      "1001",
		ExchangeTradeID:      "9001",
		Side:                 "BUY",
		OrderStatus:          "PARTIALLY_FILLED",
		ExecutionType:        "TRADE",
		LastFilledQty:        0.2,
		LastFilledPrice:      2000,
		AccumulatedFilledQty: 0.2,
		Fee:                  0.08,
		FeeAsset:             "USDT",
	}); err != nil {
		t.Fatalf("handle event: %v", err)
	}

	select {
	case event := <-store.savedCh:
		if event.EventSource != lifecycle.EventSourceWebsocket || event.EventType != "fill" || event.ExchangeTradeID != "9001" {
			t.Fatalf("unexpected lifecycle event: %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for lifecycle event")
	}
}

func TestUserDataStreamManagerDeduplicatesActiveVenueStream(t *testing.T) {
	stream := newCaptureUserDataStream()
	registry := exchangeadapter.NewRegistry()
	route := exchangeadapter.Route{Exchange: domain.ExchangeBinance, Environment: domain.EnvironmentDemo, Market: domain.MarketPerpetualFutures}
	registry.Register(route, streamManagerFactory{stream: stream})
	store := &streamManagerStore{openOrders: []lifecycle.OpenOrder{
		{AccountID: 10, VenueID: 20, Environment: int32(domain.EnvironmentDemo), Exchange: int32(domain.ExchangeBinance), Market: int32(domain.MarketPerpetualFutures), ExchangeOrderID: "1001", ClientOrderID: "cid-1", Symbol: "ETHUSDT", Side: "BUY"},
		{AccountID: 10, VenueID: 20, Environment: int32(domain.EnvironmentDemo), Exchange: int32(domain.ExchangeBinance), Market: int32(domain.MarketPerpetualFutures), ExchangeOrderID: "1002", ClientOrderID: "cid-2", Symbol: "ETHUSDT", Side: "BUY"},
	}}
	manager := NewUserDataStreamManager(registry, streamManagerMetaGetter{meta: accountmeta.Meta{
		AccountID: 10, VenueID: 20, UserID: 30, Environment: int32(domain.EnvironmentDemo),
		Exchange: int32(domain.ExchangeBinance), Market: int32(domain.MarketPerpetualFutures),
		APIKey: "key", APISecret: "secret", CredentialJSON: `{"api_secret":"secret"}`,
	}}, store, UserDataStreamManagerConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started, err := manager.SyncOnce(ctx)
	if err != nil {
		t.Fatalf("first SyncOnce: %v", err)
	}
	if started != 1 {
		t.Fatalf("first started = %d, want 1", started)
	}
	<-stream.started
	<-stream.handlers
	started, err = manager.SyncOnce(ctx)
	if err != nil {
		t.Fatalf("second SyncOnce: %v", err)
	}
	if started != 0 {
		t.Fatalf("second started = %d, want 0", started)
	}
	if calls := stream.Calls(); calls != 1 {
		t.Fatalf("stream calls = %d, want 1", calls)
	}
}

func TestUserDataStreamManagerSkipsUnsupportedStreamCapability(t *testing.T) {
	registry := exchangeadapter.NewRegistry()
	route := exchangeadapter.Route{Exchange: domain.ExchangeOKX, Environment: domain.EnvironmentDemo, Market: domain.MarketPerpetualFutures}
	registry.Register(route, streamManagerFactory{})
	store := &streamManagerStore{openOrders: []lifecycle.OpenOrder{
		{AccountID: 10, VenueID: 20, Environment: int32(domain.EnvironmentDemo), Exchange: int32(domain.ExchangeOKX), Market: int32(domain.MarketPerpetualFutures), ExchangeOrderID: "1001", ClientOrderID: "cid-1", Symbol: "ETHUSDT", Side: "BUY"},
	}}
	manager := NewUserDataStreamManager(registry, streamManagerMetaGetter{meta: accountmeta.Meta{
		AccountID: 10, VenueID: 20, UserID: 30, Environment: int32(domain.EnvironmentDemo),
		Exchange: int32(domain.ExchangeOKX), Market: int32(domain.MarketPerpetualFutures),
		APIKey: "key", APISecret: "secret", CredentialJSON: `{"api_secret":"secret"}`,
	}}, store, UserDataStreamManagerConfig{})

	started, err := manager.SyncOnce(context.Background())
	if err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	if started != 0 {
		t.Fatalf("started = %d, want 0", started)
	}
}
