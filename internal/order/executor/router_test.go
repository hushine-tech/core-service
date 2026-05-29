package executor

import (
	"context"
	"testing"

	"github.com/hushine-tech/core-service/internal/order/accountmeta"
)

// stubExecutor records which executor was called.
type stubExecutor struct{ name string }

func (s *stubExecutor) Execute(_ context.Context, req OrderRequest, _ accountmeta.Meta) (OrderResult, error) {
	return OrderResult{Status: s.name, Symbol: req.Symbol}, nil
}

func (s *stubExecutor) Resolve(_ context.Context, req RecoveryRequest, _ accountmeta.Meta) (OrderResult, error) {
	return OrderResult{Status: s.name, Symbol: req.Symbol}, nil
}

func TestRouter_routesByVenueRoute(t *testing.T) {
	mock := &stubExecutor{"mock"}
	live := &stubExecutor{"live"}
	testnet := &stubExecutor{"testnet"}
	router := NewRouter(mock, live, testnet)

	req := OrderRequest{Symbol: "BTCUSDT", Side: "BUY", Qty: 1}

	for _, tc := range []struct {
		name        string
		environment int32
		exchange    int32
		market      int32
		wantName    string
	}{
		{"backtest", environmentBacktest, exchangeBinance, marketPerpetualFutures, "mock"},
		{"live-binance-perpetual", environmentLive, exchangeBinance, marketPerpetualFutures, "live"},
		{"demo-binance-perpetual", environmentDemo, exchangeBinance, marketPerpetualFutures, "testnet"},
	} {
		meta := accountmeta.Meta{Environment: tc.environment, Exchange: tc.exchange, Market: tc.market}
		res, err := router.Execute(context.Background(), req, meta)
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", tc.name, err)
		}
		if res.Status != tc.wantName {
			t.Errorf("%s: got %q, want %q", tc.name, res.Status, tc.wantName)
		}
	}
}

func TestRouter_unsupportedRoute(t *testing.T) {
	router := NewRouter(&stubExecutor{"m"}, &stubExecutor{"l"}, &stubExecutor{"t"})
	meta := accountmeta.Meta{Environment: environmentLive, Exchange: exchangeBinance, Market: 99}
	res, err := router.Execute(context.Background(), OrderRequest{}, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != "FAILED" {
		t.Errorf("status: got %q, want FAILED", res.Status)
	}
}

func TestRouterResolve_routesByVenueRoute(t *testing.T) {
	mock := &stubExecutor{"mock"}
	live := &stubExecutor{"live"}
	testnet := &stubExecutor{"testnet"}
	router := NewRouter(mock, live, testnet)

	req := RecoveryRequest{Symbol: "BTCUSDT", ClientOrderID: "coid-1"}

	for _, tc := range []struct {
		name        string
		environment int32
		exchange    int32
		market      int32
		wantName    string
	}{
		{"backtest", environmentBacktest, exchangeBinance, marketPerpetualFutures, "mock"},
		{"live-binance-perpetual", environmentLive, exchangeBinance, marketPerpetualFutures, "live"},
		{"demo-binance-perpetual", environmentDemo, exchangeBinance, marketPerpetualFutures, "testnet"},
	} {
		meta := accountmeta.Meta{Environment: tc.environment, Exchange: tc.exchange, Market: tc.market}
		res, err := router.Resolve(context.Background(), req, meta)
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", tc.name, err)
		}
		if res.Status != tc.wantName {
			t.Errorf("%s: got %q, want %q", tc.name, res.Status, tc.wantName)
		}
	}
}
