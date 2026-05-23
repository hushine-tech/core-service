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

func TestRouter_routesByMode(t *testing.T) {
	mock := &stubExecutor{"mock"}
	live := &stubExecutor{"live"}
	testnet := &stubExecutor{"testnet"}
	router := NewRouter(mock, live, testnet)

	req := OrderRequest{Symbol: "BTCUSDT", Side: "BUY", Qty: 1}

	for _, tc := range []struct {
		mode     int32
		wantName string
	}{
		{0, "mock"},
		{1, "live"},
		{2, "testnet"},
	} {
		meta := accountmeta.Meta{Mode: tc.mode}
		res, err := router.Execute(context.Background(), req, meta)
		if err != nil {
			t.Fatalf("mode %d: unexpected error: %v", tc.mode, err)
		}
		if res.Status != tc.wantName {
			t.Errorf("mode %d: got %q, want %q", tc.mode, res.Status, tc.wantName)
		}
	}
}

func TestRouter_unknownMode(t *testing.T) {
	router := NewRouter(&stubExecutor{"m"}, &stubExecutor{"l"}, &stubExecutor{"t"})
	meta := accountmeta.Meta{Mode: 99}
	res, err := router.Execute(context.Background(), OrderRequest{}, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != "FAILED" {
		t.Errorf("status: got %q, want FAILED", res.Status)
	}
}

func TestRouterResolve_routesByMode(t *testing.T) {
	mock := &stubExecutor{"mock"}
	live := &stubExecutor{"live"}
	testnet := &stubExecutor{"testnet"}
	router := NewRouter(mock, live, testnet)

	req := RecoveryRequest{Symbol: "BTCUSDT", ClientOrderID: "coid-1"}

	for _, tc := range []struct {
		mode     int32
		wantName string
	}{
		{0, "mock"},
		{1, "live"},
		{2, "testnet"},
	} {
		meta := accountmeta.Meta{Mode: tc.mode}
		res, err := router.Resolve(context.Background(), req, meta)
		if err != nil {
			t.Fatalf("mode %d: unexpected error: %v", tc.mode, err)
		}
		if res.Status != tc.wantName {
			t.Errorf("mode %d: got %q, want %q", tc.mode, res.Status, tc.wantName)
		}
	}
}
