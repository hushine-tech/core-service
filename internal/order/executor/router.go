package executor

import (
	"context"
	"fmt"

	"github.com/hushine-tech/core-service/internal/order/accountmeta"
)

// Account mode constants mirror core-service definitions.
// Defined locally to avoid a circular import dependency.
const (
	modeBacktest = 0
	modeLive     = 1
	modeTestnet  = 2
)

// Router dispatches orders to the appropriate executor based on account mode.
type Router struct {
	mock           Executor
	binanceLive    Executor
	binanceTestnet Executor
}

func NewRouter(mock, live, testnet Executor) *Router {
	return &Router{mock: mock, binanceLive: live, binanceTestnet: testnet}
}

func (r *Router) Execute(ctx context.Context, req OrderRequest, meta accountmeta.Meta) (OrderResult, error) {
	switch meta.Mode {
	case modeBacktest:
		return r.mock.Execute(ctx, req, meta)
	case modeLive:
		return r.binanceLive.Execute(ctx, req, meta)
	case modeTestnet:
		return r.binanceTestnet.Execute(ctx, req, meta)
	default:
		return OrderResult{Status: "FAILED", ErrorMessage: fmt.Sprintf("unsupported account mode: %d", meta.Mode)}, nil
	}
}

func (r *Router) Resolve(ctx context.Context, req RecoveryRequest, meta accountmeta.Meta) (OrderResult, error) {
	switch meta.Mode {
	case modeBacktest:
		return r.mock.Resolve(ctx, req, meta)
	case modeLive:
		return r.binanceLive.Resolve(ctx, req, meta)
	case modeTestnet:
		return r.binanceTestnet.Resolve(ctx, req, meta)
	default:
		return OrderResult{}, fmt.Errorf("unsupported account mode: %d", meta.Mode)
	}
}
