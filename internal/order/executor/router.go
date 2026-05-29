package executor

import (
	"context"
	"fmt"

	"github.com/hushine-tech/core-service/internal/order/accountmeta"
)

// Route constants mirror core-service domain enums.
// Defined locally to keep the order package independent from the account domain.
const (
	environmentBacktest = 0
	environmentDemo     = 1
	environmentLive     = 2

	exchangeBinance        = 1
	marketPerpetualFutures = 2
)

// Router dispatches orders to the appropriate executor based on venue route metadata.
type Router struct {
	mock           Executor
	binanceLive    Executor
	binanceTestnet Executor
}

func NewRouter(mock, live, testnet Executor) *Router {
	return &Router{mock: mock, binanceLive: live, binanceTestnet: testnet}
}

func (r *Router) Execute(ctx context.Context, req OrderRequest, meta accountmeta.Meta) (OrderResult, error) {
	switch {
	case meta.Environment == environmentBacktest:
		return r.mock.Execute(ctx, req, meta)
	case meta.Exchange == exchangeBinance && meta.Market == marketPerpetualFutures && meta.Environment == environmentLive:
		return r.binanceLive.Execute(ctx, req, meta)
	case meta.Exchange == exchangeBinance && meta.Market == marketPerpetualFutures && meta.Environment == environmentDemo:
		return r.binanceTestnet.Execute(ctx, req, meta)
	default:
		return OrderResult{Status: "FAILED", ErrorMessage: fmt.Sprintf(
			"unsupported order route: environment=%d exchange=%d market=%d",
			meta.Environment, meta.Exchange, meta.Market,
		)}, nil
	}
}

func (r *Router) Resolve(ctx context.Context, req RecoveryRequest, meta accountmeta.Meta) (OrderResult, error) {
	switch {
	case meta.Environment == environmentBacktest:
		return r.mock.Resolve(ctx, req, meta)
	case meta.Exchange == exchangeBinance && meta.Market == marketPerpetualFutures && meta.Environment == environmentLive:
		return r.binanceLive.Resolve(ctx, req, meta)
	case meta.Exchange == exchangeBinance && meta.Market == marketPerpetualFutures && meta.Environment == environmentDemo:
		return r.binanceTestnet.Resolve(ctx, req, meta)
	default:
		return OrderResult{}, fmt.Errorf(
			"unsupported order route: environment=%d exchange=%d market=%d",
			meta.Environment, meta.Exchange, meta.Market,
		)
	}
}
