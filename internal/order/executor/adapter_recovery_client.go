package executor

import (
	"context"

	exchangeadapter "github.com/hushine-tech/core-service/internal/exchange/adapter"
	"github.com/hushine-tech/core-service/internal/order/accountmeta"
	"github.com/hushine-tech/core-service/internal/order/lifecycle"
)

type RecoveryMetaGetter interface {
	Get(ctx context.Context, accountID int64, exchange int32, market int32) (accountmeta.Meta, error)
}

type AdapterRecoveryClient struct {
	router     *AdapterRouter
	metaGetter RecoveryMetaGetter
}

func NewAdapterRecoveryClient(registry *exchangeadapter.Registry, metaGetter RecoveryMetaGetter) *AdapterRecoveryClient {
	return &AdapterRecoveryClient{
		router:     NewAdapterRouter(registry),
		metaGetter: metaGetter,
	}
}

func (c *AdapterRecoveryClient) QueryOrder(ctx context.Context, order lifecycle.OpenOrder) (lifecycle.OrderState, error) {
	meta, route, credential, err := c.resolve(ctx, order)
	if err != nil {
		return lifecycle.OrderState{}, err
	}
	reader, err := c.router.registry.OrderStateReader(route)
	if err != nil {
		return lifecycle.OrderState{}, err
	}
	state, err := reader.QueryOrder(ctx, exchangeadapter.QueryOrderRequest{
		AccountID:       order.AccountID,
		VenueID:         meta.VenueID,
		Symbol:          order.Symbol,
		ClientOrderID:   order.ClientOrderID,
		ExchangeOrderID: order.ExchangeOrderID,
		Credential:      credential,
	})
	if err != nil {
		return lifecycle.OrderState{}, err
	}
	return lifecycle.OrderState{
		ExchangeOrderID: state.ExchangeOrderID,
		ClientOrderID:   state.ClientOrderID,
		Symbol:          firstNonEmpty(state.Symbol, order.Symbol),
		Status:          state.Status,
		OrigQty:         state.OrigQty,
		ExecutedQty:     state.ExecutedQty,
		RemainingQty:    state.RemainingQty,
		AvgPrice:        state.AvgPrice,
		UpdatedAt:       state.UpdatedAt,
	}, nil
}

func (c *AdapterRecoveryClient) QueryTrades(ctx context.Context, order lifecycle.OpenOrder) ([]lifecycle.FillDelta, error) {
	meta, route, credential, err := c.resolve(ctx, order)
	if err != nil {
		return nil, err
	}
	reader, err := c.router.registry.OrderStateReader(route)
	if err != nil {
		return nil, err
	}
	trades, err := reader.QueryTrades(ctx, exchangeadapter.QueryTradesRequest{
		AccountID:       order.AccountID,
		VenueID:         meta.VenueID,
		Symbol:          order.Symbol,
		ExchangeOrderID: order.ExchangeOrderID,
		Credential:      credential,
	})
	if err != nil {
		return nil, err
	}
	out := make([]lifecycle.FillDelta, 0, len(trades))
	for _, trade := range trades {
		out = append(out, lifecycle.FillDelta{
			ExchangeTradeID: trade.ExchangeTradeID,
			ExchangeOrderID: trade.ExchangeOrderID,
			Symbol:          firstNonEmpty(trade.Symbol, order.Symbol),
			Qty:             trade.Qty,
			FillPrice:       trade.FillPrice,
			Fee:             trade.Fee,
			FeeAsset:        trade.FeeAsset,
			FeeMissing:      trade.FeeMissing,
			TradeTime:       trade.TradeTime,
		})
	}
	return out, nil
}

func (c *AdapterRecoveryClient) CancelOrder(ctx context.Context, order lifecycle.OpenOrder) (lifecycle.CancelResult, error) {
	meta, route, credential, err := c.resolve(ctx, order)
	if err != nil {
		return lifecycle.CancelResult{}, err
	}
	canceller, err := c.router.registry.OrderCanceller(route)
	if err != nil {
		return lifecycle.CancelResult{}, err
	}
	result, err := canceller.CancelOrder(ctx, exchangeadapter.CancelOrderRequest{
		AccountID:       order.AccountID,
		VenueID:         meta.VenueID,
		Symbol:          order.Symbol,
		ClientOrderID:   order.ClientOrderID,
		ExchangeOrderID: order.ExchangeOrderID,
		Credential:      credential,
	})
	if err != nil {
		return lifecycle.CancelResult{}, err
	}
	return lifecycle.CancelResult{
		ExchangeOrderID: result.ExchangeOrderID,
		ClientOrderID:   result.ClientOrderID,
		Status:          result.Status,
	}, nil
}

func (c *AdapterRecoveryClient) resolve(ctx context.Context, order lifecycle.OpenOrder) (accountmeta.Meta, exchangeadapter.Route, exchangeadapter.ParsedCredential, error) {
	meta, err := c.metaGetter.Get(ctx, order.AccountID, order.Exchange, order.Market)
	if err != nil {
		return accountmeta.Meta{}, exchangeadapter.Route{}, exchangeadapter.ParsedCredential{}, err
	}
	route := routeFromMeta(meta)
	credential, err := c.router.parseCredential(ctx, route, meta)
	if err != nil {
		return accountmeta.Meta{}, exchangeadapter.Route{}, exchangeadapter.ParsedCredential{}, err
	}
	return meta, route, credential, nil
}
