package binance

import (
	"context"
	"fmt"

	"github.com/hushine-tech/core-service/internal/exchange/adapter"
	"github.com/hushine-tech/core-service/internal/order/accountmeta"
	orderexecutor "github.com/hushine-tech/core-service/internal/order/executor"
)

type orderStateReader struct {
	exec interface {
		Resolve(context.Context, orderexecutor.RecoveryRequest, accountmeta.Meta) (orderexecutor.OrderResult, error)
	}
}

func (r orderStateReader) QueryOrder(ctx context.Context, req adapter.QueryOrderRequest) (adapter.OrderState, error) {
	result, err := r.exec.Resolve(ctx, orderexecutor.RecoveryRequest{
		AccountID:       req.AccountID,
		Symbol:          req.Symbol,
		ClientOrderID:   req.ClientOrderID,
		ExchangeOrderID: req.ExchangeOrderID,
	}, accountmeta.Meta{
		AccountID:      req.AccountID,
		VenueID:        req.VenueID,
		APIKey:         req.Credential.Metadata["api_key"],
		APISecret:      req.Credential.Metadata["api_secret"],
		CredentialJSON: string(req.Credential.Raw),
	})
	if err != nil {
		return adapter.OrderState{}, err
	}
	return adapter.OrderState{
		ExchangeOrderID: result.ExchangeOrderID,
		ClientOrderID:   result.ClientOrderID,
		Symbol:          result.Symbol,
		Status:          result.Status,
		OrigQty:         result.OrigQty,
		ExecutedQty:     result.ExecutedQty,
		RemainingQty:    result.RemainingQty,
		AvgPrice:        result.AvgPrice,
	}, nil
}

func (r orderStateReader) QueryTrades(ctx context.Context, req adapter.QueryTradesRequest) ([]adapter.FillDelta, error) {
	result, err := r.exec.Resolve(ctx, orderexecutor.RecoveryRequest{
		AccountID:       req.AccountID,
		Symbol:          req.Symbol,
		ExchangeOrderID: req.ExchangeOrderID,
	}, accountmeta.Meta{
		AccountID:      req.AccountID,
		VenueID:        req.VenueID,
		APIKey:         req.Credential.Metadata["api_key"],
		APISecret:      req.Credential.Metadata["api_secret"],
		CredentialJSON: string(req.Credential.Raw),
	})
	if err != nil {
		return nil, err
	}
	if result.FillPending {
		return nil, fmt.Errorf("order fills pending: %s", result.ErrorMessage)
	}
	return fromLegacyOrderResult(result).Fills, nil
}

type simulatedOrderStateReader struct{}

func (simulatedOrderStateReader) QueryOrder(context.Context, adapter.QueryOrderRequest) (adapter.OrderState, error) {
	return adapter.OrderState{}, adapter.CapabilityUnsupported("order_state_reader")
}

func (simulatedOrderStateReader) QueryTrades(context.Context, adapter.QueryTradesRequest) ([]adapter.FillDelta, error) {
	return nil, adapter.CapabilityUnsupported("order_state_reader")
}
