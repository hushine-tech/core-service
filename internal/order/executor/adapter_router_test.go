package executor

import (
	"context"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
	exchangeadapter "github.com/hushine-tech/core-service/internal/exchange/adapter"
	"github.com/hushine-tech/core-service/internal/order/accountmeta"
)

type stubAdapterFactory struct {
	orderExecutor exchangeadapter.OrderExecutor
}

func (f stubAdapterFactory) CredentialValidator() (exchangeadapter.CredentialValidator, error) {
	return nil, exchangeadapter.CapabilityUnsupported("credential_validator")
}

func (f stubAdapterFactory) AccountSnapshotReader() (exchangeadapter.AccountSnapshotReader, error) {
	return nil, exchangeadapter.CapabilityUnsupported("account_snapshot_reader")
}

func (f stubAdapterFactory) SymbolRulesReader() (exchangeadapter.SymbolRulesReader, error) {
	return nil, exchangeadapter.CapabilityUnsupported("symbol_rules_reader")
}

func (f stubAdapterFactory) OrderExecutor() (exchangeadapter.OrderExecutor, error) {
	return f.orderExecutor, nil
}

func (f stubAdapterFactory) OrderStateReader() (exchangeadapter.OrderStateReader, error) {
	return nil, exchangeadapter.CapabilityUnsupported("order_state_reader")
}

func (f stubAdapterFactory) OrderCanceller() (exchangeadapter.OrderCanceller, error) {
	return nil, exchangeadapter.CapabilityUnsupported("order_canceller")
}

type recordingAdapterOrderExecutor struct {
	lastReq exchangeadapter.OrderRequest
}

func (e *recordingAdapterOrderExecutor) PlaceOrder(_ context.Context, req exchangeadapter.OrderRequest) (exchangeadapter.OrderResult, error) {
	e.lastReq = req
	return exchangeadapter.OrderResult{
		ExchangeOrderID: "adapter-order-1",
		Symbol:          req.Symbol,
		Status:          "NEW",
		OrigQty:         req.Qty,
		RemainingQty:    req.Qty,
	}, nil
}

func TestAdapterRouter_ForwardsAdvancedOrderContractFields(t *testing.T) {
	adapterExec := &recordingAdapterOrderExecutor{}
	registry := exchangeadapter.NewRegistry()
	registry.Register(exchangeadapter.Route{
		Exchange:    domain.ExchangeBinance,
		Environment: domain.EnvironmentBacktest,
		Market:      domain.MarketPerpetualFutures,
	}, stubAdapterFactory{orderExecutor: adapterExec})
	router := NewAdapterRouter(registry)

	goodTillDate := time.Unix(1893456000, 0).UTC()
	price := 2499.0
	_, err := router.Execute(context.Background(), OrderRequest{
		AccountID:    1,
		Symbol:       "ETHUSDT",
		Side:         "BUY",
		OrderType:    "LIMIT",
		TimeInForce:  "GTD",
		PostOnly:     true,
		GoodTillDate: &goodTillDate,
		ReduceOnly:   true,
		Qty:          1,
		Price:        &price,
		MarkPrice:    2500,
	}, accountmeta.Meta{
		AccountID:      1,
		VenueID:        10,
		UserID:         77,
		Environment:    int32(domain.EnvironmentBacktest),
		Exchange:       int32(domain.ExchangeBinance),
		Market:         int32(domain.MarketPerpetualFutures),
		DefaultFeeRate: 0.0004,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !adapterExec.lastReq.PostOnly {
		t.Fatalf("post_only was not forwarded")
	}
	if !adapterExec.lastReq.ReduceOnly {
		t.Fatalf("reduce_only was not forwarded")
	}
	if adapterExec.lastReq.GoodTillDate == nil || adapterExec.lastReq.GoodTillDate.Unix() != 1893456000 {
		t.Fatalf("good_till_date = %v, want 1893456000", adapterExec.lastReq.GoodTillDate)
	}
}
