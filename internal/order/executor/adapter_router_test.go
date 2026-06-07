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
	orderExecutor    exchangeadapter.OrderExecutor
	orderStateReader exchangeadapter.OrderStateReader
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

func (f stubAdapterFactory) OrderCapabilityProvider() (exchangeadapter.OrderCapabilityProvider, error) {
	return nil, exchangeadapter.CapabilityUnsupported("order_capability_provider")
}

func (f stubAdapterFactory) OrderStateReader() (exchangeadapter.OrderStateReader, error) {
	if f.orderStateReader == nil {
		return nil, exchangeadapter.CapabilityUnsupported("order_state_reader")
	}
	return f.orderStateReader, nil
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

type stubAdapterOrderStateReader struct {
	state  exchangeadapter.OrderState
	trades []exchangeadapter.FillDelta
}

func (r stubAdapterOrderStateReader) QueryOrder(context.Context, exchangeadapter.QueryOrderRequest) (exchangeadapter.OrderState, error) {
	return r.state, nil
}

func (r stubAdapterOrderStateReader) QueryTrades(context.Context, exchangeadapter.QueryTradesRequest) ([]exchangeadapter.FillDelta, error) {
	return r.trades, nil
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

func TestAdapterRouter_ResolveMarksInconsistentRecoveredTradesFillPending(t *testing.T) {
	registry := exchangeadapter.NewRegistry()
	registry.Register(exchangeadapter.Route{
		Exchange:    domain.ExchangeBinance,
		Environment: domain.EnvironmentBacktest,
		Market:      domain.MarketSpot,
	}, stubAdapterFactory{
		orderStateReader: stubAdapterOrderStateReader{
			state: exchangeadapter.OrderState{
				ExchangeOrderID: "spot-order-1",
				ClientOrderID:   "spot-client-1",
				Symbol:          "ETHUSDT",
				Status:          "FILLED",
				OrigQty:         0.2,
				ExecutedQty:     0.2,
			},
			trades: []exchangeadapter.FillDelta{{
				ExchangeTradeID: "spot-trade-1",
				Qty:             0.3,
				FillPrice:       2500,
			}},
		},
	})
	router := NewAdapterRouter(registry)

	result, err := router.Resolve(context.Background(), RecoveryRequest{
		AccountID:       1,
		Symbol:          "ETHUSDT",
		ClientOrderID:   "spot-client-1",
		ExchangeOrderID: "spot-order-1",
	}, accountmeta.Meta{
		AccountID:   1,
		VenueID:     10,
		UserID:      77,
		Environment: int32(domain.EnvironmentBacktest),
		Exchange:    int32(domain.ExchangeBinance),
		Market:      int32(domain.MarketSpot),
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if !result.FillPending {
		t.Fatal("FillPending = false, want true for inconsistent recovered trades")
	}
	if len(result.Fills) != 0 {
		t.Fatalf("fills = %+v, want no settleable recovered fills", result.Fills)
	}
	if result.ErrorMessage == "" {
		t.Fatal("ErrorMessage is empty, want inconsistency reason")
	}
}
