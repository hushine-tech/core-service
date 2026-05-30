package binance

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/exchange/adapter"
	orderexecutor "github.com/hushine-tech/core-service/internal/order/executor"
)

func TestBinanceDemoPerpFactoryProvidesAllPhase2Capabilities(t *testing.T) {
	factory := NewFactory(adapter.Route{
		Exchange:    domain.ExchangeBinance,
		Environment: domain.EnvironmentDemo,
		Market:      domain.MarketPerpetualFutures,
	}, nil)

	assertCapability(t, factory.CredentialValidator)
	assertCapability(t, factory.AccountSnapshotReader)
	assertCapability(t, factory.SymbolRulesReader)
	assertCapability(t, factory.OrderExecutor)
	assertCapability(t, factory.OrderStateReader)
	assertCapability(t, factory.OrderCanceller)
}

func TestBinanceBacktestFactoryUsesSimulatedExecutor(t *testing.T) {
	factory := NewBacktestFactory(adapter.Route{
		Exchange:    domain.ExchangeBinance,
		Environment: domain.EnvironmentBacktest,
		Market:      domain.MarketPerpetualFutures,
	})
	exec, err := factory.OrderExecutor()
	if err != nil {
		t.Fatalf("OrderExecutor() error = %v", err)
	}

	result, err := exec.PlaceOrder(context.Background(), adapter.OrderRequest{
		Symbol:        "ETHUSDT",
		Side:          "BUY",
		OrderType:     "MARKET",
		Qty:           0.5,
		Price:         ptr(2000.0),
		ClientOrderID: "client-1",
	})
	if err != nil {
		t.Fatalf("PlaceOrder() error = %v", err)
	}
	if result.Status != "FILLED" {
		t.Fatalf("PlaceOrder() status = %q, want FILLED", result.Status)
	}
	if result.ExchangeOrderID == "" {
		t.Fatal("PlaceOrder() ExchangeOrderID is empty")
	}
}

func TestBinanceCredentialValidatorRejectsMissingSecret(t *testing.T) {
	validator, err := NewFactory(adapter.Route{
		Exchange:    domain.ExchangeBinance,
		Environment: domain.EnvironmentDemo,
		Market:      domain.MarketPerpetualFutures,
	}, nil).CredentialValidator()
	if err != nil {
		t.Fatalf("CredentialValidator() error = %v", err)
	}

	_, err = validator.ValidateCredential(context.Background(), json.RawMessage(`{"api_key":"key-only"}`))
	if err == nil {
		t.Fatal("ValidateCredential() error = nil, want error")
	}
	if !errors.Is(err, ErrInvalidCredential) {
		t.Fatalf("ValidateCredential() error = %v, want ErrInvalidCredential", err)
	}
}

func TestBinanceFactoryRejectsUnsupportedMarkets(t *testing.T) {
	factory := NewFactory(adapter.Route{
		Exchange:    domain.ExchangeBinance,
		Environment: domain.EnvironmentDemo,
		Market:      domain.MarketSpot,
	}, nil)

	_, err := factory.OrderExecutor()
	if !errors.Is(err, adapter.ErrCapabilityUnsupported) {
		t.Fatalf("OrderExecutor() error = %v, want capability unsupported", err)
	}
}

func TestBinanceSymbolRulesParseExchangeInfoFilters(t *testing.T) {
	rules, err := parseSymbolRules([]byte(`{
		"symbols": [{
			"symbol": "ETHUSDT",
			"filters": [
				{"filterType":"PRICE_FILTER","tickSize":"0.01000000"},
				{"filterType":"LOT_SIZE","minQty":"0.001","stepSize":"0.001"},
				{"filterType":"MIN_NOTIONAL","notional":"5"}
			]
		}]
	}`), []string{"ETHUSDT"})
	if err != nil {
		t.Fatalf("parseSymbolRules() error = %v", err)
	}
	if len(rules.Symbols) != 1 {
		t.Fatalf("len(Symbols) = %d, want 1", len(rules.Symbols))
	}
	rule := rules.Symbols[0]
	if rule.TickSize != 0.01 || rule.StepSize != 0.001 || rule.MinQty != 0.001 || rule.MinNotional != 5 {
		t.Fatalf("rule = %+v, want parsed non-zero filters", rule)
	}
}

func TestLegacyOrderResultPreservesRecoverabilityFlags(t *testing.T) {
	result := fromLegacyOrderResult(orderexecutor.OrderResult{
		Symbol:      "ETHUSDT",
		Status:      "FILLED",
		FillPending: true,
		Fills: []orderexecutor.FillResult{
			{ExchangeTradeID: "t1", Qty: 1, FillPrice: 100, FeeMissing: true},
		},
	})

	if !result.FillPending {
		t.Fatal("FillPending = false, want true")
	}
	if len(result.Fills) != 1 || !result.Fills[0].FeeMissing {
		t.Fatalf("fills = %+v, want FeeMissing preserved", result.Fills)
	}
}

func assertCapability[T any](t *testing.T, build func() (T, error)) {
	t.Helper()
	capability, err := build()
	if err != nil {
		t.Fatalf("capability error = %v", err)
	}
	var zero T
	if any(capability) == any(zero) {
		t.Fatal("capability is zero")
	}
}

func ptr(v float64) *float64 {
	return &v
}
