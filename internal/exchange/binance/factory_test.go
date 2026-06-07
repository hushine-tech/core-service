package binance

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
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
	assertCapability(t, factory.OrderCapabilityProvider)
	assertCapability(t, factory.OrderStateReader)
	assertCapability(t, factory.OrderCanceller)
}

func TestBinanceSpotFactoryProvidesOrderExecutorAndCapabilityProvider(t *testing.T) {
	factory := NewFactory(adapter.Route{
		Exchange:    domain.ExchangeBinance,
		Environment: domain.EnvironmentDemo,
		Market:      domain.MarketSpot,
	}, nil)

	assertCapability(t, factory.OrderExecutor)
	assertCapability(t, factory.OrderStateReader)
	provider, err := factory.OrderCapabilityProvider()
	if err != nil {
		t.Fatalf("OrderCapabilityProvider() error = %v", err)
	}
	capability, err := provider.OrderCapability(context.Background(), adapter.ParsedCredential{})
	if err != nil {
		t.Fatalf("OrderCapability() error = %v", err)
	}
	if capability.Market != domain.MarketSpot {
		t.Fatalf("Market = %v, want spot", capability.Market)
	}
	if !capability.SupportsPostOnly || !capability.SupportsReduceOnly || capability.SupportsGTD {
		t.Fatalf("spot capability = %+v, want post_only/reduce_only platform support without GTD", capability)
	}
	assertContains(t, capability.OrderTypes, "MARKET")
	assertContains(t, capability.OrderTypes, "LIMIT")
	assertContains(t, capability.TimeInForce, "GTC")
	assertContains(t, capability.TimeInForce, "IOC")
	assertContains(t, capability.TimeInForce, "FOK")
}

func TestBinanceFuturesOrderCapabilityIncludesAdvancedNativeSemantics(t *testing.T) {
	factory := NewFactory(adapter.Route{
		Exchange:    domain.ExchangeBinance,
		Environment: domain.EnvironmentDemo,
		Market:      domain.MarketPerpetualFutures,
	}, nil)

	provider, err := factory.OrderCapabilityProvider()
	if err != nil {
		t.Fatalf("OrderCapabilityProvider() error = %v", err)
	}
	capability, err := provider.OrderCapability(context.Background(), adapter.ParsedCredential{})
	if err != nil {
		t.Fatalf("OrderCapability() error = %v", err)
	}
	if !capability.SupportsPostOnly || !capability.SupportsGTD || !capability.SupportsReduceOnly {
		t.Fatalf("futures capability = %+v, want post_only/GTD/reduce_only support", capability)
	}
	assertContains(t, capability.TimeInForce, "GTD")
}

func TestBinanceBacktestOrderCapabilityKeepsGTDUnsupported(t *testing.T) {
	factory := NewBacktestFactory(adapter.Route{
		Exchange:    domain.ExchangeBinance,
		Environment: domain.EnvironmentBacktest,
		Market:      domain.MarketPerpetualFutures,
	})

	provider, err := factory.OrderCapabilityProvider()
	if err != nil {
		t.Fatalf("OrderCapabilityProvider() error = %v", err)
	}
	capability, err := provider.OrderCapability(context.Background(), adapter.ParsedCredential{})
	if err != nil {
		t.Fatalf("OrderCapability() error = %v", err)
	}
	if capability.SupportsGTD {
		t.Fatalf("SupportsGTD = true, want false until simulated expiry exists")
	}
	if contains(capability.TimeInForce, "GTD") {
		t.Fatalf("TimeInForce = %v, want no GTD for backtest", capability.TimeInForce)
	}
}

func TestBinanceBacktestSymbolRulesReaderIsLocalNoop(t *testing.T) {
	factory := NewBacktestFactory(adapter.Route{
		Exchange:    domain.ExchangeBinance,
		Environment: domain.EnvironmentBacktest,
		Market:      domain.MarketPerpetualFutures,
	})

	reader, err := factory.SymbolRulesReader()
	if err != nil {
		t.Fatalf("SymbolRulesReader() error = %v", err)
	}
	rules, err := reader.ReadSymbolRules(context.Background(), adapter.SymbolRulesRequest{Symbols: []string{"ETHUSDT"}})
	if err != nil {
		t.Fatalf("ReadSymbolRules() error = %v", err)
	}
	if len(rules.Symbols) != 0 {
		t.Fatalf("rules = %+v, want empty local backtest rules", rules.Symbols)
	}
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
		MarkPrice:     2000.0,
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

func TestBinanceBacktestMarketOrderPreservesFeeAndSlippage(t *testing.T) {
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
		Symbol:         "ETHUSDT",
		Side:           "BUY",
		OrderType:      "MARKET",
		Qty:            0.5,
		MarkPrice:      2000,
		DefaultFeeRate: 0.001,
		SlippageBps:    10,
		ClientOrderID:  "client-market-cost",
	})
	if err != nil {
		t.Fatalf("PlaceOrder() error = %v", err)
	}
	if result.AvgPrice != 2002 {
		t.Fatalf("avg_price = %v, want slippage-adjusted 2002", result.AvgPrice)
	}
	if len(result.Fills) != 1 {
		t.Fatalf("fills = %d, want 1", len(result.Fills))
	}
	if math.Abs(result.Fills[0].Fee-1.001) > 1e-12 {
		t.Fatalf("fee = %v, want 1.001", result.Fills[0].Fee)
	}
}

func TestBinanceBacktestLimitOrderRemainsOpenWhenMarkDoesNotTouch(t *testing.T) {
	factory := NewBacktestFactory(adapter.Route{
		Exchange:    domain.ExchangeBinance,
		Environment: domain.EnvironmentBacktest,
		Market:      domain.MarketPerpetualFutures,
	})
	exec, err := factory.OrderExecutor()
	if err != nil {
		t.Fatalf("OrderExecutor() error = %v", err)
	}

	price := 19.885
	result, err := exec.PlaceOrder(context.Background(), adapter.OrderRequest{
		AccountID:     38,
		VenueID:       1,
		Exchange:      domain.ExchangeBinance,
		Environment:   domain.EnvironmentBacktest,
		Market:        domain.MarketPerpetualFutures,
		Symbol:        "ETHUSDT",
		Side:          "BUY",
		OrderType:     "LIMIT",
		TimeInForce:   "GTC",
		Qty:           0.004,
		Price:         &price,
		MarkPrice:     1988.5,
		ClientOrderID: "client-limit-open",
	})
	if err != nil {
		t.Fatalf("PlaceOrder() error = %v", err)
	}
	if result.Status != "NEW" {
		t.Fatalf("status = %q, want NEW (result=%+v)", result.Status, result)
	}
	if result.ExecutedQty != 0 || result.RemainingQty != 0.004 || len(result.Fills) != 0 {
		t.Fatalf("result = %+v, want open order without fills", result)
	}
	if result.Price != price {
		t.Fatalf("price = %v, want %v", result.Price, price)
	}
}

func TestBinanceBacktestLimitOrderPreservesFeeWhenFilled(t *testing.T) {
	factory := NewBacktestFactory(adapter.Route{
		Exchange:    domain.ExchangeBinance,
		Environment: domain.EnvironmentBacktest,
		Market:      domain.MarketPerpetualFutures,
	})
	exec, err := factory.OrderExecutor()
	if err != nil {
		t.Fatalf("OrderExecutor() error = %v", err)
	}

	price := 3000.0
	result, err := exec.PlaceOrder(context.Background(), adapter.OrderRequest{
		Symbol:         "ETHUSDT",
		Side:           "BUY",
		OrderType:      "LIMIT",
		TimeInForce:    "GTC",
		Qty:            0.2,
		Price:          &price,
		MarkPrice:      2999,
		DefaultFeeRate: 0.001,
		ClientOrderID:  "client-limit-cost",
	})
	if err != nil {
		t.Fatalf("PlaceOrder() error = %v", err)
	}
	if result.Status != "FILLED" {
		t.Fatalf("status = %q, want FILLED", result.Status)
	}
	if len(result.Fills) != 1 {
		t.Fatalf("fills = %d, want 1", len(result.Fills))
	}
	if math.Abs(result.Fills[0].Fee-0.6) > 1e-12 {
		t.Fatalf("fee = %v, want 0.6", result.Fills[0].Fee)
	}
}

func TestBinanceBacktestGTDStillFailsClosed(t *testing.T) {
	factory := NewBacktestFactory(adapter.Route{
		Exchange:    domain.ExchangeBinance,
		Environment: domain.EnvironmentBacktest,
		Market:      domain.MarketPerpetualFutures,
	})
	exec, err := factory.OrderExecutor()
	if err != nil {
		t.Fatalf("OrderExecutor() error = %v", err)
	}

	price := 3000.0
	result, err := exec.PlaceOrder(context.Background(), adapter.OrderRequest{
		Symbol:      "ETHUSDT",
		Side:        "BUY",
		OrderType:   "LIMIT",
		TimeInForce: "GTD",
		Qty:         0.2,
		Price:       &price,
		MarkPrice:   2999,
	})
	if err != nil {
		t.Fatalf("PlaceOrder() error = %v", err)
	}
	if result.Status != "FAILED" {
		t.Fatalf("status = %q, want FAILED", result.Status)
	}
	if !strings.Contains(result.ErrorMessage, "time_in_force=GTD") {
		t.Fatalf("error = %q, want to contain time_in_force=GTD", result.ErrorMessage)
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
		Market:      domain.MarketDeliveryFutures,
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

func assertContains(t *testing.T, got []string, want string) {
	t.Helper()
	if !contains(got, want) {
		t.Fatalf("%v does not contain %q", got, want)
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
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
