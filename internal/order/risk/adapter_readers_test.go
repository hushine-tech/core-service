package risk

import (
	"context"
	"testing"

	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/exchange/adapter"
)

type riskTestFactory struct {
	symbolRules adapter.SymbolRulesReader
}

func (f riskTestFactory) CredentialValidator() (adapter.CredentialValidator, error) {
	return nil, adapter.CapabilityUnsupported("credential_validator")
}

func (f riskTestFactory) AccountSnapshotReader() (adapter.AccountSnapshotReader, error) {
	return nil, adapter.CapabilityUnsupported("account_snapshot_reader")
}

func (f riskTestFactory) SymbolRulesReader() (adapter.SymbolRulesReader, error) {
	return f.symbolRules, nil
}

func (f riskTestFactory) OrderExecutor() (adapter.OrderExecutor, error) {
	return nil, adapter.CapabilityUnsupported("order_executor")
}

func (f riskTestFactory) OrderCapabilityProvider() (adapter.OrderCapabilityProvider, error) {
	return nil, adapter.CapabilityUnsupported("order_capability_provider")
}

func (f riskTestFactory) OrderStateReader() (adapter.OrderStateReader, error) {
	return nil, adapter.CapabilityUnsupported("order_state_reader")
}

func (f riskTestFactory) OrderCanceller() (adapter.OrderCanceller, error) {
	return nil, adapter.CapabilityUnsupported("order_canceller")
}

type riskTestSymbolRulesReader struct {
	lastReq adapter.SymbolRulesRequest
}

func (r *riskTestSymbolRulesReader) ReadSymbolRules(_ context.Context, req adapter.SymbolRulesRequest) (adapter.SymbolRules, error) {
	r.lastReq = req
	return adapter.SymbolRules{Symbols: []adapter.SymbolRule{{
		Symbol:      "ETHUSDT",
		MinQty:      0.001,
		StepSize:    0.001,
		MinNotional: 100,
		TickSize:    0.01,
	}}}, nil
}

func TestAdapterSymbolRulesReaderMapsExchangeRules(t *testing.T) {
	registry := adapter.NewRegistry()
	route := adapter.Route{
		Exchange:    domain.ExchangeBinance,
		Environment: domain.EnvironmentDemo,
		Market:      domain.MarketPerpetualFutures,
	}
	reader := &riskTestSymbolRulesReader{}
	registry.Register(route, riskTestFactory{symbolRules: reader})

	rules, err := NewAdapterSymbolRulesReader(registry).ReadSymbolRules(context.Background(), SnapshotRequest{
		RouteKey: RouteKey{
			Exchange:    int32(domain.ExchangeBinance),
			Environment: int32(domain.EnvironmentDemo),
			Market:      int32(domain.MarketPerpetualFutures),
		},
		Symbol: "ethusdt",
	})
	if err != nil {
		t.Fatalf("ReadSymbolRules() error = %v", err)
	}
	if len(reader.lastReq.Symbols) != 1 || reader.lastReq.Symbols[0] != "ETHUSDT" {
		t.Fatalf("symbols = %+v, want ETHUSDT", reader.lastReq.Symbols)
	}
	if len(rules) != 1 {
		t.Fatalf("rules = %+v, want one symbol rule", rules)
	}
	got := rules[0]
	if got.Symbol != "ETHUSDT" || got.MinQty != 0.001 || got.MinNotional != 100 || got.StepSize != 0.001 || got.TickSize != 0.01 {
		t.Fatalf("mapped rule = %+v, want exchange symbol filters", got)
	}
	if got.ConfiguredLeverage != 0 {
		t.Fatalf("configured leverage = %g, want symbol rules reader not to invent leverage", got.ConfiguredLeverage)
	}
}
