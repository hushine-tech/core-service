package adapter

import (
	"context"
	"errors"
	"testing"

	"github.com/hushine-tech/core-service/internal/domain"
)

type testFactory struct {
	orderExecutor OrderExecutor
}

func (f testFactory) CredentialValidator() (CredentialValidator, error) {
	return nil, CapabilityUnsupported("credential_validator")
}

func (f testFactory) AccountSnapshotReader() (AccountSnapshotReader, error) {
	return nil, CapabilityUnsupported("account_snapshot_reader")
}

func (f testFactory) SymbolRulesReader() (SymbolRulesReader, error) {
	return nil, CapabilityUnsupported("symbol_rules_reader")
}

func (f testFactory) OrderExecutor() (OrderExecutor, error) {
	if f.orderExecutor == nil {
		return nil, CapabilityUnsupported("order_executor")
	}
	return f.orderExecutor, nil
}

func (f testFactory) OrderStateReader() (OrderStateReader, error) {
	return nil, CapabilityUnsupported("order_state_reader")
}

func (f testFactory) OrderCanceller() (OrderCanceller, error) {
	return nil, CapabilityUnsupported("order_canceller")
}

type testOrderExecutor struct{}

func (testOrderExecutor) PlaceOrder(context.Context, OrderRequest) (OrderResult, error) {
	return OrderResult{}, nil
}

func TestRegistryReturnsCapabilityForRegisteredRoute(t *testing.T) {
	registry := NewRegistry()
	route := Route{
		Exchange:    domain.ExchangeBinance,
		Environment: domain.EnvironmentDemo,
		Market:      domain.MarketPerpetualFutures,
	}
	registry.Register(route, testFactory{orderExecutor: testOrderExecutor{}})

	executor, err := registry.OrderExecutor(route)
	if err != nil {
		t.Fatalf("OrderExecutor() error = %v", err)
	}
	if executor == nil {
		t.Fatal("OrderExecutor() = nil, want capability")
	}
}

func TestRegistryRejectsUnsupportedRoute(t *testing.T) {
	registry := NewRegistry()
	_, err := registry.OrderExecutor(Route{
		Exchange:    domain.ExchangeOKX,
		Environment: domain.EnvironmentDemo,
		Market:      domain.MarketPerpetualFutures,
	})
	if !errors.Is(err, ErrRouteUnsupported) {
		t.Fatalf("OrderExecutor() error = %v, want route unsupported", err)
	}
}

func TestRegistryRejectsUnsupportedCapability(t *testing.T) {
	registry := NewRegistry()
	route := Route{
		Exchange:    domain.ExchangeOKX,
		Environment: domain.EnvironmentDemo,
		Market:      domain.MarketPerpetualFutures,
	}
	registry.Register(route, testFactory{})

	_, err := registry.OrderExecutor(route)
	if !errors.Is(err, ErrCapabilityUnsupported) {
		t.Fatalf("OrderExecutor() error = %v, want capability unsupported", err)
	}
}
