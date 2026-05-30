package okx

import (
	"errors"
	"testing"

	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/exchange/adapter"
)

func TestOKXFactoryFailsClosed(t *testing.T) {
	factory := NewFactory(adapter.Route{
		Exchange:    domain.ExchangeOKX,
		Environment: domain.EnvironmentDemo,
		Market:      domain.MarketPerpetualFutures,
	})

	_, err := factory.OrderExecutor()
	if !errors.Is(err, adapter.ErrCapabilityUnsupported) {
		t.Fatalf("OrderExecutor() error = %v, want capability unsupported", err)
	}
}
