package main

import (
	"testing"

	"github.com/hushine-tech/core-service/internal/domain"
	exchangeadapter "github.com/hushine-tech/core-service/internal/exchange/adapter"
)

func TestRegisterExchangeFactoriesIncludesBinanceSpotOrderRoutes(t *testing.T) {
	registry := exchangeadapter.NewRegistry()

	registerExchangeFactories(registry, nil)

	for _, env := range []domain.Environment{domain.EnvironmentBacktest, domain.EnvironmentDemo, domain.EnvironmentLive} {
		route := exchangeadapter.Route{
			Exchange:    domain.ExchangeBinance,
			Environment: env,
			Market:      domain.MarketSpot,
		}
		executor, err := registry.OrderExecutor(route)
		if err != nil {
			t.Fatalf("OrderExecutor(%+v) error = %v", route, err)
		}
		if executor == nil {
			t.Fatalf("OrderExecutor(%+v) = nil", route)
		}

		provider, err := registry.OrderCapabilityProvider(route)
		if err != nil {
			t.Fatalf("OrderCapabilityProvider(%+v) error = %v", route, err)
		}
		capability, err := provider.OrderCapability(t.Context(), exchangeadapter.ParsedCredential{})
		if err != nil {
			t.Fatalf("OrderCapability(%+v) error = %v", route, err)
		}
		if capability.Market != domain.MarketSpot {
			t.Fatalf("Market = %v, want spot", capability.Market)
		}
	}
}
