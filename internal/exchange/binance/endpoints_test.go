package binance

import (
	"testing"

	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/exchange/adapter"
)

func TestEndpointsFromEnvUsesMarketSpecificOverrides(t *testing.T) {
	t.Setenv("BINANCE_SPOT_REST_BASE_URL", "http://127.0.0.1:19001")
	t.Setenv("BINANCE_FUTURES_REST_BASE_URL", "http://127.0.0.1:19002")
	t.Setenv("BINANCE_SPOT_WS_BASE_URL", "ws://127.0.0.1:19003")
	t.Setenv("BINANCE_FUTURES_WS_BASE_URL", "ws://127.0.0.1:19004")

	spot := EndpointsFromEnv(adapter.Route{Market: domain.MarketSpot, Environment: domain.EnvironmentDemo})
	if spot.RESTBaseURL != "http://127.0.0.1:19001" || spot.WSBaseURL != "ws://127.0.0.1:19003" {
		t.Fatalf("spot endpoints = %+v", spot)
	}

	futures := EndpointsFromEnv(adapter.Route{Market: domain.MarketPerpetualFutures, Environment: domain.EnvironmentDemo})
	if futures.RESTBaseURL != "http://127.0.0.1:19002" || futures.WSBaseURL != "ws://127.0.0.1:19004" {
		t.Fatalf("futures endpoints = %+v", futures)
	}
}

func TestEndpointsFromEnvFallsBackToBinanceDefaults(t *testing.T) {
	spot := EndpointsFromEnv(adapter.Route{Market: domain.MarketSpot, Environment: domain.EnvironmentDemo})
	if spot.RESTBaseURL == "" || spot.WSBaseURL == "" {
		t.Fatalf("spot default endpoints must be populated: %+v", spot)
	}
	futures := EndpointsFromEnv(adapter.Route{Market: domain.MarketPerpetualFutures, Environment: domain.EnvironmentDemo})
	if futures.RESTBaseURL == "" || futures.WSBaseURL == "" {
		t.Fatalf("futures default endpoints must be populated: %+v", futures)
	}
}
