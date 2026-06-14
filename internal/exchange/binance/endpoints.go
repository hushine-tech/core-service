package binance

import (
	"os"
	"strings"

	"github.com/hushine-tech/core-service/internal/domain"
	legacyexchange "github.com/hushine-tech/core-service/internal/exchange"
	"github.com/hushine-tech/core-service/internal/exchange/adapter"
)

const (
	binanceSpotLiveWSBaseURL       = "wss://stream.binance.com:9443"
	binanceSpotTestnetWSBaseURL    = "wss://testnet.binance.vision"
	binanceFuturesLiveWSBaseURL    = "wss://fstream.binance.com"
	binanceFuturesTestnetWSBaseURL = "wss://stream.binancefuture.com"
)

type Endpoints struct {
	RESTBaseURL string
	WSBaseURL   string
}

func EndpointsFromEnv(route adapter.Route) Endpoints {
	defaults := defaultEndpoints(route)
	switch route.Market {
	case domain.MarketSpot:
		return Endpoints{
			RESTBaseURL: envOrDefault("BINANCE_SPOT_REST_BASE_URL", defaults.RESTBaseURL),
			WSBaseURL:   envOrDefault("BINANCE_SPOT_WS_BASE_URL", defaults.WSBaseURL),
		}
	case domain.MarketPerpetualFutures:
		return Endpoints{
			RESTBaseURL: envOrDefault("BINANCE_FUTURES_REST_BASE_URL", defaults.RESTBaseURL),
			WSBaseURL:   envOrDefault("BINANCE_FUTURES_WS_BASE_URL", defaults.WSBaseURL),
		}
	default:
		return defaults
	}
}

func defaultEndpoints(route adapter.Route) Endpoints {
	switch route.Market {
	case domain.MarketSpot:
		if route.Environment == domain.EnvironmentDemo {
			return Endpoints{
				RESTBaseURL: legacyexchange.BinanceSpotTestnetURL,
				WSBaseURL:   binanceSpotTestnetWSBaseURL,
			}
		}
		return Endpoints{
			RESTBaseURL: legacyexchange.BinanceSpotBaseURL,
			WSBaseURL:   binanceSpotLiveWSBaseURL,
		}
	case domain.MarketPerpetualFutures:
		if route.Environment == domain.EnvironmentLive {
			return Endpoints{
				RESTBaseURL: legacyexchange.BinanceLiveBaseURL,
				WSBaseURL:   binanceFuturesLiveWSBaseURL,
			}
		}
		return Endpoints{
			RESTBaseURL: legacyexchange.BinanceTestnetBaseURL,
			WSBaseURL:   binanceFuturesTestnetWSBaseURL,
		}
	default:
		return Endpoints{}
	}
}

func envOrDefault(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return strings.TrimRight(fallback, "/")
	}
	return strings.TrimRight(value, "/")
}
