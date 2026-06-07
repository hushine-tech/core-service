package binance

import (
	"context"

	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/exchange/adapter"
)

type orderCapabilityProvider struct {
	route adapter.Route
}

func (p orderCapabilityProvider) OrderCapability(context.Context, adapter.ParsedCredential) (adapter.OrderCapability, error) {
	base := adapter.OrderCapability{
		Market:             p.route.Market,
		OrderTypes:         []string{"MARKET", "LIMIT"},
		TimeInForce:        []string{"GTC", "IOC", "FOK"},
		SupportsPostOnly:   true,
		SupportsReduceOnly: true,
	}
	switch p.route.Market {
	case domain.MarketSpot:
		return base, nil
	case domain.MarketPerpetualFutures:
		if p.route.Environment != domain.EnvironmentBacktest {
			base.TimeInForce = append(base.TimeInForce, "GTD")
			base.SupportsGTD = true
		}
		return base, nil
	default:
		return adapter.OrderCapability{}, adapter.CapabilityUnsupported("order_capability_provider")
	}
}
