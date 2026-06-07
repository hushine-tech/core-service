package main

import (
	"github.com/hushine-tech/core-service/internal/domain"
	exchangeadapter "github.com/hushine-tech/core-service/internal/exchange/adapter"
	exchangebinance "github.com/hushine-tech/core-service/internal/exchange/binance"
	exchangeokx "github.com/hushine-tech/core-service/internal/exchange/okx"
	"github.com/hushine-tech/golang-lib/pkg/log"
)

func registerExchangeFactories(registry *exchangeadapter.Registry, logger log.Logger) {
	if registry == nil {
		return
	}
	for _, env := range []domain.Environment{domain.EnvironmentBacktest, domain.EnvironmentDemo, domain.EnvironmentLive} {
		for _, market := range []domain.Market{domain.MarketPerpetualFutures, domain.MarketSpot} {
			route := exchangeadapter.Route{Exchange: domain.ExchangeBinance, Environment: env, Market: market}
			if env == domain.EnvironmentBacktest {
				registry.Register(route, exchangebinance.NewBacktestFactory(route))
			} else {
				registry.Register(route, exchangebinance.NewFactory(route, logger))
			}
		}
	}
	for _, env := range []domain.Environment{domain.EnvironmentDemo, domain.EnvironmentLive} {
		route := exchangeadapter.Route{Exchange: domain.ExchangeOKX, Environment: env, Market: domain.MarketPerpetualFutures}
		registry.Register(route, exchangeokx.NewFactory(route))
	}
}
