package binance

import (
	"github.com/hushine-tech/core-service/internal/domain"
	legacyexchange "github.com/hushine-tech/core-service/internal/exchange"
	"github.com/hushine-tech/core-service/internal/exchange/adapter"
	orderexecutor "github.com/hushine-tech/core-service/internal/order/executor"
	"github.com/hushine-tech/golang-lib/pkg/log"
)

type Factory struct {
	route  adapter.Route
	logger log.Logger
}

func NewFactory(route adapter.Route, logger log.Logger) *Factory {
	return &Factory{route: route, logger: logger}
}

func NewBacktestFactory(route adapter.Route) *Factory {
	return &Factory{route: route}
}

func (f *Factory) CredentialValidator() (adapter.CredentialValidator, error) {
	return credentialValidator{route: f.route}, nil
}

func (f *Factory) AccountSnapshotReader() (adapter.AccountSnapshotReader, error) {
	if err := f.requirePerpetualFutures("account_snapshot_reader"); err != nil {
		return nil, err
	}
	switch f.route.Environment {
	case domain.EnvironmentBacktest:
		return backtestPortfolioSnapshotReader{}, nil
	case domain.EnvironmentDemo:
		return portfolioSnapshotReader{
			route:  f.route,
			client: legacyexchange.NewBinanceTestnetAdapter(f.logger),
		}, nil
	case domain.EnvironmentLive:
		return portfolioSnapshotReader{
			route:  f.route,
			client: legacyexchange.NewBinanceLiveAdapter(f.logger),
		}, nil
	default:
		return nil, adapter.CapabilityUnsupported("account_snapshot_reader")
	}
}

func (f *Factory) SymbolRulesReader() (adapter.SymbolRulesReader, error) {
	if err := f.requirePerpetualFutures("symbol_rules_reader"); err != nil {
		return nil, err
	}
	return symbolRulesReader{baseURL: f.futuresBaseURL()}, nil
}

func (f *Factory) OrderExecutor() (adapter.OrderExecutor, error) {
	if err := f.requirePerpetualFutures("order_executor"); err != nil {
		return nil, err
	}
	switch f.route.Environment {
	case domain.EnvironmentBacktest:
		return simulatedOrderExecutor{}, nil
	case domain.EnvironmentDemo:
		return orderExecutor{exec: orderexecutor.NewBinanceTestnetExecutor(f.logger)}, nil
	case domain.EnvironmentLive:
		return orderExecutor{exec: orderexecutor.NewBinanceLiveExecutor(f.logger)}, nil
	default:
		return nil, adapter.CapabilityUnsupported("order_executor")
	}
}

func (f *Factory) OrderStateReader() (adapter.OrderStateReader, error) {
	if err := f.requirePerpetualFutures("order_state_reader"); err != nil {
		return nil, err
	}
	switch f.route.Environment {
	case domain.EnvironmentBacktest:
		return simulatedOrderStateReader{}, nil
	case domain.EnvironmentDemo:
		return orderStateReader{exec: orderexecutor.NewBinanceTestnetExecutor(f.logger)}, nil
	case domain.EnvironmentLive:
		return orderStateReader{exec: orderexecutor.NewBinanceLiveExecutor(f.logger)}, nil
	default:
		return nil, adapter.CapabilityUnsupported("order_state_reader")
	}
}

func (f *Factory) OrderCanceller() (adapter.OrderCanceller, error) {
	if err := f.requirePerpetualFutures("order_canceller"); err != nil {
		return nil, err
	}
	return orderCanceller{route: f.route}, nil
}

func (f *Factory) requirePerpetualFutures(capability string) error {
	if f.route.Market != domain.MarketPerpetualFutures {
		return adapter.CapabilityUnsupported(capability)
	}
	return nil
}

func (f *Factory) futuresBaseURL() string {
	if f.route.Environment == domain.EnvironmentDemo {
		return legacyexchange.BinanceTestnetBaseURL
	}
	return legacyexchange.BinanceLiveBaseURL
}
