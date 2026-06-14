package binance

import (
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hushine-tech/core-service/internal/domain"
	legacyexchange "github.com/hushine-tech/core-service/internal/exchange"
	"github.com/hushine-tech/core-service/internal/exchange/adapter"
	orderexecutor "github.com/hushine-tech/core-service/internal/order/executor"
	"github.com/hushine-tech/golang-lib/pkg/log"
)

type Factory struct {
	route     adapter.Route
	logger    log.Logger
	endpoints Endpoints
}

func NewFactory(route adapter.Route, logger log.Logger) *Factory {
	return NewFactoryWithEndpoints(route, logger, EndpointsFromEnv(route))
}

func NewFactoryWithEndpoints(route adapter.Route, logger log.Logger, endpoints Endpoints) *Factory {
	defaults := EndpointsFromEnv(route)
	if endpoints.RESTBaseURL == "" {
		endpoints.RESTBaseURL = defaults.RESTBaseURL
	}
	if endpoints.WSBaseURL == "" {
		endpoints.WSBaseURL = defaults.WSBaseURL
	}
	return &Factory{route: route, logger: logger, endpoints: endpoints}
}

func NewBacktestFactory(route adapter.Route) *Factory {
	return &Factory{route: route, endpoints: EndpointsFromEnv(route)}
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
	if f.route.Environment == domain.EnvironmentBacktest {
		return backtestSymbolRulesReader{}, nil
	}
	return symbolRulesReader{baseURL: f.futuresBaseURL()}, nil
}

func (f *Factory) OrderExecutor() (adapter.OrderExecutor, error) {
	if err := f.requireOrderMarket("order_executor"); err != nil {
		return nil, err
	}
	switch f.route.Environment {
	case domain.EnvironmentBacktest:
		return simulatedOrderExecutor{route: f.route}, nil
	case domain.EnvironmentDemo:
		return f.remoteOrderExecutor(orderexecutor.NewBinanceExecutorWithBaseURL(f.futuresBaseURL(), f.logger, "binance_order_testnet")), nil
	case domain.EnvironmentLive:
		return f.remoteOrderExecutor(orderexecutor.NewBinanceExecutorWithBaseURL(f.futuresBaseURL(), f.logger, "binance_order_live")), nil
	default:
		return nil, adapter.CapabilityUnsupported("order_executor")
	}
}

func (f *Factory) OrderCapabilityProvider() (adapter.OrderCapabilityProvider, error) {
	if err := f.requireOrderMarket("order_capability_provider"); err != nil {
		return nil, err
	}
	return orderCapabilityProvider{route: f.route}, nil
}

func (f *Factory) OrderStateReader() (adapter.OrderStateReader, error) {
	if err := f.requireOrderMarket("order_state_reader"); err != nil {
		return nil, err
	}
	if f.route.Market == domain.MarketSpot {
		switch f.route.Environment {
		case domain.EnvironmentBacktest:
			return simulatedOrderStateReader{}, nil
		case domain.EnvironmentDemo, domain.EnvironmentLive:
			return spotOrderStateReader{
				baseURL:    f.spotBaseURL(),
				httpClient: &http.Client{Timeout: 10 * time.Second},
			}, nil
		default:
			return nil, adapter.CapabilityUnsupported("order_state_reader")
		}
	}
	switch f.route.Environment {
	case domain.EnvironmentBacktest:
		return simulatedOrderStateReader{}, nil
	case domain.EnvironmentDemo:
		return orderStateReader{exec: orderexecutor.NewBinanceExecutorWithBaseURL(f.futuresBaseURL(), f.logger, "binance_order_testnet")}, nil
	case domain.EnvironmentLive:
		return orderStateReader{exec: orderexecutor.NewBinanceExecutorWithBaseURL(f.futuresBaseURL(), f.logger, "binance_order_live")}, nil
	default:
		return nil, adapter.CapabilityUnsupported("order_state_reader")
	}
}

func (f *Factory) OrderCanceller() (adapter.OrderCanceller, error) {
	if err := f.requireOrderMarket("order_canceller"); err != nil {
		return nil, err
	}
	return orderCanceller{
		route:      f.route,
		baseURL:    f.remoteBaseURL(),
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}, nil
}

func (f *Factory) UserDataStream() (adapter.UserDataStream, error) {
	if err := f.requireOrderMarket("user_data_stream"); err != nil {
		return nil, err
	}
	if f.route.Environment == domain.EnvironmentBacktest {
		return nil, adapter.CapabilityUnsupported("user_data_stream")
	}
	return binanceUserDataStreamClient{
		route:      f.route,
		endpoints:  f.endpoints,
		httpClient: &http.Client{Timeout: 10 * time.Second},
		dialer:     websocket.DefaultDialer,
	}, nil
}

func (f *Factory) requirePerpetualFutures(capability string) error {
	if f.route.Market != domain.MarketPerpetualFutures {
		return adapter.CapabilityUnsupported(capability)
	}
	return nil
}

func (f *Factory) requireOrderMarket(capability string) error {
	switch f.route.Market {
	case domain.MarketSpot, domain.MarketPerpetualFutures:
		return nil
	default:
		return adapter.CapabilityUnsupported(capability)
	}
}

func (f *Factory) futuresBaseURL() string {
	return f.endpoints.RESTBaseURL
}

func (f *Factory) spotBaseURL() string {
	return f.endpoints.RESTBaseURL
}

func (f *Factory) remoteBaseURL() string {
	if f.route.Market == domain.MarketSpot {
		return f.spotBaseURL()
	}
	return f.futuresBaseURL()
}

func (f *Factory) remoteOrderExecutor(futuresExec *orderexecutor.BinanceExecutor) adapter.OrderExecutor {
	exec := orderExecutor{
		route:      f.route,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
	switch f.route.Market {
	case domain.MarketSpot:
		exec.baseURL = f.spotBaseURL()
	case domain.MarketPerpetualFutures:
		exec.exec = futuresExec
		exec.baseURL = f.futuresBaseURL()
	}
	return exec
}
