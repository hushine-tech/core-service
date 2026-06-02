package exchange

import (
	"context"
	"fmt"

	"github.com/hushine-tech/core-service/internal/domain"
)

// OnlineInfoFetcher fetches real-time account info for a given account.
//
// Callers MUST pass the full domain.Account, not just accountID. The adapter
// needs per-account credentials (api_key / api_secret) and per-account routing
// hints (environment).
type OnlineInfoFetcher interface {
	FetchOnlineAccountInfo(ctx context.Context, account domain.Account) (domain.OnlineAccountInfo, error)
}

// ExchangeProvider identifies an exchange family (Binance, OKX, …).
type ExchangeProvider string

const (
	ProviderLocal   ExchangeProvider = "local" // backtest, data from DB
	ProviderBinance ExchangeProvider = "binance"
)

// ExchangeEnvironment identifies the concrete environment within a provider.
type ExchangeEnvironment string

const (
	EnvNone ExchangeEnvironment = "" // local / backtest
	EnvLive ExchangeEnvironment = "live"
	EnvDemo ExchangeEnvironment = "demo"
)

// ExchangeTarget is the decoded routing intent for an account.
type ExchangeTarget struct {
	Provider    ExchangeProvider
	Environment ExchangeEnvironment
}

// ResolveTarget decodes account environment into (provider, environment).
func ResolveTarget(env domain.Environment) (ExchangeTarget, error) {
	switch env {
	case domain.EnvironmentBacktest:
		return ExchangeTarget{Provider: ProviderLocal, Environment: EnvNone}, nil
	case domain.EnvironmentDemo:
		return ExchangeTarget{Provider: ProviderBinance, Environment: EnvDemo}, nil
	case domain.EnvironmentLive:
		return ExchangeTarget{Provider: ProviderBinance, Environment: EnvLive}, nil
	default:
		return ExchangeTarget{}, fmt.Errorf("unsupported account environment: %d", env)
	}
}

// AdapterRouter dispatches fetches by ExchangeTarget.
//
// Adapters are environment-scoped (one instance per provider+environment),
// not credential-scoped. Per-account credentials travel inside the
// domain.Account passed to FetchOnlineAccountInfo.
type AdapterRouter struct {
	fetchers  map[ExchangeTarget]OnlineInfoFetcher
	getFromDB func(ctx context.Context, accountID int64) (domain.OnlineAccountInfo, error)
}

// NewAdapterRouter builds a router from the provided environment-scoped
// fetchers. The fetchers map key is ExchangeTarget; a nil value means the
// corresponding route is not configured and will fail explicitly at call time.
//
// getFromDB is used for backtest to read the persisted wallet state.
func NewAdapterRouter(
	fetchers map[ExchangeTarget]OnlineInfoFetcher,
	getFromDB func(ctx context.Context, accountID int64) (domain.OnlineAccountInfo, error),
) *AdapterRouter {
	if fetchers == nil {
		fetchers = map[ExchangeTarget]OnlineInfoFetcher{}
	}
	return &AdapterRouter{
		fetchers:  fetchers,
		getFromDB: getFromDB,
	}
}

// GetOnlineInfo returns the online account info according to account.Environment.
//
// Behavior per target:
//   - local/backtest: read current state from DB; no credential / external call
//   - exchange-backed: delegate to the registered fetcher, passing the full
//     account (so the fetcher can use per-account api_key/api_secret)
//
// Unsupported or unconfigured environments return an explicit error — never a
// silent fallback to a different target.
func (r *AdapterRouter) GetOnlineInfo(ctx context.Context, account domain.Account) (domain.OnlineAccountInfo, error) {
	target, err := ResolveTarget(account.Environment)
	if err != nil {
		return domain.OnlineAccountInfo{}, err
	}

	if target.Provider == ProviderLocal {
		info, err := r.getFromDB(ctx, account.AccountID)
		if err != nil {
			return domain.OnlineAccountInfo{}, fmt.Errorf("backtest: fetch from db: %w", err)
		}
		info.Environment = domain.EnvironmentBacktest
		return info, nil
	}

	fetcher := r.fetchers[target]
	if fetcher == nil {
		return domain.OnlineAccountInfo{}, fmt.Errorf(
			"exchange adapter not configured: provider=%s env=%s",
			target.Provider, target.Environment,
		)
	}

	// Credential validation is delegated to each fetcher: the real Binance
	// adapter requires api_key/api_secret on the account; mock / local
	// fetchers do not. This keeps the router provider-neutral.
	info, err := fetcher.FetchOnlineAccountInfo(ctx, account)
	if err != nil {
		return domain.OnlineAccountInfo{}, fmt.Errorf("%s %s: %w", target.Provider, target.Environment, err)
	}
	info.Environment = account.Environment
	return info, nil
}
