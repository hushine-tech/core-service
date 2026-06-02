package exchange

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/hushine-tech/core-service/internal/domain"
)

// --- fakeFetcher: in-memory OnlineInfoFetcher for router tests ---

type fakeFetcher struct {
	fetched      domain.Account
	calls        int
	returnErr    error
	requireCreds bool
	fixedTotal   float64
}

func (f *fakeFetcher) FetchOnlineAccountInfo(_ context.Context, account domain.Account) (domain.OnlineAccountInfo, error) {
	f.fetched = account
	f.calls++
	if f.returnErr != nil {
		return domain.OnlineAccountInfo{}, f.returnErr
	}
	if f.requireCreds && (account.APIKey == "" || account.APISecret == "") {
		return domain.OnlineAccountInfo{}, fmt.Errorf("missing credentials")
	}
	return domain.OnlineAccountInfo{
		AccountID:  account.AccountID,
		TotalValue: f.fixedTotal,
	}, nil
}

// ResolveTarget unit tests --------------------------------------------------

func TestResolveTarget_Backtest(t *testing.T) {
	target, err := ResolveTarget(domain.EnvironmentBacktest)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if target.Provider != ProviderLocal || target.Environment != EnvNone {
		t.Fatalf("unexpected target: %+v", target)
	}
}

func TestResolveTarget_BinanceLive(t *testing.T) {
	target, err := ResolveTarget(domain.EnvironmentLive)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if target.Provider != ProviderBinance || target.Environment != EnvLive {
		t.Fatalf("unexpected target: %+v", target)
	}
}

func TestResolveTarget_BinanceTestnet(t *testing.T) {
	target, err := ResolveTarget(domain.EnvironmentDemo)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if target.Provider != ProviderBinance || target.Environment != EnvDemo {
		t.Fatalf("unexpected target: %+v", target)
	}
}

func TestResolveTarget_UnsupportedFailsExplicitly(t *testing.T) {
	_, err := ResolveTarget(domain.Environment(99))
	if err == nil {
		t.Fatal("expected error for unsupported mode")
	}
	if !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("expected unsupported-mode error, got %v", err)
	}
}

// AdapterRouter unit tests --------------------------------------------------

func TestRouter_Backtest_ReadsFromDBAndSetsMode(t *testing.T) {
	getFromDB := func(_ context.Context, accountID int64) (domain.OnlineAccountInfo, error) {
		return domain.OnlineAccountInfo{
			AccountID:  accountID,
			TotalValue: 12345,
			// mode intentionally left as zero — router must stamp backtest
		}, nil
	}
	r := NewAdapterRouter(nil, getFromDB)
	info, err := r.GetOnlineInfo(context.Background(), domain.Account{
		AccountID:   7,
		Environment: domain.EnvironmentBacktest,
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if info.Environment != domain.EnvironmentBacktest {
		t.Fatalf("expected backtest mode stamped, got %d", info.Environment)
	}
	if info.TotalValue != 12345 {
		t.Fatalf("expected DB value passthrough, got %v", info.TotalValue)
	}
}

func TestRouter_BinanceTestnet_UsesAccountCredentials(t *testing.T) {
	fake := &fakeFetcher{requireCreds: true, fixedTotal: 9999}
	r := NewAdapterRouter(
		map[ExchangeTarget]OnlineInfoFetcher{
			{Provider: ProviderBinance, Environment: EnvDemo}: fake,
		},
		nil,
	)
	info, err := r.GetOnlineInfo(context.Background(), domain.Account{
		AccountID:   42,
		Environment: domain.EnvironmentDemo,
		APIKey:      "acct-key",
		APISecret:   "acct-secret",
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if fake.calls != 1 {
		t.Fatalf("expected 1 fetch call, got %d", fake.calls)
	}
	if fake.fetched.APIKey != "acct-key" || fake.fetched.APISecret != "acct-secret" {
		t.Fatalf("adapter did not receive per-account credentials: %+v", fake.fetched)
	}
	if info.Environment != domain.EnvironmentDemo {
		t.Fatalf("expected testnet mode stamped, got %d", info.Environment)
	}
	if info.TotalValue != 9999 {
		t.Fatalf("expected fake value passthrough, got %v", info.TotalValue)
	}
}

func TestRouter_BinanceLive_AdapterNotConfiguredFailsExplicitly(t *testing.T) {
	// only testnet registered
	r := NewAdapterRouter(
		map[ExchangeTarget]OnlineInfoFetcher{
			{Provider: ProviderBinance, Environment: EnvDemo}: &fakeFetcher{},
		},
		nil,
	)
	_, err := r.GetOnlineInfo(context.Background(), domain.Account{
		AccountID:   1,
		Environment: domain.EnvironmentLive,
		APIKey:      "x",
		APISecret:   "y",
	})
	if err == nil {
		t.Fatal("expected error when live adapter is missing")
	}
	if !strings.Contains(err.Error(), "adapter not configured") {
		t.Fatalf("expected adapter-not-configured error, got %v", err)
	}
}

func TestRouter_UnsupportedModeDoesNotFallback(t *testing.T) {
	r := NewAdapterRouter(
		map[ExchangeTarget]OnlineInfoFetcher{
			{Provider: ProviderBinance, Environment: EnvLive}: &fakeFetcher{},
		},
		func(_ context.Context, id int64) (domain.OnlineAccountInfo, error) {
			t.Fatalf("backtest fallback must NOT be called for unsupported mode")
			return domain.OnlineAccountInfo{}, nil
		},
	)
	_, err := r.GetOnlineInfo(context.Background(), domain.Account{
		AccountID:   1,
		Environment: domain.Environment(99),
	})
	if err == nil {
		t.Fatal("expected error for unsupported mode")
	}
}

func TestRouter_AdapterErrorBubblesWithContext(t *testing.T) {
	cause := errors.New("binance 500")
	r := NewAdapterRouter(
		map[ExchangeTarget]OnlineInfoFetcher{
			{Provider: ProviderBinance, Environment: EnvDemo}: &fakeFetcher{returnErr: cause},
		},
		nil,
	)
	_, err := r.GetOnlineInfo(context.Background(), domain.Account{
		AccountID:   1,
		Environment: domain.EnvironmentDemo,
		APIKey:      "k",
		APISecret:   "s",
	})
	if err == nil {
		t.Fatal("expected bubbled error")
	}
	if !strings.Contains(err.Error(), "binance 500") {
		t.Fatalf("expected underlying cause in error chain, got %v", err)
	}
	if !strings.Contains(err.Error(), "demo") {
		t.Fatalf("expected provider/env in error context, got %v", err)
	}
}

func TestRouter_MissingCredentialsReportedByAdapter(t *testing.T) {
	// fake enforces credential presence; router passes account through.
	fake := &fakeFetcher{requireCreds: true}
	r := NewAdapterRouter(
		map[ExchangeTarget]OnlineInfoFetcher{
			{Provider: ProviderBinance, Environment: EnvDemo}: fake,
		},
		nil,
	)
	_, err := r.GetOnlineInfo(context.Background(), domain.Account{
		AccountID:   1,
		Environment: domain.EnvironmentDemo,
		// APIKey / APISecret left empty
	})
	if err == nil {
		t.Fatal("expected credential error")
	}
	if !strings.Contains(err.Error(), "missing credentials") {
		t.Fatalf("expected missing-credentials error, got %v", err)
	}
}
