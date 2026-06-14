package service

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/gen/accountv1"
	"github.com/hushine-tech/core-service/internal/config"
	"github.com/hushine-tech/core-service/internal/credential"
	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/exchange/adapter"
	"github.com/hushine-tech/core-service/internal/reconciliation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func testPortfolioCredentialManager(t *testing.T) *credential.Manager {
	t.Helper()
	mgr, err := credential.NewManager("0123456789abcdef0123456789abcdef", "v1")
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return mgr
}

type snapshotFactory struct {
	reader adapter.AccountSnapshotReader
}

func (f snapshotFactory) CredentialValidator() (adapter.CredentialValidator, error) {
	return snapshotCredentialValidator{}, nil
}

func (f snapshotFactory) AccountSnapshotReader() (adapter.AccountSnapshotReader, error) {
	return f.reader, nil
}

func (f snapshotFactory) SymbolRulesReader() (adapter.SymbolRulesReader, error) {
	return nil, adapter.CapabilityUnsupported("symbol_rules_reader")
}

func (f snapshotFactory) OrderExecutor() (adapter.OrderExecutor, error) {
	return nil, adapter.CapabilityUnsupported("order_executor")
}

func (f snapshotFactory) OrderCapabilityProvider() (adapter.OrderCapabilityProvider, error) {
	return nil, adapter.CapabilityUnsupported("order_capability_provider")
}

func (f snapshotFactory) OrderStateReader() (adapter.OrderStateReader, error) {
	return nil, adapter.CapabilityUnsupported("order_state_reader")
}

func (f snapshotFactory) OrderCanceller() (adapter.OrderCanceller, error) {
	return nil, adapter.CapabilityUnsupported("order_canceller")
}

type snapshotCredentialValidator struct{}

func (snapshotCredentialValidator) ValidateCredential(_ context.Context, raw json.RawMessage) (adapter.ParsedCredential, error) {
	var payload map[string]string
	if err := json.Unmarshal(raw, &payload); err != nil {
		return adapter.ParsedCredential{}, err
	}
	if payload["api_key"] == "" || payload["api_secret"] == "" {
		return adapter.ParsedCredential{}, errors.New("missing credential")
	}
	return adapter.ParsedCredential{Raw: raw, Metadata: payload}, nil
}

type snapshotReader struct {
	calls int
	req   adapter.PortfolioSnapshotRequest
	resp  adapter.PortfolioSnapshot
}

func (r *snapshotReader) ReadPortfolioSnapshot(_ context.Context, req adapter.PortfolioSnapshotRequest) (adapter.PortfolioSnapshot, error) {
	r.calls++
	r.req = req
	if r.resp.UpdatedAt.IsZero() {
		r.resp.UpdatedAt = time.Unix(100, 0).UTC()
	}
	r.resp.UserID = req.UserID
	r.resp.AccountID = req.AccountID
	r.resp.VenueID = req.VenueID
	return r.resp, nil
}

type reconciliationCaptureRepo struct {
	*sessionStubRepo
	mu   sync.Mutex
	runs []domain.ReconciliationRun
}

func (r *reconciliationCaptureRepo) SaveReconciliationRun(_ context.Context, run domain.ReconciliationRun) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.runs = append(r.runs, run)
	return nil
}

func (r *reconciliationCaptureRepo) reconciliationRuns() []domain.ReconciliationRun {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]domain.ReconciliationRun(nil), r.runs...)
}

func waitForCapturedReconciliationRuns(t *testing.T, repo *reconciliationCaptureRepo, want int) []domain.ReconciliationRun {
	t.Helper()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		runs := repo.reconciliationRuns()
		if len(runs) >= want {
			return runs
		}
		time.Sleep(10 * time.Millisecond)
	}
	runs := repo.reconciliationRuns()
	t.Fatalf("timed out waiting for %d reconciliation runs; got %d", want, len(runs))
	return nil
}

func enabledReconciler(repo *reconciliationCaptureRepo) *reconciliation.Service {
	cfg := config.DefaultReconciliationConfig()
	cfg.Enabled = true
	cfg.GoroutineTimeoutSeconds = 1
	return reconciliation.NewService(cfg, repo)
}

func TestGetPortfolioSnapshotReturnsVenueArray(t *testing.T) {
	accountID := int64(15)
	venueID := int64(88)
	mgr := testPortfolioCredentialManager(t)
	encrypted, err := mgr.Encrypt(`{"api_secret":"s1"}`)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	reader := &snapshotReader{resp: adapter.PortfolioSnapshot{
		Exchange:         domain.ExchangeBinance,
		Environment:      domain.EnvironmentDemo,
		Market:           domain.MarketPerpetualFutures,
		TotalValue:       1000,
		WalletBalance:    900,
		AvailableBalance: 800,
		Balances: []adapter.BalanceEntry{
			{Asset: "USDT", WalletBalance: 900, AvailableBalance: 800, ValueUSDT: 900},
		},
		Positions: []adapter.PositionEntry{
			{Symbol: "ETHUSDT", PositionSide: "BOTH", Qty: 0.2, MarkPrice: 2000},
		},
		OnlineInfo: &domain.OnlineAccountInfo{
			Environment:      domain.EnvironmentDemo,
			WalletBalance:    900,
			AvailableBalance: 800,
			Futures: domain.FuturesWallet{
				MarginMode:       "cross",
				PositionMode:     "one_way",
				WalletBalance:    900,
				AvailableBalance: 800,
				MarginBalance:    940,
				RiskMetadata: []domain.FuturesRiskMetadata{{
					Symbol:               "ETHUSDT",
					ConfiguredLeverage:   20,
					ConfiguredMarginMode: "cross",
				}},
				Positions: []domain.FuturesPosition{{
					Symbol:        "ETHUSDT",
					PositionSide:  "BOTH",
					PositionQty:   0.2,
					EntryPrice:    1900,
					MarkPrice:     2000,
					Leverage:      20,
					MarginMode:    "cross",
					MarginType:    "cross",
					InitialMargin: 20,
				}},
			},
		},
	}}
	registry := adapter.NewRegistry()
	registry.Register(adapter.Route{Exchange: domain.ExchangeBinance, Environment: domain.EnvironmentDemo, Market: domain.MarketPerpetualFutures}, snapshotFactory{reader: reader})
	repo := &stubRepo{
		account: domain.Account{AccountID: accountID, UserID: serviceTestUserID, Environment: domain.EnvironmentDemo},
		venues: []domain.Venue{{
			VenueID:        venueID,
			UserID:         serviceTestUserID,
			AccountID:      &accountID,
			Exchange:       domain.ExchangeBinance,
			Environment:    domain.EnvironmentDemo,
			Market:         domain.MarketPerpetualFutures,
			Status:         domain.VenueStatusActive,
			APIKey:         "k1",
			CredentialInfo: encrypted,
			MarginMode:     domain.MarginModeCross,
			PositionMode:   domain.PositionModeOneWay,
		}},
	}
	svc := NewAccountGRPCService(repo, nil, nil, nil, WithCredentialManager(mgr), WithExchangeRegistry(registry))

	resp, err := svc.GetPortfolioSnapshot(context.Background(), &accountv1.GetPortfolioSnapshotRequest{AccountId: accountID, UserId: serviceTestUserID})
	if err != nil {
		t.Fatalf("GetPortfolioSnapshot() error = %v", err)
	}
	if got := len(resp.GetSnapshot().GetVenues()); got != 1 {
		t.Fatalf("len(venues) = %d, want 1", got)
	}
	venue := resp.GetSnapshot().GetVenues()[0]
	if venue.GetVenueId() != venueID || venue.GetExchange() != int32(domain.ExchangeBinance) || venue.GetBalances()[0].GetAsset() != "USDT" {
		t.Fatalf("venue snapshot = %+v", venue)
	}
	if venue.GetWallet() == nil || venue.GetWallet().GetFutures().GetPositions()[0].GetLeverage() != 20 {
		t.Fatalf("venue wallet futures = %+v, want full canonical futures wallet", venue.GetWallet().GetFutures())
	}
	if venue.GetWallet().GetFutures().GetRiskMetadata()[0].GetConfiguredLeverage() != 20 {
		t.Fatalf("venue wallet risk metadata = %+v, want leverage 20", venue.GetWallet().GetFutures().GetRiskMetadata())
	}
}

func TestBacktestPortfolioSnapshotIncludesBoundSimulatedVenue(t *testing.T) {
	accountID := int64(15)
	venueID := int64(88)
	repo := &stubRepo{
		account: domain.Account{
			AccountID:   accountID,
			UserID:      serviceTestUserID,
			Environment: domain.EnvironmentBacktest,
		},
		venues: []domain.Venue{{
			VenueID:     venueID,
			UserID:      serviceTestUserID,
			AccountID:   &accountID,
			Exchange:    domain.ExchangeBinance,
			Environment: domain.EnvironmentBacktest,
			Market:      domain.MarketPerpetualFutures,
			Status:      domain.VenueStatusActive,
		}, {
			VenueID:     99,
			UserID:      serviceTestUserID,
			Exchange:    domain.ExchangeBinance,
			Environment: domain.EnvironmentBacktest,
			Market:      domain.MarketSpot,
			Status:      domain.VenueStatusActive,
		}},
		venueStates: map[int64]domain.OnlineAccountInfo{
			venueID: {
				AccountID:        accountID,
				Environment:      domain.EnvironmentBacktest,
				TotalValue:       1500,
				WalletBalance:    1000,
				AvailableBalance: 900,
				Futures: domain.FuturesWallet{
					WalletBalance:    1000,
					AvailableBalance: 900,
					MarginBalance:    1000,
				},
				Spot:      domain.SpotWallet{Free: 500},
				UpdatedAt: time.Unix(100, 0).UTC(),
			},
			99: {
				Environment:      domain.EnvironmentBacktest,
				TotalValue:       999999,
				WalletBalance:    999999,
				AvailableBalance: 999999,
				Spot:             domain.SpotWallet{Free: 999999},
				UpdatedAt:        time.Unix(101, 0).UTC(),
			},
		},
	}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	resp, err := svc.GetPortfolioSnapshot(context.Background(), &accountv1.GetPortfolioSnapshotRequest{
		AccountId: accountID,
		UserId:    serviceTestUserID,
	})
	if err != nil {
		t.Fatalf("GetPortfolioSnapshot() error = %v", err)
	}
	venues := resp.GetSnapshot().GetVenues()
	if len(venues) != 1 {
		t.Fatalf("len(venues) = %d, want 1", len(venues))
	}
	if resp.GetSnapshot().GetTotalValue() != 1000 {
		t.Fatalf("portfolio total = %v, want only bound futures venue total 1000", resp.GetSnapshot().GetTotalValue())
	}
	venue := venues[0]
	if venue.GetVenueId() != venueID || venue.GetExchange() != int32(domain.ExchangeBinance) || venue.GetMarket() != int32(domain.MarketPerpetualFutures) {
		t.Fatalf("venue snapshot = %+v", venue)
	}
	if venue.GetWalletBalance() != 1000 || venue.GetAvailableBalance() != 900 {
		t.Fatalf("venue balances = wallet %v available %v, want 1000/900", venue.GetWalletBalance(), venue.GetAvailableBalance())
	}
	if venue.GetWallet() == nil || venue.GetWallet().GetEnvironment() != int32(domain.EnvironmentBacktest) {
		t.Fatalf("venue wallet = %+v, want backtest account wallet state", venue.GetWallet())
	}
	if venue.GetWallet().GetSpot().GetFree() != 0 || venue.GetWallet().GetSpot().GetLocked() != 0 {
		t.Fatalf("futures venue snapshot must not carry spot wallet: %+v", venue.GetWallet().GetSpot())
	}
}

func TestBacktestPortfolioSnapshotUsesPersistedVenueWalletDefaults(t *testing.T) {
	accountID := int64(15)
	venueID := int64(88)
	repo := &stubRepo{
		account: domain.Account{
			AccountID:    accountID,
			UserID:       serviceTestUserID,
			Environment:  domain.EnvironmentBacktest,
			MarginMode:   "cross",
			PositionMode: "one_way",
		},
		venues: []domain.Venue{{
			VenueID:      venueID,
			UserID:       serviceTestUserID,
			AccountID:    &accountID,
			Exchange:     domain.ExchangeBinance,
			Environment:  domain.EnvironmentBacktest,
			Market:       domain.MarketPerpetualFutures,
			Status:       domain.VenueStatusActive,
			MarginMode:   domain.MarginModeCross,
			PositionMode: domain.PositionModeOneWay,
		}},
		venueStates: map[int64]domain.OnlineAccountInfo{
			venueID: {
				AccountID:   accountID,
				Environment: domain.EnvironmentBacktest,
				Futures: domain.FuturesWallet{
					MarginMode:   "cross",
					PositionMode: "one_way",
				},
				UpdatedAt: time.Unix(100, 0).UTC(),
			},
		},
	}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	resp, err := svc.GetPortfolioSnapshot(context.Background(), &accountv1.GetPortfolioSnapshotRequest{
		AccountId: accountID,
		UserId:    serviceTestUserID,
	})
	if err != nil {
		t.Fatalf("GetPortfolioSnapshot() error = %v", err)
	}
	venues := resp.GetSnapshot().GetVenues()
	if len(venues) != 1 {
		t.Fatalf("len(venues) = %d, want 1", len(venues))
	}
	venue := venues[0]
	if venue.GetVenueId() != venueID || venue.GetWallet() == nil || venue.GetWallet().GetFutures() == nil {
		t.Fatalf("venue snapshot = %+v, want full futures wallet", venue)
	}
	futures := venue.GetWallet().GetFutures()
	if futures.GetMarginMode() != "cross" || futures.GetPositionMode() != "one_way" {
		t.Fatalf("venue futures modes = %q/%q, want cross/one_way", futures.GetMarginMode(), futures.GetPositionMode())
	}
	if venue.GetWalletBalance() != 0 || venue.GetAvailableBalance() != 0 || venue.GetTotalValue() != 0 {
		t.Fatalf("venue balances = total:%v wallet:%v available:%v, want zero defaults",
			venue.GetTotalValue(), venue.GetWalletBalance(), venue.GetAvailableBalance())
	}
}

func TestBacktestPortfolioSnapshotFailsWhenVenueWalletStateIsMissing(t *testing.T) {
	accountID := int64(15)
	venueID := int64(88)
	repo := &stubRepo{
		account: domain.Account{
			AccountID:   accountID,
			UserID:      serviceTestUserID,
			Environment: domain.EnvironmentBacktest,
		},
		venues: []domain.Venue{{
			VenueID:      venueID,
			UserID:       serviceTestUserID,
			AccountID:    &accountID,
			Exchange:     domain.ExchangeBinance,
			Environment:  domain.EnvironmentBacktest,
			Market:       domain.MarketPerpetualFutures,
			Status:       domain.VenueStatusActive,
			MarginMode:   domain.MarginModeCross,
			PositionMode: domain.PositionModeOneWay,
		}},
	}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	_, err := svc.GetPortfolioSnapshot(context.Background(), &accountv1.GetPortfolioSnapshotRequest{
		AccountId: accountID,
		UserId:    serviceTestUserID,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("GetPortfolioSnapshot() code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
}

func TestBacktestPortfolioSnapshotWithoutActiveVenueReturnsEmptySnapshot(t *testing.T) {
	accountID := int64(15)
	repo := &stubRepo{
		account: domain.Account{
			AccountID:   accountID,
			UserID:      serviceTestUserID,
			Environment: domain.EnvironmentBacktest,
		},
	}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	resp, err := svc.GetPortfolioSnapshot(context.Background(), &accountv1.GetPortfolioSnapshotRequest{
		AccountId: accountID,
		UserId:    serviceTestUserID,
	})
	if err != nil {
		t.Fatalf("GetPortfolioSnapshot() unexpected error: %v", err)
	}
	snapshot := resp.GetSnapshot()
	if snapshot.GetAccountId() != accountID || snapshot.GetUserId() != serviceTestUserID {
		t.Fatalf("snapshot ids = account:%d user:%d", snapshot.GetAccountId(), snapshot.GetUserId())
	}
	if len(snapshot.GetVenues()) != 0 {
		t.Fatalf("venues len = %d, want 0", len(snapshot.GetVenues()))
	}
	if snapshot.GetWallet() == nil {
		t.Fatal("empty snapshot should still include account-level wallet shell")
	}
}

func TestExchangePortfolioSnapshotWithoutActiveVenueReturnsEmptySnapshot(t *testing.T) {
	accountID := int64(16)
	repo := &stubRepo{
		account: domain.Account{
			AccountID:   accountID,
			UserID:      serviceTestUserID,
			Environment: domain.EnvironmentDemo,
		},
	}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	resp, err := svc.GetPortfolioSnapshot(context.Background(), &accountv1.GetPortfolioSnapshotRequest{
		AccountId: accountID,
		UserId:    serviceTestUserID,
	})
	if err != nil {
		t.Fatalf("GetPortfolioSnapshot() unexpected error: %v", err)
	}
	snapshot := resp.GetSnapshot()
	if snapshot.GetAccountId() != accountID || snapshot.GetWallet().GetEnvironment() != int32(domain.EnvironmentDemo) {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	if len(snapshot.GetVenues()) != 0 {
		t.Fatalf("venues len = %d, want 0", len(snapshot.GetVenues()))
	}
}

func TestGetVenueOnlineInfoBacktestReturnsPersistedVenueWalletState(t *testing.T) {
	accountID := int64(15)
	venueID := int64(88)
	repo := &stubRepo{
		account: domain.Account{
			AccountID:   accountID,
			UserID:      serviceTestUserID,
			Environment: domain.EnvironmentBacktest,
		},
		venues: []domain.Venue{{
			VenueID:      venueID,
			UserID:       serviceTestUserID,
			AccountID:    &accountID,
			Exchange:     domain.ExchangeBinance,
			Environment:  domain.EnvironmentBacktest,
			Market:       domain.MarketPerpetualFutures,
			Status:       domain.VenueStatusActive,
			MarginMode:   domain.MarginModeCross,
			PositionMode: domain.PositionModeOneWay,
		}},
		venueStates: map[int64]domain.OnlineAccountInfo{
			venueID: {
				AccountID:        accountID,
				Environment:      domain.EnvironmentBacktest,
				TotalValue:       1200,
				WalletBalance:    1000,
				AvailableBalance: 900,
				Futures: domain.FuturesWallet{
					MarginMode:       "cross",
					PositionMode:     "one_way",
					WalletBalance:    1000,
					AvailableBalance: 900,
					MarginBalance:    1000,
				},
				UpdatedAt: time.Unix(100, 0).UTC(),
			},
		},
	}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	resp, err := svc.GetVenueOnlineInfo(context.Background(), &accountv1.GetVenueOnlineInfoRequest{
		UserId:  serviceTestUserID,
		VenueId: venueID,
	})
	if err != nil {
		t.Fatalf("GetVenueOnlineInfo() error = %v", err)
	}
	wallet := resp.GetWallet()
	if wallet.GetTotalValue() != 1200 || wallet.GetFutures().GetWalletBalance() != 1000 {
		t.Fatalf("wallet = %+v, want persisted venue wallet", wallet)
	}
}

func TestUpdatePortfolioSnapshotReadsVenueThroughRegistry(t *testing.T) {
	accountID := int64(15)
	venueID := int64(88)
	mgr := testPortfolioCredentialManager(t)
	encrypted, err := mgr.Encrypt(`{"api_secret":"s1"}`)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	reader := &snapshotReader{resp: adapter.PortfolioSnapshot{
		Exchange:         domain.ExchangeBinance,
		Environment:      domain.EnvironmentDemo,
		Market:           domain.MarketPerpetualFutures,
		TotalValue:       1200,
		WalletBalance:    1100,
		AvailableBalance: 1000,
		Balances:         []adapter.BalanceEntry{{Asset: "USDT", WalletBalance: 1100, AvailableBalance: 1000}},
	}}
	registry := adapter.NewRegistry()
	registry.Register(adapter.Route{Exchange: domain.ExchangeBinance, Environment: domain.EnvironmentDemo, Market: domain.MarketPerpetualFutures}, snapshotFactory{reader: reader})
	repo := &stubRepo{
		account: domain.Account{AccountID: accountID, UserID: serviceTestUserID, Environment: domain.EnvironmentDemo},
		venues: []domain.Venue{{
			VenueID:        venueID,
			UserID:         serviceTestUserID,
			AccountID:      &accountID,
			Exchange:       domain.ExchangeBinance,
			Environment:    domain.EnvironmentDemo,
			Market:         domain.MarketPerpetualFutures,
			Status:         domain.VenueStatusActive,
			APIKey:         "k1",
			CredentialInfo: encrypted,
			MarginMode:     domain.MarginModeCross,
			PositionMode:   domain.PositionModeOneWay,
		}},
	}
	svc := NewAccountGRPCService(repo, nil, nil, nil, WithCredentialManager(mgr), WithExchangeRegistry(registry))

	resp, err := svc.UpdatePortfolioSnapshot(context.Background(), &accountv1.UpdatePortfolioSnapshotRequest{AccountId: accountID, UserId: serviceTestUserID})
	if err != nil {
		t.Fatalf("UpdatePortfolioSnapshot() error = %v", err)
	}
	if reader.calls != 1 {
		t.Fatalf("snapshot reader calls = %d, want 1", reader.calls)
	}
	if reader.req.VenueID != venueID {
		t.Fatalf("reader venue_id = %d, want %d", reader.req.VenueID, venueID)
	}
	if repo.state.TotalValue != 1200 || resp.GetSnapshot().GetTotalValue() != 1200 {
		t.Fatalf("state=%+v snapshot=%+v, want total value persisted", repo.state, resp.GetSnapshot())
	}
}

func TestUpdateAccountWalletStateDemoLaunchesReconciliation(t *testing.T) {
	accountID := int64(15)
	venueID := int64(88)
	sessionID := "sess-demo-reconcile"
	mgr := testPortfolioCredentialManager(t)
	encrypted, err := mgr.Encrypt(`{"api_secret":"s1"}`)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	reader := &snapshotReader{resp: adapter.PortfolioSnapshot{
		Exchange:         domain.ExchangeBinance,
		Environment:      domain.EnvironmentDemo,
		Market:           domain.MarketPerpetualFutures,
		TotalValue:       1300,
		WalletBalance:    1200,
		AvailableBalance: 1100,
		OnlineInfo: &domain.OnlineAccountInfo{
			Environment:      domain.EnvironmentDemo,
			TotalValue:       1300,
			WalletBalance:    1200,
			AvailableBalance: 1100,
			Futures: domain.FuturesWallet{
				MarginMode:       "cross",
				PositionMode:     "one_way",
				WalletBalance:    1200,
				AvailableBalance: 1100,
				MarginBalance:    1300,
				Positions: []domain.FuturesPosition{{
					Symbol:       "ZECUSDT",
					PositionSide: "BOTH",
					PositionQty:  0.1,
					Qty:          0.1,
					EntryPrice:   360,
					MarkPrice:    361,
					MarginMode:   "cross",
					MarginType:   "cross",
				}},
			},
		},
	}}
	registry := adapter.NewRegistry()
	registry.Register(adapter.Route{Exchange: domain.ExchangeBinance, Environment: domain.EnvironmentDemo, Market: domain.MarketPerpetualFutures}, snapshotFactory{reader: reader})
	baseRepo := newSessionStubRepo()
	baseRepo.account = domain.Account{AccountID: accountID, UserID: serviceTestUserID, Environment: domain.EnvironmentDemo}
	baseRepo.venues = []domain.Venue{{
		VenueID:        venueID,
		UserID:         serviceTestUserID,
		AccountID:      &accountID,
		Exchange:       domain.ExchangeBinance,
		Environment:    domain.EnvironmentDemo,
		Market:         domain.MarketPerpetualFutures,
		Status:         domain.VenueStatusActive,
		APIKey:         "k1",
		CredentialInfo: encrypted,
		MarginMode:     domain.MarginModeCross,
		PositionMode:   domain.PositionModeOneWay,
	}}
	baseRepo.sessions[sessionID] = domain.StrategySession{
		SessionID:  sessionID,
		AccountID:  accountID,
		UserID:     serviceTestUserID,
		StrategyID: 43,
		Status:     "running",
	}
	repo := &reconciliationCaptureRepo{sessionStubRepo: baseRepo}
	svc := NewAccountGRPCService(
		repo,
		nil,
		nil,
		enabledReconciler(repo),
		WithCredentialManager(mgr),
		WithExchangeRegistry(registry),
	)

	resp, err := svc.UpdateAccountWalletState(context.Background(), &accountv1.UpdateAccountWalletStateRequest{
		AccountId:        accountID,
		UserId:           serviceTestUserID,
		TotalValue:       1000,
		WalletBalance:    900,
		AvailableBalance: 800,
		SnapshotReason:   int32(domain.SnapshotReasonOrderFill),
		StrategyId:       43,
		SessionId:        sessionID,
		Futures: &accountv1.FuturesWallet{
			MarginMode:       "cross",
			PositionMode:     "one_way",
			WalletBalance:    900,
			AvailableBalance: 800,
			MarginBalance:    1000,
			Positions: []*accountv1.FuturesPosition{{
				Symbol:       "ZECUSDT",
				PositionSide: "BOTH",
				PositionQty:  0.1,
				Qty:          0.1,
				EntryPrice:   358,
				MarkPrice:    359,
				MarginMode:   "cross",
				MarginType:   "cross",
			}},
		},
	})
	if err != nil {
		t.Fatalf("UpdateAccountWalletState() error = %v", err)
	}
	if resp.GetWallet().GetTotalValue() != 1300 || repo.state.TotalValue != 1300 {
		t.Fatalf("wallet/state = %+v/%+v, want exchange-authoritative total 1300", resp.GetWallet(), repo.state)
	}
	if len(repo.venueStates) != 1 {
		t.Fatalf("venue wallet states len = %d, want 1", len(repo.venueStates))
	}
	venueState := repo.venueStates[venueID]
	if venueState.WalletBalance != 1200 || venueState.AvailableBalance != 1100 {
		t.Fatalf("venue wallet state = %+v, want exchange-authoritative wallet 1200/1100", venueState)
	}
	if len(repo.snapshotTimes) != 1 {
		t.Fatalf("snapshots written = %d, want 1", len(repo.snapshotTimes))
	}
	runs := waitForCapturedReconciliationRuns(t, repo, 1)
	run := runs[0]
	if run.SessionID != sessionID || run.RunType != domain.ReconciliationRunEvent {
		t.Fatalf("reconciliation run = %+v, want event run for session", run)
	}
	if run.LocalSnapshot.WalletBalance != 900 || run.ExchangeSnapshot.WalletBalance != 1200 {
		t.Fatalf("local/exchange wallet_balance = %v/%v, want 900/1200",
			run.LocalSnapshot.WalletBalance, run.ExchangeSnapshot.WalletBalance)
	}
	if len(run.VenueDiffs) != 1 {
		t.Fatalf("venue_diffs len = %d, want 1", len(run.VenueDiffs))
	}
	venueDiff := run.VenueDiffs[0]
	if venueDiff.VenueID != venueID {
		t.Fatalf("venue_diff venue_id = %d, want %d", venueDiff.VenueID, venueID)
	}
	if venueDiff.LocalSnapshot.WalletBalance != 900 || venueDiff.ExchangeSnapshot.WalletBalance != 1200 {
		t.Fatalf("venue local/exchange wallet_balance = %v/%v, want 900/1200",
			venueDiff.LocalSnapshot.WalletBalance, venueDiff.ExchangeSnapshot.WalletBalance)
	}
}

func TestUpdateAccountWalletStatePersistsBacktestVenueAndSnapshot(t *testing.T) {
	accountID := int64(15)
	venueID := int64(88)
	repo := newSessionStubRepo()
	repo.account = domain.Account{
		AccountID:   accountID,
		UserID:      serviceTestUserID,
		Environment: domain.EnvironmentBacktest,
	}
	repo.venues = []domain.Venue{{
		VenueID:      venueID,
		UserID:       serviceTestUserID,
		AccountID:    &accountID,
		Exchange:     domain.ExchangeBinance,
		Environment:  domain.EnvironmentBacktest,
		Market:       domain.MarketPerpetualFutures,
		Status:       domain.VenueStatusActive,
		MarginMode:   domain.MarginModeCross,
		PositionMode: domain.PositionModeOneWay,
	}}
	repo.sessions["sess-wallet-sync"] = domain.StrategySession{
		SessionID:  "sess-wallet-sync",
		AccountID:  accountID,
		UserID:     serviceTestUserID,
		StrategyID: 43,
		Status:     "running",
	}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	resp, err := svc.UpdateAccountWalletState(context.Background(), &accountv1.UpdateAccountWalletStateRequest{
		AccountId:        accountID,
		UserId:           serviceTestUserID,
		TotalValue:       1234.5,
		WalletBalance:    1200,
		AvailableBalance: 1100,
		SnapshotReason:   int32(domain.SnapshotReasonOrderFill),
		StrategyId:       43,
		SessionId:        "sess-wallet-sync",
		Futures: &accountv1.FuturesWallet{
			MarginMode:       "cross",
			PositionMode:     "one_way",
			WalletBalance:    1200,
			AvailableBalance: 1100,
			MarginBalance:    1234.5,
			Positions: []*accountv1.FuturesPosition{{
				Symbol:       "ZECUSDT",
				PositionSide: "BOTH",
				PositionQty:  0.1,
				Qty:          0.1,
				EntryPrice:   582.85,
				MarkPrice:    590,
				MarginMode:   "cross",
				MarginType:   "cross",
			}},
		},
	})
	if err != nil {
		t.Fatalf("UpdateAccountWalletState() error = %v", err)
	}
	if resp.GetWallet().GetTotalValue() != 1234.5 {
		t.Fatalf("wallet total_value = %v, want 1234.5", resp.GetWallet().GetTotalValue())
	}
	venueState := repo.venueStates[venueID]
	if venueState.TotalValue != 1234.5 ||
		venueState.WalletBalance != 1200 ||
		venueState.Futures.Positions[0].Symbol != "ZECUSDT" {
		t.Fatalf("venue wallet state = %+v, want pushed futures wallet", venueState)
	}
	if repo.state.TotalValue != 1234.5 {
		t.Fatalf("account state = %+v, want pushed wallet aggregate", repo.state)
	}
	if len(repo.snapshotTimes) != 1 {
		t.Fatalf("snapshots written = %d, want 1", len(repo.snapshotTimes))
	}
}
