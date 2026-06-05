package tests

import (
	"context"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/gen/accountv1"
	"github.com/hushine-tech/core-service/internal/catalog"
	"github.com/hushine-tech/core-service/internal/credential"
	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/exchange"
	"github.com/hushine-tech/core-service/internal/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const testsUserID int64 = 1

func testCatalog() *catalog.Catalog {
	return catalog.NewWithFetchers(time.Hour,
		func(ctx context.Context) ([]string, error) { return []string{"BTCUSDT", "ETHUSDT"}, nil },
		func(ctx context.Context) ([]string, error) { return []string{"BTCUSDT", "ETHUSDT"}, nil },
	)
}

// setupService creates a service with one pre-seeded backtest account (ID=1, balance=10000).
func setupService(t *testing.T) (*service.AccountGRPCService, *mockRepo, int64) {
	t.Helper()
	repo := newMockRepo()
	ctx := context.Background()

	id, err := repo.CreateAccount(ctx, domain.Account{
		UserID:      testsUserID,
		Name:        "test-backtest",
		Environment: domain.EnvironmentBacktest,
		CreatedAt:   time.Now(),
	})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}

	venue, err := repo.CreateVenue(ctx, domain.Venue{
		UserID:       testsUserID,
		AccountID:    &id,
		Exchange:     domain.ExchangeBinance,
		Market:       domain.MarketPerpetualFutures,
		Environment:  domain.EnvironmentBacktest,
		Status:       domain.VenueStatusActive,
		APIKey:       "sim_btv_00000000000000000000000000000000",
		MarginMode:   domain.MarginModeCross,
		PositionMode: domain.PositionModeOneWay,
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("create venue: %v", err)
	}

	info := domain.OnlineAccountInfo{
		AccountID:   id,
		Environment: domain.EnvironmentBacktest,
		Futures: domain.FuturesWallet{
			MarginMode:         "cross",
			PositionMode:       "one_way",
			InitialBalance:     10000,
			WalletBalance:      10000,
			AvailableBalance:   10000,
			TotalMarginBalance: 10000,
			MarginBalance:      10000,
		},
		WalletBalance:    10000,
		AvailableBalance: 10000,
		TotalValue:       10000,
		UpdatedAt:        time.Now(),
	}
	_ = repo.UpsertVenueWalletState(ctx, venue, info)
	_ = repo.UpdateAccountState(ctx, info)

	router := exchange.NewAdapterRouter(nil, repo.GetAccountState)
	return service.NewAccountGRPCService(repo, router, testCatalog(), nil), repo, id
}

func seedBinancePerpVenue(t *testing.T, repo *mockRepo, accountID int64, env domain.Environment, apiKey, apiSecret string) *credential.Manager {
	t.Helper()
	credManager, err := credential.NewManager("0123456789abcdef0123456789abcdef", "v1")
	if err != nil {
		t.Fatalf("credential manager: %v", err)
	}
	encryptedCredential, err := credManager.Encrypt(`{"api_key":"` + apiKey + `","api_secret":"` + apiSecret + `"}`)
	if err != nil {
		t.Fatalf("encrypt venue credential: %v", err)
	}
	_, err = repo.CreateVenue(context.Background(), domain.Venue{
		UserID:         testsUserID,
		AccountID:      &accountID,
		Exchange:       domain.ExchangeBinance,
		Market:         domain.MarketPerpetualFutures,
		Environment:    env,
		Status:         domain.VenueStatusActive,
		APIKey:         apiKey,
		CredentialInfo: encryptedCredential,
		MarginMode:     domain.MarginModeCross,
		PositionMode:   domain.PositionModeOneWay,
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("create venue: %v", err)
	}
	return credManager
}

func TestGetPortfolioSnapshot_Backtest(t *testing.T) {
	svc, _, id := setupService(t)
	resp, err := svc.GetPortfolioSnapshot(context.Background(), &accountv1.GetPortfolioSnapshotRequest{
		AccountId: id,
		UserId:    testsUserID,
	})
	if err != nil {
		t.Fatalf("GetPortfolioSnapshot: %v", err)
	}
	w := resp.GetSnapshot().GetWallet()
	if w == nil {
		t.Fatal("expected wallet")
	}
	if w.GetFutures().GetWalletBalance() != 10000 {
		t.Fatalf("unexpected futures.wallet_balance: %f", w.GetFutures().GetWalletBalance())
	}
	if w.GetEnvironment() != int32(domain.EnvironmentBacktest) {
		t.Fatalf("expected environment=0 (backtest), got %d", w.GetEnvironment())
	}
}

func TestUpdatePortfolioSnapshot_TestnetFetchesExchangeAndRefreshesStoredState(t *testing.T) {
	repo := newMockRepo()
	ctx := context.Background()
	accountID, err := repo.CreateAccount(ctx, domain.Account{
		UserID:      testsUserID,
		Name:        "testnet",
		Environment: domain.EnvironmentDemo,
		CreatedAt:   time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	credManager := seedBinancePerpVenue(t, repo, accountID, domain.EnvironmentDemo, "test-key", "test-secret")
	_ = repo.UpdateAccountState(ctx, domain.OnlineAccountInfo{
		AccountID:   accountID,
		Environment: domain.EnvironmentDemo,
		Futures: domain.FuturesWallet{
			MarginMode:       "cross",
			PositionMode:     "one_way",
			WalletBalance:    100,
			MarginBalance:    100,
			AvailableBalance: 100,
		},
		WalletBalance:    100,
		AvailableBalance: 100,
		TotalValue:       100,
		UpdatedAt:        time.Now().Add(-time.Hour),
	})

	exchangeInfo := domain.OnlineAccountInfo{
		Futures: domain.FuturesWallet{
			MarginMode:         "cross",
			PositionMode:       "one_way",
			WalletBalance:      4321,
			MarginBalance:      4310,
			AvailableBalance:   4200,
			TotalMarginBalance: 4310,
		},
		WalletBalance:    4321,
		AvailableBalance: 4200,
		TotalValue:       4321,
		UpdatedAt:        time.Now().UTC(),
	}
	reader := &testPortfolioSnapshotReader{info: exchangeInfo}
	router := exchange.NewAdapterRouter(nil, func(_ context.Context, _ int64) (domain.OnlineAccountInfo, error) {
		t.Fatal("testnet portfolio snapshot must not read local wallet state")
		return domain.OnlineAccountInfo{}, nil
	})
	registry := newBinancePerpSnapshotRegistry(reader, domain.EnvironmentDemo)
	svc := service.NewAccountGRPCService(repo, router, testCatalog(), nil, service.WithCredentialManager(credManager), service.WithExchangeRegistry(registry))

	resp, err := svc.UpdatePortfolioSnapshot(ctx, &accountv1.UpdatePortfolioSnapshotRequest{
		AccountId: accountID,
		UserId:    testsUserID,
	})
	if err != nil {
		t.Fatalf("UpdatePortfolioSnapshot: %v", err)
	}
	wallet := resp.GetSnapshot().GetWallet()
	if wallet.GetEnvironment() != int32(domain.EnvironmentDemo) {
		t.Fatalf("expected environment=1, got %d", wallet.GetEnvironment())
	}
	if got := wallet.GetFutures().GetWalletBalance(); got != 4321 {
		t.Fatalf("expected exchange wallet balance, got %f", got)
	}
	if reader.seen.Credential.Metadata["api_key"] != "test-key" || reader.seen.Credential.Metadata["api_secret"] != "test-secret" {
		t.Fatalf("exchange snapshot did not receive venue credentials: %+v", reader.seen.Credential.Metadata)
	}

	stored, err := repo.GetAccountState(ctx, accountID)
	if err != nil {
		t.Fatalf("GetAccountState: %v", err)
	}
	if stored.WalletBalance != 4321 || stored.AvailableBalance != 4200 {
		t.Fatalf("stored state was not refreshed from exchange: %+v", stored)
	}
}

func TestCreateAccount_BacktestCreatesDefaultVenueAndVenueWalletState(t *testing.T) {
	repo := newMockRepo()
	router := exchange.NewAdapterRouter(nil, repo.GetAccountState)
	svc := service.NewAccountGRPCService(repo, router, testCatalog(), nil)

	resp, err := svc.CreateAccount(context.Background(), &accountv1.CreateAccountRequest{
		Name:        "backtest",
		Environment: int32(domain.EnvironmentBacktest),
		UserId:      testsUserID,
	})
	if err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}

	if len(repo.snapshots) != 0 {
		t.Fatalf("unexpected snapshot log: %+v", repo.snapshots)
	}
	if len(repo.venues) != 1 {
		t.Fatalf("venues len = %d, want 1", len(repo.venues))
	}
	for _, venue := range repo.venues {
		if venue.AccountID == nil || *venue.AccountID != resp.GetAccountId() {
			t.Fatalf("venue account_id = %v, want %d", venue.AccountID, resp.GetAccountId())
		}
		if venue.Environment != domain.EnvironmentBacktest || venue.Exchange != domain.ExchangeBinance || venue.Market != domain.MarketPerpetualFutures {
			t.Fatalf("venue route = env:%v exchange:%v market:%v", venue.Environment, venue.Exchange, venue.Market)
		}
		state, err := repo.GetVenueWalletState(context.Background(), venue.VenueID, testsUserID)
		if err != nil {
			t.Fatalf("GetVenueWalletState: %v", err)
		}
		if state.AccountID != resp.GetAccountId() {
			t.Fatalf("state account_id = %d, want %d", state.AccountID, resp.GetAccountId())
		}
		if state.Futures.MarginMode != "cross" || state.Futures.PositionMode != "one_way" {
			t.Fatalf("state futures modes = %q/%q, want cross/one_way", state.Futures.MarginMode, state.Futures.PositionMode)
		}
	}
}

func TestGetPortfolioSnapshot_ZeroAccountID(t *testing.T) {
	svc, _, _ := setupService(t)
	_, err := svc.GetPortfolioSnapshot(context.Background(), &accountv1.GetPortfolioSnapshotRequest{
		AccountId: 0,
		UserId:    testsUserID,
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %s", status.Code(err))
	}
}

func TestGetPortfolioSnapshot_NotFound(t *testing.T) {
	svc, _, _ := setupService(t)
	_, err := svc.GetPortfolioSnapshot(context.Background(), &accountv1.GetPortfolioSnapshotRequest{
		AccountId: 9999,
		UserId:    testsUserID,
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound, got %s", status.Code(err))
	}
}

func TestUpdatePortfolioSnapshot_Live_NoAdapter(t *testing.T) {
	repo := newMockRepo()
	ctx := context.Background()
	id, err := repo.CreateAccount(ctx, domain.Account{
		UserID:      testsUserID,
		Name:        "live",
		Environment: domain.EnvironmentLive,
		CreatedAt:   time.Now(),
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	credManager := seedBinancePerpVenue(t, repo, id, domain.EnvironmentLive, "live-key", "live-secret")
	router := exchange.NewAdapterRouter(nil, repo.GetAccountState)
	svc := service.NewAccountGRPCService(repo, router, testCatalog(), nil, service.WithCredentialManager(credManager))

	_, err = svc.UpdatePortfolioSnapshot(ctx, &accountv1.UpdatePortfolioSnapshotRequest{AccountId: id, UserId: testsUserID})
	if err == nil {
		t.Fatal("expected error")
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %s", status.Code(err))
	}
}

// TestGetPortfolioSnapshot_DoesNotWriteSnapshot verifies that reads do not
// append snapshot rows.
func TestGetPortfolioSnapshot_DoesNotWriteSnapshot(t *testing.T) {
	svc, repo, id := setupService(t)
	ctx := context.Background()

	before := len(repo.snapshots)
	_, err := svc.GetPortfolioSnapshot(ctx, &accountv1.GetPortfolioSnapshotRequest{AccountId: id, UserId: testsUserID})
	if err != nil {
		t.Fatalf("GetPortfolioSnapshot: %v", err)
	}
	if len(repo.snapshots) != before {
		t.Fatalf("expected no new snapshot, got %d new", len(repo.snapshots)-before)
	}
}

// TestUpdatePortfolioSnapshot_NoSnapshotReason verifies that update without
// snapshot_reason writes no snapshot.
func TestUpdatePortfolioSnapshot_NoSnapshotReason(t *testing.T) {
	svc, repo, id := setupService(t)
	ctx := context.Background()
	before := len(repo.snapshots)

	_, err := svc.UpdatePortfolioSnapshot(ctx, &accountv1.UpdatePortfolioSnapshotRequest{
		AccountId:      id,
		UserId:         testsUserID,
		SnapshotReason: 0, // no snapshot
	})
	if err != nil {
		t.Fatalf("UpdatePortfolioSnapshot: %v", err)
	}
	if len(repo.snapshots) != before {
		t.Fatalf("expected no snapshot, got %d new", len(repo.snapshots)-before)
	}
}

// TestUpdatePortfolioSnapshot_WithSnapshotReason verifies that snapshot_reason>0 writes a snapshot.
func TestUpdatePortfolioSnapshot_WithSnapshotReason(t *testing.T) {
	svc, repo, id := setupService(t)
	ctx := context.Background()
	before := len(repo.snapshots)

	_, err := svc.UpdatePortfolioSnapshot(ctx, &accountv1.UpdatePortfolioSnapshotRequest{
		AccountId:      id,
		UserId:         testsUserID,
		SnapshotReason: 1, // order_fill
	})
	if err != nil {
		t.Fatalf("UpdatePortfolioSnapshot: %v", err)
	}
	if len(repo.snapshots) != before+1 {
		t.Fatalf("expected 1 new snapshot, got %d", len(repo.snapshots)-before)
	}
	if repo.snapshots[before] != domain.SnapshotReasonOrderFill {
		t.Fatalf("expected reason OrderFill, got %d", repo.snapshots[before])
	}
}

func TestUpdatePortfolioSnapshot_ZeroAccountID(t *testing.T) {
	svc, _, _ := setupService(t)
	_, err := svc.UpdatePortfolioSnapshot(context.Background(), &accountv1.UpdatePortfolioSnapshotRequest{
		AccountId: 0,
		UserId:    testsUserID,
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %s", status.Code(err))
	}
}

func TestListSymbols(t *testing.T) {
	svc, _, _ := setupService(t)
	ctx := context.Background()
	resp, err := svc.ListSymbols(ctx, &accountv1.ListSymbolsRequest{Market: "spot", Query: "BTC", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetSymbols()) != 1 || resp.GetSymbols()[0] != "BTCUSDT" {
		t.Fatalf("symbols: %v", resp.GetSymbols())
	}
	if resp.GetStale() {
		t.Fatal("unexpected stale")
	}
}
