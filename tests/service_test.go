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
		UserID:    testsUserID,
		Name:      "test-backtest",
		Mode:      domain.AccountModeBacktest,
		CreatedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}

	_ = repo.UpdateAccountState(ctx, domain.OnlineAccountInfo{
		AccountID: id,
		Mode:      domain.AccountModeBacktest,
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
	})

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

func TestGetOnlineAccountInfo_Backtest(t *testing.T) {
	svc, _, id := setupService(t)
	resp, err := svc.GetOnlineAccountInfo(context.Background(), &accountv1.GetOnlineAccountInfoRequest{
		AccountId: id,
		UserId:    testsUserID,
	})
	if err != nil {
		t.Fatalf("GetOnlineAccountInfo: %v", err)
	}
	w := resp.GetWallet()
	if w == nil {
		t.Fatal("expected wallet")
	}
	if w.GetFutures().GetWalletBalance() != 10000 {
		t.Fatalf("unexpected futures.wallet_balance: %f", w.GetFutures().GetWalletBalance())
	}
	if w.GetMode() != 0 {
		t.Fatalf("expected mode=0 (backtest), got %d", w.GetMode())
	}
}

func TestGetOnlineAccountInfo_TestnetFetchesExchangeAndRefreshesStoredState(t *testing.T) {
	repo := newMockRepo()
	ctx := context.Background()
	accountID, err := repo.CreateAccount(ctx, domain.Account{
		UserID:      testsUserID,
		Name:        "testnet",
		Environment: domain.EnvironmentDemo,
		Mode:        domain.AccountModeBinanceTestnet,
		CreatedAt:   time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	credManager := seedBinancePerpVenue(t, repo, accountID, domain.EnvironmentDemo, "test-key", "test-secret")
	_ = repo.UpdateAccountState(ctx, domain.OnlineAccountInfo{
		AccountID: accountID,
		Mode:      domain.AccountModeBinanceTestnet,
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
	fetcher := &fakeOnlineInfoFetcher{info: exchangeInfo}
	router := exchange.NewAdapterRouter(
		map[exchange.ExchangeTarget]exchange.OnlineInfoFetcher{
			{Provider: exchange.ProviderBinance, Environment: exchange.EnvTestnet}: fetcher,
		},
		func(_ context.Context, _ int64) (domain.OnlineAccountInfo, error) {
			t.Fatal("testnet GetOnlineAccountInfo must not read local wallet state")
			return domain.OnlineAccountInfo{}, nil
		},
	)
	svc := service.NewAccountGRPCService(repo, router, testCatalog(), nil, service.WithCredentialManager(credManager))

	resp, err := svc.GetOnlineAccountInfo(ctx, &accountv1.GetOnlineAccountInfoRequest{
		AccountId: accountID,
		UserId:    testsUserID,
	})
	if err != nil {
		t.Fatalf("GetOnlineAccountInfo: %v", err)
	}
	wallet := resp.GetWallet()
	if wallet.GetMode() != int32(domain.AccountModeBinanceTestnet) {
		t.Fatalf("expected mode=2, got %d", wallet.GetMode())
	}
	if got := wallet.GetFutures().GetWalletBalance(); got != 4321 {
		t.Fatalf("expected exchange wallet balance, got %f", got)
	}
	if fetcher.seen.APIKey != "test-key" || fetcher.seen.APISecret != "test-secret" {
		t.Fatalf("exchange fetch did not receive venue credentials: api_key=%q api_secret=%q", fetcher.seen.APIKey, fetcher.seen.APISecret)
	}

	stored, err := repo.GetAccountState(ctx, accountID)
	if err != nil {
		t.Fatalf("GetAccountState: %v", err)
	}
	if stored.WalletBalance != 4321 || stored.AvailableBalance != 4200 {
		t.Fatalf("stored state was not refreshed from exchange: %+v", stored)
	}
}

func TestCreateAccount_BacktestSeedsSpotAndFuturesLedgers(t *testing.T) {
	repo := newMockRepo()
	router := exchange.NewAdapterRouter(nil, repo.GetAccountState)
	svc := service.NewAccountGRPCService(repo, router, testCatalog(), nil)

	resp, err := svc.CreateAccount(context.Background(), &accountv1.CreateAccountRequest{
		Name:           "seeded-backtest",
		Environment:    int32(domain.EnvironmentBacktest),
		InitialBalance: 5000,
		UserId:         testsUserID,
	})
	if err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}

	state, err := repo.GetAccountState(context.Background(), resp.GetAccountId())
	if err != nil {
		t.Fatalf("GetAccountState: %v", err)
	}
	if state.Futures.WalletBalance != 5000 {
		t.Fatalf("unexpected futures wallet seed: %f", state.Futures.WalletBalance)
	}
	if state.Spot.Free != 5000 {
		t.Fatalf("unexpected spot free seed: %f", state.Spot.Free)
	}
	if state.TotalValue != 10000 {
		t.Fatalf("unexpected total_value seed: %f", state.TotalValue)
	}
	if len(repo.snapshots) != 1 || repo.snapshots[0] != domain.SnapshotReasonInitialSeed {
		t.Fatalf("unexpected snapshot log: %+v", repo.snapshots)
	}
}

func TestGetOnlineAccountInfo_ZeroAccountID(t *testing.T) {
	svc, _, _ := setupService(t)
	_, err := svc.GetOnlineAccountInfo(context.Background(), &accountv1.GetOnlineAccountInfoRequest{
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

func TestGetOnlineAccountInfo_NotFound(t *testing.T) {
	svc, _, _ := setupService(t)
	_, err := svc.GetOnlineAccountInfo(context.Background(), &accountv1.GetOnlineAccountInfoRequest{
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

func TestGetOnlineAccountInfo_Live_NoAdapter(t *testing.T) {
	repo := newMockRepo()
	ctx := context.Background()
	id, err := repo.CreateAccount(ctx, domain.Account{
		UserID:      testsUserID,
		Name:        "live",
		Environment: domain.EnvironmentLive,
		Mode:        domain.AccountModeBinanceLive,
		CreatedAt:   time.Now(),
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	credManager := seedBinancePerpVenue(t, repo, id, domain.EnvironmentLive, "live-key", "live-secret")
	router := exchange.NewAdapterRouter(nil, repo.GetAccountState)
	svc := service.NewAccountGRPCService(repo, router, testCatalog(), nil, service.WithCredentialManager(credManager))

	_, err = svc.GetOnlineAccountInfo(ctx, &accountv1.GetOnlineAccountInfoRequest{AccountId: id, UserId: testsUserID})
	if err == nil {
		t.Fatal("expected error")
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %s", status.Code(err))
	}
}

// TestGetOnlineAccountInfo_DoesNotWriteSnapshot verifies that GetOnlineAccountInfo
// no longer appends snapshot rows.
func TestGetOnlineAccountInfo_DoesNotWriteSnapshot(t *testing.T) {
	svc, repo, id := setupService(t)
	ctx := context.Background()

	before := len(repo.snapshots)
	_, err := svc.GetOnlineAccountInfo(ctx, &accountv1.GetOnlineAccountInfoRequest{AccountId: id, UserId: testsUserID})
	if err != nil {
		t.Fatalf("GetOnlineAccountInfo: %v", err)
	}
	if len(repo.snapshots) != before {
		t.Fatalf("expected no new snapshot, got %d new", len(repo.snapshots)-before)
	}
}

func TestUpdateAccountWalletState_BacktestEcho(t *testing.T) {
	svc, repo, id := setupService(t)
	ctx := context.Background()

	resp, err := svc.UpdateAccountWalletState(ctx, &accountv1.UpdateAccountWalletStateRequest{
		AccountId:        id,
		WalletBalance:    12000,
		AvailableBalance: 11000,
		TotalValue:       12500,
		Futures: &accountv1.FuturesWallet{
			MarginMode:   "cross",
			PositionMode: "one_way",
			Positions: []*accountv1.FuturesPosition{
				{Symbol: "BTCUSDT", Direction: 0, InitialBalance: 1000, Leverage: 10, FeeRate: 0.0004},
			},
		},
		Spot: &accountv1.SpotWallet{Free: 500, Locked: 0},
	})
	if err != nil {
		t.Fatalf("UpdateAccountWalletState: %v", err)
	}
	w := resp.GetWallet()
	if w == nil {
		t.Fatal("expected wallet in response")
	}
	if w.GetFutures().GetWalletBalance() != 12000 || w.GetTotalValue() != 12500 {
		t.Fatalf("unexpected response aggregates: wb=%f tv=%f", w.GetFutures().GetWalletBalance(), w.GetTotalValue())
	}
	if w.GetFutures().GetMarginMode() != "cross" {
		t.Fatalf("expected cross margin in echoed futures")
	}

	state, err := repo.GetAccountState(ctx, id)
	if err != nil {
		t.Fatalf("GetAccountState: %v", err)
	}
	if state.WalletBalance != 12000 {
		t.Fatalf("expected 12000 in repo, got %f", state.WalletBalance)
	}
}

// TestUpdateAccountWalletState_NoSnapshotReason verifies that update without snapshot_reason
// writes no snapshot.
func TestUpdateAccountWalletState_NoSnapshotReason(t *testing.T) {
	svc, repo, id := setupService(t)
	ctx := context.Background()
	before := len(repo.snapshots)

	_, err := svc.UpdateAccountWalletState(ctx, &accountv1.UpdateAccountWalletStateRequest{
		AccountId:      id,
		WalletBalance:  11000,
		SnapshotReason: 0, // no snapshot
	})
	if err != nil {
		t.Fatalf("UpdateAccountWalletState: %v", err)
	}
	if len(repo.snapshots) != before {
		t.Fatalf("expected no snapshot, got %d new", len(repo.snapshots)-before)
	}
}

// TestUpdateAccountWalletState_WithSnapshotReason verifies that snapshot_reason>0 writes a snapshot.
func TestUpdateAccountWalletState_WithSnapshotReason(t *testing.T) {
	svc, repo, id := setupService(t)
	ctx := context.Background()
	before := len(repo.snapshots)

	_, err := svc.UpdateAccountWalletState(ctx, &accountv1.UpdateAccountWalletStateRequest{
		AccountId:      id,
		WalletBalance:  11000,
		SnapshotReason: 1, // order_fill
	})
	if err != nil {
		t.Fatalf("UpdateAccountWalletState: %v", err)
	}
	if len(repo.snapshots) != before+1 {
		t.Fatalf("expected 1 new snapshot, got %d", len(repo.snapshots)-before)
	}
	if repo.snapshots[before] != domain.SnapshotReasonOrderFill {
		t.Fatalf("expected reason OrderFill, got %d", repo.snapshots[before])
	}
}

func TestUpdateAccountWalletState_Live_NoAdapter(t *testing.T) {
	repo := newMockRepo()
	ctx := context.Background()
	id, err := repo.CreateAccount(ctx, domain.Account{
		UserID:      testsUserID,
		Name:        "live",
		Environment: domain.EnvironmentLive,
		Mode:        domain.AccountModeBinanceLive,
		CreatedAt:   time.Now(),
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	credManager := seedBinancePerpVenue(t, repo, id, domain.EnvironmentLive, "live-key", "live-secret")
	router := exchange.NewAdapterRouter(nil, repo.GetAccountState)
	svc := service.NewAccountGRPCService(repo, router, testCatalog(), nil, service.WithCredentialManager(credManager))

	_, err = svc.UpdateAccountWalletState(ctx, &accountv1.UpdateAccountWalletStateRequest{
		AccountId:     id,
		WalletBalance: 99999,
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %s", status.Code(err))
	}
}

func TestUpdateAccountWalletState_ZeroAccountID(t *testing.T) {
	svc, _, _ := setupService(t)
	_, err := svc.UpdateAccountWalletState(context.Background(), &accountv1.UpdateAccountWalletStateRequest{
		AccountId: 0,
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
