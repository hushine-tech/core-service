package tests

import (
	"context"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/gen/accountv1"
	"github.com/hushine-tech/core-service/internal/config"
	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/exchange"
	"github.com/hushine-tech/core-service/internal/reconciliation"
	"github.com/hushine-tech/core-service/internal/service"
)

// These tests verify the wiring contract between the gRPC handler and the
// reconciliation goroutine: demo environment triggers a compare run; backtest does NOT.
// The wiring is the only place where "UpdateAccountWalletState in demo
// launches a compare async" is actually asserted end-to-end. Unit tests in
// internal/reconciliation prove the service semantics in isolation; this
// file proves the handler actually calls into the service.

// waitForReconRuns polls the mockRepo's reconRuns list for `target` entries.
// Needed because LaunchAsync is fire-and-forget. Uses the race-safe
// accessor so the race detector stays quiet.
func waitForReconRuns(t *testing.T, repo *mockRepo, target int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if repo.reconRunsLen() >= target {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d reconciliation runs; got %d", target, repo.reconRunsLen())
}

// enabledReconConfig enables reconciliation for test paths and tightens
// the goroutine timeout so tests run fast.
func enabledReconConfig() config.ReconciliationConfig {
	c := config.DefaultReconciliationConfig()
	c.Enabled = true
	c.GoroutineTimeoutSeconds = 2
	return c
}

// TestUpdateAccountWalletState_Mode2_LaunchesReconciliation verifies the
// spec: "demo wallet sync SHALL produce reconciliation runs" end-to-end
// through the gRPC handler. Without this test, the wiring in grpc.go could
// silently break and no other test would catch it.
func TestUpdateAccountWalletState_Mode2_LaunchesReconciliation(t *testing.T) {
	ctx := context.Background()
	repo := newMockRepo()

	// Create a demo account.
	accountID, err := repo.CreateAccount(ctx, domain.Account{
		UserID:      testsUserID,
		Name:        "testnet-acc",
		Environment: domain.EnvironmentDemo,
		CreatedAt:   time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	credManager := seedBinancePerpVenue(t, repo, accountID, domain.EnvironmentDemo, "test-key", "test-secret")

	// Seed an authoritative snapshot the fake registry-backed exchange will return.
	reader := &testPortfolioSnapshotReader{
		info: domain.OnlineAccountInfo{
			Environment: domain.EnvironmentDemo,
			Futures: domain.FuturesWallet{
				WalletBalance:      10000,
				AvailableBalance:   9500,
				MarginBalance:      10050,
				TotalMarginBalance: 10050,
			},
			TotalValue:       10050,
			WalletBalance:    10000,
			AvailableBalance: 9500,
		},
	}
	router := exchange.NewAdapterRouter(nil, repo.GetAccountState)
	registry := newBinancePerpSnapshotRegistry(reader, domain.EnvironmentDemo)
	reconciler := reconciliation.NewService(enabledReconConfig(), repo)
	svc := service.NewAccountGRPCService(repo, router, testCatalog(), reconciler, service.WithCredentialManager(credManager), service.WithExchangeRegistry(registry))

	// Call UpdateAccountWalletState with a slightly-different local snapshot.
	resp, err := svc.UpdateAccountWalletState(ctx, &accountv1.UpdateAccountWalletStateRequest{
		AccountId:      accountID,
		SnapshotReason: int32(domain.SnapshotReasonOrderFill),
		Futures: &accountv1.FuturesWallet{
			WalletBalance:      10000,
			AvailableBalance:   9499, // 1 USDT drift vs exchange — within threshold
			MarginBalance:      10050,
			TotalMarginBalance: 10050,
		},
	})
	if err != nil {
		t.Fatalf("UpdateAccountWalletState: %v", err)
	}
	if resp.GetWallet() == nil {
		t.Fatal("expected wallet in response")
	}
	if reader.seen.Credential.Metadata["api_key"] != "test-key" || reader.seen.Credential.Metadata["api_secret"] != "test-secret" {
		t.Fatalf("snapshot reader did not receive venue credentials: %+v", reader.seen.Credential.Metadata)
	}

	// Response is returned immediately — the compare runs in the background.
	waitForReconRuns(t, repo, 1, 500*time.Millisecond)

	runs := repo.reconRunsSnapshot()
	run := runs[0]
	if run.AccountID != accountID {
		t.Errorf("recon run account_id: got %d, want %d", run.AccountID, accountID)
	}
	if run.Environment != domain.EnvironmentDemo {
		t.Errorf("recon run environment: got %d, want demo", run.Environment)
	}
	if run.RunType != domain.ReconciliationRunEvent {
		t.Errorf("recon run type: got %q, want event (OrderFill)", run.RunType)
	}
	if !run.HardPass {
		t.Errorf("recon run hard_pass should be true (no structural diff); got false, diffs=%+v", run.FieldDiffs)
	}
}

// TestUpdateAccountWalletState_Mode0_DoesNotLaunchReconciliation locks in
// the invariant that backtest sessions NEVER produce reconciliation runs
// (spec: "Backtest environment ignores periodic trigger" + "backtest must NOT produce
// reconciliation runs").
func TestUpdateAccountWalletState_Mode0_DoesNotLaunchReconciliation(t *testing.T) {
	ctx := context.Background()
	repo := newMockRepo()

	accountID, err := repo.CreateAccount(ctx, domain.Account{
		UserID:      testsUserID,
		Name:        "backtest-acc",
		Environment: domain.EnvironmentBacktest,
		CreatedAt:   time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}

	venue, err := repo.CreateVenue(ctx, domain.Venue{
		UserID:       testsUserID,
		AccountID:    &accountID,
		Exchange:     domain.ExchangeBinance,
		Market:       domain.MarketPerpetualFutures,
		Environment:  domain.EnvironmentBacktest,
		Status:       domain.VenueStatusActive,
		APIKey:       "sim_btv_00000000000000000000000000000001",
		MarginMode:   domain.MarginModeCross,
		PositionMode: domain.PositionModeOneWay,
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("create venue: %v", err)
	}

	info := domain.OnlineAccountInfo{
		AccountID:   accountID,
		Environment: domain.EnvironmentBacktest,
		Futures: domain.FuturesWallet{
			WalletBalance: 10000,
		},
		WalletBalance: 10000,
		UpdatedAt:     time.Now().UTC(),
	}
	_ = repo.UpsertVenueWalletState(ctx, venue, info)
	_ = repo.UpdateAccountState(ctx, info)

	router := exchange.NewAdapterRouter(nil, repo.GetAccountState)
	reconciler := reconciliation.NewService(enabledReconConfig(), repo)
	svc := service.NewAccountGRPCService(repo, router, testCatalog(), reconciler)

	_, err = svc.UpdateAccountWalletState(ctx, &accountv1.UpdateAccountWalletStateRequest{
		AccountId:      accountID,
		SnapshotReason: int32(domain.SnapshotReasonOrderFill),
		Futures: &accountv1.FuturesWallet{
			WalletBalance: 10500,
		},
		TotalValue:       10500,
		WalletBalance:    10500,
		AvailableBalance: 10500,
	})
	if err != nil {
		t.Fatalf("UpdateAccountWalletState (backtest): %v", err)
	}

	// Give any stray goroutine a chance to run — there should be none.
	time.Sleep(100 * time.Millisecond)
	if got := repo.reconRunsLen(); got != 0 {
		t.Errorf("backtest environment must NOT produce reconciliation runs; got %d", got)
	}
}

// TestUpdateAccountWalletState_Mode2_DisabledReconcilerSkips verifies that
// when reconciliation.enabled=false, no compare runs are produced even on
// demo environment paths. This protects rollbacks: flipping the feature flag MUST
// cleanly stop all compare activity.
func TestUpdateAccountWalletState_Mode2_DisabledReconcilerSkips(t *testing.T) {
	ctx := context.Background()
	repo := newMockRepo()

	accountID, _ := repo.CreateAccount(ctx, domain.Account{
		UserID:      testsUserID,
		Name:        "testnet-acc",
		Environment: domain.EnvironmentDemo,
		CreatedAt:   time.Now().UTC(),
	})
	credManager := seedBinancePerpVenue(t, repo, accountID, domain.EnvironmentDemo, "test-key", "test-secret")

	reader := &testPortfolioSnapshotReader{
		info: domain.OnlineAccountInfo{
			Environment: domain.EnvironmentDemo,
			Futures: domain.FuturesWallet{
				WalletBalance: 10000,
			},
		},
	}
	router := exchange.NewAdapterRouter(nil, repo.GetAccountState)
	registry := newBinancePerpSnapshotRegistry(reader, domain.EnvironmentDemo)

	// Reconciler constructed but explicitly disabled.
	disabled := config.DefaultReconciliationConfig()
	disabled.Enabled = false
	reconciler := reconciliation.NewService(disabled, repo)
	svc := service.NewAccountGRPCService(repo, router, testCatalog(), reconciler, service.WithCredentialManager(credManager), service.WithExchangeRegistry(registry))

	_, err := svc.UpdateAccountWalletState(ctx, &accountv1.UpdateAccountWalletStateRequest{
		AccountId:      accountID,
		SnapshotReason: int32(domain.SnapshotReasonOrderFill),
		Futures: &accountv1.FuturesWallet{
			WalletBalance: 10000,
		},
	})
	if err != nil {
		t.Fatalf("UpdateAccountWalletState: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	if got := repo.reconRunsLen(); got != 0 {
		t.Errorf("disabled reconciler must not produce runs; got %d", got)
	}
}
