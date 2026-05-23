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
// reconciliation goroutine: mode=2 triggers a compare run; mode=0 does NOT.
// The wiring is the only place where "UpdateAccountWalletState in mode=2
// launches a compare async" is actually asserted end-to-end. Unit tests in
// internal/reconciliation prove the service semantics in isolation; this
// file proves the handler actually calls into the service.

// fakeOnlineInfoFetcher returns a deterministic authoritative snapshot so
// mode=2 UpdateAccountWalletState flows through successfully without hitting
// real Binance.
type fakeOnlineInfoFetcher struct {
	info domain.OnlineAccountInfo
}

func (f *fakeOnlineInfoFetcher) FetchOnlineAccountInfo(_ context.Context, account domain.Account) (domain.OnlineAccountInfo, error) {
	resp := f.info
	resp.AccountID = account.AccountID
	resp.Mode = account.Mode
	if resp.UpdatedAt.IsZero() {
		resp.UpdatedAt = time.Now().UTC()
	}
	return resp, nil
}

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
// spec: "Mode 2 wallet sync SHALL produce reconciliation runs" end-to-end
// through the gRPC handler. Without this test, the wiring in grpc.go could
// silently break and no other test would catch it.
func TestUpdateAccountWalletState_Mode2_LaunchesReconciliation(t *testing.T) {
	ctx := context.Background()
	repo := newMockRepo()

	// Create a mode=2 testnet account.
	accountID, err := repo.CreateAccount(ctx, domain.Account{
		UserID:    testsUserID,
		Name:      "testnet-acc",
		Mode:      domain.AccountModeBinanceTestnet,
		APIKey:    "test-key",
		APISecret: "test-secret",
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}

	// Seed an authoritative snapshot the fake exchange will return.
	fakeExchange := &fakeOnlineInfoFetcher{
		info: domain.OnlineAccountInfo{
			Mode: domain.AccountModeBinanceTestnet,
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
	router := exchange.NewAdapterRouter(
		map[exchange.ExchangeTarget]exchange.OnlineInfoFetcher{
			{Provider: exchange.ProviderBinance, Environment: exchange.EnvTestnet}: fakeExchange,
		},
		repo.GetAccountState,
	)
	reconciler := reconciliation.NewService(enabledReconConfig(), repo)
	svc := service.NewAccountGRPCService(repo, router, testCatalog(), reconciler)

	// Call UpdateAccountWalletState with a slightly-different local snapshot.
	resp, err := svc.UpdateAccountWalletState(ctx, &accountv1.UpdateAccountWalletStateRequest{
		AccountId:      accountID,
		SnapshotReason: int32(domain.SnapshotReasonOrderFill),
		Futures: &accountv1.FuturesWallet{
			WalletBalance:      10000,
			AvailableBalance:   9499,   // 1 USDT drift vs exchange — within threshold
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

	// Response is returned immediately — the compare runs in the background.
	waitForReconRuns(t, repo, 1, 500*time.Millisecond)

	runs := repo.reconRunsSnapshot()
	run := runs[0]
	if run.AccountID != accountID {
		t.Errorf("recon run account_id: got %d, want %d", run.AccountID, accountID)
	}
	if run.Mode != domain.AccountModeBinanceTestnet {
		t.Errorf("recon run mode: got %d, want testnet", run.Mode)
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
// (spec: "Backtest mode ignores periodic trigger" + "mode=0 must NOT produce
// reconciliation runs").
func TestUpdateAccountWalletState_Mode0_DoesNotLaunchReconciliation(t *testing.T) {
	ctx := context.Background()
	repo := newMockRepo()

	accountID, err := repo.CreateAccount(ctx, domain.Account{
		UserID:    testsUserID,
		Name:      "backtest-acc",
		Mode:      domain.AccountModeBacktest,
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}

	// Seed the current state — backtest reads from DB.
	_ = repo.UpdateAccountState(ctx, domain.OnlineAccountInfo{
		AccountID: accountID,
		Mode:      domain.AccountModeBacktest,
		Futures: domain.FuturesWallet{
			WalletBalance: 10000,
		},
		WalletBalance: 10000,
		UpdatedAt:     time.Now().UTC(),
	})

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
		t.Errorf("mode=0 must NOT produce reconciliation runs; got %d", got)
	}
}

// TestUpdateAccountWalletState_Mode2_DisabledReconcilerSkips verifies that
// when reconciliation.enabled=false, no compare runs are produced even on
// mode=2 paths. This protects rollbacks: flipping the feature flag MUST
// cleanly stop all compare activity.
func TestUpdateAccountWalletState_Mode2_DisabledReconcilerSkips(t *testing.T) {
	ctx := context.Background()
	repo := newMockRepo()

	accountID, _ := repo.CreateAccount(ctx, domain.Account{
		UserID:    testsUserID,
		Name:      "testnet-acc",
		Mode:      domain.AccountModeBinanceTestnet,
		APIKey:    "test-key",
		APISecret: "test-secret",
		CreatedAt: time.Now().UTC(),
	})

	fakeExchange := &fakeOnlineInfoFetcher{
		info: domain.OnlineAccountInfo{
			Mode: domain.AccountModeBinanceTestnet,
			Futures: domain.FuturesWallet{
				WalletBalance: 10000,
			},
		},
	}
	router := exchange.NewAdapterRouter(
		map[exchange.ExchangeTarget]exchange.OnlineInfoFetcher{
			{Provider: exchange.ProviderBinance, Environment: exchange.EnvTestnet}: fakeExchange,
		},
		repo.GetAccountState,
	)

	// Reconciler constructed but explicitly disabled.
	disabled := config.DefaultReconciliationConfig()
	disabled.Enabled = false
	reconciler := reconciliation.NewService(disabled, repo)
	svc := service.NewAccountGRPCService(repo, router, testCatalog(), reconciler)

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
