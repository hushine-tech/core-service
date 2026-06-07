package reconciliation

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/internal/config"
	"github.com/hushine-tech/core-service/internal/domain"
)

// fakeRepo captures SaveReconciliationRun calls for assertion. It also lets
// tests simulate slow / failing DB writes.
type fakeRepo struct {
	mu      sync.Mutex
	runs    []domain.ReconciliationRun
	wait    chan struct{} // if non-nil, goroutine blocks on this before returning
	saveErr error
	panicOn bool // panic instead of returning
}

func (f *fakeRepo) SaveReconciliationRun(_ context.Context, run domain.ReconciliationRun) error {
	if f.wait != nil {
		<-f.wait
	}
	if f.panicOn {
		panic("simulated DB panic")
	}
	f.mu.Lock()
	f.runs = append(f.runs, run)
	f.mu.Unlock()
	return f.saveErr
}

func (f *fakeRepo) runCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.runs)
}

func baseCfg() config.ReconciliationConfig {
	c := config.DefaultReconciliationConfig()
	c.Enabled = true
	c.GoroutineTimeoutSeconds = 2
	return c
}

func simpleAccount() domain.Account {
	return domain.Account{
		AccountID:   42,
		UserID:      7,
		Environment: domain.EnvironmentDemo,
	}
}

func identicalSnapshot() domain.OnlineAccountInfo {
	return domain.OnlineAccountInfo{
		AccountID:   42,
		Environment: domain.EnvironmentDemo,
		Futures: domain.FuturesWallet{
			WalletBalance: 10000,
		},
	}
}

// waitForRuns polls runCount() until it reaches target or hits timeout.
func waitForRuns(t *testing.T, f *fakeRepo, target int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if f.runCount() >= target {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d runs; got %d", target, f.runCount())
}

// ── tests ────────────────────────────────────────────────────────────────

func TestLaunchAsync_DisabledIsNoOp(t *testing.T) {
	cfg := baseCfg()
	cfg.Enabled = false
	repo := &fakeRepo{}
	s := NewService(cfg, repo)

	s.LaunchAsync(Task{
		Account:        simpleAccount(),
		Local:          identicalSnapshot(),
		Exchange:       identicalSnapshot(),
		SnapshotReason: domain.SnapshotReasonOrderFill,
		TriggerTime:    time.Now().UTC(),
	})
	// Give any rogue goroutine a chance to run; none should.
	time.Sleep(50 * time.Millisecond)
	if repo.runCount() != 0 {
		t.Errorf("disabled service must not persist runs; got %d", repo.runCount())
	}
}

func TestLaunchAsync_NilServiceIsNoOp(t *testing.T) {
	var s *Service
	// Must not panic.
	s.LaunchAsync(Task{SnapshotReason: domain.SnapshotReasonOrderFill})
}

func TestLaunchAsync_DoesNotBlockCaller(t *testing.T) {
	// Block the repo save in a goroutine; LaunchAsync must still return
	// immediately.
	gate := make(chan struct{})
	repo := &fakeRepo{wait: gate}
	s := NewService(baseCfg(), repo)

	start := time.Now()
	s.LaunchAsync(Task{
		Account:        simpleAccount(),
		Local:          identicalSnapshot(),
		Exchange:       identicalSnapshot(),
		SnapshotReason: domain.SnapshotReasonOrderFill,
		TriggerTime:    time.Now().UTC(),
	})
	elapsed := time.Since(start)
	if elapsed > 100*time.Millisecond {
		t.Errorf("LaunchAsync blocked caller for %v (must be fire-and-forget)", elapsed)
	}
	// Release the fake repo so the goroutine can finish without leaking.
	close(gate)
	waitForRuns(t, repo, 1, 500*time.Millisecond)
}

func TestLaunchAsync_PanicInRepoDoesNotCrash(t *testing.T) {
	repo := &fakeRepo{panicOn: true}
	s := NewService(baseCfg(), repo)

	// Capture every counter emission so we can assert the error counter
	// fired even though the panic kept the goroutine from completing its
	// normal DB persist path.
	type counterCall struct {
		name    string
		account int64
		runType string
		extra   map[string]any
	}
	var (
		mu       sync.Mutex
		captured []counterCall
	)
	CounterHook = func(name string, accountID, userID int64, runType string, extra map[string]any) {
		mu.Lock()
		defer mu.Unlock()
		captured = append(captured, counterCall{name: name, account: accountID, runType: runType, extra: extra})
	}
	defer func() { CounterHook = nil }()

	// Must not panic the test runner. The compare goroutine's defer
	// recover() is the only line standing between us and test failure.
	s.LaunchAsync(Task{
		Account:        simpleAccount(),
		Local:          identicalSnapshot(),
		Exchange:       identicalSnapshot(),
		SnapshotReason: domain.SnapshotReasonOrderFill,
		TriggerTime:    time.Now().UTC(),
	})
	// Give the panicking goroutine time to panic and be caught.
	time.Sleep(100 * time.Millisecond)

	// Expected sequence: runs_total (every run) + error_total (from recover).
	mu.Lock()
	defer mu.Unlock()
	if len(captured) < 2 {
		t.Fatalf("expected at least runs_total + error_total counters; got %d: %+v", len(captured), captured)
	}
	sawRuns := false
	sawErrorPanic := false
	for _, c := range captured {
		if c.name == MetricRunsTotal {
			sawRuns = true
		}
		if c.name == MetricErrorTotal {
			if cause, _ := c.extra["cause"].(string); cause == "panic" {
				sawErrorPanic = true
			}
		}
	}
	if !sawRuns {
		t.Error("expected reconciliation_runs_total counter emission")
	}
	if !sawErrorPanic {
		t.Errorf("expected reconciliation_error_total with extra.cause=panic; got %+v", captured)
	}
}

func TestLaunchAsync_DBErrorDoesNotPropagate(t *testing.T) {
	repo := &fakeRepo{saveErr: errors.New("db unavailable")}
	s := NewService(baseCfg(), repo)

	s.LaunchAsync(Task{
		Account:        simpleAccount(),
		Local:          identicalSnapshot(),
		Exchange:       identicalSnapshot(),
		SnapshotReason: domain.SnapshotReasonOrderFill,
		TriggerTime:    time.Now().UTC(),
	})
	// Goroutine should complete (with error logged internally). Test passes
	// if no panic happens.
	time.Sleep(100 * time.Millisecond)
}

func TestLaunchAsync_BacktestEnvironmentIsSkipped(t *testing.T) {
	repo := &fakeRepo{}
	s := NewService(baseCfg(), repo)

	acc := simpleAccount()
	acc.Environment = domain.EnvironmentBacktest

	s.LaunchAsync(Task{
		Account:        acc,
		Local:          identicalSnapshot(),
		Exchange:       identicalSnapshot(),
		SnapshotReason: domain.SnapshotReasonOrderFill,
		TriggerTime:    time.Now().UTC(),
	})
	time.Sleep(100 * time.Millisecond)
	if repo.runCount() != 0 {
		t.Errorf("backtest environment must NOT produce reconciliation runs; got %d", repo.runCount())
	}
}

func TestLaunchAsync_InitialSeedReasonIsSkipped(t *testing.T) {
	// InitialSeed maps to empty RunType in RunTypeFromReason → should not run.
	repo := &fakeRepo{}
	s := NewService(baseCfg(), repo)

	s.LaunchAsync(Task{
		Account:        simpleAccount(),
		Local:          identicalSnapshot(),
		Exchange:       identicalSnapshot(),
		SnapshotReason: domain.SnapshotReasonInitialSeed,
		TriggerTime:    time.Now().UTC(),
	})
	time.Sleep(100 * time.Millisecond)
	if repo.runCount() != 0 {
		t.Errorf("InitialSeed reason must NOT produce runs; got %d", repo.runCount())
	}
}

func TestLaunchAsync_HappyPathPersistsRun(t *testing.T) {
	repo := &fakeRepo{}
	s := NewService(baseCfg(), repo)

	triggerAt := time.Now().UTC()
	s.LaunchAsync(Task{
		Account:        simpleAccount(),
		Local:          identicalSnapshot(),
		Exchange:       identicalSnapshot(),
		SessionID:      "sess-1",
		StrategyID:     11,
		SnapshotReason: domain.SnapshotReasonOrderFill,
		TriggerTime:    triggerAt,
	})
	waitForRuns(t, repo, 1, 500*time.Millisecond)

	repo.mu.Lock()
	run := repo.runs[0]
	repo.mu.Unlock()

	if run.AccountID != 42 || run.UserID != 7 {
		t.Errorf("account/user id mismatch: %+v", run)
	}
	if run.SessionID != "sess-1" || run.StrategyID != 11 {
		t.Errorf("session/strategy id mismatch: %+v", run)
	}
	if run.RunType != domain.ReconciliationRunEvent {
		t.Errorf("run_type for OrderFill: got %q, want event", run.RunType)
	}
	if !run.HardPass || !run.SoftPass {
		t.Errorf("identical snapshots should produce pass; hard=%v soft=%v", run.HardPass, run.SoftPass)
	}
	if !run.Time.Equal(triggerAt) {
		t.Errorf("trigger time not preserved: got %v, want %v", run.Time, triggerAt)
	}
	// Snapshots captured by value — mutating the originals after LaunchAsync
	// returns must not affect what was persisted.
	if run.ExchangeSnapshot.Futures.WalletBalance != 10000 {
		t.Errorf("exchange snapshot not captured correctly: %+v", run.ExchangeSnapshot)
	}
}

func TestLaunchAsync_PersistsVenueDiffsAndOverallPassIncludesVenueFailures(t *testing.T) {
	repo := &fakeRepo{}
	s := NewService(baseCfg(), repo)

	localAccount := identicalSnapshot()
	exchangeAccount := identicalSnapshot()
	localVenue := domain.VenueWalletSnapshot{
		VenueID:     88,
		Exchange:    domain.ExchangeBinance,
		Environment: domain.EnvironmentDemo,
		Market:      domain.MarketPerpetualFutures,
		Snapshot: domain.OnlineAccountInfo{
			AccountID:   42,
			Environment: domain.EnvironmentDemo,
			Futures: domain.FuturesWallet{
				WalletBalance: 900,
			},
		},
	}
	exchangeVenue := localVenue
	exchangeVenue.Snapshot.Futures.WalletBalance = 1000

	s.LaunchAsync(Task{
		Account:        simpleAccount(),
		Local:          localAccount,
		Exchange:       exchangeAccount,
		LocalVenues:    []domain.VenueWalletSnapshot{localVenue},
		ExchangeVenues: []domain.VenueWalletSnapshot{exchangeVenue},
		SessionID:      "sess-venue",
		StrategyID:     11,
		SnapshotReason: domain.SnapshotReasonOrderFill,
		TriggerTime:    time.Now().UTC(),
	})
	waitForRuns(t, repo, 1, 500*time.Millisecond)

	repo.mu.Lock()
	run := repo.runs[0]
	repo.mu.Unlock()

	if run.SoftPass {
		t.Fatalf("overall soft_pass must include venue-level soft failures: %+v", run)
	}
	if len(run.VenueDiffs) != 1 {
		t.Fatalf("venue_diffs len = %d, want 1", len(run.VenueDiffs))
	}
	venueDiff := run.VenueDiffs[0]
	if venueDiff.VenueID != 88 || venueDiff.Market != domain.MarketPerpetualFutures {
		t.Fatalf("venue diff metadata = %+v, want venue 88 perpetual futures", venueDiff)
	}
	if venueDiff.SoftPass {
		t.Fatalf("venue soft_pass = true, want false because wallet_balance differs")
	}
	if len(venueDiff.FieldDiffs) == 0 || venueDiff.FieldDiffs[0].Field != "futures.wallet_balance" {
		t.Fatalf("venue field diffs = %+v, want futures.wallet_balance", venueDiff.FieldDiffs)
	}
	if len(run.FieldDiffs) != 0 {
		t.Fatalf("account field diffs len = %d, want 0 because reconciliation is venue-only", len(run.FieldDiffs))
	}
	if len(run.AdvisoryDiffs) != 0 {
		t.Fatalf("account advisory diffs len = %d, want 0 because reconciliation is venue-only", len(run.AdvisoryDiffs))
	}
}

func TestLaunchAsync_AccountAggregateDiffDoesNotAffectVenueOnlyPass(t *testing.T) {
	repo := &fakeRepo{}
	s := NewService(baseCfg(), repo)

	localAccount := identicalSnapshot()
	localAccount.Futures.WalletBalance = 900
	exchangeAccount := identicalSnapshot()
	exchangeAccount.Futures.WalletBalance = 1000
	venue := domain.VenueWalletSnapshot{
		VenueID:     88,
		Exchange:    domain.ExchangeBinance,
		Environment: domain.EnvironmentDemo,
		Market:      domain.MarketPerpetualFutures,
		Snapshot:    identicalSnapshot(),
	}

	s.LaunchAsync(Task{
		Account:        simpleAccount(),
		Local:          localAccount,
		Exchange:       exchangeAccount,
		LocalVenues:    []domain.VenueWalletSnapshot{venue},
		ExchangeVenues: []domain.VenueWalletSnapshot{venue},
		SessionID:      "sess-account-aggregate",
		StrategyID:     11,
		SnapshotReason: domain.SnapshotReasonOrderFill,
		TriggerTime:    time.Now().UTC(),
	})
	waitForRuns(t, repo, 1, 500*time.Millisecond)

	repo.mu.Lock()
	run := repo.runs[0]
	repo.mu.Unlock()

	if !run.HardPass || !run.SoftPass {
		t.Fatalf("venue-only reconciliation should ignore account aggregate drift: %+v", run)
	}
	if len(run.FieldDiffs) != 0 || len(run.AdvisoryDiffs) != 0 {
		t.Fatalf("account diffs = %d/%d, want both empty", len(run.FieldDiffs), len(run.AdvisoryDiffs))
	}
	if len(run.VenueDiffs) != 1 {
		t.Fatalf("venue diffs len = %d, want 1", len(run.VenueDiffs))
	}
}
