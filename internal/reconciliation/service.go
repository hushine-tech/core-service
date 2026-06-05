package reconciliation

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/hushine-tech/core-service/internal/config"
	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/logger"
)

// SaveRunRepo is the narrow dependency injection surface needed by the
// reconciliation service. In production this is wired to
// repository.TimescaleRepository; in tests an in-memory fake can implement it.
type SaveRunRepo interface {
	SaveReconciliationRun(ctx context.Context, run domain.ReconciliationRun) error
}

// Task is the compare payload the gRPC handler hands to LaunchAsync.
//
// Ownership contract: once LaunchAsync is called, the caller MUST NOT mutate
// Task fields (including slices inside the embedded OnlineAccountInfo such as
// Futures.Positions / RiskMetadata / Spot.Assets). The goroutine reads them
// asynchronously. Struct fields are copied by value, but slice headers share
// their backing arrays — current callers hand us freshly-built snapshots and
// never touch them again, which satisfies the contract.
type Task struct {
	Account        domain.Account
	Local          domain.OnlineAccountInfo // strategy-computed canonical state
	Exchange       domain.OnlineAccountInfo // exchange authoritative, already fetched by main flow
	SessionID      string
	StrategyID     int64
	SnapshotReason domain.SnapshotReason
	TriggerTime    time.Time
}

// Service is the Phase C reconciliation coordinator. It runs compare work
// in detached goroutines so request paths are never blocked.
//
// Hard non-negotiables (enforced in runIsolated):
//  1. never panic out — defer recover()
//  2. never return error to the caller — LaunchAsync returns nothing
//  3. never mutate anything but the reconciliation_runs table and ELK metrics
//  4. never re-fetch Binance — the Task already carries the authoritative snapshot
type Service struct {
	cfg        config.ReconciliationConfig
	thresholds Thresholds
	repo       SaveRunRepo
}

// NewService builds a reconciliation service. When cfg.Enabled is false
// LaunchAsync becomes a no-op, so wiring into the gRPC handler is safe even
// in mock / dev scenarios.
func NewService(cfg config.ReconciliationConfig, repo SaveRunRepo) *Service {
	return &Service{
		cfg:        cfg,
		thresholds: NewThresholds(cfg.Thresholds),
		repo:       repo,
	}
}

// Enabled reports whether reconciliation is active.
func (s *Service) Enabled() bool {
	return s != nil && s.cfg.Enabled && s.repo != nil
}

// LaunchAsync spawns the compare goroutine and returns immediately.
// Safe to call with a nil/disabled service — silently no-ops in that case.
// Caller MUST NOT pass a Task whose fields mutate after return.
func (s *Service) LaunchAsync(task Task) {
	if !s.Enabled() {
		return
	}
	// Backtest has no external oracle → nothing to compare against.
	if task.Account.Environment == domain.EnvironmentBacktest {
		return
	}
	// Non-compare snapshot reasons (InitialSeed, ReconciliationLocal/Exchange)
	// don't trigger a run.
	if domain.RunTypeFromReason(task.SnapshotReason) == "" {
		return
	}
	go s.runIsolated(task)
}

// runIsolated is the goroutine body. MUST have the defer recover() at the top
// and MUST use an independent context so the request ctx being cancelled by
// gRPC response write doesn't cancel the compare DB write.
func (s *Service) runIsolated(task Task) {
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.GoroutineTimeout())
	defer cancel()

	runType := domain.RunTypeFromReason(task.SnapshotReason)

	// Panic tomb — log + increment error counter, never propagate.
	defer func() {
		if r := recover(); r != nil {
			emitCounter(ctx, MetricErrorTotal, task.Account.AccountID, task.Account.UserID, string(runType),
				map[string]any{"cause": "panic"})
			logger.Error(ctx, "system", fmt.Sprintf(
				"reconciliation goroutine panic: account=%d session=%s reason=%d cause=%v stack=%s",
				task.Account.AccountID, task.SessionID, task.SnapshotReason, r, debug.Stack(),
			))
		}
	}()

	// Every run increments the total counter — this is the denominator for
	// soft-fail-ratio observation.
	emitCounter(ctx, MetricRunsTotal, task.Account.AccountID, task.Account.UserID, string(runType), nil)

	// Compute diff.
	result := Compare(task.Local, task.Exchange, s.thresholds)

	// Decide log level + DB-write policy by severity.
	allPass := result.HardPass && result.SoftPass
	summary := fmt.Sprintf(
		"reconciliation account=%d user=%d session=%s reason=%d run_type=%s hard=%t soft=%t field_diffs=%d advisory=%d",
		task.Account.AccountID, task.Account.UserID, task.SessionID,
		task.SnapshotReason, runType,
		result.HardPass, result.SoftPass,
		len(result.FieldDiffs), len(result.AdvisoryDiffs),
	)
	switch {
	case !result.HardPass:
		emitCounter(ctx, MetricHardFailTotal, task.Account.AccountID, task.Account.UserID, string(runType), nil)
		logger.Error(ctx, "system", summary)
	case !result.SoftPass:
		emitCounter(ctx, MetricSoftFailTotal, task.Account.AccountID, task.Account.UserID, string(runType), nil)
		logger.Warn(ctx, "system", summary)
	default:
		logger.Info(ctx, "system", summary)
	}

	// Persist the run. Per spec, we write EVERY run (pass or fail) — the
	// canonical dual snapshots are valuable audit trail regardless of
	// threshold outcome.
	run := domain.ReconciliationRun{
		Time:             task.TriggerTime,
		AccountID:        task.Account.AccountID,
		UserID:           task.Account.UserID,
		SessionID:        task.SessionID,
		StrategyID:       task.StrategyID,
		Environment:      task.Account.Environment,
		SnapshotReason:   task.SnapshotReason,
		RunType:          runType,
		ExchangeSnapshot: task.Exchange,
		LocalSnapshot:    task.Local,
		FieldDiffs:       result.FieldDiffs,
		AdvisoryDiffs:    result.AdvisoryDiffs,
		HardPass:         result.HardPass,
		SoftPass:         result.SoftPass,
	}
	if err := s.repo.SaveReconciliationRun(ctx, run); err != nil {
		emitCounter(ctx, MetricErrorTotal, task.Account.AccountID, task.Account.UserID, string(runType),
			map[string]any{"cause": "db_write_failed"})
		// Log with a sample of the payload so we can diagnose without
		// keeping the DB round-trip on the request path.
		sampleJSON, _ := json.Marshal(map[string]any{
			"hard_pass":      run.HardPass,
			"soft_pass":      run.SoftPass,
			"field_diff_len": len(run.FieldDiffs),
		})
		logger.Error(ctx, "system", fmt.Sprintf(
			"reconciliation DB write failed: account=%d reason=%d err=%v sample=%s",
			task.Account.AccountID, task.SnapshotReason, err, string(sampleJSON),
		))
	}
	_ = allPass // summary was already logged above; keep local for clarity
}
