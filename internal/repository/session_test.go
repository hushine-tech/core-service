package repository

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
)

func TestUpdateSessionFinishedPersistsBarsAndCompletion(t *testing.T) {
	repo, ctx := notificationTestRepo(t)
	user := createNotificationTestUser(t, ctx, repo)

	accountID, err := repo.CreateAccount(ctx, domain.Account{
		UserID:      user.ID,
		Name:        fmt.Sprintf("session-update-%d", time.Now().UnixNano()),
		Description: "session update regression",
		Environment: domain.EnvironmentBacktest,
		Status:      domain.AccountStatusActive,
		CreatedAt:   time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}

	sessionID := fmt.Sprintf("session-update-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = repo.db.ExecContext(context.Background(), `DELETE FROM strategy_sessions WHERE session_id = $1`, sessionID)
		_, _ = repo.db.ExecContext(context.Background(), `DELETE FROM accounts WHERE account_id = $1`, accountID)
	})

	startMs := int64(1779465600000)
	endMs := int64(1779552000000)
	if err := repo.SaveSession(ctx, domain.StrategySession{
		SessionID:      sessionID,
		AccountID:      accountID,
		UserID:         user.ID,
		Environment:    domain.EnvironmentBacktest,
		Status:         "running",
		Interval:       "1m",
		StartTimeMs:    &startMs,
		EndTimeMs:      &endMs,
		RuntimeID:      "rt-session-update",
		RuntimeSource:  "hosted",
		RuntimeName:    "session-update-runtime",
		SessionType:    "backtest",
		RuntimeVersion: "test",
		StartedAt:      time.Now().UTC(),
	}); err != nil {
		t.Fatalf("save session: %v", err)
	}

	if err := repo.UpdateSession(ctx, sessionID, "finished", 1440, "", "rt-session-update"); err != nil {
		t.Fatalf("update session: %v", err)
	}

	got, err := repo.GetSession(ctx, sessionID, user.ID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if got.Status != "finished" {
		t.Fatalf("status = %q, want finished", got.Status)
	}
	if got.BarsProcessed != 1440 {
		t.Fatalf("bars processed = %d, want 1440", got.BarsProcessed)
	}
	if got.CompletedAt == nil {
		t.Fatalf("completed_at is nil, want completion timestamp")
	}
}

func TestRecoverableSessionDoesNotBlockAccountStart(t *testing.T) {
	repo, ctx := notificationTestRepo(t)
	user := createNotificationTestUser(t, ctx, repo)

	accountID, err := repo.CreateAccount(ctx, domain.Account{
		UserID:      user.ID,
		Name:        fmt.Sprintf("session-recoverable-restart-%d", time.Now().UnixNano()),
		Description: "recoverable session restart regression",
		Environment: domain.EnvironmentDemo,
		Status:      domain.AccountStatusActive,
		CreatedAt:   time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}

	recoverableSessionID := fmt.Sprintf("session-recoverable-%d", time.Now().UnixNano())
	runningSessionID := fmt.Sprintf("session-running-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = repo.db.ExecContext(context.Background(), `DELETE FROM strategy_sessions WHERE session_id IN ($1, $2)`, recoverableSessionID, runningSessionID)
		_, _ = repo.db.ExecContext(context.Background(), `DELETE FROM accounts WHERE account_id = $1`, accountID)
	})

	completedAt := time.Now().UTC()
	if err := repo.SaveSession(ctx, domain.StrategySession{
		SessionID:      recoverableSessionID,
		AccountID:      accountID,
		UserID:         user.ID,
		Environment:    domain.EnvironmentDemo,
		Status:         "recoverable",
		Interval:       "1m",
		BarsProcessed:  10,
		Error:          "runtime heartbeat stale",
		RuntimeID:      "rt-recoverable-restart",
		RuntimeSource:  "hosted",
		RuntimeName:    "recoverable-runtime",
		SessionType:    "demo",
		RuntimeVersion: "test",
		StartedAt:      time.Now().Add(-time.Hour).UTC(),
		CompletedAt:    &completedAt,
	}); err != nil {
		t.Fatalf("save recoverable session: %v", err)
	}

	count, err := repo.CountActiveSessionsForAccount(ctx, user.ID, accountID)
	if err != nil {
		t.Fatalf("count active sessions: %v", err)
	}
	if count != 0 {
		t.Fatalf("active session count = %d, want 0 for recoverable-only account", count)
	}

	if err := repo.SaveSession(ctx, domain.StrategySession{
		SessionID:      runningSessionID,
		AccountID:      accountID,
		UserID:         user.ID,
		Environment:    domain.EnvironmentDemo,
		Status:         "running",
		Interval:       "1m",
		RuntimeID:      "rt-running-after-recoverable",
		RuntimeSource:  "hosted",
		RuntimeName:    "running-runtime",
		SessionType:    "demo",
		RuntimeVersion: "test",
		StartedAt:      time.Now().UTC(),
	}); err != nil {
		t.Fatalf("save running session after recoverable: %v", err)
	}
}

func TestSaveSnapshotAllowsRepeatedBacktestMarketTimeAcrossSessions(t *testing.T) {
	repo, ctx := notificationTestRepo(t)
	user := createNotificationTestUser(t, ctx, repo)

	accountID, err := repo.CreateAccount(ctx, domain.Account{
		UserID:      user.ID,
		Name:        fmt.Sprintf("snapshot-repeat-%d", time.Now().UnixNano()),
		Description: "snapshot repeat regression",
		Environment: domain.EnvironmentBacktest,
		Status:      domain.AccountStatusActive,
		CreatedAt:   time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}

	sessionOne := fmt.Sprintf("snapshot-repeat-a-%d", time.Now().UnixNano())
	sessionTwo := fmt.Sprintf("snapshot-repeat-b-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = repo.db.ExecContext(context.Background(), `DELETE FROM account_snapshots WHERE account_id = $1`, accountID)
		_, _ = repo.db.ExecContext(context.Background(), `DELETE FROM accounts WHERE account_id = $1`, accountID)
	})

	if err := repo.UpdateAccountState(ctx, domain.OnlineAccountInfo{
		AccountID:        accountID,
		Environment:      domain.EnvironmentBacktest,
		TotalValue:       1600,
		WalletBalance:    1600,
		AvailableBalance: 1600,
		Futures: domain.FuturesWallet{
			MarginMode:     "cross",
			PositionMode:   "one_way",
			InitialBalance: 1600,
			WalletBalance:  1600,
		},
	}); err != nil {
		t.Fatalf("update account state: %v", err)
	}

	marketTime := time.UnixMilli(1780243200000).UTC()
	if err := repo.SaveSnapshot(ctx, accountID, domain.SnapshotReasonStrategyStart, 44, sessionOne, marketTime); err != nil {
		t.Fatalf("save first snapshot: %v", err)
	}
	if err := repo.SaveSnapshot(ctx, accountID, domain.SnapshotReasonStrategyStart, 44, sessionTwo, marketTime); err != nil {
		t.Fatalf("save second snapshot at same market time: %v", err)
	}

	for _, sessionID := range []string{sessionOne, sessionTwo} {
		rows, total, _, err := repo.ListSessionSnapshots(ctx, sessionID, user.ID, 10, 0)
		if err != nil {
			t.Fatalf("list snapshots for %s: %v", sessionID, err)
		}
		if total != 1 || len(rows) != 1 {
			t.Fatalf("snapshots for %s = total:%d rows:%d, want 1/1", sessionID, total, len(rows))
		}
		if !rows[0].Time.Equal(marketTime) || rows[0].SessionID != sessionID {
			t.Fatalf("snapshot row for %s = %+v, want same market time and session id", sessionID, rows[0])
		}
	}
}

func TestSavePreflightFailedSessionPersistsStructuredError(t *testing.T) {
	repo, ctx := notificationTestRepo(t)
	user := createNotificationTestUser(t, ctx, repo)

	accountID, err := repo.CreateAccount(ctx, domain.Account{
		UserID:      user.ID,
		Name:        fmt.Sprintf("preflight-failed-%d", time.Now().UnixNano()),
		Description: "preflight failed regression",
		Environment: domain.EnvironmentDemo,
		Status:      domain.AccountStatusActive,
		CreatedAt:   time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}

	sessionID := fmt.Sprintf("preflight-failed-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = repo.db.ExecContext(context.Background(), `DELETE FROM strategy_sessions WHERE session_id = $1`, sessionID)
		_, _ = repo.db.ExecContext(context.Background(), `DELETE FROM accounts WHERE account_id = $1`, accountID)
	})

	if err := repo.SaveSession(ctx, domain.StrategySession{
		SessionID:       sessionID,
		UserID:          user.ID,
		AccountID:       accountID,
		StrategyID:      2,
		Environment:     domain.EnvironmentDemo,
		Status:          domain.SessionStatusPreflightFailed,
		ErrorCode:       "VENUE_MISSING",
		ErrorMessage:    "active venue is missing",
		ErrorDetailJSON: `{"exchange":1,"market":2}`,
	}); err != nil {
		t.Fatalf("SaveSession: %v", err)
	}

	got, err := repo.GetSession(ctx, sessionID, user.ID)
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if got.Status != domain.SessionStatusPreflightFailed {
		t.Fatalf("status = %q, want %q", got.Status, domain.SessionStatusPreflightFailed)
	}
	if got.ErrorCode != "VENUE_MISSING" || got.ErrorMessage != "active venue is missing" {
		t.Fatalf("unexpected structured error fields: %+v", got)
	}
	if !strings.Contains(got.ErrorDetailJSON, `"exchange": 1`) || !got.StartedAt.IsZero() {
		t.Fatalf("unexpected preflight failure session: %+v", got)
	}
}
