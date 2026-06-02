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
