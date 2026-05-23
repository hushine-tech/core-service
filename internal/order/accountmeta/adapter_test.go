package accountmeta

import (
	"context"
	"errors"
	"strings"
	"testing"

	accountdomain "github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeAccountRepository struct {
	account       accountdomain.Account
	accountErr    error
	accountCalls  int
	accountUserID int64
	session       accountdomain.StrategySession
	sessionErr    error
	sessionCalls  int
	sessionID     string
	sessionUserID int64
}

func (f *fakeAccountRepository) GetAccount(_ context.Context, accountID, userID int64) (accountdomain.Account, error) {
	f.accountCalls++
	f.accountUserID = userID
	if f.accountErr != nil {
		return accountdomain.Account{}, f.accountErr
	}
	f.account.AccountID = accountID
	return f.account, nil
}

func (f *fakeAccountRepository) GetSession(_ context.Context, sessionID string, userID int64) (accountdomain.StrategySession, error) {
	f.sessionCalls++
	f.sessionID = sessionID
	f.sessionUserID = userID
	if f.sessionErr != nil {
		return accountdomain.StrategySession{}, f.sessionErr
	}
	return f.session, nil
}

func TestAdapterGetReturnsAccountMeta(t *testing.T) {
	repo := &fakeAccountRepository{
		account: accountdomain.Account{
			UserID:         42,
			Mode:           accountdomain.AccountModeBinanceTestnet,
			MarginMode:     "cross",
			PositionMode:   "one_way",
			APIKey:         "key",
			APISecret:      "secret",
			DefaultFeeRate: 0.0004,
			SlippageBps:    2.5,
		},
	}

	meta, err := NewAdapter(repo).Get(context.Background(), 7)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if repo.accountCalls != 1 {
		t.Fatalf("GetAccount calls = %d, want 1", repo.accountCalls)
	}
	if repo.accountUserID != 0 {
		t.Fatalf("GetAccount userID = %d, want 0", repo.accountUserID)
	}
	if meta.AccountID != 7 || meta.UserID != 42 || meta.Mode != 2 {
		t.Fatalf("meta identity = (%d,%d,%d), want (7,42,2)", meta.AccountID, meta.UserID, meta.Mode)
	}
	if meta.MarginMode != "cross" || meta.PositionMode != "one_way" {
		t.Fatalf("meta modes = (%q,%q)", meta.MarginMode, meta.PositionMode)
	}
	if meta.APIKey != "key" || meta.APISecret != "secret" {
		t.Fatalf("meta credentials = (%q,%q)", meta.APIKey, meta.APISecret)
	}
	if meta.DefaultFeeRate != 0.0004 || meta.SlippageBps != 2.5 {
		t.Fatalf("meta fee/slippage = (%v,%v)", meta.DefaultFeeRate, meta.SlippageBps)
	}
}

func TestValidateActiveSessionEmptySessionIDSkipsLookup(t *testing.T) {
	repo := &fakeAccountRepository{}
	meta := Meta{AccountID: 7, UserID: 42}

	err := NewAdapter(repo).ValidateActiveSession(context.Background(), meta, 99, " \t ")
	if err != nil {
		t.Fatalf("ValidateActiveSession() error = %v", err)
	}
	if repo.sessionCalls != 0 {
		t.Fatalf("GetSession calls = %d, want 0", repo.sessionCalls)
	}
}

func TestValidateActiveSessionAllowsActiveStatus(t *testing.T) {
	repo := &fakeAccountRepository{
		session: accountdomain.StrategySession{
			AccountID:  7,
			UserID:     42,
			StrategyID: 99,
			Status:     " StOpPiNg ",
		},
	}
	meta := Meta{AccountID: 7, UserID: 42}

	err := NewAdapter(repo).ValidateActiveSession(context.Background(), meta, 99, "session-1")
	if err != nil {
		t.Fatalf("ValidateActiveSession() error = %v", err)
	}
}

func TestValidateActiveSessionAccountMismatch(t *testing.T) {
	repo := &fakeAccountRepository{
		session: accountdomain.StrategySession{
			AccountID:  8,
			UserID:     42,
			StrategyID: 99,
			Status:     "running",
		},
	}
	meta := Meta{AccountID: 7, UserID: 42}

	err := NewAdapter(repo).ValidateActiveSession(context.Background(), meta, 99, " session-1 ")
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("code = %v, want %v; err = %v", status.Code(err), codes.PermissionDenied, err)
	}
	if repo.sessionID != "session-1" || repo.sessionUserID != 42 {
		t.Fatalf("lookup = (%q,%d), want (session-1,42)", repo.sessionID, repo.sessionUserID)
	}
}

func TestValidateActiveSessionStrategyMismatch(t *testing.T) {
	repo := &fakeAccountRepository{
		session: accountdomain.StrategySession{
			AccountID:  7,
			UserID:     42,
			StrategyID: 100,
			Status:     "stopping",
		},
	}
	meta := Meta{AccountID: 7, UserID: 42}

	err := NewAdapter(repo).ValidateActiveSession(context.Background(), meta, 99, "session-1")
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("code = %v, want %v; err = %v", status.Code(err), codes.PermissionDenied, err)
	}
}

func TestValidateActiveSessionNonActiveStatus(t *testing.T) {
	repo := &fakeAccountRepository{
		session: accountdomain.StrategySession{
			SessionID:  "session-1",
			AccountID:  7,
			UserID:     42,
			StrategyID: 99,
			Status:     " finished ",
		},
	}
	meta := Meta{AccountID: 7, UserID: 42}

	err := NewAdapter(repo).ValidateActiveSession(context.Background(), meta, 99, "session-1")
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code = %v, want %v; err = %v", status.Code(err), codes.FailedPrecondition, err)
	}
	if !strings.Contains(err.Error(), "session-1") || !strings.Contains(err.Error(), "finished") {
		t.Fatalf("error = %q, want session and status", err.Error())
	}
}

func TestAdapterMapsNotFound(t *testing.T) {
	repo := &fakeAccountRepository{accountErr: repository.ErrNotFound}

	_, err := NewAdapter(repo).Get(context.Background(), 7)
	if status.Code(err) != codes.NotFound {
		t.Fatalf("code = %v, want %v; err = %v", status.Code(err), codes.NotFound, err)
	}
}

func TestAdapterMapsPermissionDenied(t *testing.T) {
	repo := &fakeAccountRepository{accountErr: repository.ErrPermissionDenied}

	_, err := NewAdapter(repo).Get(context.Background(), 7)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("code = %v, want %v; err = %v", status.Code(err), codes.PermissionDenied, err)
	}
}

func TestAdapterMapsUnexpectedError(t *testing.T) {
	repo := &fakeAccountRepository{accountErr: errors.New("database unavailable")}

	_, err := NewAdapter(repo).Get(context.Background(), 7)
	if status.Code(err) != codes.Internal {
		t.Fatalf("code = %v, want %v; err = %v", status.Code(err), codes.Internal, err)
	}
}

func TestAdapterMapsContextCanceled(t *testing.T) {
	repo := &fakeAccountRepository{accountErr: context.Canceled}

	_, err := NewAdapter(repo).Get(context.Background(), 7)
	if status.Code(err) != codes.Canceled {
		t.Fatalf("code = %v, want %v; err = %v", status.Code(err), codes.Canceled, err)
	}
}

func TestAdapterMapsContextDeadlineExceeded(t *testing.T) {
	repo := &fakeAccountRepository{accountErr: context.DeadlineExceeded}

	_, err := NewAdapter(repo).Get(context.Background(), 7)
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("code = %v, want %v; err = %v", status.Code(err), codes.DeadlineExceeded, err)
	}
}

func TestValidateActiveSessionMapsSessionLookupError(t *testing.T) {
	repo := &fakeAccountRepository{sessionErr: repository.ErrNotFound}
	meta := Meta{AccountID: 7, UserID: 42}

	err := NewAdapter(repo).ValidateActiveSession(context.Background(), meta, 99, "session-1")
	if status.Code(err) != codes.NotFound {
		t.Fatalf("code = %v, want %v; err = %v", status.Code(err), codes.NotFound, err)
	}
}

func TestValidateActiveSessionMapsContextDeadlineExceeded(t *testing.T) {
	repo := &fakeAccountRepository{sessionErr: context.DeadlineExceeded}
	meta := Meta{AccountID: 7, UserID: 42}

	err := NewAdapter(repo).ValidateActiveSession(context.Background(), meta, 99, "session-1")
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("code = %v, want %v; err = %v", status.Code(err), codes.DeadlineExceeded, err)
	}
}
