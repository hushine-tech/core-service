package accountmeta

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/hushine-tech/core-service/internal/credential"
	accountdomain "github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const testCredentialKey = "0123456789abcdef0123456789abcdef"

type fakeAccountRepository struct {
	route         accountdomain.VenueRouteMeta
	routeErr      error
	routeCalls    int
	routeAccount  int64
	routeExchange accountdomain.Exchange
	routeMarket   accountdomain.Market
	session       accountdomain.StrategySession
	sessionErr    error
	sessionCalls  int
	sessionID     string
	sessionUserID int64
}

func (f *fakeAccountRepository) ResolveVenueRouteMeta(_ context.Context, accountID int64, exchange accountdomain.Exchange, market accountdomain.Market) (accountdomain.VenueRouteMeta, error) {
	f.routeCalls++
	f.routeAccount = accountID
	f.routeExchange = exchange
	f.routeMarket = market
	if f.routeErr != nil {
		return accountdomain.VenueRouteMeta{}, f.routeErr
	}
	f.route.AccountID = accountID
	f.route.Exchange = exchange
	f.route.Market = market
	return f.route, nil
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

func testCredentialManager(t *testing.T) *credential.Manager {
	t.Helper()
	mgr, err := credential.NewManager(testCredentialKey, "v1")
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return mgr
}

func TestAdapterGetReturnsBacktestRouteMeta(t *testing.T) {
	repo := &fakeAccountRepository{
		route: accountdomain.VenueRouteMeta{
			VenueID:        88,
			UserID:         42,
			Environment:    accountdomain.EnvironmentBacktest,
			MarginMode:     accountdomain.MarginModeCross,
			PositionMode:   accountdomain.PositionModeOneWay,
			DefaultFeeRate: 0.0004,
			SlippageBps:    2.5,
		},
	}

	meta, err := NewAdapter(repo, nil).Get(context.Background(), 7, 1, 2)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if repo.routeCalls != 1 {
		t.Fatalf("ResolveVenueRouteMeta calls = %d, want 1", repo.routeCalls)
	}
	if repo.routeAccount != 7 || repo.routeExchange != accountdomain.ExchangeBinance || repo.routeMarket != accountdomain.MarketPerpetualFutures {
		t.Fatalf("route lookup = (%d,%d,%d)", repo.routeAccount, repo.routeExchange, repo.routeMarket)
	}
	if meta.AccountID != 7 || meta.VenueID != 88 || meta.UserID != 42 {
		t.Fatalf("meta identity = (%d,%d,%d), want (7,88,42)", meta.AccountID, meta.VenueID, meta.UserID)
	}
	if meta.Environment != 0 || meta.Exchange != 1 || meta.Market != 2 {
		t.Fatalf("meta route = (%d,%d,%d), want (0,1,2)", meta.Environment, meta.Exchange, meta.Market)
	}
	if meta.MarginMode != "cross" || meta.PositionMode != "one_way" {
		t.Fatalf("meta modes = (%q,%q)", meta.MarginMode, meta.PositionMode)
	}
	if meta.APIKey != "" || meta.APISecret != "" || meta.CredentialJSON != "" {
		t.Fatalf("backtest route should not expose credentials: %+v", meta)
	}
	if meta.DefaultFeeRate != 0.0004 || meta.SlippageBps != 2.5 {
		t.Fatalf("meta fee/slippage = (%v,%v)", meta.DefaultFeeRate, meta.SlippageBps)
	}
}

func TestAdapterGetDecryptsExchangeRouteCredential(t *testing.T) {
	mgr := testCredentialManager(t)
	ciphertext, err := mgr.Encrypt(`{"api_key":"key","api_secret":"secret"}`)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	repo := &fakeAccountRepository{
		route: accountdomain.VenueRouteMeta{
			VenueID:        88,
			UserID:         42,
			Environment:    accountdomain.EnvironmentDemo,
			APIKey:         "key",
			CredentialInfo: ciphertext,
			MarginMode:     accountdomain.MarginModeIsolated,
			PositionMode:   accountdomain.PositionModeHedge,
		},
	}

	meta, err := NewAdapter(repo, mgr).Get(context.Background(), 7, 1, 2)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if meta.APIKey != "key" || meta.APISecret != "secret" {
		t.Fatalf("meta credentials = (%q,%q)", meta.APIKey, meta.APISecret)
	}
	if meta.CredentialJSON != `{"api_key":"key","api_secret":"secret"}` {
		t.Fatalf("credential json = %q", meta.CredentialJSON)
	}
	if meta.MarginMode != "isolated" || meta.PositionMode != "hedge" {
		t.Fatalf("meta modes = (%q,%q)", meta.MarginMode, meta.PositionMode)
	}
}

func TestAdapterGetExchangeRouteRequiresCredentialManager(t *testing.T) {
	repo := &fakeAccountRepository{
		route: accountdomain.VenueRouteMeta{
			Environment:    accountdomain.EnvironmentDemo,
			CredentialInfo: "v1:anything",
		},
	}

	_, err := NewAdapter(repo, nil).Get(context.Background(), 7, 1, 2)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code = %v, want %v; err = %v", status.Code(err), codes.FailedPrecondition, err)
	}
}

func TestAdapterGetRouteMissingIsFailedPrecondition(t *testing.T) {
	repo := &fakeAccountRepository{routeErr: repository.ErrNotFound}

	_, err := NewAdapter(repo, nil).Get(context.Background(), 7, 1, 2)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code = %v, want %v; err = %v", status.Code(err), codes.FailedPrecondition, err)
	}
}

func TestValidateActiveSessionEmptySessionIDSkipsLookup(t *testing.T) {
	repo := &fakeAccountRepository{}
	meta := Meta{AccountID: 7, UserID: 42}

	err := NewAdapter(repo, nil).ValidateActiveSession(context.Background(), meta, 99, " \t ")
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

	err := NewAdapter(repo, nil).ValidateActiveSession(context.Background(), meta, 99, "session-1")
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

	err := NewAdapter(repo, nil).ValidateActiveSession(context.Background(), meta, 99, " session-1 ")
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

	err := NewAdapter(repo, nil).ValidateActiveSession(context.Background(), meta, 99, "session-1")
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

	err := NewAdapter(repo, nil).ValidateActiveSession(context.Background(), meta, 99, "session-1")
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code = %v, want %v; err = %v", status.Code(err), codes.FailedPrecondition, err)
	}
	if !strings.Contains(err.Error(), "session-1") || !strings.Contains(err.Error(), "finished") {
		t.Fatalf("error = %q, want session and status", err.Error())
	}
}

func TestAdapterMapsPermissionDenied(t *testing.T) {
	repo := &fakeAccountRepository{routeErr: repository.ErrPermissionDenied}

	_, err := NewAdapter(repo, nil).Get(context.Background(), 7, 1, 2)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("code = %v, want %v; err = %v", status.Code(err), codes.PermissionDenied, err)
	}
}

func TestAdapterMapsUnexpectedError(t *testing.T) {
	repo := &fakeAccountRepository{routeErr: errors.New("database unavailable")}

	_, err := NewAdapter(repo, nil).Get(context.Background(), 7, 1, 2)
	if status.Code(err) != codes.Internal {
		t.Fatalf("code = %v, want %v; err = %v", status.Code(err), codes.Internal, err)
	}
}

func TestAdapterMapsContextCanceled(t *testing.T) {
	repo := &fakeAccountRepository{routeErr: context.Canceled}

	_, err := NewAdapter(repo, nil).Get(context.Background(), 7, 1, 2)
	if status.Code(err) != codes.Canceled {
		t.Fatalf("code = %v, want %v; err = %v", status.Code(err), codes.Canceled, err)
	}
}

func TestAdapterMapsContextDeadlineExceeded(t *testing.T) {
	repo := &fakeAccountRepository{routeErr: context.DeadlineExceeded}

	_, err := NewAdapter(repo, nil).Get(context.Background(), 7, 1, 2)
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("code = %v, want %v; err = %v", status.Code(err), codes.DeadlineExceeded, err)
	}
}

func TestValidateActiveSessionMapsSessionLookupError(t *testing.T) {
	repo := &fakeAccountRepository{sessionErr: repository.ErrNotFound}
	meta := Meta{AccountID: 7, UserID: 42}

	err := NewAdapter(repo, nil).ValidateActiveSession(context.Background(), meta, 99, "session-1")
	if status.Code(err) != codes.NotFound {
		t.Fatalf("code = %v, want %v; err = %v", status.Code(err), codes.NotFound, err)
	}
}

func TestValidateActiveSessionMapsContextDeadlineExceeded(t *testing.T) {
	repo := &fakeAccountRepository{sessionErr: context.DeadlineExceeded}
	meta := Meta{AccountID: 7, UserID: 42}

	err := NewAdapter(repo, nil).ValidateActiveSession(context.Background(), meta, 99, "session-1")
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("code = %v, want %v; err = %v", status.Code(err), codes.DeadlineExceeded, err)
	}
}
