package service

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/gen/accountv1"
	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const validCode = `
class MyStrategy:
    def on_market_data(self, data, wallet):
        return None
`

const validAnnotatedCode = `
from strategy_service.types import MarketData, OrderDecision

class MyStrategy:
    def on_market_data(
        self,
        data: MarketData,
        wallet,
    ) -> OrderDecision | None:
        return None
`

const strategyTestUserID int64 = 11

// strategyStubRepo extends stubRepo with controllable strategy operations.
type strategyStubRepo struct {
	stubRepo
	strategies    []domain.Strategy
	nextID        int64
	accountStrats []domain.AccountStrategy
	stratErr      error
}

type sessionStubRepo struct {
	stubRepo
	sessions map[string]domain.StrategySession
}

func newSessionStubRepo() *sessionStubRepo {
	return &sessionStubRepo{sessions: make(map[string]domain.StrategySession)}
}

func (r *sessionStubRepo) SaveSession(_ context.Context, s domain.StrategySession) error {
	for _, existing := range r.sessions {
		if existing.AccountID == s.AccountID && (existing.Status == "running" || existing.Status == "stopping") {
			return repository.ErrConflict
		}
	}
	s.UserID = strategyTestUserID
	if s.CreatedAt.IsZero() {
		s.CreatedAt = time.Now().UTC()
	}
	r.sessions[s.SessionID] = s
	return nil
}

func (r *sessionStubRepo) UpdateSession(_ context.Context, sessionID string, status string, barsProcessed int, errMsg string, runtimeID string) error {
	s, ok := r.sessions[sessionID]
	if !ok {
		return repository.ErrNotFound
	}
	if runtimeID != "" && s.RuntimeID != runtimeID {
		return repository.ErrNotFound
	}
	s.Status = status
	s.BarsProcessed = barsProcessed
	s.Error = errMsg
	r.sessions[sessionID] = s
	return nil
}

func (r *sessionStubRepo) GetSession(_ context.Context, sessionID string, userID int64) (domain.StrategySession, error) {
	s, ok := r.sessions[sessionID]
	if !ok {
		return domain.StrategySession{}, repository.ErrNotFound
	}
	if userID > 0 && s.UserID != userID {
		return domain.StrategySession{}, repository.ErrNotFound
	}
	return s, nil
}

func (r *sessionStubRepo) ListSessions(_ context.Context, accountID, userID int64, limit, offset int) ([]domain.StrategySession, error) {
	_ = offset
	out := make([]domain.StrategySession, 0, len(r.sessions))
	for _, s := range r.sessions {
		if accountID > 0 && s.AccountID != accountID {
			continue
		}
		if userID > 0 && s.UserID != userID {
			continue
		}
		out = append(out, s)
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *sessionStubRepo) ListSessionsPage(ctx context.Context, filter repository.SessionListFilter) ([]domain.StrategySession, repository.PageMeta, error) {
	out, err := r.ListSessions(ctx, filter.AccountID, filter.UserID, 0, 0)
	if err != nil {
		return nil, repository.PageMeta{}, err
	}
	filtered := make([]domain.StrategySession, 0, len(out))
	for _, s := range out {
		if filter.RuntimeID != "" && s.RuntimeID != filter.RuntimeID {
			continue
		}
		if filter.StrategyID > 0 && s.StrategyID != filter.StrategyID {
			continue
		}
		if filter.ModeSet && s.Mode != filter.Mode {
			continue
		}
		if filter.Status != "" && s.Status != filter.Status {
			continue
		}
		filtered = append(filtered, s)
	}
	total := len(filtered)
	offset := filter.Offset
	if offset > total {
		offset = total
	}
	end := total
	if filter.Limit > 0 && offset+filter.Limit < end {
		end = offset + filter.Limit
	}
	return filtered[offset:end], repository.PageMeta{Total: int64(total), HasMore: end < total}, nil
}

func (r *sessionStubRepo) MarkRuntimeSessionsRecoverable(_ context.Context, runtimeID string, errMsg string) (int64, error) {
	var count int64
	for id, s := range r.sessions {
		if s.RuntimeID != runtimeID {
			continue
		}
		if s.Status != "running" && s.Status != "stopping" {
			continue
		}
		s.Status = "recoverable"
		s.Error = errMsg
		r.sessions[id] = s
		count++
	}
	return count, nil
}

func (r *sessionStubRepo) ListRunningSessions(_ context.Context, runtimeID string) ([]domain.StrategySession, error) {
	out := make([]domain.StrategySession, 0, len(r.sessions))
	for _, s := range r.sessions {
		if s.Status != "running" && s.Status != "stopping" {
			continue
		}
		if runtimeID != "" && s.RuntimeID != runtimeID {
			continue
		}
		out = append(out, s)
	}
	return out, nil
}

func (r *strategyStubRepo) CreateStrategy(_ context.Context, s domain.Strategy) (int64, error) {
	if r.stratErr != nil {
		return 0, r.stratErr
	}
	r.nextID++
	s.StrategyID = r.nextID
	s.CreatedAt = time.Now().UTC()
	r.strategies = append(r.strategies, s)
	return r.nextID, nil
}

func (r *strategyStubRepo) GetStrategy(_ context.Context, id, userID int64) (domain.Strategy, error) {
	if r.stratErr != nil {
		return domain.Strategy{}, r.stratErr
	}
	for _, s := range r.strategies {
		if s.StrategyID == id && (userID == 0 || s.UserID == userID) {
			return s, nil
		}
	}
	return domain.Strategy{}, repository.ErrNotFound
}

func (r *strategyStubRepo) ListStrategies(_ context.Context, userID int64, prefix string, activeOnly bool) ([]domain.Strategy, error) {
	if r.stratErr != nil {
		return nil, r.stratErr
	}
	var out []domain.Strategy
	for _, s := range r.strategies {
		if userID > 0 && s.UserID != userID {
			continue
		}
		if prefix != "" && !strings.HasPrefix(s.Name, prefix) {
			continue
		}
		if activeOnly && s.Archived {
			continue
		}
		out = append(out, s)
	}
	return out, nil
}

func (r *strategyStubRepo) ListStrategiesPage(ctx context.Context, userID int64, prefix string, activeOnly bool, limit, offset int) ([]domain.Strategy, repository.PageMeta, error) {
	out, err := r.ListStrategies(ctx, userID, prefix, activeOnly)
	if err != nil {
		return nil, repository.PageMeta{}, err
	}
	total := len(out)
	if offset > total {
		offset = total
	}
	end := total
	if limit > 0 && offset+limit < end {
		end = offset + limit
	}
	return out[offset:end], repository.PageMeta{Total: int64(total), HasMore: end < total}, nil
}

func (r *strategyStubRepo) ArchiveStrategy(_ context.Context, id int64) error {
	if r.stratErr != nil {
		return r.stratErr
	}
	for i, s := range r.strategies {
		if s.StrategyID == id {
			r.strategies[i].Archived = true
			return nil
		}
	}
	return repository.ErrNotFound
}

func (r *strategyStubRepo) MountStrategy(_ context.Context, accountID, strategyID int64) error {
	if r.stratErr != nil {
		return r.stratErr
	}
	r.accountStrats = append(r.accountStrats, domain.AccountStrategy{
		AccountID: accountID, StrategyID: strategyID, MountedAt: time.Now(),
	})
	return nil
}

func (r *strategyStubRepo) UnmountStrategy(_ context.Context, accountID, strategyID int64) error {
	if r.stratErr != nil {
		return r.stratErr
	}
	for i, e := range r.accountStrats {
		if e.AccountID == accountID && e.StrategyID == strategyID {
			r.accountStrats = append(r.accountStrats[:i], r.accountStrats[i+1:]...)
			return nil
		}
	}
	return repository.ErrNotFound
}

func (r *strategyStubRepo) ActivateStrategy(_ context.Context, accountID, strategyID int64) error {
	if r.stratErr != nil {
		return r.stratErr
	}
	found := false
	for i := range r.accountStrats {
		if r.accountStrats[i].AccountID != accountID {
			continue
		}
		if r.accountStrats[i].StrategyID == strategyID {
			r.accountStrats[i].Active = true
			found = true
		} else {
			r.accountStrats[i].Active = false
		}
	}
	if !found {
		return repository.ErrNotFound
	}
	return nil
}

func (r *strategyStubRepo) DeactivateStrategy(_ context.Context, accountID, strategyID int64) error {
	if r.stratErr != nil {
		return r.stratErr
	}
	for i := range r.accountStrats {
		if r.accountStrats[i].AccountID == accountID && r.accountStrats[i].StrategyID == strategyID {
			r.accountStrats[i].Active = false
			return nil
		}
	}
	return repository.ErrNotFound
}

func (r *strategyStubRepo) ListAccountStrategies(_ context.Context, accountID int64) ([]domain.AccountStrategy, error) {
	if r.stratErr != nil {
		return nil, r.stratErr
	}
	var out []domain.AccountStrategy
	for _, e := range r.accountStrats {
		if e.AccountID == accountID {
			for _, s := range r.strategies {
				if s.StrategyID == e.StrategyID {
					e.Strategy = s
					break
				}
			}
			out = append(out, e)
		}
	}
	return out, nil
}

func (r *strategyStubRepo) GetActiveStrategy(_ context.Context, accountID int64) (domain.Strategy, error) {
	if r.stratErr != nil {
		return domain.Strategy{}, r.stratErr
	}
	for _, e := range r.accountStrats {
		if e.AccountID == accountID && e.Active {
			for _, s := range r.strategies {
				if s.StrategyID == e.StrategyID {
					return s, nil
				}
			}
		}
	}
	return domain.Strategy{}, repository.ErrNotFound
}

// ── CreateStrategy tests ──────────────────────────────────────────────────────

func TestCreateStrategy_success(t *testing.T) {
	repo := &strategyStubRepo{}
	svc := NewAccountGRPCService(repo, nil, nil, nil)
	resp, err := svc.CreateStrategy(context.Background(), &accountv1.CreateStrategyRequest{
		Name: "my-strat", Version: "1.0.0", Description: "test", Code: validCode, UserId: strategyTestUserID,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetStrategy().GetStrategyId() == 0 {
		t.Error("expected non-zero strategy_id")
	}
	if resp.GetStrategy().GetName() != "my-strat" {
		t.Errorf("name: got %q", resp.GetStrategy().GetName())
	}
	if resp.GetStrategy().GetUserId() != strategyTestUserID {
		t.Errorf("user_id: got %d", resp.GetStrategy().GetUserId())
	}
	if resp.GetStrategy().GetRuntimeVersion() != defaultRuntimeVersion {
		t.Errorf("runtime_version: got %q", resp.GetStrategy().GetRuntimeVersion())
	}
	if resp.GetStrategy().GetRuntimeProfile() != defaultRuntimeProfile {
		t.Errorf("runtime_profile: got %q", resp.GetStrategy().GetRuntimeProfile())
	}
}

func TestCreateStrategyCarriesRuntimeMetadata(t *testing.T) {
	repo := &strategyStubRepo{}
	svc := NewAccountGRPCService(repo, nil, nil, nil)
	resp, err := svc.CreateStrategy(context.Background(), &accountv1.CreateStrategyRequest{
		Name: "runtime-strat", Version: "1.0.0", Description: "test", Code: validCode,
		UserId: strategyTestUserID, RuntimeVersion: "1.2.3", RuntimeProfile: "platform-python-3.13",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetStrategy().GetRuntimeVersion() != "1.2.3" {
		t.Fatalf("runtime_version = %q, want 1.2.3", resp.GetStrategy().GetRuntimeVersion())
	}
	if repo.strategies[0].RuntimeProfile != "platform-python-3.13" {
		t.Fatalf("stored runtime_profile = %q", repo.strategies[0].RuntimeProfile)
	}
}

func TestCreateStrategy_acceptsAnnotatedOnMarketDataSignature(t *testing.T) {
	repo := &strategyStubRepo{}
	svc := NewAccountGRPCService(repo, nil, nil, nil)
	resp, err := svc.CreateStrategy(context.Background(), &accountv1.CreateStrategyRequest{
		Name: "annotated-strat", Version: "1.0.0", Description: "typed", Code: validAnnotatedCode, UserId: strategyTestUserID,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetStrategy().GetStrategyId() == 0 {
		t.Error("expected non-zero strategy_id")
	}
}

func TestCreateStrategy_missingClass(t *testing.T) {
	repo := &strategyStubRepo{}
	svc := NewAccountGRPCService(repo, nil, nil, nil)
	_, err := svc.CreateStrategy(context.Background(), &accountv1.CreateStrategyRequest{
		Name: "x", Version: "1.0.0", Code: "def on_market_data(self, data, wallet): pass", UserId: strategyTestUserID,
	})
	if err == nil {
		t.Fatal("expected error")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("code: got %v, want InvalidArgument", st.Code())
	}
}

func TestCreateStrategy_missingMethod(t *testing.T) {
	repo := &strategyStubRepo{}
	svc := NewAccountGRPCService(repo, nil, nil, nil)
	_, err := svc.CreateStrategy(context.Background(), &accountv1.CreateStrategyRequest{
		Name: "x", Version: "1.0.0", Code: "class MyStrategy: pass", UserId: strategyTestUserID,
	})
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("code: got %v, want InvalidArgument", st.Code())
	}
}

func TestCreateStrategy_invalidVersion(t *testing.T) {
	repo := &strategyStubRepo{}
	svc := NewAccountGRPCService(repo, nil, nil, nil)
	_, err := svc.CreateStrategy(context.Background(), &accountv1.CreateStrategyRequest{
		Name: "x", Version: "v1.0", Code: validCode, UserId: strategyTestUserID,
	})
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("code: got %v, want InvalidArgument", st.Code())
	}
}

func TestCreateStrategy_emptyName(t *testing.T) {
	svc := NewAccountGRPCService(&strategyStubRepo{}, nil, nil, nil)
	_, err := svc.CreateStrategy(context.Background(), &accountv1.CreateStrategyRequest{
		Name: "", Version: "1.0.0", Code: validCode, UserId: strategyTestUserID,
	})
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("code: got %v, want InvalidArgument", st.Code())
	}
}

// ── GetStrategy tests ─────────────────────────────────────────────────────────

func TestGetStrategy_success(t *testing.T) {
	repo := &strategyStubRepo{}
	svc := NewAccountGRPCService(repo, nil, nil, nil)
	// Create first
	cr, _ := svc.CreateStrategy(context.Background(), &accountv1.CreateStrategyRequest{
		Name: "s1", Version: "1.0.0", Code: validCode, UserId: strategyTestUserID,
	})
	id := cr.GetStrategy().GetStrategyId()

	resp, err := svc.GetStrategy(context.Background(), &accountv1.GetStrategyRequest{StrategyId: id, UserId: strategyTestUserID})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetStrategy().GetCode() == "" {
		t.Error("expected code to be populated in GetStrategy")
	}
}

func TestGetStrategy_notFound(t *testing.T) {
	svc := NewAccountGRPCService(&strategyStubRepo{}, nil, nil, nil)
	_, err := svc.GetStrategy(context.Background(), &accountv1.GetStrategyRequest{StrategyId: 999, UserId: strategyTestUserID})
	st, _ := status.FromError(err)
	if st.Code() != codes.NotFound {
		t.Errorf("code: got %v, want NotFound", st.Code())
	}
}

// ── ArchiveStrategy tests ─────────────────────────────────────────────────────

func TestArchiveStrategy_success(t *testing.T) {
	repo := &strategyStubRepo{}
	svc := NewAccountGRPCService(repo, nil, nil, nil)
	cr, _ := svc.CreateStrategy(context.Background(), &accountv1.CreateStrategyRequest{
		Name: "s2", Version: "1.0.0", Code: validCode, UserId: strategyTestUserID,
	})
	id := cr.GetStrategy().GetStrategyId()

	_, err := svc.ArchiveStrategy(context.Background(), &accountv1.ArchiveStrategyRequest{StrategyId: id, UserId: strategyTestUserID})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Verify archived
	gr, _ := svc.GetStrategy(context.Background(), &accountv1.GetStrategyRequest{StrategyId: id, UserId: strategyTestUserID})
	if !gr.GetStrategy().GetArchived() {
		t.Error("expected strategy to be archived")
	}
}

// ── Mount / Unmount / Activate tests ─────────────────────────────────────────

func TestMountActivateUnmount(t *testing.T) {
	const accID = int64(42)
	repo := &strategyStubRepo{
		stubRepo: stubRepo{
			account: domain.Account{AccountID: accID, UserID: strategyTestUserID},
		},
	}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	// Create strategy
	cr, _ := svc.CreateStrategy(context.Background(), &accountv1.CreateStrategyRequest{
		Name: "s3", Version: "1.0.0", Code: validCode, UserId: strategyTestUserID,
	})
	sid := cr.GetStrategy().GetStrategyId()

	// Mount
	_, err := svc.MountStrategy(context.Background(), &accountv1.MountStrategyRequest{AccountId: accID, StrategyId: sid, UserId: strategyTestUserID})
	if err != nil {
		t.Fatalf("MountStrategy: %v", err)
	}

	// Activate
	_, err = svc.ActivateStrategy(context.Background(), &accountv1.ActivateStrategyRequest{AccountId: accID, StrategyId: sid, UserId: strategyTestUserID})
	if err != nil {
		t.Fatalf("ActivateStrategy: %v", err)
	}

	// GetActiveStrategy
	ar, err := svc.GetActiveStrategy(context.Background(), &accountv1.GetActiveStrategyRequest{AccountId: accID})
	if err != nil {
		t.Fatalf("GetActiveStrategy: %v", err)
	}
	if ar.GetStrategyId() != sid {
		t.Errorf("active strategy_id: got %d, want %d", ar.GetStrategyId(), sid)
	}
	if ar.GetCode() == "" {
		t.Error("expected code in GetActiveStrategy response")
	}

	// Unmount active → should be rejected
	_, err = svc.UnmountStrategy(context.Background(), &accountv1.UnmountStrategyRequest{AccountId: accID, StrategyId: sid, UserId: strategyTestUserID})
	if err == nil {
		t.Fatal("expected error when unmounting active strategy")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.FailedPrecondition {
		t.Errorf("unmount active: code got %v, want FailedPrecondition", st.Code())
	}

	// Create + mount + activate a second strategy to deactivate the first
	cr2, _ := svc.CreateStrategy(context.Background(), &accountv1.CreateStrategyRequest{
		Name: "s3b", Version: "1.0.0", Code: validCode, UserId: strategyTestUserID,
	})
	sid2 := cr2.GetStrategy().GetStrategyId()
	svc.MountStrategy(context.Background(), &accountv1.MountStrategyRequest{AccountId: accID, StrategyId: sid2, UserId: strategyTestUserID})       //nolint:errcheck
	svc.ActivateStrategy(context.Background(), &accountv1.ActivateStrategyRequest{AccountId: accID, StrategyId: sid2, UserId: strategyTestUserID}) //nolint:errcheck

	// Now unmount the first (no longer active)
	_, err = svc.UnmountStrategy(context.Background(), &accountv1.UnmountStrategyRequest{AccountId: accID, StrategyId: sid, UserId: strategyTestUserID})
	if err != nil {
		t.Fatalf("UnmountStrategy (non-active): %v", err)
	}

	// Active strategy is now sid2
	ar, err = svc.GetActiveStrategy(context.Background(), &accountv1.GetActiveStrategyRequest{AccountId: accID})
	if err != nil {
		t.Fatalf("GetActiveStrategy after unmount: %v", err)
	}
	if ar.GetStrategyId() != sid2 {
		t.Errorf("expected active strategy_id=%d, got %d", sid2, ar.GetStrategyId())
	}
}

func TestMountStrategy_archivedRejected(t *testing.T) {
	repo := &strategyStubRepo{
		stubRepo: stubRepo{
			account: domain.Account{AccountID: 1, UserID: strategyTestUserID},
		},
	}
	svc := NewAccountGRPCService(repo, nil, nil, nil)
	cr, _ := svc.CreateStrategy(context.Background(), &accountv1.CreateStrategyRequest{
		Name: "s4", Version: "1.0.0", Code: validCode, UserId: strategyTestUserID,
	})
	sid := cr.GetStrategy().GetStrategyId()
	svc.ArchiveStrategy(context.Background(), &accountv1.ArchiveStrategyRequest{StrategyId: sid, UserId: strategyTestUserID}) //nolint:errcheck

	_, err := svc.MountStrategy(context.Background(), &accountv1.MountStrategyRequest{AccountId: 1, StrategyId: sid, UserId: strategyTestUserID})
	st, _ := status.FromError(err)
	if st.Code() != codes.FailedPrecondition {
		t.Errorf("code: got %v, want FailedPrecondition", st.Code())
	}
}

// ── ListStrategies tests ──────────────────────────────────────────────────────

func TestListStrategies(t *testing.T) {
	repo := &strategyStubRepo{}
	svc := NewAccountGRPCService(repo, nil, nil, nil)
	for _, name := range []string{"alpha", "beta", "gamma"} {
		svc.CreateStrategy(context.Background(), &accountv1.CreateStrategyRequest{ //nolint:errcheck
			Name: name, Version: "1.0.0", Code: validCode, UserId: strategyTestUserID,
		})
	}
	// Archive one
	gr, _ := svc.GetStrategy(context.Background(), &accountv1.GetStrategyRequest{StrategyId: 1, UserId: strategyTestUserID})
	svc.ArchiveStrategy(context.Background(), &accountv1.ArchiveStrategyRequest{StrategyId: gr.GetStrategy().GetStrategyId(), UserId: strategyTestUserID}) //nolint:errcheck

	// active_only=true should exclude archived
	resp, err := svc.ListStrategies(context.Background(), &accountv1.ListStrategiesRequest{ActiveOnly: true, UserId: strategyTestUserID})
	if err != nil {
		t.Fatalf("ListStrategies: %v", err)
	}
	for _, s := range resp.GetStrategies() {
		if s.GetArchived() {
			t.Errorf("active_only=true returned archived strategy %q", s.GetName())
		}
		// code should not be included in list
		if s.GetCode() != "" {
			t.Errorf("list should not include code for strategy %q", s.GetName())
		}
	}
}

// ── Strategy session runtime binding tests ─────────────────────────────────

func TestSaveGetListSessionCarriesRuntimeName(t *testing.T) {
	repo := newSessionStubRepo()
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	_, err := svc.SaveSession(context.Background(), &accountv1.SaveSessionRequest{
		SessionId:     "sess-1",
		AccountId:     42,
		StrategyId:    7,
		Mode:          2,
		Interval:      "1m",
		RuntimeId:     "rt-hosted-1",
		RuntimeSource: "hosted",
		RuntimeName:   "default",
	})
	if err != nil {
		t.Fatalf("SaveSession: %v", err)
	}

	got, err := svc.GetSession(context.Background(), &accountv1.GetSessionRequest{
		SessionId: "sess-1",
		UserId:    strategyTestUserID,
	})
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if got.GetSession().GetRuntimeId() != "rt-hosted-1" {
		t.Fatalf("runtime_id = %q, want rt-hosted-1", got.GetSession().GetRuntimeId())
	}
	if got.GetSession().GetRuntimeSource() != "hosted" {
		t.Fatalf("runtime_source = %q, want hosted", got.GetSession().GetRuntimeSource())
	}
	if got.GetSession().GetRuntimeName() != "default" {
		t.Fatalf("runtime_name = %q, want default", got.GetSession().GetRuntimeName())
	}

	list, err := svc.ListSessions(context.Background(), &accountv1.ListSessionsRequest{
		AccountId: 42,
		UserId:    strategyTestUserID,
	})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(list.GetSessions()) != 1 || list.GetSessions()[0].GetRuntimeId() != "rt-hosted-1" {
		t.Fatalf("session list did not carry runtime binding: %+v", list.GetSessions())
	}
}

func TestSaveSessionRequiresRuntimeID(t *testing.T) {
	repo := newSessionStubRepo()
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	_, err := svc.SaveSession(context.Background(), &accountv1.SaveSessionRequest{
		SessionId:  "sess-no-runtime",
		AccountId:  42,
		StrategyId: 7,
	})
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Fatalf("SaveSession without runtime_id code = %v, want InvalidArgument", st.Code())
	}
}

func TestSaveSessionAllowsDebuggingWithoutStrategy(t *testing.T) {
	repo := newSessionStubRepo()
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	_, err := svc.SaveSession(context.Background(), &accountv1.SaveSessionRequest{
		SessionId:      "debug-session-1",
		AccountId:      42,
		StrategyId:     0,
		Mode:           0,
		Interval:       "1m",
		RuntimeId:      "rt-debug-1",
		RuntimeSource:  "self_hosted",
		RuntimeName:    "debug-local",
		SessionType:    "debugging",
		RuntimeVersion: "1.2.3",
		SessionName:    "manual-debug",
	})
	if err != nil {
		t.Fatalf("SaveSession debugging: %v", err)
	}

	got, err := svc.GetSession(context.Background(), &accountv1.GetSessionRequest{
		SessionId: "debug-session-1",
		UserId:    strategyTestUserID,
	})
	if err != nil {
		t.Fatalf("GetSession debugging: %v", err)
	}
	if got.GetSession().GetStrategyId() != 0 {
		t.Fatalf("strategy_id = %d, want 0", got.GetSession().GetStrategyId())
	}
	if got.GetSession().GetSessionType() != "debugging" {
		t.Fatalf("session_type = %q, want debugging", got.GetSession().GetSessionType())
	}
	if got.GetSession().GetRuntimeVersion() != "1.2.3" {
		t.Fatalf("runtime_version = %q, want 1.2.3", got.GetSession().GetRuntimeVersion())
	}
	if got.GetSession().GetSessionName() != "manual-debug" {
		t.Fatalf("session_name = %q, want manual-debug", got.GetSession().GetSessionName())
	}
}

func TestSaveSessionRejectsBacktestWithoutStrategy(t *testing.T) {
	repo := newSessionStubRepo()
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	_, err := svc.SaveSession(context.Background(), &accountv1.SaveSessionRequest{
		SessionId:   "bad-session-1",
		AccountId:   42,
		StrategyId:  0,
		Mode:        0,
		RuntimeId:   "rt-executor-1",
		SessionType: "backtest",
	})
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Fatalf("SaveSession without strategy code = %v, want InvalidArgument", st.Code())
	}
	if !strings.Contains(st.Message(), "session_type=debugging") {
		t.Fatalf("SaveSession error = %q, want debugging guidance", st.Message())
	}
}

func TestSaveSessionActiveAccountConflict(t *testing.T) {
	repo := newSessionStubRepo()
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	_, err := svc.SaveSession(context.Background(), &accountv1.SaveSessionRequest{
		SessionId:  "sess-active-1",
		AccountId:  42,
		StrategyId: 7,
		RuntimeId:  "rt-1",
	})
	if err != nil {
		t.Fatalf("first SaveSession: %v", err)
	}
	_, err = svc.SaveSession(context.Background(), &accountv1.SaveSessionRequest{
		SessionId:  "sess-active-2",
		AccountId:  42,
		StrategyId: 8,
		RuntimeId:  "rt-2",
	})
	st, _ := status.FromError(err)
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("second SaveSession code = %v, want FailedPrecondition", st.Code())
	}
}

func TestUpdateSessionRuntimeGuard(t *testing.T) {
	repo := newSessionStubRepo()
	svc := NewAccountGRPCService(repo, nil, nil, nil)
	_, err := svc.SaveSession(context.Background(), &accountv1.SaveSessionRequest{
		SessionId:     "sess-guard",
		AccountId:     42,
		StrategyId:    7,
		RuntimeId:     "rt-owning",
		RuntimeSource: "hosted",
		RuntimeName:   "default",
	})
	if err != nil {
		t.Fatalf("SaveSession: %v", err)
	}

	_, err = svc.UpdateSession(context.Background(), &accountv1.UpdateSessionRequest{
		SessionId:     "sess-guard",
		Status:        "stopped",
		BarsProcessed: 3,
		RuntimeId:     "rt-other",
	})
	st, _ := status.FromError(err)
	if st.Code() != codes.NotFound {
		t.Fatalf("wrong runtime update code = %v, want NotFound", st.Code())
	}

	_, err = svc.UpdateSession(context.Background(), &accountv1.UpdateSessionRequest{
		SessionId:     "sess-guard",
		Status:        "stopped",
		BarsProcessed: 3,
		RuntimeId:     "rt-owning",
	})
	if err != nil {
		t.Fatalf("owning runtime UpdateSession: %v", err)
	}
	if got := repo.sessions["sess-guard"]; got.Status != "stopped" || got.BarsProcessed != 3 {
		t.Fatalf("session not updated by owning runtime: %+v", got)
	}
}

func TestUpdateSessionRuntimeGuardAllowsRecoverable(t *testing.T) {
	repo := newSessionStubRepo()
	svc := NewAccountGRPCService(repo, nil, nil, nil)
	_, err := svc.SaveSession(context.Background(), &accountv1.SaveSessionRequest{
		SessionId:  "sess-recoverable",
		AccountId:  42,
		StrategyId: 7,
		RuntimeId:  "rt-owning",
	})
	if err != nil {
		t.Fatalf("SaveSession: %v", err)
	}

	_, err = svc.UpdateSession(context.Background(), &accountv1.UpdateSessionRequest{
		SessionId: "sess-recoverable",
		Status:    "recoverable",
		Error:     "runtime failed",
		RuntimeId: "rt-owning",
	})
	if err != nil {
		t.Fatalf("UpdateSession recoverable: %v", err)
	}
	got := repo.sessions["sess-recoverable"]
	if got.Status != "recoverable" || got.Error != "runtime failed" {
		t.Fatalf("recoverable transition not persisted: %+v", got)
	}
}

func TestUpdateSessionRejectsTerminalSessionMutation(t *testing.T) {
	repo := newSessionStubRepo()
	svc := NewAccountGRPCService(repo, nil, nil, nil)
	_, err := svc.SaveSession(context.Background(), &accountv1.SaveSessionRequest{
		SessionId:  "sess-terminal",
		AccountId:  42,
		StrategyId: 7,
		RuntimeId:  "rt-owning",
	})
	if err != nil {
		t.Fatalf("SaveSession: %v", err)
	}
	_, err = svc.UpdateSession(context.Background(), &accountv1.UpdateSessionRequest{
		SessionId: "sess-terminal",
		Status:    "recoverable",
		Error:     "runtime failed",
		RuntimeId: "rt-owning",
	})
	if err != nil {
		t.Fatalf("terminal UpdateSession: %v", err)
	}

	_, err = svc.UpdateSession(context.Background(), &accountv1.UpdateSessionRequest{
		SessionId:     "sess-terminal",
		Status:        "running",
		BarsProcessed: 12,
		RuntimeId:     "rt-owning",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("terminal mutation code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
	if got := repo.sessions["sess-terminal"]; got.Status != "recoverable" {
		t.Fatalf("terminal session was mutated: %+v", got)
	}
}

func TestUpdateAccountWalletStateRejectsTerminalSession(t *testing.T) {
	repo := newSessionStubRepo()
	repo.account = domain.Account{
		AccountID: 42,
		UserID:    strategyTestUserID,
		Mode:      domain.AccountModeBacktest,
	}
	svc := NewAccountGRPCService(repo, nil, nil, nil)
	_, err := svc.SaveSession(context.Background(), &accountv1.SaveSessionRequest{
		SessionId:  "sess-wallet-terminal",
		AccountId:  42,
		StrategyId: 7,
		RuntimeId:  "rt-owning",
	})
	if err != nil {
		t.Fatalf("SaveSession: %v", err)
	}
	_, err = svc.UpdateSession(context.Background(), &accountv1.UpdateSessionRequest{
		SessionId: "sess-wallet-terminal",
		Status:    "recoverable",
		Error:     "runtime failed",
		RuntimeId: "rt-owning",
	})
	if err != nil {
		t.Fatalf("terminal UpdateSession: %v", err)
	}

	_, err = svc.UpdateAccountWalletState(context.Background(), &accountv1.UpdateAccountWalletStateRequest{
		AccountId:      42,
		StrategyId:     7,
		SessionId:      "sess-wallet-terminal",
		SnapshotReason: int32(domain.SnapshotReasonPeriodicSample),
		WalletBalance:  1000,
		Futures: &accountv1.FuturesWallet{
			WalletBalance: 1000,
		},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("terminal wallet update code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
}

func TestMarkRuntimeSessionsRecoverable(t *testing.T) {
	repo := newSessionStubRepo()
	svc := NewAccountGRPCService(repo, nil, nil, nil)
	for _, req := range []*accountv1.SaveSessionRequest{
		{SessionId: "sess-r1", AccountId: 42, StrategyId: 7, RuntimeId: "rt-1"},
		{SessionId: "sess-r2", AccountId: 43, StrategyId: 8, RuntimeId: "rt-2"},
	} {
		if _, err := svc.SaveSession(context.Background(), req); err != nil {
			t.Fatalf("SaveSession(%s): %v", req.GetSessionId(), err)
		}
	}

	resp, err := svc.MarkRuntimeSessionsRecoverable(context.Background(), &accountv1.MarkRuntimeSessionsRecoverableRequest{
		RuntimeId: "rt-1",
		Error:     "runtime unhealthy",
	})
	if err != nil {
		t.Fatalf("MarkRuntimeSessionsRecoverable: %v", err)
	}
	if resp.GetSessionsMarked() != 1 {
		t.Fatalf("sessions_marked = %d, want 1", resp.GetSessionsMarked())
	}
	if got := repo.sessions["sess-r1"]; got.Status != "recoverable" || got.Error != "runtime unhealthy" {
		t.Fatalf("rt-1 session not recoverable: %+v", got)
	}
	if got := repo.sessions["sess-r2"]; got.Status != "running" {
		t.Fatalf("rt-2 session should be unaffected: %+v", got)
	}
	resp, err = svc.MarkRuntimeSessionsRecoverable(context.Background(), &accountv1.MarkRuntimeSessionsRecoverableRequest{
		RuntimeId: "rt-1",
		Error:     "runtime unhealthy again",
	})
	if err != nil {
		t.Fatalf("second MarkRuntimeSessionsRecoverable: %v", err)
	}
	if resp.GetSessionsMarked() != 0 {
		t.Fatalf("second sessions_marked = %d, want 0", resp.GetSessionsMarked())
	}
}

func TestListRunningSessionsRuntimeFilter(t *testing.T) {
	repo := newSessionStubRepo()
	svc := NewAccountGRPCService(repo, nil, nil, nil)
	for _, req := range []*accountv1.SaveSessionRequest{
		{SessionId: "sess-r1", AccountId: 42, StrategyId: 7, RuntimeId: "rt-1"},
		{SessionId: "sess-r2", AccountId: 43, StrategyId: 8, RuntimeId: "rt-2"},
	} {
		if _, err := svc.SaveSession(context.Background(), req); err != nil {
			t.Fatalf("SaveSession(%s): %v", req.GetSessionId(), err)
		}
	}

	resp, err := svc.ListRunningSessions(context.Background(), &accountv1.ListRunningSessionsRequest{
		RuntimeId: "rt-1",
	})
	if err != nil {
		t.Fatalf("ListRunningSessions: %v", err)
	}
	if len(resp.GetSessions()) != 1 || resp.GetSessions()[0].GetSessionId() != "sess-r1" {
		t.Fatalf("filtered running sessions = %+v, want only sess-r1", resp.GetSessions())
	}
}

func TestListSessionsIncludesRecoverableHistoricalSession(t *testing.T) {
	repo := newSessionStubRepo()
	svc := NewAccountGRPCService(repo, nil, nil, nil)
	_, err := svc.SaveSession(context.Background(), &accountv1.SaveSessionRequest{
		SessionId:  "sess-history",
		AccountId:  42,
		StrategyId: 7,
		RuntimeId:  "rt-old",
	})
	if err != nil {
		t.Fatalf("SaveSession: %v", err)
	}
	_, err = svc.UpdateSession(context.Background(), &accountv1.UpdateSessionRequest{
		SessionId: "sess-history",
		Status:    "recoverable",
		Error:     "runtime failed",
		RuntimeId: "rt-old",
	})
	if err != nil {
		t.Fatalf("UpdateSession recoverable: %v", err)
	}

	list, err := svc.ListSessions(context.Background(), &accountv1.ListSessionsRequest{
		AccountId: 42,
		UserId:    strategyTestUserID,
	})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(list.GetSessions()) != 1 {
		t.Fatalf("sessions = %+v, want one recoverable history row", list.GetSessions())
	}
	got := list.GetSessions()[0]
	if got.GetStatus() != "recoverable" || got.GetRuntimeId() != "rt-old" || got.GetError() != "runtime failed" {
		t.Fatalf("history session = %+v, want recoverable runtime-bound row", got)
	}

	running, err := svc.ListRunningSessions(context.Background(), &accountv1.ListRunningSessionsRequest{
		RuntimeId: "rt-old",
	})
	if err != nil {
		t.Fatalf("ListRunningSessions: %v", err)
	}
	if len(running.GetSessions()) != 0 {
		t.Fatalf("running sessions = %+v, want recoverable excluded", running.GetSessions())
	}
}
