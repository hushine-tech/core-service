package service

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/gen/accountv1"
	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const serviceTestUserID int64 = 7

// stubRepo is a minimal Repository stub for unit tests.
type stubRepo struct {
	account              domain.Account
	createdAccount       domain.Account
	state                domain.OnlineAccountInfo
	reconciliationRuns   []domain.ReconciliationRun
	sessionSnapshots     []domain.SnapshotRow
	notificationSettings domain.NotificationSettings
	notificationChannel  domain.NotificationChannel
	notificationPlan     domain.NotificationPlan
	deliveryStatus       string
	deliveryError        string

	// Captures last paging args seen on the two paginated list methods so
	// tests can assert that the gRPC handlers forwarded limit/offset
	// correctly after the paginate-session-detail-lists change.
	lastReconciliationLimit  int
	lastReconciliationOffset int
	lastSnapshotsLimit       int
	lastSnapshotsOffset      int

	err   error
	users map[string]domain.User
}

func (s *stubRepo) CreateUser(_ context.Context, user domain.User) (domain.User, error) {
	if s.users == nil {
		s.users = map[string]domain.User{}
	}
	if _, ok := s.users[user.Username]; ok {
		return domain.User{}, errors.New("duplicate key")
	}
	user.ID = int64(len(s.users) + 1)
	s.users[user.Username] = user
	return user, nil
}

func (s *stubRepo) GetUserByUsername(_ context.Context, username string) (domain.User, error) {
	if s.users == nil {
		return domain.User{}, repository.ErrNotFound
	}
	user, ok := s.users[username]
	if !ok {
		return domain.User{}, repository.ErrNotFound
	}
	return user, nil
}

func (s *stubRepo) GetUser(_ context.Context, userID int64) (domain.User, error) {
	for _, u := range s.users {
		if u.ID == userID {
			return u, nil
		}
	}
	return domain.User{}, repository.ErrNotFound
}

func (s *stubRepo) CreateAccount(_ context.Context, account domain.Account) (int64, error) {
	s.createdAccount = account
	return 1, nil
}
func (s *stubRepo) GetAccount(_ context.Context, id, userID int64) (domain.Account, error) {
	if s.err != nil {
		return domain.Account{}, s.err
	}
	if s.account.AccountID != 0 && s.account.AccountID != id {
		return domain.Account{}, repository.ErrNotFound
	}
	if userID > 0 && s.account.UserID != userID {
		return domain.Account{}, repository.ErrNotFound
	}
	return s.account, s.err
}
func (s *stubRepo) ListAccounts(_ context.Context, userID int64) ([]domain.Account, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.account.AccountID == 0 {
		return nil, nil
	}
	if userID > 0 && s.account.UserID != userID {
		return nil, nil
	}
	return []domain.Account{s.account}, nil
}
func (s *stubRepo) ListAccountsPage(ctx context.Context, userID int64, limit, offset int) ([]domain.Account, repository.PageMeta, error) {
	list, err := s.ListAccounts(ctx, userID)
	if err != nil {
		return nil, repository.PageMeta{}, err
	}
	return list, repository.PageMeta{Total: int64(len(list))}, nil
}
func (s *stubRepo) CreateVenue(_ context.Context, venue domain.Venue) (domain.Venue, error) {
	return venue, s.err
}
func (s *stubRepo) GetVenue(_ context.Context, _ int64, _ int64) (domain.Venue, error) {
	return domain.Venue{}, errors.New("not implemented")
}
func (s *stubRepo) ListVenues(_ context.Context, _ int64, _ int64, _ bool, _ bool, _ int, _ int) ([]domain.Venue, repository.PageMeta, error) {
	return nil, repository.PageMeta{}, errors.New("not implemented")
}
func (s *stubRepo) BindVenue(_ context.Context, _ int64, _ int64, _ int64, _ string) (domain.Venue, error) {
	return domain.Venue{}, errors.New("not implemented")
}
func (s *stubRepo) ReleaseVenue(_ context.Context, _ int64, _ int64, _ string) (domain.Venue, error) {
	return domain.Venue{}, errors.New("not implemented")
}
func (s *stubRepo) ArchiveVenue(_ context.Context, _ int64, _ int64, _ string) error {
	return errors.New("not implemented")
}
func (s *stubRepo) ListActiveAccountVenues(_ context.Context, _ int64, _ int64) ([]domain.Venue, error) {
	return nil, errors.New("not implemented")
}
func (s *stubRepo) CountActiveSessionsForAccount(_ context.Context, _ int64, _ int64) (int64, error) {
	return 0, errors.New("not implemented")
}
func (s *stubRepo) SaveSessionVenues(_ context.Context, _ string, _ []domain.Venue) error {
	return errors.New("not implemented")
}
func (s *stubRepo) ResolveVenueRouteMeta(_ context.Context, _ int64, _ domain.Exchange, _ domain.Market) (domain.VenueRouteMeta, error) {
	return domain.VenueRouteMeta{}, errors.New("not implemented")
}
func (s *stubRepo) UpdateAccountState(_ context.Context, info domain.OnlineAccountInfo) error {
	s.state = info
	return nil
}
func (s *stubRepo) GetAccountState(_ context.Context, _ int64) (domain.OnlineAccountInfo, error) {
	return s.state, s.err
}
func (s *stubRepo) SaveSnapshot(_ context.Context, _ int64, _ domain.SnapshotReason, _ int64, _ string) error {
	return nil
}

// Strategy management stubs (no-op; overridden in strategy-specific tests via strategyStubRepo).
func (s *stubRepo) CreateStrategy(_ context.Context, _ domain.Strategy) (int64, error) {
	return 0, errors.New("not implemented")
}
func (s *stubRepo) GetStrategy(_ context.Context, _, _ int64) (domain.Strategy, error) {
	return domain.Strategy{}, errors.New("not implemented")
}
func (s *stubRepo) ListStrategies(_ context.Context, _ int64, _ string, _ bool) ([]domain.Strategy, error) {
	return nil, errors.New("not implemented")
}
func (s *stubRepo) ListStrategiesPage(_ context.Context, _ int64, _ string, _ bool, _, _ int) ([]domain.Strategy, repository.PageMeta, error) {
	return nil, repository.PageMeta{}, errors.New("not implemented")
}
func (s *stubRepo) ArchiveStrategy(_ context.Context, _ int64) error {
	return errors.New("not implemented")
}
func (s *stubRepo) MountStrategy(_ context.Context, _, _ int64) error {
	return errors.New("not implemented")
}
func (s *stubRepo) UnmountStrategy(_ context.Context, _, _ int64) error {
	return errors.New("not implemented")
}
func (s *stubRepo) ActivateStrategy(_ context.Context, _, _ int64) error {
	return errors.New("not implemented")
}
func (s *stubRepo) DeactivateStrategy(_ context.Context, _, _ int64) error {
	return errors.New("not implemented")
}
func (s *stubRepo) SaveSession(_ context.Context, _ domain.StrategySession) error {
	return errors.New("not implemented")
}
func (s *stubRepo) UpdateSession(_ context.Context, _ string, _ string, _ int, _ string, _ string) error {
	return errors.New("not implemented")
}
func (s *stubRepo) GetSession(_ context.Context, _ string, _ int64) (domain.StrategySession, error) {
	return domain.StrategySession{}, errors.New("not implemented")
}
func (s *stubRepo) ListSessions(_ context.Context, _, _ int64, _, _ int) ([]domain.StrategySession, error) {
	return nil, errors.New("not implemented")
}
func (s *stubRepo) ListSessionsPage(_ context.Context, _ repository.SessionListFilter) ([]domain.StrategySession, repository.PageMeta, error) {
	return nil, repository.PageMeta{}, errors.New("not implemented")
}
func (s *stubRepo) ListSessionSnapshots(_ context.Context, _ string, _ int64, limit, offset int) ([]domain.SnapshotRow, int64, bool, error) {
	s.lastSnapshotsLimit = limit
	s.lastSnapshotsOffset = offset
	if s.sessionSnapshots == nil {
		return nil, 0, false, nil
	}
	start := offset
	if start > len(s.sessionSnapshots) {
		start = len(s.sessionSnapshots)
	}
	end := start + limit
	if end > len(s.sessionSnapshots) {
		end = len(s.sessionSnapshots)
	}
	page := s.sessionSnapshots[start:end]
	hasMore := end < len(s.sessionSnapshots)
	return append([]domain.SnapshotRow(nil), page...), int64(len(s.sessionSnapshots)), hasMore, nil
}
func (s *stubRepo) ListReconciliationRuns(_ context.Context, _ string, _ int64, limit, offset int) ([]domain.ReconciliationRun, int64, bool, error) {
	s.lastReconciliationLimit = limit
	s.lastReconciliationOffset = offset
	start := offset
	if start > len(s.reconciliationRuns) {
		start = len(s.reconciliationRuns)
	}
	end := start + limit
	if end > len(s.reconciliationRuns) {
		end = len(s.reconciliationRuns)
	}
	page := s.reconciliationRuns[start:end]
	hasMore := end < len(s.reconciliationRuns)
	return append([]domain.ReconciliationRun(nil), page...), int64(len(s.reconciliationRuns)), hasMore, nil
}
func (s *stubRepo) GetSessionReconciliationSummary(_ context.Context, _ string, _ int64) (int64, int64, int64, error) {
	var hardFails, softFails int64
	for _, r := range s.reconciliationRuns {
		if !r.HardPass {
			hardFails++
		}
		if !r.SoftPass {
			softFails++
		}
	}
	return int64(len(s.reconciliationRuns)), hardFails, softFails, nil
}
func (s *stubRepo) ListRunningSessions(_ context.Context, _ string) ([]domain.StrategySession, error) {
	return nil, errors.New("not implemented")
}
func (s *stubRepo) MarkRuntimeSessionsRecoverable(_ context.Context, _ string, _ string) (int64, error) {
	return 0, errors.New("not implemented")
}
func (s *stubRepo) ListAccountStrategies(_ context.Context, _ int64) ([]domain.AccountStrategy, error) {
	return nil, errors.New("not implemented")
}
func (s *stubRepo) GetActiveStrategy(_ context.Context, _ int64) (domain.Strategy, error) {
	return domain.Strategy{}, errors.New("not implemented")
}
func (s *stubRepo) SaveReconciliationRun(_ context.Context, _ domain.ReconciliationRun) error {
	return nil
}
func (s *stubRepo) GetNotificationSettings(_ context.Context, userID int64) (domain.NotificationSettings, error) {
	if s.notificationSettings.UserID == 0 {
		return domain.NotificationSettings{UserID: userID, SystemEnabled: true, StrategyEnabled: true, CustomEnabled: true}, nil
	}
	return s.notificationSettings, nil
}
func (s *stubRepo) UpsertNotificationSettings(_ context.Context, settings domain.NotificationSettings) (domain.NotificationSettings, error) {
	s.notificationSettings = settings
	return settings, nil
}
func (s *stubRepo) GetNotificationChannel(_ context.Context, userID int64, channel string) (domain.NotificationChannel, error) {
	if s.notificationChannel.UserID == 0 {
		return domain.NotificationChannel{UserID: userID, Channel: channel, Status: domain.NotificationChannelStatusUnbound}, nil
	}
	return s.notificationChannel, nil
}
func (s *stubRepo) FindNotificationChannelByBindCodeHash(_ context.Context, codeHash string, _ time.Time) (domain.NotificationChannel, error) {
	if s.notificationChannel.BindCodeHash == codeHash && s.notificationChannel.Status == domain.NotificationChannelStatusPending {
		return s.notificationChannel, nil
	}
	return domain.NotificationChannel{}, repository.ErrNotFound
}
func (s *stubRepo) UpsertNotificationBindCode(_ context.Context, userID int64, channel string, codeHash string, expiresAt time.Time) (domain.NotificationChannel, error) {
	s.notificationChannel = domain.NotificationChannel{UserID: userID, Channel: channel, Status: domain.NotificationChannelStatusPending, BindCodeHash: codeHash, BindCodeExpiresAt: &expiresAt}
	return s.notificationChannel, nil
}
func (s *stubRepo) BindNotificationChannel(_ context.Context, userID int64, channel string, targetID string, targetType string, targetLabel string, now time.Time) (domain.NotificationChannel, error) {
	s.notificationChannel = domain.NotificationChannel{UserID: userID, Channel: channel, Status: domain.NotificationChannelStatusBound, TargetID: targetID, TargetType: targetType, TargetLabel: targetLabel, BoundAt: &now}
	return s.notificationChannel, nil
}
func (s *stubRepo) RevokeNotificationChannel(_ context.Context, userID int64, channel string, now time.Time) error {
	s.notificationChannel = domain.NotificationChannel{UserID: userID, Channel: channel, Status: domain.NotificationChannelStatusRevoked, RevokedAt: &now}
	return nil
}
func (s *stubRepo) UpdateNotificationDeliveryStatus(_ context.Context, _ int64, _ string, status string, errText string, _ time.Time) error {
	s.deliveryStatus = status
	s.deliveryError = errText
	return nil
}
func (s *stubRepo) GetNotificationPlan(_ context.Context, planCode string) (domain.NotificationPlan, error) {
	if s.notificationPlan.PlanCode == "" {
		return domain.NotificationPlan{PlanCode: planCode}, nil
	}
	return s.notificationPlan, nil
}

// Phase D2 (2026-05-06): market-data control-plane stub methods removed
// alongside the proto + repository methods. The control plane now lives
// in control-panel-service.

func TestCreateAccountStoresDescription(t *testing.T) {
	repo := &stubRepo{}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	resp, err := svc.CreateAccount(context.Background(), &accountv1.CreateAccountRequest{
		UserId:      serviceTestUserID,
		Name:        "  backtest-main  ",
		Mode:        int32(domain.AccountModeBacktest),
		Description: "  ETH backtest workspace  ",
	})
	if err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
	if got := repo.createdAccount.Description; got != "ETH backtest workspace" {
		t.Fatalf("stored description = %q", got)
	}
	if got := resp.GetDescription(); got != "ETH backtest workspace" {
		t.Fatalf("response description = %q", got)
	}
}

func TestListAndGetAccountsReturnDescription(t *testing.T) {
	repo := &stubRepo{account: domain.Account{
		AccountID:   1,
		UserID:      serviceTestUserID,
		Name:        "testnet",
		Description: "Binance testnet account",
		Mode:        domain.AccountModeBinanceTestnet,
		CreatedAt:   time.Now().UTC(),
	}}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	listResp, err := svc.ListAccounts(context.Background(), &accountv1.ListAccountsRequest{UserId: serviceTestUserID})
	if err != nil {
		t.Fatalf("ListAccounts: %v", err)
	}
	if len(listResp.GetAccounts()) != 1 {
		t.Fatalf("accounts len = %d", len(listResp.GetAccounts()))
	}
	if got := listResp.GetAccounts()[0].GetDescription(); got != "Binance testnet account" {
		t.Fatalf("list description = %q", got)
	}

	getResp, err := svc.GetAccount(context.Background(), &accountv1.GetAccountRequest{AccountId: 1, UserId: serviceTestUserID})
	if err != nil {
		t.Fatalf("GetAccount: %v", err)
	}
	if got := getResp.GetAccount().GetDescription(); got != "Binance testnet account" {
		t.Fatalf("get description = %q", got)
	}
}

func TestGetAccountMeta_success(t *testing.T) {
	repo := &stubRepo{account: domain.Account{
		AccountID:      1,
		UserID:         serviceTestUserID,
		Mode:           domain.AccountModeBinanceLive,
		MarginMode:     "isolated",
		PositionMode:   "hedge",
		APIKey:         "key-abc",
		APISecret:      "secret-xyz",
		DefaultFeeRate: 0.0002,
		SlippageBps:    5.0,
		CreatedAt:      time.Now(),
	}}
	svc := NewAccountGRPCService(repo, nil, nil, nil)
	resp, err := svc.GetAccountMeta(context.Background(), &accountv1.GetAccountMetaRequest{AccountId: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetAccountId() != 1 {
		t.Errorf("account_id: got %d", resp.GetAccountId())
	}
	if resp.GetMode() != 1 {
		t.Errorf("mode: got %d", resp.GetMode())
	}
	if resp.GetMarginMode() != "isolated" {
		t.Errorf("margin_mode: got %q", resp.GetMarginMode())
	}
	if resp.GetPositionMode() != "hedge" {
		t.Errorf("position_mode: got %q", resp.GetPositionMode())
	}
	if resp.GetApiKey() != "key-abc" {
		t.Errorf("api_key: got %q", resp.GetApiKey())
	}
	if resp.GetSlippageBps() != 5.0 {
		t.Errorf("slippage_bps: got %v", resp.GetSlippageBps())
	}
	if resp.GetDefaultFeeRate() != 0.0002 {
		t.Errorf("default_fee_rate: got %v", resp.GetDefaultFeeRate())
	}
	if resp.GetUserId() != serviceTestUserID {
		t.Errorf("user_id: got %d", resp.GetUserId())
	}
}

func TestGetAccountMeta_notFound(t *testing.T) {
	repo := &stubRepo{err: repository.ErrNotFound}
	svc := NewAccountGRPCService(repo, nil, nil, nil)
	_, err := svc.GetAccountMeta(context.Background(), &accountv1.GetAccountMetaRequest{AccountId: 999})
	if err == nil {
		t.Fatal("expected error")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.NotFound {
		t.Errorf("code: got %v, want NotFound", st.Code())
	}
}

func TestGetAccountMeta_emptyID(t *testing.T) {
	svc := NewAccountGRPCService(&stubRepo{}, nil, nil, nil)
	_, err := svc.GetAccountMeta(context.Background(), &accountv1.GetAccountMetaRequest{AccountId: 0})
	if err == nil {
		t.Fatal("expected error for zero account_id")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("code: got %v, want InvalidArgument", st.Code())
	}
}

func TestGetAccountMeta_repoError(t *testing.T) {
	repo := &stubRepo{err: errors.New("db unavailable")}
	svc := NewAccountGRPCService(repo, nil, nil, nil)
	_, err := svc.GetAccountMeta(context.Background(), &accountv1.GetAccountMetaRequest{AccountId: 2})
	if err == nil {
		t.Fatal("expected error")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Unavailable {
		t.Errorf("code: got %v, want Unavailable", st.Code())
	}
}

func TestListReconciliationRuns_success(t *testing.T) {
	repo := &stubRepo{
		reconciliationRuns: []domain.ReconciliationRun{
			{
				Time:           time.Unix(1713441600, 0).UTC(),
				RunID:          "run-1",
				AccountID:      101,
				UserID:         serviceTestUserID,
				SessionID:      "sess-1",
				StrategyID:     202,
				Mode:           domain.AccountModeBinanceTestnet,
				SnapshotReason: domain.SnapshotReasonPeriodicSample,
				RunType:        domain.ReconciliationRunSampled,
				LocalSnapshot: domain.OnlineAccountInfo{
					AccountID:  101,
					Mode:       domain.AccountModeBinanceTestnet,
					TotalValue: 1000,
				},
				ExchangeSnapshot: domain.OnlineAccountInfo{
					AccountID:  101,
					Mode:       domain.AccountModeBinanceTestnet,
					TotalValue: 1000.5,
				},
				FieldDiffs: []domain.FieldDiff{{
					Field:     "futures.wallet_balance",
					Severity:  domain.FieldDiffSoft,
					Exchange:  1000.5,
					Local:     1000,
					DiffAbs:   0.5,
					DiffRatio: 0.0005,
					Threshold: map[string]any{"abs": 0.01, "ratio": 0.0002},
					Passed:    false,
				}},
				AdvisoryDiffs: []domain.FieldDiff{{
					Field:     "futures.positions[BTCUSDT:BOTH].mark_price",
					Severity:  domain.FieldDiffAdvisory,
					Exchange:  65000,
					Local:     64999.5,
					DiffAbs:   0.5,
					DiffRatio: 0.0000077,
					Threshold: map[string]any{"rule": "drift_only"},
					Passed:    true,
				}},
				HardPass: true,
				SoftPass: false,
			},
		},
	}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	resp, err := svc.ListReconciliationRuns(context.Background(), &accountv1.ListReconciliationRunsRequest{
		SessionId: "sess-1",
		UserId:    serviceTestUserID,
		Limit:     50,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := len(resp.GetItems()); got != 1 {
		t.Fatalf("runs len: got %d, want 1", got)
	}
	run := resp.GetItems()[0]
	if run.GetRunId() != "run-1" {
		t.Errorf("run_id: got %q", run.GetRunId())
	}
	if run.GetRunType() != "sampled" {
		t.Errorf("run_type: got %q", run.GetRunType())
	}
	if run.GetSnapshotReason() != int32(domain.SnapshotReasonPeriodicSample) {
		t.Errorf("snapshot_reason: got %d", run.GetSnapshotReason())
	}
	if run.GetSoftPass() {
		t.Error("soft_pass: got true, want false")
	}
	if got := len(run.GetFieldDiffs()); got != 1 {
		t.Fatalf("field_diffs len: got %d, want 1", got)
	}
	if got := run.GetFieldDiffs()[0].GetThresholdJson(); got == "" {
		t.Error("threshold_json: expected non-empty JSON")
	}
	if got := len(run.GetAdvisoryDiffs()); got != 1 {
		t.Fatalf("advisory_diffs len: got %d, want 1", got)
	}
	if run.GetLocalSnapshotJson() == "" || run.GetExchangeSnapshotJson() == "" {
		t.Error("expected snapshot json payloads")
	}
}

func TestListReconciliationRuns_invalidRequest(t *testing.T) {
	svc := NewAccountGRPCService(&stubRepo{}, nil, nil, nil)

	_, err := svc.ListReconciliationRuns(context.Background(), &accountv1.ListReconciliationRunsRequest{
		SessionId: "",
		UserId:    serviceTestUserID,
	})
	if err == nil {
		t.Fatal("expected error for empty session_id")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("code: got %v, want InvalidArgument", st.Code())
	}
}

// ── paginate-session-detail-lists pagination coverage ─────────────────────

func makeReconciliationRuns(n int) []domain.ReconciliationRun {
	out := make([]domain.ReconciliationRun, n)
	for i := range out {
		out[i] = domain.ReconciliationRun{
			Time:           time.Unix(int64(1_700_000_000+i*60), 0).UTC(),
			RunID:          fmt.Sprintf("run-%d", i),
			AccountID:      101,
			UserID:         serviceTestUserID,
			SessionID:      "sess-pagination",
			StrategyID:     202,
			Mode:           domain.AccountModeBinanceTestnet,
			SnapshotReason: domain.SnapshotReasonPeriodicSample,
			RunType:        domain.ReconciliationRunSampled,
			HardPass:       true,
			SoftPass:       true,
		}
	}
	return out
}

func makeSnapshots(n int) []domain.SnapshotRow {
	out := make([]domain.SnapshotRow, n)
	for i := range out {
		out[i] = domain.SnapshotRow{
			Time:           time.Unix(int64(1_700_000_000+i*60), 0).UTC(),
			AccountID:      101,
			SnapshotReason: domain.SnapshotReasonPeriodicSample,
			TotalValue:     10000 + float64(i),
			WalletBalance:  10000,
			SessionID:      "sess-pagination",
			StrategyID:     202,
			FuturesJSON:    "{}",
			SpotJSON:       "{}",
		}
	}
	return out
}

func TestListReconciliationRuns_defaultPagingReturns20Newest(t *testing.T) {
	repo := &stubRepo{reconciliationRuns: makeReconciliationRuns(55)}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	resp, err := svc.ListReconciliationRuns(context.Background(), &accountv1.ListReconciliationRunsRequest{
		SessionId: "sess-pagination",
		UserId:    serviceTestUserID,
		// No limit / offset → default 20 / 0.
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if repo.lastReconciliationLimit != 20 {
		t.Errorf("repo limit = %d, want 20 (default)", repo.lastReconciliationLimit)
	}
	if repo.lastReconciliationOffset != 0 {
		t.Errorf("repo offset = %d, want 0 (default)", repo.lastReconciliationOffset)
	}
	if got := len(resp.GetItems()); got != 20 {
		t.Errorf("items len = %d, want 20", got)
	}
	if resp.GetNextOffset() != 20 {
		t.Errorf("next_offset = %d, want 20", resp.GetNextOffset())
	}
	if !resp.GetHasMore() {
		t.Errorf("has_more = false, want true (55 total vs 20 returned)")
	}
	if resp.GetTotal() != 55 {
		t.Errorf("total = %d, want 55 (session-wide count regardless of page)", resp.GetTotal())
	}
}

func TestListReconciliationRuns_lastPageReportsHasMoreFalse(t *testing.T) {
	repo := &stubRepo{reconciliationRuns: makeReconciliationRuns(35)}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	// Offset 20 + limit 20 → second page returns 15 items and no more.
	resp, err := svc.ListReconciliationRuns(context.Background(), &accountv1.ListReconciliationRunsRequest{
		SessionId: "sess-pagination",
		UserId:    serviceTestUserID,
		Limit:     20,
		Offset:    20,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := len(resp.GetItems()); got != 15 {
		t.Errorf("items len = %d, want 15 (remainder of 35 after offset 20)", got)
	}
	if resp.GetNextOffset() != 35 {
		t.Errorf("next_offset = %d, want 35", resp.GetNextOffset())
	}
	if resp.GetHasMore() {
		t.Errorf("has_more = true, want false")
	}
	if resp.GetTotal() != 35 {
		t.Errorf("total = %d, want 35 (session-wide count is stable across pages)", resp.GetTotal())
	}
}

func TestListReconciliationRuns_oversizedLimitClamped(t *testing.T) {
	repo := &stubRepo{reconciliationRuns: makeReconciliationRuns(300)}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	resp, err := svc.ListReconciliationRuns(context.Background(), &accountv1.ListReconciliationRunsRequest{
		SessionId: "sess-pagination",
		UserId:    serviceTestUserID,
		Limit:     99999,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if repo.lastReconciliationLimit != 200 {
		t.Errorf("repo limit = %d, want 200 (clamp)", repo.lastReconciliationLimit)
	}
	if got := len(resp.GetItems()); got != 200 {
		t.Errorf("items len = %d, want 200 (clamp)", got)
	}
}

func TestListSessionSnapshots_defaultPagingReturns20(t *testing.T) {
	repo := &stubRepo{sessionSnapshots: makeSnapshots(42)}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	resp, err := svc.ListSessionSnapshots(context.Background(), &accountv1.ListSessionSnapshotsRequest{
		SessionId: "sess-pagination",
		UserId:    serviceTestUserID,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if repo.lastSnapshotsLimit != 20 || repo.lastSnapshotsOffset != 0 {
		t.Errorf("repo paging = (limit=%d, offset=%d), want (20, 0)",
			repo.lastSnapshotsLimit, repo.lastSnapshotsOffset)
	}
	if got := len(resp.GetItems()); got != 20 {
		t.Errorf("items len = %d, want 20", got)
	}
	if resp.GetNextOffset() != 20 {
		t.Errorf("next_offset = %d, want 20", resp.GetNextOffset())
	}
	if !resp.GetHasMore() {
		t.Errorf("has_more = false, want true (42 total > 20 returned)")
	}
	if resp.GetTotal() != 42 {
		t.Errorf("total = %d, want 42", resp.GetTotal())
	}
}

func TestListSessionSnapshots_secondPageNextOffsetAdvances(t *testing.T) {
	repo := &stubRepo{sessionSnapshots: makeSnapshots(42)}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	resp, err := svc.ListSessionSnapshots(context.Background(), &accountv1.ListSessionSnapshotsRequest{
		SessionId: "sess-pagination",
		UserId:    serviceTestUserID,
		Limit:     20,
		Offset:    20,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := len(resp.GetItems()); got != 20 {
		t.Errorf("items len = %d, want 20", got)
	}
	if resp.GetNextOffset() != 40 {
		t.Errorf("next_offset = %d, want 40", resp.GetNextOffset())
	}
	if !resp.GetHasMore() {
		t.Errorf("has_more = false, want true (42 total > 40 returned so far)")
	}
	if resp.GetTotal() != 42 {
		t.Errorf("total = %d, want 42 (session-wide count is stable across pages)", resp.GetTotal())
	}
}

// ── GetSessionReconciliationSummary ───────────────────────────────────────

func TestGetSessionReconciliationSummary_mixedPassFail(t *testing.T) {
	// 5 runs total: 2 hard fails, 3 soft fails.
	runs := makeReconciliationRuns(5)
	runs[0].HardPass = false
	runs[1].HardPass = false
	runs[1].SoftPass = false
	runs[2].SoftPass = false
	runs[3].SoftPass = false

	repo := &stubRepo{reconciliationRuns: runs}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	resp, err := svc.GetSessionReconciliationSummary(context.Background(), &accountv1.GetSessionReconciliationSummaryRequest{
		SessionId: "sess-pagination",
		UserId:    serviceTestUserID,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetTotalRuns() != 5 {
		t.Errorf("total_runs = %d, want 5", resp.GetTotalRuns())
	}
	if resp.GetHardFailRuns() != 2 {
		t.Errorf("hard_fail_runs = %d, want 2", resp.GetHardFailRuns())
	}
	if resp.GetSoftFailRuns() != 3 {
		t.Errorf("soft_fail_runs = %d, want 3", resp.GetSoftFailRuns())
	}
}

func TestGetSessionReconciliationSummary_emptySessionAllZeros(t *testing.T) {
	repo := &stubRepo{}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	resp, err := svc.GetSessionReconciliationSummary(context.Background(), &accountv1.GetSessionReconciliationSummaryRequest{
		SessionId: "sess-empty",
		UserId:    serviceTestUserID,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetTotalRuns() != 0 || resp.GetHardFailRuns() != 0 || resp.GetSoftFailRuns() != 0 {
		t.Errorf("expected all zeros for empty session, got total=%d hard=%d soft=%d",
			resp.GetTotalRuns(), resp.GetHardFailRuns(), resp.GetSoftFailRuns())
	}
}

func TestGetSessionReconciliationSummary_missingUserID(t *testing.T) {
	svc := NewAccountGRPCService(&stubRepo{}, nil, nil, nil)

	_, err := svc.GetSessionReconciliationSummary(context.Background(), &accountv1.GetSessionReconciliationSummaryRequest{
		SessionId: "sess-1",
		// UserId omitted → should be rejected like every other user-scoped RPC.
	})
	if err == nil {
		t.Fatal("expected error for missing user_id")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Unauthenticated && st.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want Unauthenticated or InvalidArgument", st.Code())
	}
}

func TestGetSessionReconciliationSummary_emptySessionID(t *testing.T) {
	svc := NewAccountGRPCService(&stubRepo{}, nil, nil, nil)

	_, err := svc.GetSessionReconciliationSummary(context.Background(), &accountv1.GetSessionReconciliationSummaryRequest{
		SessionId: "",
		UserId:    serviceTestUserID,
	})
	if err == nil {
		t.Fatal("expected error for empty session_id")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", st.Code())
	}
}
