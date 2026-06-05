package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/gen/accountv1"
	"github.com/hushine-tech/core-service/internal/credential"
	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/exchange/adapter"
	"github.com/hushine-tech/core-service/internal/repository"
	"github.com/hushine-tech/core-service/internal/venuekeys"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const serviceTestUserID int64 = 7

// stubRepo is a minimal Repository stub for unit tests.
type stubRepo struct {
	account              domain.Account
	createdAccount       domain.Account
	createAccountErr     error
	venues               []domain.Venue
	createdVenues        []domain.Venue
	routeMeta            domain.VenueRouteMeta
	activeSessionCount   int64
	state                domain.OnlineAccountInfo
	stateErr             error
	venueStates          map[int64]domain.OnlineAccountInfo
	reconciliationRuns   []domain.ReconciliationRun
	sessionSnapshots     []domain.SnapshotRow
	notificationSettings domain.NotificationSettings
	notificationChannel  domain.NotificationChannel
	notificationPlan     domain.NotificationPlan
	deliveryStatus       string
	deliveryError        string
	snapshotTimes        []time.Time

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

type preflightSessionRepo struct {
	stubRepo
	sessions map[string]domain.StrategySession
}

func newPreflightSessionRepo(account domain.Account) *preflightSessionRepo {
	return &preflightSessionRepo{
		stubRepo: stubRepo{account: account},
		sessions: make(map[string]domain.StrategySession),
	}
}

func (r *preflightSessionRepo) SaveSession(_ context.Context, s domain.StrategySession) error {
	if s.CreatedAt.IsZero() {
		s.CreatedAt = time.Now().UTC()
	}
	r.sessions[s.SessionID] = s
	return nil
}

func (r *preflightSessionRepo) GetSession(_ context.Context, sessionID string, userID int64) (domain.StrategySession, error) {
	s, ok := r.sessions[sessionID]
	if !ok {
		return domain.StrategySession{}, repository.ErrNotFound
	}
	if userID > 0 && s.UserID != userID {
		return domain.StrategySession{}, repository.ErrNotFound
	}
	return s, nil
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
	if s.createAccountErr != nil {
		return 0, s.createAccountErr
	}
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
	if s.err != nil {
		return domain.Venue{}, s.err
	}
	if venue.VenueID == 0 {
		venue.VenueID = int64(len(s.createdVenues) + len(s.venues) + 1)
	}
	if venue.CreatedAt.IsZero() {
		venue.CreatedAt = time.Now().UTC()
	}
	if venue.UpdatedAt.IsZero() {
		venue.UpdatedAt = venue.CreatedAt
	}
	s.createdVenues = append(s.createdVenues, venue)
	s.venues = append(s.venues, venue)
	return venue, s.err
}
func (s *stubRepo) GetVenue(_ context.Context, venueID int64, userID int64) (domain.Venue, error) {
	for _, venue := range s.venues {
		if venue.VenueID == venueID && (userID == 0 || venue.UserID == userID) {
			return venue, nil
		}
	}
	return domain.Venue{}, repository.ErrNotFound
}
func (s *stubRepo) ListVenues(_ context.Context, userID int64, accountID int64, includeUnbound bool, includeInactive bool, _ int, _ int) ([]domain.Venue, repository.PageMeta, error) {
	var out []domain.Venue
	for _, venue := range s.venues {
		if userID > 0 && venue.UserID != userID {
			continue
		}
		if accountID > 0 {
			if venue.AccountID == nil || *venue.AccountID != accountID {
				continue
			}
		} else if !includeUnbound && venue.AccountID == nil {
			continue
		}
		if !includeInactive && venue.Status != domain.VenueStatusActive {
			continue
		}
		out = append(out, venue)
	}
	return out, repository.PageMeta{Total: int64(len(out))}, nil
}
func (s *stubRepo) BindVenue(_ context.Context, userID int64, accountID int64, venueID int64, _ string) (domain.Venue, error) {
	account, err := s.GetAccount(context.Background(), accountID, userID)
	if err != nil {
		return domain.Venue{}, err
	}
	for i, venue := range s.venues {
		if venue.VenueID == venueID && venue.UserID == userID {
			if venue.Environment != account.Environment {
				return domain.Venue{}, repository.ErrConflict
			}
			venue.AccountID = &accountID
			s.venues[i] = venue
			return venue, nil
		}
	}
	return domain.Venue{}, repository.ErrNotFound
}
func (s *stubRepo) ReleaseVenue(_ context.Context, userID int64, venueID int64, _ string) (domain.Venue, error) {
	for i, venue := range s.venues {
		if venue.VenueID == venueID && venue.UserID == userID {
			venue.AccountID = nil
			s.venues[i] = venue
			return venue, nil
		}
	}
	return domain.Venue{}, repository.ErrNotFound
}
func (s *stubRepo) ArchiveVenue(_ context.Context, userID int64, venueID int64, _ string) error {
	for i, venue := range s.venues {
		if venue.VenueID == venueID && venue.UserID == userID {
			s.venues[i].Status = domain.VenueStatusArchived
			return nil
		}
	}
	return repository.ErrNotFound
}
func (s *stubRepo) ListActiveAccountVenues(_ context.Context, userID int64, accountID int64) ([]domain.Venue, error) {
	var out []domain.Venue
	for _, venue := range s.venues {
		if venue.UserID == userID && venue.AccountID != nil && *venue.AccountID == accountID && venue.Status == domain.VenueStatusActive {
			out = append(out, venue)
		}
	}
	return out, nil
}
func (s *stubRepo) CountActiveSessionsForAccount(_ context.Context, _ int64, _ int64) (int64, error) {
	return s.activeSessionCount, nil
}
func (s *stubRepo) SaveSessionVenues(_ context.Context, _ string, _ []domain.Venue) error {
	return errors.New("not implemented")
}
func (s *stubRepo) ResolveVenueRouteMeta(_ context.Context, accountID int64, exchange domain.Exchange, market domain.Market) (domain.VenueRouteMeta, error) {
	if s.routeMeta.AccountID != 0 {
		if s.routeMeta.AccountID == accountID && s.routeMeta.Exchange == exchange && s.routeMeta.Market == market {
			return s.routeMeta, nil
		}
		return domain.VenueRouteMeta{}, repository.ErrNotFound
	}
	for _, venue := range s.venues {
		if venue.AccountID != nil && *venue.AccountID == accountID && venue.Exchange == exchange && venue.Market == market && venue.Status == domain.VenueStatusActive {
			return domain.VenueRouteMeta{
				AccountID:      accountID,
				VenueID:        venue.VenueID,
				UserID:         venue.UserID,
				Environment:    venue.Environment,
				Exchange:       venue.Exchange,
				Market:         venue.Market,
				MarginMode:     venue.MarginMode,
				PositionMode:   venue.PositionMode,
				APIKey:         venue.APIKey,
				CredentialInfo: venue.CredentialInfo,
			}, nil
		}
	}
	return domain.VenueRouteMeta{}, repository.ErrNotFound
}
func (s *stubRepo) UpdateAccountState(_ context.Context, info domain.OnlineAccountInfo) error {
	s.state = info
	return nil
}
func (s *stubRepo) GetAccountState(_ context.Context, _ int64) (domain.OnlineAccountInfo, error) {
	if s.stateErr != nil {
		return domain.OnlineAccountInfo{}, s.stateErr
	}
	return s.state, s.err
}
func (s *stubRepo) UpsertVenueWalletState(_ context.Context, venue domain.Venue, info domain.OnlineAccountInfo) error {
	if s.venueStates == nil {
		s.venueStates = map[int64]domain.OnlineAccountInfo{}
	}
	s.venueStates[venue.VenueID] = info
	return nil
}
func (s *stubRepo) GetVenueWalletState(_ context.Context, venueID int64, _ int64) (domain.OnlineAccountInfo, error) {
	if s.venueStates == nil {
		return domain.OnlineAccountInfo{}, repository.ErrNotFound
	}
	info, ok := s.venueStates[venueID]
	if !ok {
		return domain.OnlineAccountInfo{}, repository.ErrNotFound
	}
	return info, nil
}
func (s *stubRepo) SaveSnapshot(_ context.Context, _ int64, _ domain.SnapshotReason, _ int64, _ string, snapshotTime time.Time) error {
	s.snapshotTimes = append(s.snapshotTimes, snapshotTime)
	return nil
}

type preflightFactory struct {
	symbolRulesReader adapter.SymbolRulesReader
	symbolRulesErr    error
}

type preflightSymbolRulesReader struct {
	rules adapter.SymbolRules
	err   error
}

func (r preflightSymbolRulesReader) ReadSymbolRules(_ context.Context, _ adapter.SymbolRulesRequest) (adapter.SymbolRules, error) {
	if r.err != nil {
		return adapter.SymbolRules{}, r.err
	}
	return r.rules, nil
}

func (f preflightFactory) CredentialValidator() (adapter.CredentialValidator, error) {
	return nil, errors.New("not implemented")
}
func (f preflightFactory) AccountSnapshotReader() (adapter.AccountSnapshotReader, error) {
	return nil, errors.New("not implemented")
}
func (f preflightFactory) SymbolRulesReader() (adapter.SymbolRulesReader, error) {
	if f.symbolRulesErr != nil {
		return nil, f.symbolRulesErr
	}
	return f.symbolRulesReader, nil
}
func (f preflightFactory) OrderExecutor() (adapter.OrderExecutor, error) {
	return nil, errors.New("not implemented")
}
func (f preflightFactory) OrderStateReader() (adapter.OrderStateReader, error) {
	return nil, errors.New("not implemented")
}
func (f preflightFactory) OrderCanceller() (adapter.OrderCanceller, error) {
	return nil, errors.New("not implemented")
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
		Environment: int32(domain.EnvironmentBacktest),
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

func TestCreateBacktestAccountCreatesSimulatedPerpetualFuturesVenueOnly(t *testing.T) {
	repo := &stubRepo{}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	resp, err := svc.CreateAccount(context.Background(), &accountv1.CreateAccountRequest{
		UserId:      serviceTestUserID,
		Name:        "backtest-main",
		Environment: int32(domain.EnvironmentBacktest),
		Description: "simulation",
	})
	if err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
	if resp.GetEnvironment() != int32(domain.EnvironmentBacktest) {
		t.Fatalf("response environment = %d", resp.GetEnvironment())
	}
	if got := repo.createdAccount.Environment; got != domain.EnvironmentBacktest {
		t.Fatalf("account environment = %v", got)
	}
	if len(repo.createdVenues) != 1 {
		t.Fatalf("created venues len = %d, want 1", len(repo.createdVenues))
	}
	byMarket := map[domain.Market]domain.Venue{}
	for _, venue := range repo.createdVenues {
		byMarket[venue.Market] = venue
		if venue.AccountID == nil || *venue.AccountID != resp.GetAccountId() {
			t.Fatalf("venue account_id = %v, want %d", venue.AccountID, resp.GetAccountId())
		}
		if venue.Exchange != domain.ExchangeBinance {
			t.Fatalf("venue exchange = %v", venue.Exchange)
		}
		if venue.Environment != domain.EnvironmentBacktest || venue.Status != domain.VenueStatusActive {
			t.Fatalf("venue env/status = %v/%v", venue.Environment, venue.Status)
		}
		if !venuekeys.IsBacktestAPIKey(venue.APIKey) {
			t.Fatalf("backtest venue api_key = %q, want synthetic key", venue.APIKey)
		}
		if venue.CredentialInfo != "" {
			t.Fatalf("backtest venue credential_info = %q, want empty", venue.CredentialInfo)
		}
		if venue.CredentialKeyVersion != "synthetic" {
			t.Fatalf("backtest venue credential_key_version = %q, want synthetic", venue.CredentialKeyVersion)
		}
		if venue.CredentialFingerprint != "" {
			t.Fatalf("backtest venue credential_fingerprint = %q, want empty", venue.CredentialFingerprint)
		}
	}
	if _, ok := byMarket[domain.MarketPerpetualFutures]; !ok {
		t.Fatal("missing simulated perpetual futures venue")
	}
	if _, ok := byMarket[domain.MarketSpot]; ok {
		t.Fatal("simulated spot venue should not be created until spot execution is supported")
	}
	if len(repo.venueStates) != 1 {
		t.Fatalf("venue wallet states len = %d, want 1", len(repo.venueStates))
	}
	perpVenue := byMarket[domain.MarketPerpetualFutures]
	state := repo.venueStates[perpVenue.VenueID]
	if state.AccountID != resp.GetAccountId() || state.Environment != domain.EnvironmentBacktest {
		t.Fatalf("venue wallet state = %+v, want backtest account %d", state, resp.GetAccountId())
	}
	if state.Futures.MarginMode != "cross" || state.Futures.PositionMode != "one_way" {
		t.Fatalf("state futures modes = %q/%q, want cross/one_way", state.Futures.MarginMode, state.Futures.PositionMode)
	}
	if state.WalletBalance != 0 || state.AvailableBalance != 0 || state.TotalValue != 0 {
		t.Fatalf("state balances = total:%v wallet:%v available:%v, want zero",
			state.TotalValue, state.WalletBalance, state.AvailableBalance)
	}
}

func TestCreateBacktestAccountSyntheticKeyFailureDoesNotPersistAccount(t *testing.T) {
	original := newBacktestVenueAPIKey
	newBacktestVenueAPIKey = func() (string, error) {
		return "", errors.New("rng unavailable")
	}
	defer func() {
		newBacktestVenueAPIKey = original
	}()

	repo := &stubRepo{}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	_, err := svc.CreateAccount(context.Background(), &accountv1.CreateAccountRequest{
		UserId:      serviceTestUserID,
		Name:        "backtest-main",
		Environment: int32(domain.EnvironmentBacktest),
		Description: "simulation",
	})
	if status.Code(err) != codes.Internal {
		t.Fatalf("CreateAccount error = %v, want Internal", err)
	}
	if repo.createdAccount.Name != "" || len(repo.createdVenues) != 0 || repo.state.AccountID != 0 || len(repo.venueStates) != 0 {
		t.Fatalf("synthetic key failure must not write state: account=%+v venues=%+v state=%+v venue_states=%+v",
			repo.createdAccount, repo.createdVenues, repo.state, repo.venueStates)
	}
}

func TestCreateAccountConflictReturnsAlreadyExists(t *testing.T) {
	repo := &stubRepo{createAccountErr: repository.ErrConflict}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	_, err := svc.CreateAccount(context.Background(), &accountv1.CreateAccountRequest{
		Name:        "duplicate",
		Environment: int32(domain.EnvironmentDemo),
		UserId:      serviceTestUserID,
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("CreateAccount code = %v, want AlreadyExists (err=%v)", status.Code(err), err)
	}
	if !strings.Contains(status.Convert(err).Message(), "duplicate") {
		t.Fatalf("CreateAccount message = %q, want account name", status.Convert(err).Message())
	}
}

func TestCreateDemoVenueEncryptsCredentials(t *testing.T) {
	mgr, err := credential.NewManager("12345678901234567890123456789012", "test-v1")
	if err != nil {
		t.Fatalf("credential manager: %v", err)
	}
	repo := &stubRepo{}
	svc := NewAccountGRPCService(repo, nil, nil, nil, WithCredentialManager(mgr))

	resp, err := svc.CreateVenue(context.Background(), &accountv1.CreateVenueRequest{
		UserId:         serviceTestUserID,
		Environment:    int32(domain.EnvironmentDemo),
		Exchange:       int32(domain.ExchangeBinance),
		Market:         int32(domain.MarketPerpetualFutures),
		Status:         int32(domain.VenueStatusActive),
		DisplayName:    "Binance testnet",
		ApiKey:         " api-key-1 ",
		CredentialJson: `{"api_secret":"secret-1"}`,
		MarginMode:     int32(domain.MarginModeCross),
		PositionMode:   int32(domain.PositionModeOneWay),
	})
	if err != nil {
		t.Fatalf("CreateVenue: %v", err)
	}
	expectedFingerprint := credential.Fingerprint("api-key-1")
	if resp.GetVenue().GetCredentialFingerprint() != expectedFingerprint {
		t.Fatalf("response credential_fingerprint = %q, want %q",
			resp.GetVenue().GetCredentialFingerprint(), expectedFingerprint)
	}
	if len(repo.createdVenues) != 1 {
		t.Fatalf("created venues len = %d, want 1", len(repo.createdVenues))
	}
	stored := repo.createdVenues[0]
	if stored.CredentialFingerprint != expectedFingerprint {
		t.Fatalf("stored credential_fingerprint = %q, want %q",
			stored.CredentialFingerprint, expectedFingerprint)
	}
	if stored.CredentialInfo == "" || stored.CredentialInfo == `{"api_secret":"secret-1"}` {
		t.Fatalf("credential_info was not encrypted: %q", stored.CredentialInfo)
	}
	plain, err := mgr.Decrypt(stored.CredentialInfo)
	if err != nil {
		t.Fatalf("decrypt stored credential: %v", err)
	}
	if plain != `{"api_secret":"secret-1"}` {
		t.Fatalf("decrypted credential = %q", plain)
	}
	if stored.APIKey != "api-key-1" {
		t.Fatalf("api_key = %q", stored.APIKey)
	}
}

func TestCreateBacktestVenueRejectsCallerAPIKey(t *testing.T) {
	repo := &stubRepo{}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	_, err := svc.CreateVenue(context.Background(), &accountv1.CreateVenueRequest{
		UserId:       serviceTestUserID,
		Environment:  int32(domain.EnvironmentBacktest),
		Exchange:     int32(domain.ExchangeBinance),
		Market:       int32(domain.MarketPerpetualFutures),
		ApiKey:       "user-supplied-key",
		MarginMode:   int32(domain.MarginModeCross),
		PositionMode: int32(domain.PositionModeOneWay),
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateVenue error = %v, want InvalidArgument", err)
	}
	if len(repo.createdVenues) != 0 {
		t.Fatalf("created venues len = %d, want 0", len(repo.createdVenues))
	}
}

func TestCreateBacktestVenueRejectsCallerCredentialJSON(t *testing.T) {
	repo := &stubRepo{}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	_, err := svc.CreateVenue(context.Background(), &accountv1.CreateVenueRequest{
		UserId:         serviceTestUserID,
		Environment:    int32(domain.EnvironmentBacktest),
		Exchange:       int32(domain.ExchangeBinance),
		Market:         int32(domain.MarketPerpetualFutures),
		CredentialJson: `{}`,
		MarginMode:     int32(domain.MarginModeCross),
		PositionMode:   int32(domain.PositionModeOneWay),
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateVenue error = %v, want InvalidArgument", err)
	}
	if len(repo.createdVenues) != 0 {
		t.Fatalf("created venues len = %d, want 0", len(repo.createdVenues))
	}
}

func TestCreateBacktestVenueGeneratesSyntheticAPIKey(t *testing.T) {
	accountID := int64(42)
	repo := &stubRepo{account: domain.Account{
		AccountID:   accountID,
		UserID:      serviceTestUserID,
		Environment: domain.EnvironmentBacktest,
		Status:      domain.AccountStatusActive,
	}}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	resp, err := svc.CreateVenue(context.Background(), &accountv1.CreateVenueRequest{
		UserId:       serviceTestUserID,
		AccountId:    accountID,
		Environment:  int32(domain.EnvironmentBacktest),
		Exchange:     int32(domain.ExchangeBinance),
		Market:       int32(domain.MarketPerpetualFutures),
		MarginMode:   int32(domain.MarginModeCross),
		PositionMode: int32(domain.PositionModeOneWay),
	})
	if err != nil {
		t.Fatalf("CreateVenue: %v", err)
	}
	if len(repo.createdVenues) != 1 {
		t.Fatalf("created venues len = %d, want 1", len(repo.createdVenues))
	}
	stored := repo.createdVenues[0]
	if !venuekeys.IsBacktestAPIKey(stored.APIKey) {
		t.Fatalf("stored api_key = %q, want synthetic", stored.APIKey)
	}
	if resp.GetVenue().GetApiKey() != stored.APIKey {
		t.Fatalf("response api_key = %q, want stored %q", resp.GetVenue().GetApiKey(), stored.APIKey)
	}
	if stored.CredentialInfo != "" || stored.CredentialKeyVersion != "synthetic" || stored.CredentialFingerprint != "" {
		t.Fatalf("stored credential fields = info:%q version:%q fingerprint:%q",
			stored.CredentialInfo, stored.CredentialKeyVersion, stored.CredentialFingerprint)
	}
}

func TestCreateUnboundBacktestVenueInitializesWalletState(t *testing.T) {
	repo := &stubRepo{}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	resp, err := svc.CreateVenue(context.Background(), &accountv1.CreateVenueRequest{
		UserId:       serviceTestUserID,
		Environment:  int32(domain.EnvironmentBacktest),
		Exchange:     int32(domain.ExchangeBinance),
		Market:       int32(domain.MarketPerpetualFutures),
		MarginMode:   int32(domain.MarginModeCross),
		PositionMode: int32(domain.PositionModeOneWay),
	})
	if err != nil {
		t.Fatalf("CreateVenue: %v", err)
	}
	if resp.GetVenue().GetAccountId() != 0 {
		t.Fatalf("response account_id = %d, want unbound", resp.GetVenue().GetAccountId())
	}
	if len(repo.createdVenues) != 1 {
		t.Fatalf("created venues len = %d, want 1", len(repo.createdVenues))
	}
	if repo.createdVenues[0].AccountID != nil {
		t.Fatalf("created venue account_id = %v, want nil", *repo.createdVenues[0].AccountID)
	}
	if len(repo.venueStates) != 1 {
		t.Fatalf("venue wallet states len = %d, want 1", len(repo.venueStates))
	}
	state := repo.venueStates[repo.createdVenues[0].VenueID]
	if state.AccountID != 0 {
		t.Fatalf("wallet state account_id = %d, want 0 for unbound venue", state.AccountID)
	}
	if state.Futures.MarginMode != "cross" || state.Futures.PositionMode != "one_way" {
		t.Fatalf("state futures modes = %q/%q, want cross/one_way", state.Futures.MarginMode, state.Futures.PositionMode)
	}
}

func TestCreateBoundBacktestVenueInitializesWalletState(t *testing.T) {
	accountID := int64(42)
	repo := &stubRepo{
		account: domain.Account{
			AccountID:   accountID,
			UserID:      serviceTestUserID,
			Environment: domain.EnvironmentBacktest,
			Status:      domain.AccountStatusActive,
		},
	}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	_, err := svc.CreateVenue(context.Background(), &accountv1.CreateVenueRequest{
		UserId:       serviceTestUserID,
		AccountId:    accountID,
		Environment:  int32(domain.EnvironmentBacktest),
		Exchange:     int32(domain.ExchangeBinance),
		Market:       int32(domain.MarketPerpetualFutures),
		MarginMode:   int32(domain.MarginModeCross),
		PositionMode: int32(domain.PositionModeOneWay),
	})
	if err != nil {
		t.Fatalf("CreateVenue: %v", err)
	}
	if len(repo.venueStates) != 1 {
		t.Fatalf("venue wallet states len = %d, want 1", len(repo.venueStates))
	}
	venueID := repo.createdVenues[0].VenueID
	state := repo.venueStates[venueID]
	if state.AccountID != accountID || state.Environment != domain.EnvironmentBacktest {
		t.Fatalf("state = %+v, want backtest account %d", state, accountID)
	}
	if state.Futures.MarginMode != "cross" || state.Futures.PositionMode != "one_way" {
		t.Fatalf("state futures modes = %q/%q, want cross/one_way", state.Futures.MarginMode, state.Futures.PositionMode)
	}
}

func TestCreateBacktestVenueUsesBootstrapWalletState(t *testing.T) {
	repo := &stubRepo{}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	resp, err := svc.CreateVenue(context.Background(), &accountv1.CreateVenueRequest{
		UserId:           serviceTestUserID,
		Environment:      int32(domain.EnvironmentBacktest),
		Exchange:         int32(domain.ExchangeBinance),
		Market:           int32(domain.MarketPerpetualFutures),
		MarginMode:       int32(domain.MarginModeCross),
		PositionMode:     int32(domain.PositionModeOneWay),
		TotalValue:       1750,
		WalletBalance:    1500,
		AvailableBalance: 1400,
		Futures: &accountv1.FuturesWallet{
			MarginMode:       "cross",
			PositionMode:     "one_way",
			InitialBalance:   1500,
			WalletBalance:    1500,
			AvailableBalance: 1400,
			MarginBalance:    1500,
			Positions: []*accountv1.FuturesPosition{{
				Symbol:         "ETHUSDT",
				Direction:      1,
				InitialBalance: 500,
				Leverage:       10,
				FeeRate:        0.0004,
			}},
		},
		Spot: &accountv1.SpotWallet{
			Free: 250,
			Assets: []*accountv1.SpotAsset{{
				Symbol:        "BTCUSDT",
				Qty:           0.01,
				AvgEntryPrice: 25000,
			}},
		},
	})
	if err != nil {
		t.Fatalf("CreateVenue: %v", err)
	}
	state := repo.venueStates[resp.GetVenue().GetVenueId()]
	if state.TotalValue != 1750 || state.WalletBalance != 1500 || state.AvailableBalance != 1400 {
		t.Fatalf("state totals = %+v, want bootstrap totals", state)
	}
	if state.Futures.InitialBalance != 1500 || len(state.Futures.Positions) != 1 || state.Futures.Positions[0].Symbol != "ETHUSDT" {
		t.Fatalf("futures state = %+v, want bootstrap futures", state.Futures)
	}
	if state.Spot.Free != 250 || len(state.Spot.Assets) != 1 || state.Spot.Assets[0].Symbol != "BTCUSDT" {
		t.Fatalf("spot state = %+v, want bootstrap spot", state.Spot)
	}
}

func TestGetVenueOnlineInfoBacktestUnboundReturnsVenueWalletState(t *testing.T) {
	repo := &stubRepo{
		venues: []domain.Venue{{
			VenueID:      53,
			UserID:       serviceTestUserID,
			Exchange:     domain.ExchangeBinance,
			Market:       domain.MarketPerpetualFutures,
			Environment:  domain.EnvironmentBacktest,
			Status:       domain.VenueStatusActive,
			MarginMode:   domain.MarginModeCross,
			PositionMode: domain.PositionModeOneWay,
		}},
		venueStates: map[int64]domain.OnlineAccountInfo{
			53: {
				Environment:      domain.EnvironmentBacktest,
				TotalValue:       1000,
				WalletBalance:    1000,
				AvailableBalance: 900,
				Futures:          domain.FuturesWallet{MarginMode: "cross", PositionMode: "one_way", WalletBalance: 1000, AvailableBalance: 900},
				UpdatedAt:        time.Now().UTC(),
			},
		},
	}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	resp, err := svc.GetVenueOnlineInfo(context.Background(), &accountv1.GetVenueOnlineInfoRequest{
		UserId:  serviceTestUserID,
		VenueId: 53,
	})
	if err != nil {
		t.Fatalf("GetVenueOnlineInfo: %v", err)
	}
	if resp.GetVenue().GetAccountId() != 0 {
		t.Fatalf("venue account_id = %d, want unbound", resp.GetVenue().GetAccountId())
	}
	if resp.GetWallet().GetTotalValue() != 1000 || resp.GetWallet().GetFutures().GetAvailableBalance() != 900 {
		t.Fatalf("wallet = %+v, want persisted unbound venue state", resp.GetWallet())
	}
}

func TestBindBacktestVenuePreservesExistingWalletState(t *testing.T) {
	accountID := int64(42)
	repo := &stubRepo{
		account: domain.Account{
			AccountID:   accountID,
			UserID:      serviceTestUserID,
			Environment: domain.EnvironmentBacktest,
			Status:      domain.AccountStatusActive,
		},
		venues: []domain.Venue{{
			VenueID:      53,
			UserID:       serviceTestUserID,
			Exchange:     domain.ExchangeBinance,
			Market:       domain.MarketPerpetualFutures,
			Environment:  domain.EnvironmentBacktest,
			Status:       domain.VenueStatusActive,
			MarginMode:   domain.MarginModeCross,
			PositionMode: domain.PositionModeOneWay,
		}},
		venueStates: map[int64]domain.OnlineAccountInfo{
			53: {
				Environment:      domain.EnvironmentBacktest,
				TotalValue:       1234,
				WalletBalance:    1234,
				AvailableBalance: 1200,
				Futures:          domain.FuturesWallet{MarginMode: "cross", PositionMode: "one_way", WalletBalance: 1234, AvailableBalance: 1200},
				UpdatedAt:        time.Now().UTC(),
			},
		},
	}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	_, err := svc.BindVenue(context.Background(), &accountv1.BindVenueRequest{
		UserId:    serviceTestUserID,
		AccountId: accountID,
		VenueId:   53,
	})
	if err != nil {
		t.Fatalf("BindVenue: %v", err)
	}
	state := repo.venueStates[53]
	if state.AccountID != accountID {
		t.Fatalf("wallet state account_id = %d, want %d", state.AccountID, accountID)
	}
	if state.TotalValue != 1234 || state.AvailableBalance != 1200 {
		t.Fatalf("wallet state was reset: %+v", state)
	}
}

func TestCreateDemoVenueRequiresAPIKey(t *testing.T) {
	mgr, err := credential.NewManager("12345678901234567890123456789012", "test-v1")
	if err != nil {
		t.Fatalf("credential manager: %v", err)
	}
	svc := NewAccountGRPCService(&stubRepo{}, nil, nil, nil, WithCredentialManager(mgr))

	_, err = svc.CreateVenue(context.Background(), &accountv1.CreateVenueRequest{
		UserId:         serviceTestUserID,
		Environment:    int32(domain.EnvironmentDemo),
		Exchange:       int32(domain.ExchangeBinance),
		Market:         int32(domain.MarketPerpetualFutures),
		CredentialJson: `{"api_secret":"secret-1"}`,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument (err=%v)", status.Code(err), err)
	}
}

func TestCreateBoundVenueRejectsEnvironmentMismatch(t *testing.T) {
	repo := &stubRepo{account: domain.Account{
		AccountID:   11,
		UserID:      serviceTestUserID,
		Environment: domain.EnvironmentBacktest,
		Status:      domain.AccountStatusActive,
	}}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	_, err := svc.CreateVenue(context.Background(), &accountv1.CreateVenueRequest{
		UserId:      serviceTestUserID,
		AccountId:   11,
		Environment: int32(domain.EnvironmentDemo),
		Exchange:    int32(domain.ExchangeBinance),
		Market:      int32(domain.MarketPerpetualFutures),
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
	if len(repo.createdVenues) != 0 {
		t.Fatalf("created venues len = %d, want 0", len(repo.createdVenues))
	}
}

func TestCreateBoundVenueRejectsActiveSessions(t *testing.T) {
	repo := &stubRepo{
		account: domain.Account{
			AccountID:   11,
			UserID:      serviceTestUserID,
			Environment: domain.EnvironmentDemo,
			Status:      domain.AccountStatusActive,
		},
		activeSessionCount: 1,
	}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	_, err := svc.CreateVenue(context.Background(), &accountv1.CreateVenueRequest{
		UserId:      serviceTestUserID,
		AccountId:   11,
		Environment: int32(domain.EnvironmentDemo),
		Exchange:    int32(domain.ExchangeBinance),
		Market:      int32(domain.MarketPerpetualFutures),
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
	if len(repo.createdVenues) != 0 {
		t.Fatalf("created venues len = %d, want 0", len(repo.createdVenues))
	}
}

func TestBindVenueRejectsEnvironmentMismatch(t *testing.T) {
	repo := &stubRepo{
		account: domain.Account{
			AccountID:   11,
			UserID:      serviceTestUserID,
			Environment: domain.EnvironmentBacktest,
			Status:      domain.AccountStatusActive,
		},
		venues: []domain.Venue{{
			VenueID:      22,
			UserID:       serviceTestUserID,
			Environment:  domain.EnvironmentDemo,
			Exchange:     domain.ExchangeBinance,
			Market:       domain.MarketPerpetualFutures,
			Status:       domain.VenueStatusActive,
			MarginMode:   domain.MarginModeCross,
			PositionMode: domain.PositionModeOneWay,
		}},
	}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	_, err := svc.BindVenue(context.Background(), &accountv1.BindVenueRequest{
		UserId:    serviceTestUserID,
		AccountId: 11,
		VenueId:   22,
		Reason:    "test mismatch",
	})
	if err == nil {
		t.Fatal("expected mismatch error")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("code = %v, want FailedPrecondition", st.Code())
	}
}

func TestBindBacktestVenueInitializesWalletState(t *testing.T) {
	accountID := int64(11)
	repo := &stubRepo{
		account: domain.Account{
			AccountID:   accountID,
			UserID:      serviceTestUserID,
			Environment: domain.EnvironmentBacktest,
			Status:      domain.AccountStatusActive,
		},
		venues: []domain.Venue{{
			VenueID:      22,
			UserID:       serviceTestUserID,
			Environment:  domain.EnvironmentBacktest,
			Exchange:     domain.ExchangeBinance,
			Market:       domain.MarketPerpetualFutures,
			Status:       domain.VenueStatusActive,
			MarginMode:   domain.MarginModeCross,
			PositionMode: domain.PositionModeOneWay,
		}},
	}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	_, err := svc.BindVenue(context.Background(), &accountv1.BindVenueRequest{
		UserId:    serviceTestUserID,
		AccountId: accountID,
		VenueId:   22,
		Reason:    "test bind",
	})
	if err != nil {
		t.Fatalf("BindVenue: %v", err)
	}
	if len(repo.venueStates) != 1 {
		t.Fatalf("venue wallet states len = %d, want 1", len(repo.venueStates))
	}
	state := repo.venueStates[22]
	if state.AccountID != accountID || state.Environment != domain.EnvironmentBacktest {
		t.Fatalf("state = %+v, want backtest account %d", state, accountID)
	}
	if state.Futures.MarginMode != "cross" || state.Futures.PositionMode != "one_way" {
		t.Fatalf("state futures modes = %q/%q, want cross/one_way", state.Futures.MarginMode, state.Futures.PositionMode)
	}
}

func TestPreflightStrategySessionReportsMissingVenue(t *testing.T) {
	repo := &stubRepo{account: domain.Account{
		AccountID:   11,
		UserID:      serviceTestUserID,
		Environment: domain.EnvironmentDemo,
		Status:      domain.AccountStatusActive,
	}}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	resp, err := svc.PreflightStrategySession(context.Background(), &accountv1.PreflightStrategySessionRequest{
		UserId:    serviceTestUserID,
		AccountId: 11,
		RequiredRoutes: []*accountv1.RequiredRoute{{
			Exchange: int32(domain.ExchangeBinance),
			Market:   int32(domain.MarketPerpetualFutures),
		}},
	})
	if err != nil {
		t.Fatalf("PreflightStrategySession: %v", err)
	}
	if resp.GetOk() {
		t.Fatal("preflight ok = true, want false")
	}
	if len(resp.GetIssues()) != 1 || resp.GetIssues()[0].GetCode() != "VENUE_MISSING" {
		t.Fatalf("issues = %+v", resp.GetIssues())
	}
}

func TestPreflightStrategySessionPersistsFailedSession(t *testing.T) {
	repo := newPreflightSessionRepo(domain.Account{
		AccountID:   11,
		UserID:      serviceTestUserID,
		Environment: domain.EnvironmentDemo,
		Status:      domain.AccountStatusActive,
	})
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	resp, err := svc.PreflightStrategySession(context.Background(), &accountv1.PreflightStrategySessionRequest{
		UserId:     serviceTestUserID,
		AccountId:  11,
		SessionId:  "preflight-failed-service-1",
		StrategyId: 29,
		RequiredRoutes: []*accountv1.RequiredRoute{{
			Exchange: int32(domain.ExchangeBinance),
			Market:   int32(domain.MarketPerpetualFutures),
		}},
	})
	if err != nil {
		t.Fatalf("PreflightStrategySession: %v", err)
	}
	if resp.GetOk() {
		t.Fatal("preflight ok = true, want false")
	}

	got, err := svc.GetSession(context.Background(), &accountv1.GetSessionRequest{
		SessionId: "preflight-failed-service-1",
		UserId:    serviceTestUserID,
	})
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	sess := got.GetSession()
	if sess.GetStatus() != domain.SessionStatusPreflightFailed {
		t.Fatalf("status = %q, want %q", sess.GetStatus(), domain.SessionStatusPreflightFailed)
	}
	if sess.GetErrorCode() != "VENUE_MISSING" || sess.GetErrorMessage() != "active venue is missing" {
		t.Fatalf("unexpected structured error fields: %+v", sess)
	}
	if !strings.Contains(sess.GetErrorDetailJson(), `"code":"VENUE_MISSING"`) {
		t.Fatalf("error_detail_json = %q, want issue payload", sess.GetErrorDetailJson())
	}
}

func TestPreflightStrategySessionSymbolPreflightSucceedsWhenRulesContainSymbol(t *testing.T) {
	accountID := int64(11)
	repo := &stubRepo{
		account: domain.Account{
			AccountID:   accountID,
			UserID:      serviceTestUserID,
			Environment: domain.EnvironmentDemo,
			Status:      domain.AccountStatusActive,
		},
		venues: []domain.Venue{{
			VenueID:        22,
			UserID:         serviceTestUserID,
			AccountID:      &accountID,
			Environment:    domain.EnvironmentDemo,
			Exchange:       domain.ExchangeBinance,
			Market:         domain.MarketPerpetualFutures,
			Status:         domain.VenueStatusActive,
			MarginMode:     domain.MarginModeCross,
			PositionMode:   domain.PositionModeOneWay,
			CredentialInfo: `{"api_secret":"secret"}`,
		}},
	}
	registry := adapter.NewRegistry()
	registry.Register(adapter.Route{
		Exchange:    domain.ExchangeBinance,
		Environment: domain.EnvironmentDemo,
		Market:      domain.MarketPerpetualFutures,
	}, preflightFactory{symbolRulesReader: preflightSymbolRulesReader{rules: adapter.SymbolRules{
		Symbols: []adapter.SymbolRule{{Symbol: "ETHUSDT"}},
	}}})
	svc := NewAccountGRPCService(repo, nil, nil, nil, WithExchangeRegistry(registry))

	resp, err := svc.PreflightStrategySession(context.Background(), &accountv1.PreflightStrategySessionRequest{
		UserId:    serviceTestUserID,
		AccountId: accountID,
		RequiredRoutes: []*accountv1.RequiredRoute{{
			Exchange: int32(domain.ExchangeBinance),
			Market:   int32(domain.MarketPerpetualFutures),
		}},
		RequiredSymbols: []*accountv1.RequiredSymbol{{
			Exchange: int32(domain.ExchangeBinance),
			Market:   int32(domain.MarketPerpetualFutures),
			Symbol:   "ethusdt",
		}},
	})
	if err != nil {
		t.Fatalf("PreflightStrategySession: %v", err)
	}
	if !resp.GetOk() {
		t.Fatalf("preflight ok = false, issues = %+v", resp.GetIssues())
	}
	for _, issue := range resp.GetIssues() {
		if issue.GetCode() == "symbol_rules_missing" {
			t.Fatalf("unexpected symbol_rules_missing issue: %+v", issue)
		}
	}
}

func TestPreflightStrategySessionReportsMissingSymbolRulesWithNilRegistry(t *testing.T) {
	accountID := int64(11)
	repo := &stubRepo{account: domain.Account{
		AccountID:   accountID,
		UserID:      serviceTestUserID,
		Environment: domain.EnvironmentDemo,
		Status:      domain.AccountStatusActive,
	}}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	resp, err := svc.PreflightStrategySession(context.Background(), &accountv1.PreflightStrategySessionRequest{
		UserId:    serviceTestUserID,
		AccountId: accountID,
		RequiredSymbols: []*accountv1.RequiredSymbol{{
			Exchange: int32(domain.ExchangeBinance),
			Market:   int32(domain.MarketPerpetualFutures),
			Symbol:   "ethusdt",
		}},
	})
	if err != nil {
		t.Fatalf("PreflightStrategySession: %v", err)
	}
	issue := findPreflightIssue(resp.GetIssues(), "symbol_rules_missing")
	if issue == nil {
		t.Fatalf("symbol_rules_missing issue not found: %+v", resp.GetIssues())
	}
	if issue.GetSymbol() != "ETHUSDT" {
		t.Fatalf("symbol issue symbol = %q, want ETHUSDT", issue.GetSymbol())
	}
}

func TestPreflightStrategySessionReportsMissingSymbolRulesForEmptySymbol(t *testing.T) {
	accountID := int64(11)
	repo := &stubRepo{account: domain.Account{
		AccountID:   accountID,
		UserID:      serviceTestUserID,
		Environment: domain.EnvironmentDemo,
		Status:      domain.AccountStatusActive,
	}}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	resp, err := svc.PreflightStrategySession(context.Background(), &accountv1.PreflightStrategySessionRequest{
		UserId:    serviceTestUserID,
		AccountId: accountID,
		RequiredSymbols: []*accountv1.RequiredSymbol{{
			Exchange: int32(domain.ExchangeBinance),
			Market:   int32(domain.MarketPerpetualFutures),
			Symbol:   "  ",
		}},
	})
	if err != nil {
		t.Fatalf("PreflightStrategySession: %v", err)
	}
	issue := findPreflightIssue(resp.GetIssues(), "symbol_rules_missing")
	if issue == nil {
		t.Fatalf("symbol_rules_missing issue not found: %+v", resp.GetIssues())
	}
	if issue.GetMessage() == "" {
		t.Fatalf("symbol_rules_missing message is empty: %+v", issue)
	}
}

func TestPreflightStrategySessionReportsMissingSymbolRulesForUnsupportedRoute(t *testing.T) {
	accountID := int64(11)
	repo := &stubRepo{account: domain.Account{
		AccountID:   accountID,
		UserID:      serviceTestUserID,
		Environment: domain.EnvironmentDemo,
		Status:      domain.AccountStatusActive,
	}}
	svc := NewAccountGRPCService(repo, nil, nil, nil, WithExchangeRegistry(adapter.NewRegistry()))

	resp, err := svc.PreflightStrategySession(context.Background(), &accountv1.PreflightStrategySessionRequest{
		UserId:    serviceTestUserID,
		AccountId: accountID,
		RequiredSymbols: []*accountv1.RequiredSymbol{{
			Exchange: int32(domain.ExchangeBinance),
			Market:   int32(domain.MarketPerpetualFutures),
			Symbol:   "ETHUSDT",
		}},
	})
	if err != nil {
		t.Fatalf("PreflightStrategySession: %v", err)
	}
	if findPreflightIssue(resp.GetIssues(), "symbol_rules_missing") == nil {
		t.Fatalf("symbol_rules_missing issue not found: %+v", resp.GetIssues())
	}
}

func TestPreflightStrategySessionReportsMissingSymbolRulesWhenReaderOmitsSymbol(t *testing.T) {
	accountID := int64(11)
	repo := &stubRepo{account: domain.Account{
		AccountID:   accountID,
		UserID:      serviceTestUserID,
		Environment: domain.EnvironmentDemo,
		Status:      domain.AccountStatusActive,
	}}
	registry := adapter.NewRegistry()
	registry.Register(adapter.Route{
		Exchange:    domain.ExchangeBinance,
		Environment: domain.EnvironmentDemo,
		Market:      domain.MarketPerpetualFutures,
	}, preflightFactory{symbolRulesReader: preflightSymbolRulesReader{rules: adapter.SymbolRules{
		Symbols: []adapter.SymbolRule{{Symbol: "BTCUSDT"}},
	}}})
	svc := NewAccountGRPCService(repo, nil, nil, nil, WithExchangeRegistry(registry))

	resp, err := svc.PreflightStrategySession(context.Background(), &accountv1.PreflightStrategySessionRequest{
		UserId:    serviceTestUserID,
		AccountId: accountID,
		RequiredSymbols: []*accountv1.RequiredSymbol{{
			Exchange: int32(domain.ExchangeBinance),
			Market:   int32(domain.MarketPerpetualFutures),
			Symbol:   "ETHUSDT",
		}},
	})
	if err != nil {
		t.Fatalf("PreflightStrategySession: %v", err)
	}
	issue := findPreflightIssue(resp.GetIssues(), "symbol_rules_missing")
	if issue == nil {
		t.Fatalf("symbol_rules_missing issue not found: %+v", resp.GetIssues())
	}
	if issue.GetSymbol() != "ETHUSDT" {
		t.Fatalf("symbol issue symbol = %q, want ETHUSDT", issue.GetSymbol())
	}
}

func TestPreflightStrategySessionReportsMissingSymbolRules(t *testing.T) {
	accountID := int64(11)
	repo := &stubRepo{
		account: domain.Account{
			AccountID:   accountID,
			UserID:      serviceTestUserID,
			Environment: domain.EnvironmentDemo,
			Status:      domain.AccountStatusActive,
		},
		venues: []domain.Venue{{
			VenueID:        22,
			UserID:         serviceTestUserID,
			AccountID:      &accountID,
			Environment:    domain.EnvironmentDemo,
			Exchange:       domain.ExchangeBinance,
			Market:         domain.MarketPerpetualFutures,
			Status:         domain.VenueStatusActive,
			MarginMode:     domain.MarginModeCross,
			PositionMode:   domain.PositionModeOneWay,
			CredentialInfo: `{"api_secret":"secret"}`,
		}},
	}
	registry := adapter.NewRegistry()
	registry.Register(adapter.Route{
		Exchange:    domain.ExchangeBinance,
		Environment: domain.EnvironmentDemo,
		Market:      domain.MarketPerpetualFutures,
	}, preflightFactory{symbolRulesErr: errors.New("symbol rules unavailable")})
	svc := NewAccountGRPCService(repo, nil, nil, nil, WithExchangeRegistry(registry))

	resp, err := svc.PreflightStrategySession(context.Background(), &accountv1.PreflightStrategySessionRequest{
		UserId:    serviceTestUserID,
		AccountId: accountID,
		RequiredRoutes: []*accountv1.RequiredRoute{{
			Exchange: int32(domain.ExchangeBinance),
			Market:   int32(domain.MarketPerpetualFutures),
		}},
		RequiredSymbols: []*accountv1.RequiredSymbol{{
			Exchange: int32(domain.ExchangeBinance),
			Market:   int32(domain.MarketPerpetualFutures),
			Symbol:   "ethusdt",
		}},
	})
	if err != nil {
		t.Fatalf("PreflightStrategySession: %v", err)
	}
	if resp.GetOk() {
		t.Fatal("preflight ok = true, want false")
	}
	var got *accountv1.PreflightIssue
	for _, issue := range resp.GetIssues() {
		if issue.GetCode() == "symbol_rules_missing" {
			got = issue
			break
		}
	}
	if got == nil {
		t.Fatalf("symbol_rules_missing issue not found: %+v", resp.GetIssues())
	}
	if got.GetSymbol() != "ETHUSDT" {
		t.Fatalf("symbol issue symbol = %q, want ETHUSDT", got.GetSymbol())
	}
	if got.GetExchange() != int32(domain.ExchangeBinance) || got.GetMarket() != int32(domain.MarketPerpetualFutures) {
		t.Fatalf("symbol issue route = (%d, %d), want (%d, %d)", got.GetExchange(), got.GetMarket(), domain.ExchangeBinance, domain.MarketPerpetualFutures)
	}
}

func findPreflightIssue(issues []*accountv1.PreflightIssue, code string) *accountv1.PreflightIssue {
	for _, issue := range issues {
		if issue.GetCode() == code {
			return issue
		}
	}
	return nil
}

func TestGetVenueRouteMetaDecryptsCredential(t *testing.T) {
	mgr, err := credential.NewManager("12345678901234567890123456789012", "test-v1")
	if err != nil {
		t.Fatalf("credential manager: %v", err)
	}
	encrypted, err := mgr.Encrypt(`{"api_secret":"secret-2"}`)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	repo := &stubRepo{routeMeta: domain.VenueRouteMeta{
		AccountID:      11,
		VenueID:        22,
		UserID:         serviceTestUserID,
		Environment:    domain.EnvironmentDemo,
		Exchange:       domain.ExchangeBinance,
		Market:         domain.MarketPerpetualFutures,
		MarginMode:     domain.MarginModeCross,
		PositionMode:   domain.PositionModeOneWay,
		APIKey:         "api-key-2",
		CredentialInfo: encrypted,
		DefaultFeeRate: 0.0004,
		SlippageBps:    1.5,
	}}
	svc := NewAccountGRPCService(repo, nil, nil, nil, WithCredentialManager(mgr))

	resp, err := svc.GetVenueRouteMeta(context.Background(), &accountv1.GetVenueRouteMetaRequest{
		AccountId: 11,
		Exchange:  int32(domain.ExchangeBinance),
		Market:    int32(domain.MarketPerpetualFutures),
	})
	if err != nil {
		t.Fatalf("GetVenueRouteMeta: %v", err)
	}
	if resp.GetCredentialJson() != `{"api_secret":"secret-2"}` {
		t.Fatalf("credential_json = %q", resp.GetCredentialJson())
	}
	if resp.GetApiKey() != "api-key-2" || resp.GetVenueId() != 22 {
		t.Fatalf("route meta api_key/venue_id = %q/%d", resp.GetApiKey(), resp.GetVenueId())
	}
}

func TestListAndGetAccountsReturnDescription(t *testing.T) {
	repo := &stubRepo{account: domain.Account{
		AccountID:   1,
		UserID:      serviceTestUserID,
		Name:        "testnet",
		Description: "Binance testnet account",
		Environment: domain.EnvironmentDemo,
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
		Environment:    domain.EnvironmentLive,
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
	if resp.GetEnvironment() != 2 {
		t.Errorf("environment: got %d", resp.GetEnvironment())
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
				Environment:    domain.EnvironmentDemo,
				SnapshotReason: domain.SnapshotReasonPeriodicSample,
				RunType:        domain.ReconciliationRunSampled,
				LocalSnapshot: domain.OnlineAccountInfo{
					AccountID:   101,
					Environment: domain.EnvironmentDemo,
					TotalValue:  1000,
				},
				ExchangeSnapshot: domain.OnlineAccountInfo{
					AccountID:   101,
					Environment: domain.EnvironmentDemo,
					TotalValue:  1000.5,
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
			Environment:    domain.EnvironmentDemo,
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
