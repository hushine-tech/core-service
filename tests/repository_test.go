package tests

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/repository"
)

// mockRepo is an in-memory Repository implementing the new interface.
type mockRepo struct {
	nextID               int64
	nextUserID           int64
	nextStrategyID       int64
	accounts             map[int64]domain.Account
	states               map[int64]domain.OnlineAccountInfo
	snapshots            []domain.SnapshotReason // log of reasons written
	strategies           map[int64]domain.Strategy
	accountStrategies    map[int64]map[int64]domain.AccountStrategy
	sessions             map[string]domain.StrategySession
	users                map[string]domain.User
	reconMu              sync.Mutex                 // guards reconRuns (written from async compare goroutine)
	reconRuns            []domain.ReconciliationRun // Phase C audit log
	notificationSettings map[int64]domain.NotificationSettings
	notificationChannels map[int64]domain.NotificationChannel
	notificationPlans    map[string]domain.NotificationPlan
}

// reconRunsLen returns a race-safe count of persisted reconciliation runs.
func (m *mockRepo) reconRunsLen() int {
	m.reconMu.Lock()
	defer m.reconMu.Unlock()
	return len(m.reconRuns)
}

// reconRunsSnapshot returns a copy of the reconciliation runs for assertion.
func (m *mockRepo) reconRunsSnapshot() []domain.ReconciliationRun {
	m.reconMu.Lock()
	defer m.reconMu.Unlock()
	out := make([]domain.ReconciliationRun, len(m.reconRuns))
	copy(out, m.reconRuns)
	return out
}

func newMockRepo() *mockRepo {
	return &mockRepo{
		nextID:               1,
		nextUserID:           1,
		nextStrategyID:       1,
		accounts:             make(map[int64]domain.Account),
		states:               make(map[int64]domain.OnlineAccountInfo),
		strategies:           make(map[int64]domain.Strategy),
		accountStrategies:    make(map[int64]map[int64]domain.AccountStrategy),
		sessions:             make(map[string]domain.StrategySession),
		users:                make(map[string]domain.User),
		notificationSettings: make(map[int64]domain.NotificationSettings),
		notificationChannels: make(map[int64]domain.NotificationChannel),
		notificationPlans: map[string]domain.NotificationPlan{
			"free": {PlanCode: "free"},
			"pro":  {PlanCode: "pro", NotificationEnabled: true, AllowSystem: true, AllowStrategy: true, AllowCustom: true, CustomRateLimitPerMinute: 30, CustomRateLimitBurst: 10},
		},
	}
}

func (m *mockRepo) CreateUser(_ context.Context, user domain.User) (domain.User, error) {
	if _, ok := m.users[user.Username]; ok {
		return domain.User{}, errors.New("duplicate key")
	}
	user.ID = m.nextUserID
	m.nextUserID++
	m.users[user.Username] = user
	return user, nil
}

func (m *mockRepo) GetUserByUsername(_ context.Context, username string) (domain.User, error) {
	user, ok := m.users[username]
	if !ok {
		return domain.User{}, errNotFound
	}
	return user, nil
}

func (m *mockRepo) GetUser(_ context.Context, userID int64) (domain.User, error) {
	for _, u := range m.users {
		if u.ID == userID {
			return u, nil
		}
	}
	return domain.User{}, errNotFound
}

func (m *mockRepo) CreateAccount(_ context.Context, a domain.Account) (int64, error) {
	id := m.nextID
	m.nextID++
	a.AccountID = id
	m.accounts[id] = a
	return id, nil
}

func (m *mockRepo) GetAccount(_ context.Context, id, userID int64) (domain.Account, error) {
	a, ok := m.accounts[id]
	if !ok {
		return domain.Account{}, errNotFound
	}
	if userID > 0 && a.UserID != userID {
		return domain.Account{}, errNotFound
	}
	return a, nil
}

func (m *mockRepo) ListAccounts(_ context.Context, userID int64) ([]domain.Account, error) {
	out := make([]domain.Account, 0, len(m.accounts))
	for _, a := range m.accounts {
		if userID > 0 && a.UserID != userID {
			continue
		}
		out = append(out, a)
	}
	return out, nil
}

func (m *mockRepo) UpdateAccountState(_ context.Context, info domain.OnlineAccountInfo) error {
	m.states[info.AccountID] = info
	return nil
}

func (m *mockRepo) GetAccountState(_ context.Context, id int64) (domain.OnlineAccountInfo, error) {
	s, ok := m.states[id]
	if !ok {
		return domain.OnlineAccountInfo{}, errNotFound
	}
	return s, nil
}

func (m *mockRepo) SaveSnapshot(_ context.Context, accountID int64, reason domain.SnapshotReason, _ int64, _ string) error {
	if _, ok := m.states[accountID]; !ok {
		return errNotFound
	}
	m.snapshots = append(m.snapshots, reason)
	return nil
}

func (m *mockRepo) CreateStrategy(_ context.Context, s domain.Strategy) (int64, error) {
	id := m.nextStrategyID
	m.nextStrategyID++
	s.StrategyID = id
	m.strategies[id] = s
	return id, nil
}

func (m *mockRepo) GetStrategy(_ context.Context, strategyID, userID int64) (domain.Strategy, error) {
	s, ok := m.strategies[strategyID]
	if !ok {
		return domain.Strategy{}, errNotFound
	}
	if userID > 0 && s.UserID != userID {
		return domain.Strategy{}, errNotFound
	}
	return s, nil
}

func (m *mockRepo) ListStrategies(_ context.Context, userID int64, namePrefix string, activeOnly bool) ([]domain.Strategy, error) {
	out := make([]domain.Strategy, 0, len(m.strategies))
	for _, s := range m.strategies {
		if userID > 0 && s.UserID != userID {
			continue
		}
		if namePrefix != "" && !strings.HasPrefix(s.Name, namePrefix) {
			continue
		}
		if activeOnly && s.Archived {
			continue
		}
		out = append(out, s)
	}
	return out, nil
}

func (m *mockRepo) ArchiveStrategy(_ context.Context, strategyID int64) error {
	s, ok := m.strategies[strategyID]
	if !ok {
		return errNotFound
	}
	s.Archived = true
	m.strategies[strategyID] = s
	return nil
}

func (m *mockRepo) SaveSession(_ context.Context, s domain.StrategySession) error {
	account, ok := m.accounts[s.AccountID]
	if !ok {
		return errNotFound
	}
	for _, existing := range m.sessions {
		if existing.AccountID == s.AccountID && (existing.Status == "running" || existing.Status == "stopping") {
			return repository.ErrConflict
		}
	}
	s.UserID = account.UserID
	m.sessions[s.SessionID] = s
	return nil
}

func (m *mockRepo) UpdateSession(_ context.Context, sessionID string, status string, barsProcessed int, errMsg string, runtimeID string) error {
	s, ok := m.sessions[sessionID]
	if !ok {
		return errNotFound
	}
	if runtimeID != "" && s.RuntimeID != runtimeID {
		return errNotFound
	}
	s.Status = status
	s.BarsProcessed = barsProcessed
	s.Error = errMsg
	m.sessions[sessionID] = s
	return nil
}

func (m *mockRepo) GetSession(_ context.Context, sessionID string, userID int64) (domain.StrategySession, error) {
	s, ok := m.sessions[sessionID]
	if !ok {
		return domain.StrategySession{}, errNotFound
	}
	if userID > 0 && s.UserID != userID {
		return domain.StrategySession{}, errNotFound
	}
	return s, nil
}

func (m *mockRepo) ListSessions(_ context.Context, accountID, userID int64, limit, offset int) ([]domain.StrategySession, error) {
	_ = offset
	out := make([]domain.StrategySession, 0, len(m.sessions))
	for _, s := range m.sessions {
		if accountID != 0 && s.AccountID != accountID {
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

func (m *mockRepo) ListRunningSessions(_ context.Context, runtimeID string) ([]domain.StrategySession, error) {
	out := make([]domain.StrategySession, 0, len(m.sessions))
	for _, s := range m.sessions {
		if runtimeID != "" && s.RuntimeID != runtimeID {
			continue
		}
		if s.Status == "running" || s.Status == "stopping" {
			out = append(out, s)
		}
	}
	return out, nil
}

func (m *mockRepo) MarkRuntimeSessionsRecoverable(_ context.Context, runtimeID string, errMsg string) (int64, error) {
	var count int64
	for id, s := range m.sessions {
		if s.RuntimeID != runtimeID {
			continue
		}
		if s.Status != "running" && s.Status != "stopping" {
			continue
		}
		s.Status = "recoverable"
		s.Error = errMsg
		m.sessions[id] = s
		count++
	}
	return count, nil
}

func (m *mockRepo) ListSessionSnapshots(_ context.Context, _ string, _ int64, _, _ int) ([]domain.SnapshotRow, int64, bool, error) {
	return []domain.SnapshotRow{}, 0, false, nil
}

func (m *mockRepo) ListReconciliationRuns(_ context.Context, _ string, _ int64, _, _ int) ([]domain.ReconciliationRun, int64, bool, error) {
	return []domain.ReconciliationRun{}, 0, false, nil
}

func (m *mockRepo) GetSessionReconciliationSummary(_ context.Context, _ string, _ int64) (int64, int64, int64, error) {
	return 0, 0, 0, nil
}

func (m *mockRepo) MountStrategy(_ context.Context, accountID, strategyID int64) error {
	s, ok := m.strategies[strategyID]
	if !ok {
		return errNotFound
	}
	if _, ok := m.accounts[accountID]; !ok {
		return errNotFound
	}
	if m.accounts[accountID].UserID != s.UserID {
		return errNotFound
	}
	mm := m.accountStrategies[accountID]
	if mm == nil {
		mm = make(map[int64]domain.AccountStrategy)
		m.accountStrategies[accountID] = mm
	}
	mm[strategyID] = domain.AccountStrategy{
		AccountID:  accountID,
		StrategyID: strategyID,
		Active:     false,
		MountedAt:  time.Now(),
		Strategy:   s,
	}
	return nil
}

func (m *mockRepo) UnmountStrategy(_ context.Context, accountID, strategyID int64) error {
	mm, ok := m.accountStrategies[accountID]
	if !ok {
		return errNotFound
	}
	if _, ok := mm[strategyID]; !ok {
		return errNotFound
	}
	delete(mm, strategyID)
	return nil
}

func (m *mockRepo) ActivateStrategy(_ context.Context, accountID, strategyID int64) error {
	mm, ok := m.accountStrategies[accountID]
	if !ok {
		return errNotFound
	}
	target, ok := mm[strategyID]
	if !ok {
		return errNotFound
	}
	for id, as := range mm {
		as.Active = (id == strategyID)
		mm[id] = as
	}
	target.Active = true
	mm[strategyID] = target
	return nil
}

func (m *mockRepo) DeactivateStrategy(_ context.Context, accountID, strategyID int64) error {
	mm, ok := m.accountStrategies[accountID]
	if !ok {
		return errNotFound
	}
	as, ok := mm[strategyID]
	if !ok {
		return errNotFound
	}
	as.Active = false
	mm[strategyID] = as
	return nil
}

func (m *mockRepo) ListAccountStrategies(_ context.Context, accountID int64) ([]domain.AccountStrategy, error) {
	mm := m.accountStrategies[accountID]
	out := make([]domain.AccountStrategy, 0, len(mm))
	for _, as := range mm {
		out = append(out, as)
	}
	return out, nil
}

func (m *mockRepo) GetActiveStrategy(_ context.Context, accountID int64) (domain.Strategy, error) {
	for _, as := range m.accountStrategies[accountID] {
		if as.Active {
			return as.Strategy, nil
		}
	}
	return domain.Strategy{}, errNotFound
}

var errNotFound = repository.ErrNotFound

// --- Tests ---

func TestMockRepoCreateAndGet(t *testing.T) {
	repo := newMockRepo()
	ctx := context.Background()

	id, err := repo.CreateAccount(ctx, domain.Account{Name: "test", UserID: 1, Mode: domain.AccountModeBacktest, CreatedAt: time.Now()})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if id == 0 {
		t.Fatal("expected non-zero ID")
	}

	got, err := repo.GetAccount(ctx, id, 0)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Name != "test" {
		t.Fatalf("unexpected name: %s", got.Name)
	}
}

func TestMockRepoUpdateAndGetState(t *testing.T) {
	repo := newMockRepo()
	ctx := context.Background()

	id, err := repo.CreateAccount(ctx, domain.Account{Name: "bt", UserID: 1, Mode: domain.AccountModeBacktest, CreatedAt: time.Now()})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	info := domain.OnlineAccountInfo{
		AccountID:     id,
		Mode:          domain.AccountModeBacktest,
		WalletBalance: 10000,
		UpdatedAt:     time.Now(),
	}
	if err := repo.UpdateAccountState(ctx, info); err != nil {
		t.Fatalf("update: %v", err)
	}

	got, err := repo.GetAccountState(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.WalletBalance != 10000 {
		t.Fatalf("unexpected balance: %f", got.WalletBalance)
	}
}

func TestMockRepoSaveSnapshot(t *testing.T) {
	repo := newMockRepo()
	ctx := context.Background()

	id, _ := repo.CreateAccount(ctx, domain.Account{Name: "bt", UserID: 1, Mode: domain.AccountModeBacktest, CreatedAt: time.Now()})
	_ = repo.UpdateAccountState(ctx, domain.OnlineAccountInfo{AccountID: id, WalletBalance: 5000, UpdatedAt: time.Now()})

	if err := repo.SaveSnapshot(ctx, id, domain.SnapshotReasonOrderFill, 0, ""); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if len(repo.snapshots) != 1 || repo.snapshots[0] != domain.SnapshotReasonOrderFill {
		t.Fatalf("unexpected snapshot reasons: %v", repo.snapshots)
	}
}

func TestMockRepoSaveSnapshot_notFound(t *testing.T) {
	repo := newMockRepo()
	ctx := context.Background()
	err := repo.SaveSnapshot(ctx, 999, domain.SnapshotReasonOrderFill, 0, "")
	if err == nil {
		t.Fatal("expected error for non-existent account")
	}
}

func (m *mockRepo) SaveReconciliationRun(_ context.Context, run domain.ReconciliationRun) error {
	m.reconMu.Lock()
	defer m.reconMu.Unlock()
	m.reconRuns = append(m.reconRuns, run)
	return nil
}

func (m *mockRepo) GetNotificationSettings(_ context.Context, userID int64) (domain.NotificationSettings, error) {
	if s, ok := m.notificationSettings[userID]; ok {
		return s, nil
	}
	return domain.NotificationSettings{UserID: userID, SystemEnabled: true, StrategyEnabled: true, CustomEnabled: true}, nil
}

func (m *mockRepo) UpsertNotificationSettings(_ context.Context, settings domain.NotificationSettings) (domain.NotificationSettings, error) {
	m.notificationSettings[settings.UserID] = settings
	return settings, nil
}

func (m *mockRepo) GetNotificationChannel(_ context.Context, userID int64, channel string) (domain.NotificationChannel, error) {
	if ch, ok := m.notificationChannels[userID]; ok && ch.Channel == channel {
		return ch, nil
	}
	return domain.NotificationChannel{UserID: userID, Channel: channel, Status: domain.NotificationChannelStatusUnbound}, nil
}

func (m *mockRepo) FindNotificationChannelByBindCodeHash(_ context.Context, codeHash string, _ time.Time) (domain.NotificationChannel, error) {
	for _, ch := range m.notificationChannels {
		if ch.BindCodeHash == codeHash && ch.Status == domain.NotificationChannelStatusPending {
			return ch, nil
		}
	}
	return domain.NotificationChannel{}, repository.ErrNotFound
}

func (m *mockRepo) UpsertNotificationBindCode(_ context.Context, userID int64, channel string, codeHash string, expiresAt time.Time) (domain.NotificationChannel, error) {
	ch := domain.NotificationChannel{UserID: userID, Channel: channel, Status: domain.NotificationChannelStatusPending, BindCodeHash: codeHash, BindCodeExpiresAt: &expiresAt}
	m.notificationChannels[userID] = ch
	return ch, nil
}

func (m *mockRepo) BindNotificationChannel(_ context.Context, userID int64, channel string, targetID string, targetType string, targetLabel string, now time.Time) (domain.NotificationChannel, error) {
	ch := domain.NotificationChannel{UserID: userID, Channel: channel, Status: domain.NotificationChannelStatusBound, TargetID: targetID, TargetType: targetType, TargetLabel: targetLabel, BoundAt: &now}
	m.notificationChannels[userID] = ch
	return ch, nil
}

func (m *mockRepo) RevokeNotificationChannel(_ context.Context, userID int64, channel string, now time.Time) error {
	m.notificationChannels[userID] = domain.NotificationChannel{UserID: userID, Channel: channel, Status: domain.NotificationChannelStatusRevoked, RevokedAt: &now}
	return nil
}

func (m *mockRepo) UpdateNotificationDeliveryStatus(_ context.Context, userID int64, channel string, status string, errText string, at time.Time) error {
	s, _ := m.GetNotificationSettings(context.Background(), userID)
	s.LastDeliveryStatus = status
	s.LastDeliveryError = errText
	s.LastDeliveryAt = &at
	m.notificationSettings[userID] = s
	ch, _ := m.GetNotificationChannel(context.Background(), userID, channel)
	ch.LastDeliveryStatus = status
	ch.LastDeliveryError = errText
	ch.LastDeliveryAt = &at
	m.notificationChannels[userID] = ch
	return nil
}

func (m *mockRepo) GetNotificationPlan(_ context.Context, planCode string) (domain.NotificationPlan, error) {
	if p, ok := m.notificationPlans[planCode]; ok {
		return p, nil
	}
	return m.notificationPlans["free"], nil
}

// Phase D2 (2026-05-06): market-data control-plane mockRepo stubs removed
// alongside the proto + repository methods. The control plane lives in
// control-panel-service now.
