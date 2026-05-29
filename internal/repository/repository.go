package repository

import (
	"context"
	"errors"
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
)

var ErrNotFound = errors.New("not found")

// ErrConflict 表示请求违反唯一运行约束，例如同一个 account 已有 active session。
var ErrConflict = errors.New("conflict")

// ErrPermissionDenied 用于用户试图读/写不属于自己的资源 (如 cancel 他人的 request).
var ErrPermissionDenied = errors.New("permission denied")

type PageMeta struct {
	Total   int64
	HasMore bool
}

type SessionListFilter struct {
	AccountID         int64
	UserID            int64
	RuntimeID         string
	StrategyID        int64
	Mode              int
	ModeSet           bool
	Status            string
	SessionIDContains string
	StartedAfterMs    int64
	StartedBeforeMs   int64
	Limit             int
	Offset            int
}

// Repository is the unified data access interface for core-service.
type Repository interface {
	// User management
	CreateUser(ctx context.Context, user domain.User) (domain.User, error)
	GetUserByUsername(ctx context.Context, username string) (domain.User, error)
	GetUser(ctx context.Context, userID int64) (domain.User, error)

	// Account management
	CreateAccount(ctx context.Context, account domain.Account) (int64, error)
	GetAccount(ctx context.Context, accountID, userID int64) (domain.Account, error)
	ListAccounts(ctx context.Context, userID int64) ([]domain.Account, error)
	ListAccountsPage(ctx context.Context, userID int64, limit, offset int) ([]domain.Account, PageMeta, error)
	CreateVenue(ctx context.Context, venue domain.Venue) (domain.Venue, error)
	GetVenue(ctx context.Context, venueID, userID int64) (domain.Venue, error)
	ListVenues(ctx context.Context, userID, accountID int64, includeUnbound bool, includeInactive bool, limit, offset int) ([]domain.Venue, PageMeta, error)
	BindVenue(ctx context.Context, userID, accountID, venueID int64, reason string) (domain.Venue, error)
	ReleaseVenue(ctx context.Context, userID, venueID int64, reason string) (domain.Venue, error)
	ArchiveVenue(ctx context.Context, userID, venueID int64, reason string) error
	ListActiveAccountVenues(ctx context.Context, userID, accountID int64) ([]domain.Venue, error)
	CountActiveSessionsForAccount(ctx context.Context, userID, accountID int64) (int64, error)
	SaveSessionVenues(ctx context.Context, sessionID string, venues []domain.Venue) error
	ResolveVenueRouteMeta(ctx context.Context, accountID int64, exchange domain.Exchange, market domain.Market) (domain.VenueRouteMeta, error)

	// Current state management
	UpdateAccountState(ctx context.Context, info domain.OnlineAccountInfo) error
	GetAccountState(ctx context.Context, accountID int64) (domain.OnlineAccountInfo, error)

	// Snapshot (archive) management — event-driven writes only
	// strategyID=0 means no strategy (manual or system-triggered snapshot).
	SaveSnapshot(ctx context.Context, accountID int64, reason domain.SnapshotReason, strategyID int64, sessionID string) error

	// Strategy management
	CreateStrategy(ctx context.Context, s domain.Strategy) (int64, error)
	GetStrategy(ctx context.Context, strategyID, userID int64) (domain.Strategy, error)
	ListStrategies(ctx context.Context, userID int64, namePrefix string, activeOnly bool) ([]domain.Strategy, error)
	ListStrategiesPage(ctx context.Context, userID int64, namePrefix string, activeOnly bool, limit, offset int) ([]domain.Strategy, PageMeta, error)
	ArchiveStrategy(ctx context.Context, strategyID int64) error

	// Strategy session management
	SaveSession(ctx context.Context, s domain.StrategySession) error
	UpdateSession(ctx context.Context, sessionID string, status string, barsProcessed int, errMsg string, runtimeID string) error
	GetSession(ctx context.Context, sessionID string, userID int64) (domain.StrategySession, error)
	ListSessions(ctx context.Context, accountID, userID int64, limit, offset int) ([]domain.StrategySession, error)
	ListSessionsPage(ctx context.Context, filter SessionListFilter) ([]domain.StrategySession, PageMeta, error)
	ListRunningSessions(ctx context.Context, runtimeID string) ([]domain.StrategySession, error)
	MarkRuntimeSessionsRecoverable(ctx context.Context, runtimeID string, errMsg string) (int64, error)
	// ListSessionSnapshots returns up to ``limit`` snapshots for a session,
	// ordered ``time DESC`` (newest first). ``offset`` supports offset-based
	// paging. See ``paginate-session-detail-lists`` — repository fetches
	// ``limit+1`` rows for the ``has_more`` sentinel and runs a separate
	// COUNT(*) for the session-wide ``total`` (used by the new pager's
	// First / Last / jump-to-page controls).
	ListSessionSnapshots(ctx context.Context, sessionID string, userID int64, limit, offset int) (items []domain.SnapshotRow, total int64, hasMore bool, err error)

	// Account strategy mount management
	MountStrategy(ctx context.Context, accountID, strategyID int64) error
	UnmountStrategy(ctx context.Context, accountID, strategyID int64) error
	ActivateStrategy(ctx context.Context, accountID, strategyID int64) error
	DeactivateStrategy(ctx context.Context, accountID, strategyID int64) error
	ListAccountStrategies(ctx context.Context, accountID int64) ([]domain.AccountStrategy, error)
	GetActiveStrategy(ctx context.Context, accountID int64) (domain.Strategy, error)

	// Reconciliation (Phase C)
	SaveReconciliationRun(ctx context.Context, run domain.ReconciliationRun) error
	// ListReconciliationRuns returns up to ``limit`` runs for a session,
	// ordered ``time DESC``. ``offset`` supports offset-based paging.
	// ``total`` is the session-wide count from a separate COUNT(*) query;
	// ``hasMore`` is the sentinel derived from fetching ``limit+1`` rows —
	// see ``paginate-session-detail-lists``.
	ListReconciliationRuns(ctx context.Context, sessionID string, userID int64, limit, offset int) (items []domain.ReconciliationRun, total int64, hasMore bool, err error)

	// GetSessionReconciliationSummary returns session-wide aggregates over
	// ``reconciliation_runs`` for the requesting user. Powers the
	// SessionDetailPage reconciliation tile (total / hard fail / soft fail)
	// so the headline numbers are not silently truncated to the current page.
	GetSessionReconciliationSummary(ctx context.Context, sessionID string, userID int64) (totalRuns, hardFailRuns, softFailRuns int64, err error)

	// Notification management
	GetNotificationSettings(ctx context.Context, userID int64) (domain.NotificationSettings, error)
	UpsertNotificationSettings(ctx context.Context, settings domain.NotificationSettings) (domain.NotificationSettings, error)
	GetNotificationChannel(ctx context.Context, userID int64, channel string) (domain.NotificationChannel, error)
	FindNotificationChannelByBindCodeHash(ctx context.Context, codeHash string, at time.Time) (domain.NotificationChannel, error)
	UpsertNotificationBindCode(ctx context.Context, userID int64, channel string, codeHash string, expiresAt time.Time) (domain.NotificationChannel, error)
	BindNotificationChannel(ctx context.Context, userID int64, channel string, targetID string, targetType string, targetLabel string, now time.Time) (domain.NotificationChannel, error)
	RevokeNotificationChannel(ctx context.Context, userID int64, channel string, now time.Time) error
	UpdateNotificationDeliveryStatus(ctx context.Context, userID int64, channel string, status string, errText string, at time.Time) error
	GetNotificationPlan(ctx context.Context, planCode string) (domain.NotificationPlan, error)

	// Phase D2 (2026-05-06): the demand-driven market-data control plane
	// (requests / streams / leases / history requests) moved out of
	// core-service into control-panel-service. See
	// `control-panel-service/internal/marketdata/repository/` for the
	// new owner of these methods.
}
