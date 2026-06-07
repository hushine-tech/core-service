package domain

import (
	"errors"
	"time"
)

// Environment is the account-level runtime environment.
type Environment int16

const (
	EnvironmentBacktest Environment = 0
	EnvironmentDemo     Environment = 1
	EnvironmentLive     Environment = 2
)

func (e Environment) String() string {
	switch e {
	case EnvironmentBacktest:
		return "backtest"
	case EnvironmentDemo:
		return "demo"
	case EnvironmentLive:
		return "live"
	default:
		return "unknown"
	}
}

type Exchange int16

const (
	ExchangeBinance Exchange = 1
	ExchangeOKX     Exchange = 2
)

func (e Exchange) String() string {
	switch e {
	case ExchangeBinance:
		return "binance"
	case ExchangeOKX:
		return "okx"
	default:
		return "unknown"
	}
}

type Market int16

const (
	MarketSpot             Market = 1
	MarketPerpetualFutures Market = 2
	MarketDeliveryFutures  Market = 3
)

func (m Market) String() string {
	switch m {
	case MarketSpot:
		return "spot"
	case MarketPerpetualFutures:
		return "perpetual_futures"
	case MarketDeliveryFutures:
		return "delivery_futures"
	default:
		return "unknown"
	}
}

type AccountStatus int16

const (
	AccountStatusActive   AccountStatus = 1
	AccountStatusArchived AccountStatus = 2
)

type VenueStatus int16

const (
	VenueStatusActive   VenueStatus = 1
	VenueStatusDisabled VenueStatus = 2
	VenueStatusRevoked  VenueStatus = 3
	VenueStatusArchived VenueStatus = 4
)

type MarginMode int16

const (
	MarginModeNone     MarginMode = 0
	MarginModeCross    MarginMode = 1
	MarginModeIsolated MarginMode = 2
)

type PositionMode int16

const (
	PositionModeNone   PositionMode = 0
	PositionModeOneWay PositionMode = 1
	PositionModeHedge  PositionMode = 2
)

var ErrInvalidVenueModes = errors.New("invalid venue margin/position modes for market")

type Venue struct {
	VenueID               int64
	UserID                int64
	AccountID             *int64
	Exchange              Exchange
	Market                Market
	Environment           Environment
	Status                VenueStatus
	DisplayName           string
	Description           string
	APIKey                string
	CredentialInfo        string
	CredentialKeyVersion  string
	CredentialFingerprint string
	MarginMode            MarginMode
	PositionMode          PositionMode
	CreatedAt             time.Time
	UpdatedAt             time.Time
	LastUsedAt            *time.Time
	ArchivedAt            *time.Time
	ArchivedReason        string
}

func (v Venue) ValidateMarketModes() error {
	switch v.Market {
	case MarketSpot:
		if v.MarginMode != MarginModeNone || v.PositionMode != PositionModeNone {
			return ErrInvalidVenueModes
		}
	case MarketPerpetualFutures:
		if (v.MarginMode != MarginModeCross && v.MarginMode != MarginModeIsolated) ||
			(v.PositionMode != PositionModeOneWay && v.PositionMode != PositionModeHedge) {
			return ErrInvalidVenueModes
		}
	}
	return nil
}

type VenueEvent struct {
	EventID    int64
	VenueID    int64
	AccountID  *int64
	UserID     int64
	EventType  int16
	Reason     string
	DetailJSON string
	CreatedAt  time.Time
}

type SessionVenue struct {
	SessionID             string
	VenueID               int64
	AccountID             int64
	UserID                int64
	Exchange              Exchange
	Market                Market
	Environment           Environment
	DisplayName           string
	APIKey                string
	CredentialFingerprint string
	MarginMode            MarginMode
	PositionMode          PositionMode
	VenueStatus           VenueStatus
	CapturedAt            time.Time
}

type VenueRouteMeta struct {
	AccountID      int64
	VenueID        int64
	UserID         int64
	Environment    Environment
	Exchange       Exchange
	Market         Market
	MarginMode     MarginMode
	PositionMode   PositionMode
	APIKey         string
	CredentialInfo string
	DefaultFeeRate float64
	SlippageBps    float64
}

type PreflightIssue struct {
	Code     string
	Message  string
	Exchange Exchange
	Market   Market
}

// SnapshotReason 表示快照写入的触发原因（存储为 SMALLINT）。
type SnapshotReason int16

const (
	SnapshotReasonInitialSeed            SnapshotReason = 0 // 账号创建初始化
	SnapshotReasonOrderFill              SnapshotReason = 1 // 成交后
	SnapshotReasonStrategyStart          SnapshotReason = 2 // 策略启动
	SnapshotReasonStrategyEnd            SnapshotReason = 3 // 策略结束
	SnapshotReasonReconciliationLocal    SnapshotReason = 4 // 对账：策略本地计算值
	SnapshotReasonReconciliationExchange SnapshotReason = 5 // 对账：交易所实际值
	SnapshotReasonPeriodicSample         SnapshotReason = 6 // 定时采样
	SnapshotReasonRestartRecovery        SnapshotReason = 7 // 实盘服务重启恢复
)

// StrategySession represents a single strategy execution run (backtest or live).
type StrategySession struct {
	SessionID       string
	AccountID       int64
	UserID          int64
	StrategyID      int64
	Environment     Environment
	Status          string // running, stopping, recoverable, finished, stopped, failed, stop_failed, preflight_failed (completed = legacy)
	Interval        string // "1m", "5m", etc.
	StartTimeMs     *int64 // 回测参数（实盘为 nil）
	EndTimeMs       *int64
	BarsProcessed   int
	Error           string
	ErrorCode       string
	ErrorMessage    string
	ErrorDetailJSON string
	RuntimeID       string // owning strategy-runtime; empty means legacy/unbound
	RuntimeSource   string // hosted / self_hosted; snapshot at session creation
	RuntimeName     string // runtime name snapshot at session creation
	SessionType     string // backtest / debugging / demo
	RuntimeVersion  string
	SessionName     string
	StartedAt       time.Time
	CompletedAt     *time.Time
	CreatedAt       time.Time
}

const (
	SessionStatusPreflightFailed = "preflight_failed"
)

// SnapshotRow represents an account snapshot row (for session detail queries).
type SnapshotRow struct {
	Time             time.Time
	AccountID        int64
	SnapshotReason   SnapshotReason
	TotalValue       float64
	WalletBalance    float64
	AvailableBalance float64
	FuturesJSON      string
	SpotJSON         string
	SessionID        string
	StrategyID       int64
}

// Account represents a registered trading account.
type Account struct {
	AccountID      int64         `json:"account_id"`
	UserID         int64         `json:"user_id"`
	Name           string        `json:"name"`
	Description    string        `json:"description"`
	Environment    Environment   `json:"environment"`
	Status         AccountStatus `json:"status"`
	APIKey         string        `json:"api_key,omitempty"`
	APISecret      string        `json:"api_secret,omitempty"`
	MarginMode     string        `json:"margin_mode"`
	PositionMode   string        `json:"position_mode"`
	SlippageBps    float64       `json:"slippage_bps"`
	DefaultFeeRate float64       `json:"default_fee_rate"`
	CreatedAt      time.Time     `json:"created_at"`
	// 当前钱包状态（存储在 accounts 表）
	FuturesJSON      *FuturesWallet `json:"futures_json,omitempty"`
	SpotJSON         *SpotWallet    `json:"spot_json,omitempty"`
	TotalValue       float64        `json:"total_value"`
	WalletBalance    float64        `json:"wallet_balance"`
	AvailableBalance float64        `json:"available_balance"`
	StateUpdatedAt   *time.Time     `json:"state_updated_at,omitempty"`
}

type FuturesPosition struct {
	Symbol         string  `json:"symbol"`
	Direction      int32   `json:"direction"`
	InitialBalance float64 `json:"initial_balance"`
	Leverage       float64 `json:"leverage"`
	FeeRate        float64 `json:"fee_rate"`
	MarkPrice      float64 `json:"mark_price"`
	// Live fields from exchange
	Qty           float64 `json:"qty,omitempty"`
	PositionQty   float64 `json:"position_qty,omitempty"`
	EntryPrice    float64 `json:"entry_price,omitempty"`
	UnrealizedPnl float64 `json:"unrealized_pnl,omitempty"`
	PositionSide  string  `json:"position_side,omitempty"` // LONG / SHORT / BOTH
	// Phase A: Binance-standard position fields (additive, optional on non-exchange paths)
	MarginType             string  `json:"margin_type,omitempty"` // cross / isolated
	MarginMode             string  `json:"margin_mode,omitempty"` // canonical alias of margin_type
	Notional               float64 `json:"notional,omitempty"`
	InitialMargin          float64 `json:"initial_margin,omitempty"`
	PositionInitialMargin  float64 `json:"position_initial_margin,omitempty"`
	OpenOrderInitialMargin float64 `json:"open_order_initial_margin,omitempty"`
	MaintMargin            float64 `json:"maint_margin,omitempty"`
	IsolatedWallet         float64 `json:"isolated_wallet,omitempty"`
	LiquidationPrice       float64 `json:"liquidation_price,omitempty"`
	BreakEvenPrice         float64 `json:"break_even_price,omitempty"`
}

type FuturesRiskBracket struct {
	Bracket          int32   `json:"bracket"`
	NotionalFloor    float64 `json:"notional_floor"`
	NotionalCap      float64 `json:"notional_cap"`
	InitialLeverage  float64 `json:"initial_leverage"`
	MaintMarginRatio float64 `json:"maint_margin_ratio"`
	Cumulative       float64 `json:"cumulative"`
}

type FuturesRiskMetadata struct {
	Symbol               string               `json:"symbol"`
	ConfiguredLeverage   float64              `json:"configured_leverage,omitempty"`
	ConfiguredMarginMode string               `json:"configured_margin_mode,omitempty"`
	PricePrecision       int32                `json:"price_precision,omitempty"`
	QuantityPrecision    int32                `json:"quantity_precision,omitempty"`
	TickSize             float64              `json:"tick_size,omitempty"`
	StepSize             float64              `json:"step_size,omitempty"`
	Brackets             []FuturesRiskBracket `json:"brackets,omitempty"`
}

type FuturesWallet struct {
	MarginMode     string            `json:"margin_mode"`
	PositionMode   string            `json:"position_mode"`
	InitialBalance float64           `json:"initial_balance"`
	DepositSum     float64           `json:"deposit_sum"`
	WithdrawalSum  float64           `json:"withdrawal_sum"`
	Positions      []FuturesPosition `json:"positions"`
	// Live computed fields
	WalletBalance      float64 `json:"wallet_balance,omitempty"`
	AvailableBalance   float64 `json:"available_balance,omitempty"`
	TotalUnrealizedPnl float64 `json:"total_unrealized_pnl,omitempty"`
	UnrealizedPnl      float64 `json:"unrealized_pnl,omitempty"`
	MarginBalance      float64 `json:"margin_balance,omitempty"`
	// Phase A: Binance-standard account-level fields (additive)
	TotalMarginBalance          float64               `json:"total_margin_balance,omitempty"`
	TotalPositionInitialMargin  float64               `json:"total_position_initial_margin,omitempty"`
	TotalOpenOrderInitialMargin float64               `json:"total_open_order_initial_margin,omitempty"`
	TotalMaintMargin            float64               `json:"total_maint_margin,omitempty"`
	TotalCrossWalletBalance     float64               `json:"total_cross_wallet_balance,omitempty"`
	TotalCrossUnPnl             float64               `json:"total_cross_un_pnl,omitempty"`
	RiskMetadata                []FuturesRiskMetadata `json:"risk_metadata,omitempty"`
	MultiAssetsMode             bool                  `json:"multi_assets_mode,omitempty"`
	PortfolioMargin             bool                  `json:"portfolio_margin,omitempty"`
	DisplayWalletBalanceUsd     float64               `json:"display_wallet_balance_usd,omitempty"`
	DisplayMarginBalanceUsd     float64               `json:"display_margin_balance_usd,omitempty"`
	DisplayUnrealizedPnlUsd     float64               `json:"display_unrealized_pnl_usd,omitempty"`
}

type SpotAsset struct {
	Symbol        string   `json:"symbol"`
	Qty           float64  `json:"qty"`
	Locked        float64  `json:"locked"`
	AvgEntryPrice float64  `json:"avg_entry_price"`
	Price         *float64 `json:"price,omitempty"`
}

type SpotWallet struct {
	Free   float64     `json:"free"`
	Locked float64     `json:"locked"`
	Assets []SpotAsset `json:"assets"`
}

type SnapshotMetadata struct {
	UpdatedAt    time.Time      `json:"updated_at"`
	UpdateSource SnapshotReason `json:"update_source"`
	Version      uint64         `json:"version"`
}

type AccountSnapshot struct {
	AccountID   int64            `json:"account_id"`
	Exchange    string           `json:"exchange"`
	Environment string           `json:"environment"`
	Futures     FuturesWallet    `json:"futures"`
	Spot        SpotWallet       `json:"spot"`
	Metadata    SnapshotMetadata `json:"metadata"`
}

// Strategy represents an immutable strategy entity (only archivable, never modifiable).
type Strategy struct {
	StrategyID     int64
	UserID         int64
	Name           string
	Version        string
	Description    string
	Code           string
	Archived       bool
	CreatedAt      time.Time
	RuntimeVersion string
	RuntimeProfile string
}

type User struct {
	ID           int64
	Username     string
	PasswordHash string
	CreatedAt    time.Time
	PlanCode     string
}

// AccountStrategy represents a strategy mounted to an account.
type AccountStrategy struct {
	AccountID  int64
	StrategyID int64
	Active     bool
	MountedAt  time.Time
	Strategy   Strategy
}

// OnlineAccountInfo is the unified account state returned to strategy-service.
// For backtest: sourced from DB (strategy_push updates).
// For demo/live: sourced from exchange venues.
type OnlineAccountInfo struct {
	AccountID        int64         `json:"account_id"`
	Environment      Environment   `json:"environment"`
	Futures          FuturesWallet `json:"futures"`
	Spot             SpotWallet    `json:"spot"`
	TotalValue       float64       `json:"total_value"`
	WalletBalance    float64       `json:"wallet_balance"`
	AvailableBalance float64       `json:"available_balance"`
	UpdatedAt        time.Time     `json:"updated_at"`
}

// ReconciliationRunType distinguishes when a compare was triggered.
//
//   - checkpoint: StrategyStart / StrategyEnd / RestartRecovery
//   - event: OrderFill
//   - sampled: PeriodicSample (K-line-driven periodic trigger)
type ReconciliationRunType string

const (
	ReconciliationRunCheckpoint ReconciliationRunType = "checkpoint"
	ReconciliationRunEvent      ReconciliationRunType = "event"
	ReconciliationRunSampled    ReconciliationRunType = "sampled"
)

// FieldDiffSeverity classifies how a field diff affects pass/fail.
//
//   - hard: any breach → hard_pass = false
//   - soft: any breach → soft_pass = false (does not affect hard_pass)
//   - advisory: observed and recorded; does NOT affect hard_pass or soft_pass
type FieldDiffSeverity string

const (
	FieldDiffHard     FieldDiffSeverity = "hard"
	FieldDiffSoft     FieldDiffSeverity = "soft"
	FieldDiffAdvisory FieldDiffSeverity = "advisory"
)

// FieldDiff is one field's compare outcome between local and exchange snapshots.
// Written into reconciliation_runs.field_diffs or .advisory_diffs as a JSON array.
type FieldDiff struct {
	Field     string            `json:"field"`               // dot-path, e.g. "futures.wallet_balance" or "futures.positions[BTCUSDT].entry_price"
	Severity  FieldDiffSeverity `json:"severity"`            // hard / soft / advisory
	Exchange  float64           `json:"exchange"`            // exchange authoritative value
	Local     float64           `json:"local"`               // strategy-computed value
	DiffAbs   float64           `json:"diff_abs"`            // |exchange - local|
	DiffRatio float64           `json:"diff_ratio"`          // DiffAbs / |exchange| (0 if exchange is 0)
	Threshold map[string]any    `json:"threshold,omitempty"` // description of the threshold used (abs, ratio, tick multiple, etc.)
	Passed    bool              `json:"passed"`              // true = within threshold
}

// VenueWalletSnapshot is the canonical wallet state for one bound venue.
// Reconciliation compares these first, then compares the merged account view.
type VenueWalletSnapshot struct {
	VenueID     int64             `json:"venue_id"`
	Exchange    Exchange          `json:"exchange"`
	Environment Environment       `json:"environment"`
	Market      Market            `json:"market"`
	Snapshot    OnlineAccountInfo `json:"snapshot"`
}

// VenueReconciliationDiff is one venue-level compare result.
// Written into reconciliation_runs.venue_diffs_json.
type VenueReconciliationDiff struct {
	VenueID          int64             `json:"venue_id"`
	Exchange         Exchange          `json:"exchange"`
	Environment      Environment       `json:"environment"`
	Market           Market            `json:"market"`
	ExchangeSnapshot OnlineAccountInfo `json:"exchange_snapshot"`
	LocalSnapshot    OnlineAccountInfo `json:"local_snapshot"`
	FieldDiffs       []FieldDiff       `json:"field_diffs"`
	AdvisoryDiffs    []FieldDiff       `json:"advisory_diffs"`
	HardPass         bool              `json:"hard_pass"`
	SoftPass         bool              `json:"soft_pass"`
}

// ReconciliationRun is one compare execution — Phase C shadow-compare record.
// Written by core-service's reconciliation goroutine (never by main flow).
type ReconciliationRun struct {
	Time             time.Time
	RunID            string
	AccountID        int64
	UserID           int64
	SessionID        string // empty when not triggered by a session
	StrategyID       int64  // 0 when not triggered by a strategy
	Environment      Environment
	SnapshotReason   SnapshotReason
	RunType          ReconciliationRunType
	ExchangeSnapshot OnlineAccountInfo // canonical
	LocalSnapshot    OnlineAccountInfo // canonical
	VenueDiffs       []VenueReconciliationDiff
	FieldDiffs       []FieldDiff // Hard + Soft tier diffs only
	AdvisoryDiffs    []FieldDiff // Advisory tier (not gated)
	HardPass         bool
	SoftPass         bool
}

// RunTypeFromReason derives the ReconciliationRunType from a SnapshotReason.
// Unknown / backtest-only reasons return an empty RunType; the compare goroutine
// should skip them.
func RunTypeFromReason(reason SnapshotReason) ReconciliationRunType {
	switch reason {
	case SnapshotReasonOrderFill:
		return ReconciliationRunEvent
	case SnapshotReasonStrategyStart,
		SnapshotReasonStrategyEnd,
		SnapshotReasonRestartRecovery:
		return ReconciliationRunCheckpoint
	case SnapshotReasonPeriodicSample:
		return ReconciliationRunSampled
	default:
		// InitialSeed / ReconciliationLocal / ReconciliationExchange do not
		// themselves trigger a compare run.
		return ""
	}
}

// Phase D2 (2026-05-06): the market-data control-plane domain types
// (StreamKey / MarketDataStream / MarketDataRequest / MarketDataLease /
// MarketDataHistoryRequest plus StreamDesiredState / StreamActualState /
// MarketDataRequestStatus / MarketDataRequestScope /
// MarketDataHistoryRequestStatus enum families) moved to
// `control-panel-service/internal/domain/marketdata.go` along with the
// proto / repository / service. core-service no longer references
// any of them.
