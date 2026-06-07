package adapter

import (
	"context"
	"encoding/json"
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
)

// Route selects a concrete exchange capability implementation.
//
// It intentionally excludes account_id, venue_id, and symbol. Those values
// belong to requests handled by the selected capability, not to implementation
// selection.
type Route struct {
	Exchange    domain.Exchange
	Environment domain.Environment
	Market      domain.Market
}

type ParsedCredential struct {
	Exchange    domain.Exchange
	Environment domain.Environment
	Raw         json.RawMessage
	Metadata    map[string]string
}

type PortfolioSnapshotRequest struct {
	UserID     int64
	AccountID  int64
	VenueID    int64
	Credential ParsedCredential
	Symbols    []string
}

type PortfolioSnapshot struct {
	UserID           int64
	AccountID        int64
	VenueID          int64
	Exchange         domain.Exchange
	Environment      domain.Environment
	Market           domain.Market
	TotalValue       float64
	WalletBalance    float64
	AvailableBalance float64
	Balances         []BalanceEntry
	Positions        []PositionEntry
	VenueSnapshots   []PortfolioSnapshot
	OnlineInfo       *domain.OnlineAccountInfo
	UpdatedAt        time.Time
	RawPayload       json.RawMessage
}

type BalanceEntry struct {
	Asset            string
	WalletBalance    float64
	AvailableBalance float64
	Locked           float64
	ValueUSDT        float64
}

type PositionEntry struct {
	Symbol           string
	PositionSide     string
	Qty              float64
	EntryPrice       float64
	MarkPrice        float64
	UnrealizedPnl    float64
	MarginBalance    float64
	LiquidationPrice float64
}

type SymbolRulesRequest struct {
	Credential ParsedCredential
	Symbols    []string
}

type SymbolRules struct {
	Symbols []SymbolRule
}

type SymbolRule struct {
	Symbol      string
	Market      domain.Market
	MinQty      float64
	StepSize    float64
	MinNotional float64
	TickSize    float64
}

type OrderRequest struct {
	UserID         int64
	AccountID      int64
	VenueID        int64
	Exchange       domain.Exchange
	Environment    domain.Environment
	Market         domain.Market
	Symbol         string
	Side           string
	PositionSide   string
	MarginMode     domain.MarginMode
	PositionMode   domain.PositionMode
	OrderType      string
	TimeInForce    string
	PostOnly       bool
	GoodTillDate   *time.Time
	ReduceOnly     bool
	Qty            float64
	Price          *float64
	MarkPrice      float64
	DefaultFeeRate float64
	SlippageBps    float64
	ClientOrderID  string
	Credential     ParsedCredential
}

type OrderResult struct {
	ExchangeOrderID string
	ClientOrderID   string
	Symbol          string
	Side            string
	PositionSide    string
	OrderType       string
	TimeInForce     string
	Status          string
	OrigQty         float64
	ExecutedQty     float64
	RemainingQty    float64
	AvgPrice        float64
	Price           float64
	Fills           []FillDelta
	ErrorMessage    string
	FillPending     bool
}

type QueryOrderRequest struct {
	AccountID       int64
	VenueID         int64
	Symbol          string
	ClientOrderID   string
	ExchangeOrderID string
	Credential      ParsedCredential
}

type QueryTradesRequest struct {
	AccountID       int64
	VenueID         int64
	Symbol          string
	ExchangeOrderID string
	Credential      ParsedCredential
}

type OrderState struct {
	ExchangeOrderID string
	ClientOrderID   string
	Symbol          string
	Status          string
	OrigQty         float64
	ExecutedQty     float64
	RemainingQty    float64
	AvgPrice        float64
	UpdatedAt       time.Time
}

type FillDelta struct {
	ExchangeTradeID string
	ExchangeOrderID string
	Symbol          string
	Qty             float64
	FillPrice       float64
	Fee             float64
	FeeAsset        string
	FeeMissing      bool
	TradeTime       time.Time
}

type CancelOrderRequest struct {
	AccountID       int64
	VenueID         int64
	Symbol          string
	ClientOrderID   string
	ExchangeOrderID string
	Credential      ParsedCredential
}

type CancelOrderResult struct {
	ExchangeOrderID string
	ClientOrderID   string
	Symbol          string
	Status          string
	CancelledAt     time.Time
}

type CredentialValidator interface {
	ValidateCredential(ctx context.Context, raw json.RawMessage) (ParsedCredential, error)
}

type AccountSnapshotReader interface {
	ReadPortfolioSnapshot(ctx context.Context, req PortfolioSnapshotRequest) (PortfolioSnapshot, error)
}

type SymbolRulesReader interface {
	ReadSymbolRules(ctx context.Context, req SymbolRulesRequest) (SymbolRules, error)
}

type OrderExecutor interface {
	PlaceOrder(ctx context.Context, req OrderRequest) (OrderResult, error)
}

type OrderStateReader interface {
	QueryOrder(ctx context.Context, req QueryOrderRequest) (OrderState, error)
	QueryTrades(ctx context.Context, req QueryTradesRequest) ([]FillDelta, error)
}

type OrderCanceller interface {
	CancelOrder(ctx context.Context, req CancelOrderRequest) (CancelOrderResult, error)
}
