package risk

import (
	"context"
	"time"

	"github.com/hushine-tech/core-service/internal/exchange/adapter"
)

type DecisionStatus string

const (
	DecisionAllow  DecisionStatus = "ALLOW"
	DecisionReject DecisionStatus = "REJECT"
)

type Violation struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type Decision struct {
	Status     DecisionStatus `json:"status"`
	ReasonCode string         `json:"reason_code,omitempty"`
	Violations []Violation    `json:"violations,omitempty"`
	Warnings   []Violation    `json:"warnings,omitempty"`
	ReviewedAt time.Time      `json:"reviewed_at"`
}

type RouteKey struct {
	AccountID   int64
	VenueID     int64
	UserID      int64
	Environment int32
	Exchange    int32
	Market      int32
}

type PendingRouteKey struct {
	AccountID    int64
	VenueID      int64
	Environment  int32
	Exchange     int32
	Market       int32
	PositionSide int32
	Symbol       string
}

type SnapshotRequest struct {
	RouteKey
	Symbol string
}

type ReviewRequest struct {
	AccountID    int64
	VenueID      int64
	UserID       int64
	Environment  int32
	Exchange     int32
	Market       int32
	PositionSide int32
	Symbol       string
	Side         string
	Qty          float64
	Price        *float64
	MarkPrice    float64
	OrderType    string
	TimeInForce  string
	PostOnly     bool
	GoodTillDate *time.Time
	ReduceOnly   bool
}

type Balance struct {
	Asset     string
	Available float64
	Locked    float64
}

type Position struct {
	Symbol       string
	PositionSide string
	Qty          float64
}

type FuturesRiskMetadata struct {
	Symbol             string
	ConfiguredLeverage float64
	StepSize           float64
	TickSize           float64
	MinQty             float64
	MinNotional        float64
}

type Snapshot struct {
	AvailableBalance    float64
	Balances            []Balance
	Positions           []Position
	FuturesRiskMetadata []FuturesRiskMetadata
}

type Gate interface {
	Review(ctx context.Context, req ReviewRequest) (Decision, error)
}

type CapabilityReader interface {
	ReadOrderCapability(ctx context.Context, route RouteKey) (adapter.OrderCapability, error)
}

type SnapshotReader interface {
	ReadSnapshot(ctx context.Context, req SnapshotRequest) (Snapshot, error)
}

type PendingReader interface {
	HasPendingRoute(ctx context.Context, key PendingRouteKey) (bool, error)
}

type SymbolRulesReader interface {
	ReadSymbolRules(ctx context.Context, req SnapshotRequest) ([]FuturesRiskMetadata, error)
}
