package repository

import (
	"context"
	"errors"
	"time"

	"github.com/hushine-tech/core-service/internal/order/lifecycle"
)

var ErrNotFound = errors.New("not found")

type OrderIntent struct {
	IntentID       string
	Time           time.Time
	AccountID      int64
	VenueID        int64
	UserID         int64
	StrategyID     int64
	SessionID      string
	Environment    int32
	Exchange       int32
	Market         int32
	PositionSide   int32
	OrderType      int32
	Symbol         string
	Side           string
	RequestedQty   float64
	RequestedPrice float64
	PostOnly       bool
	GoodTillDate   *time.Time
	ReduceOnly     bool
	Status         string
	RejectCode     string
	RejectMessage  string
}

type OrderAttempt struct {
	AttemptID       string
	IntentID        string
	Time            time.Time
	AccountID       int64
	VenueID         int64
	UserID          int64
	StrategyID      int64
	SessionID       string
	Environment     int32
	Exchange        int32
	Market          int32
	PositionSide    int32
	OrderType       int32
	Symbol          string
	Side            string
	RequestedQty    float64
	RequestedPrice  float64
	PostOnly        bool
	GoodTillDate    *time.Time
	ReduceOnly      bool
	MarkPrice       float64
	Status          string // "PENDING" / "FAILED" / "ACCEPTED" / "UNKNOWN" / ...
	ErrorMessage    string
	ClientOrderID   string
	RecoveryError   string
	RiskStatus      string
	RiskReasonsJSON string
	OrderID         string
	ExchangeOrderID string
}

type Order struct {
	OrderID            string
	ExchangeOrderID    string
	ClientOrderID      string
	AttemptID          string
	IntentID           string
	Time               time.Time
	AccountID          int64
	VenueID            int64
	UserID             int64
	StrategyID         int64
	SessionID          string
	Environment        int32
	Exchange           int32
	Market             int32
	PositionSide       int32
	Symbol             string
	Side               string
	OrigQty            float64
	ExecutedQty        float64
	RemainingQty       float64
	AvgPrice           float64
	Price              float64
	PostOnly           bool
	GoodTillDate       *time.Time
	ReduceOnly         bool
	Status             string
	ErrorMessage       string
	RecoveryStatus     string
	RecoveryStartedAt  *time.Time
	NextCheckAt        *time.Time
	RecoveryDeadlineAt *time.Time
	LastRecoveryError  string
	ForceClosedAt      *time.Time
}

type OrderFill struct {
	FillID          string
	ExchangeTradeID string
	OrderID         string
	ExchangeOrderID string
	AttemptID       string
	IntentID        string
	Time            time.Time
	AccountID       int64
	VenueID         int64
	UserID          int64
	Symbol          string
	Side            string
	Qty             float64
	FillPrice       float64
	Fee             float64
	Status          string
	StrategyID      int64
	Environment     int32
	Exchange        int32
	Market          int32
	PositionSide    int32
	SessionID       string
}

// Repository is the data access interface for the order domain.
type Repository interface {
	UpsertOrderIntent(ctx context.Context, intent OrderIntent) error
	CreateOrderAttempt(ctx context.Context, attempt OrderAttempt) error
	FinalizeOrderAttempt(ctx context.Context, attempt OrderAttempt, order *Order, fills []OrderFill) error
	FindOrderAttempt(ctx context.Context, userID, accountID int64, intentID, attemptID, clientOrderID string) (OrderAttempt, error)
	FindOrderByAttempt(ctx context.Context, attemptID string) (Order, error)
	ListOrderFillsByAttempt(ctx context.Context, attemptID string) ([]OrderFill, error)
	QueryOrderIntentsPaginated(ctx context.Context, userID, accountID, strategyID int64, sessionID string, limit, offset int) ([]OrderIntent, int64, error)
	// Ancestor IDs are optional; pass "" to skip.
	QueryOrderAttemptsPaginated(ctx context.Context, userID, accountID, strategyID int64, sessionID, intentID string, limit, offset int) ([]OrderAttempt, int64, error)
	QueryOrdersPaginated(ctx context.Context, userID, accountID, strategyID int64, sessionID, intentID, attemptID string, limit, offset int) ([]Order, int64, error)
	QueryOrderFillsPaginated(ctx context.Context, userID, accountID, strategyID int64, sessionID, intentID, attemptID, orderID string, limit, offset int) ([]OrderFill, int64, error)
	ListOpenOrders(ctx context.Context, limit int) ([]lifecycle.OpenOrder, error)
	ListDueOpenOrders(ctx context.Context, limit int) ([]lifecycle.OpenOrder, error)
	SaveLifecycleEvent(ctx context.Context, event lifecycle.Event) (lifecycle.Event, error)
	ListLifecycleEvents(ctx context.Context, sessionID string, afterEventID int64, limit int) ([]lifecycle.Event, error)
}
