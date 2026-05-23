package executor

import (
	"context"
	"errors"

	"github.com/hushine-tech/core-service/internal/order/accountmeta"
)

// OrderRequest is the normalised order payload passed to all executors.
type OrderRequest struct {
	AccountID     int64
	Symbol        string
	Side          string // "BUY" / "SELL"
	Qty           float64
	Price         *float64 // nil = market order
	MarkPrice     float64  // current mark price (used by mock executor)
	ClientOrderID string
}

type RecoveryRequest struct {
	AccountID       int64
	Symbol          string
	ClientOrderID   string
	ExchangeOrderID string
}

type FillResult struct {
	ExchangeTradeID string
	Qty             float64
	FillPrice       float64
	Fee             float64
	FeeMissing      bool
}

// OrderResult captures the exchange execution outcome for one attempt.
type OrderResult struct {
	ExchangeOrderID string
	ClientOrderID   string
	Symbol          string
	Side            string
	Status          string // "NEW" / "PARTIALLY_FILLED" / "FILLED" / "FAILED" / ...
	OrigQty         float64
	ExecutedQty     float64
	RemainingQty    float64
	AvgPrice        float64
	Price           float64
	Fills           []FillResult
	ErrorMessage    string
	FillPending     bool
}

var ErrOrderNotFound = errors.New("exchange order not found")

// Executor routes an order to the appropriate execution backend.
type Executor interface {
	Execute(ctx context.Context, req OrderRequest, meta accountmeta.Meta) (OrderResult, error)
	Resolve(ctx context.Context, req RecoveryRequest, meta accountmeta.Meta) (OrderResult, error)
}
