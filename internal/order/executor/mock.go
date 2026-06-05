package executor

import (
	"context"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/hushine-tech/core-service/internal/order/accountmeta"
	"github.com/hushine-tech/core-service/internal/order/lifecycle"
)

// MockExecutor simulates order fills for the backtest environment.
// It applies a fixed slippage (in basis points) against the mark price or specified limit price.
type MockExecutor struct{}

func NewMockExecutor() *MockExecutor { return &MockExecutor{} }

func (e *MockExecutor) Execute(_ context.Context, req OrderRequest, meta accountmeta.Meta) (OrderResult, error) {
	orderType := strings.ToUpper(strings.TrimSpace(firstNonEmpty(req.OrderType, "MARKET")))
	if orderType == "LIMIT" {
		return e.executeLimit(req, meta), nil
	}

	basePrice := req.MarkPrice
	if req.Price != nil {
		basePrice = *req.Price
	}

	// Apply slippage: BUY pays more, SELL receives less.
	slippageFactor := meta.SlippageBps / 10000.0
	side, ok := normalizeMockOrderSide(req.Side)
	if !ok {
		return OrderResult{
			ClientOrderID: req.ClientOrderID,
			Symbol:        req.Symbol,
			Side:          req.Side,
			OrderType:     orderType,
			TimeInForce:   req.TimeInForce,
			Status:        "FAILED",
			ErrorMessage:  "unsupported order side: " + req.Side,
		}, nil
	}
	var fillPrice float64
	if side == "BUY" {
		fillPrice = basePrice * (1 + slippageFactor)
	} else {
		fillPrice = basePrice * (1 - slippageFactor)
	}
	fillPrice = math.Round(fillPrice*1e8) / 1e8 // avoid floating-point drift

	fee := math.Abs(req.Qty) * fillPrice * meta.DefaultFeeRate

	return OrderResult{
		ExchangeOrderID: uuid.New().String(),
		ClientOrderID:   req.ClientOrderID,
		Symbol:          req.Symbol,
		Side:            side,
		OrderType:       orderType,
		TimeInForce:     req.TimeInForce,
		Status:          "FILLED",
		OrigQty:         math.Abs(req.Qty),
		ExecutedQty:     math.Abs(req.Qty),
		RemainingQty:    0,
		AvgPrice:        fillPrice,
		Price:           basePrice,
		Fills: []FillResult{{
			Qty:       math.Abs(req.Qty),
			FillPrice: fillPrice,
			Fee:       fee,
		}},
	}, nil
}

func (e *MockExecutor) executeLimit(req OrderRequest, meta accountmeta.Meta) OrderResult {
	exchangeOrderID := uuid.New().String()
	side, ok := normalizeMockOrderSide(req.Side)
	if !ok {
		return OrderResult{
			ExchangeOrderID: exchangeOrderID,
			ClientOrderID:   req.ClientOrderID,
			Symbol:          req.Symbol,
			Side:            req.Side,
			OrderType:       "LIMIT",
			TimeInForce:     firstNonEmpty(req.TimeInForce, "GTC"),
			Status:          "FAILED",
			ErrorMessage:    "unsupported order side: " + req.Side,
		}
	}
	req.Side = side
	limitPrice := 0.0
	if req.Price != nil {
		limitPrice = *req.Price
	}
	if req.MarkPrice <= 0 || limitPrice <= 0 {
		return openLimitResult(req, exchangeOrderID, limitPrice)
	}
	now := time.Now().UTC()
	result := lifecycle.MatchBacktestLimitGTC(lifecycle.BacktestLimitOrder{
		AccountID:       req.AccountID,
		VenueID:         meta.VenueID,
		ExchangeOrderID: exchangeOrderID,
		ClientOrderID:   req.ClientOrderID,
		Symbol:          req.Symbol,
		Side:            side,
		Qty:             req.Qty,
		LimitPrice:      limitPrice,
		FeeRate:         meta.DefaultFeeRate,
	}, lifecycle.BacktestBar{
		Symbol: req.Symbol,
		Time:   now,
		Open:   req.MarkPrice,
		High:   req.MarkPrice,
		Low:    req.MarkPrice,
		Close:  req.MarkPrice,
	})

	out := OrderResult{
		ExchangeOrderID: exchangeOrderID,
		ClientOrderID:   req.ClientOrderID,
		Symbol:          req.Symbol,
		Side:            side,
		OrderType:       "LIMIT",
		TimeInForce:     firstNonEmpty(req.TimeInForce, "GTC"),
		Status:          result.State.Status,
		OrigQty:         result.State.OrigQty,
		ExecutedQty:     result.State.ExecutedQty,
		RemainingQty:    result.State.RemainingQty,
		AvgPrice:        result.State.AvgPrice,
		Price:           limitPrice,
	}
	if result.Event != nil {
		out.Fills = []FillResult{{
			Qty:       result.Event.FillDelta.Qty,
			FillPrice: result.Event.FillDelta.FillPrice,
			Fee:       result.Event.FillDelta.Fee,
		}}
	}
	return out
}

func openLimitResult(req OrderRequest, exchangeOrderID string, limitPrice float64) OrderResult {
	return OrderResult{
		ExchangeOrderID: exchangeOrderID,
		ClientOrderID:   req.ClientOrderID,
		Symbol:          req.Symbol,
		Side:            req.Side,
		OrderType:       "LIMIT",
		TimeInForce:     firstNonEmpty(req.TimeInForce, "GTC"),
		Status:          "NEW",
		OrigQty:         math.Abs(req.Qty),
		ExecutedQty:     0,
		RemainingQty:    math.Abs(req.Qty),
		Price:           limitPrice,
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func normalizeMockOrderSide(side string) (string, bool) {
	normalized := strings.ToUpper(strings.TrimSpace(side))
	switch normalized {
	case "BUY", "SELL":
		return normalized, true
	default:
		return "", false
	}
}

func (e *MockExecutor) Resolve(_ context.Context, _ RecoveryRequest, _ accountmeta.Meta) (OrderResult, error) {
	return OrderResult{}, ErrOrderNotFound
}
