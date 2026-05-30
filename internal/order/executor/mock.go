package executor

import (
	"context"
	"math"
	"strings"

	"github.com/google/uuid"

	"github.com/hushine-tech/core-service/internal/order/accountmeta"
)

// MockExecutor simulates order fills for backtest mode.
// It applies a fixed slippage (in basis points) against the mark price or specified limit price.
type MockExecutor struct{}

func NewMockExecutor() *MockExecutor { return &MockExecutor{} }

func (e *MockExecutor) Execute(_ context.Context, req OrderRequest, meta accountmeta.Meta) (OrderResult, error) {
	basePrice := req.MarkPrice
	if req.Price != nil {
		basePrice = *req.Price
	}

	// Apply slippage: BUY/LONG pays more, SELL/SHORT receives less.
	slippageFactor := meta.SlippageBps / 10000.0
	side := strings.ToUpper(req.Side)
	var fillPrice float64
	if side == "BUY" || side == "LONG" {
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
		Side:            req.Side,
		OrderType:       firstNonEmpty(req.OrderType, "MARKET"),
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

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func (e *MockExecutor) Resolve(_ context.Context, _ RecoveryRequest, _ accountmeta.Meta) (OrderResult, error) {
	return OrderResult{}, ErrOrderNotFound
}
