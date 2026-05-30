package executor

import (
	"context"
	"testing"

	"github.com/hushine-tech/core-service/internal/order/accountmeta"
)

func TestMockLimitBuyFillsWhenMarkTouchesPrice(t *testing.T) {
	result, err := NewMockExecutor().Execute(context.Background(), OrderRequest{
		AccountID:   1,
		Symbol:      "ETHUSDT",
		Side:        "BUY",
		OrderType:   "LIMIT",
		TimeInForce: "GTC",
		Qty:         0.2,
		Price:       floatPtr(3000),
		MarkPrice:   2999,
	}, accountmeta.Meta{VenueID: 10, DefaultFeeRate: 0.001})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Status != "FILLED" || result.AvgPrice != 3000 || len(result.Fills) != 1 {
		t.Fatalf("result = %+v, want filled limit", result)
	}
}

func TestMockLimitRemainsOpenWhenMarkDoesNotTouchPrice(t *testing.T) {
	result, err := NewMockExecutor().Execute(context.Background(), OrderRequest{
		AccountID:   1,
		Symbol:      "ETHUSDT",
		Side:        "BUY",
		OrderType:   "LIMIT",
		TimeInForce: "GTC",
		Qty:         0.2,
		Price:       floatPtr(3000),
		MarkPrice:   3001,
	}, accountmeta.Meta{VenueID: 10, DefaultFeeRate: 0.001})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Status != "NEW" || result.ExecutedQty != 0 || result.RemainingQty != 0.2 {
		t.Fatalf("result = %+v, want open limit", result)
	}
}

func floatPtr(value float64) *float64 {
	return &value
}
