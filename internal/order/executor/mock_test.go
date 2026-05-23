package executor

import (
	"context"
	"testing"

	"github.com/hushine-tech/core-service/internal/order/accountmeta"
)

var testMeta = accountmeta.Meta{
	AccountID:      1,
	Mode:           0,
	DefaultFeeRate: 0.0004,
	SlippageBps:    5.0,
}

func TestMockExecutor_BUY_slippage(t *testing.T) {
	e := NewMockExecutor()
	req := OrderRequest{Symbol: "BTCUSDT", Side: "BUY", Qty: 0.1, MarkPrice: 50000}
	res, err := e.Execute(context.Background(), req, testMeta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != "FILLED" {
		t.Errorf("status: got %q", res.Status)
	}
	expected := 50000 * 1.0005 // slippage_bps=5 → factor=0.0005
	if res.AvgPrice != expected {
		t.Errorf("avg_price: got %v, want %v", res.AvgPrice, expected)
	}
}

func TestMockExecutor_SELL_slippage(t *testing.T) {
	e := NewMockExecutor()
	req := OrderRequest{Symbol: "BTCUSDT", Side: "SELL", Qty: 0.1, MarkPrice: 50000}
	res, _ := e.Execute(context.Background(), req, testMeta)
	expected := 50000 * 0.9995
	if res.AvgPrice != expected {
		t.Errorf("avg_price: got %v, want %v", res.AvgPrice, expected)
	}
}

func TestMockExecutor_zeroSlippage(t *testing.T) {
	e := NewMockExecutor()
	meta := testMeta
	meta.SlippageBps = 0
	req := OrderRequest{Symbol: "BTCUSDT", Side: "BUY", Qty: 1, MarkPrice: 30000}
	res, _ := e.Execute(context.Background(), req, meta)
	if res.AvgPrice != 30000 {
		t.Errorf("avg_price with zero slippage: got %v", res.AvgPrice)
	}
}

func TestMockExecutor_fee(t *testing.T) {
	e := NewMockExecutor()
	// qty=0.1, fill_price=50025 (50000*1.0005), fee_rate=0.0004
	req := OrderRequest{Symbol: "BTCUSDT", Side: "BUY", Qty: 0.1, MarkPrice: 50000}
	res, _ := e.Execute(context.Background(), req, testMeta)
	expectedFee := 0.1 * res.AvgPrice * 0.0004
	if len(res.Fills) != 1 || res.Fills[0].Fee != expectedFee {
		t.Errorf("fee: got %+v, want %v", res.Fills, expectedFee)
	}
}

func TestMockExecutor_limitPrice(t *testing.T) {
	e := NewMockExecutor()
	price := 49000.0
	req := OrderRequest{Symbol: "BTCUSDT", Side: "BUY", Qty: 1, MarkPrice: 50000, Price: &price}
	res, _ := e.Execute(context.Background(), req, testMeta)
	expected := 49000 * 1.0005
	if res.AvgPrice != expected {
		t.Errorf("avg_price with limit: got %v, want %v", res.AvgPrice, expected)
	}
}

func TestMockExecutor_orderIDUnique(t *testing.T) {
	e := NewMockExecutor()
	req := OrderRequest{Symbol: "BTCUSDT", Side: "BUY", Qty: 1, MarkPrice: 50000}
	r1, _ := e.Execute(context.Background(), req, testMeta)
	r2, _ := e.Execute(context.Background(), req, testMeta)
	if r1.ExchangeOrderID == r2.ExchangeOrderID {
		t.Error("exchange_order_id should be unique per order")
	}
}
