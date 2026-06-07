package binance

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/exchange/adapter"
)

func TestBinanceSpotPostOnlyMapsToLimitMaker(t *testing.T) {
	var gotPath string
	var got url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		got, err = url.ParseQuery(string(body))
		if err != nil {
			t.Fatalf("parse query: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"symbol":"ETHUSDT",
			"orderId":1001,
			"clientOrderId":"spot-post-only",
			"price":"2500.00",
			"origQty":"0.2",
			"executedQty":"0.2",
			"cummulativeQuoteQty":"500.00",
			"status":"FILLED",
			"type":"LIMIT_MAKER",
			"side":"BUY",
			"fills":[{"price":"2500.00","qty":"0.2","commission":"0.25","commissionAsset":"USDT","tradeId":9001}]
		}`))
	}))
	defer srv.Close()

	price := 2500.0
	exec := orderExecutor{
		route: adapter.Route{
			Exchange:    domain.ExchangeBinance,
			Environment: domain.EnvironmentDemo,
			Market:      domain.MarketSpot,
		},
		baseURL:    srv.URL,
		httpClient: srv.Client(),
	}
	result, err := exec.PlaceOrder(context.Background(), adapter.OrderRequest{
		Market:        domain.MarketSpot,
		Symbol:        "ETHUSDT",
		Side:          "BUY",
		OrderType:     "LIMIT",
		TimeInForce:   "GTC",
		PostOnly:      true,
		Qty:           0.2,
		Price:         &price,
		ClientOrderID: "spot-post-only",
		Credential:    testParsedCredential(),
	})
	if err != nil {
		t.Fatalf("PlaceOrder() error = %v", err)
	}
	if gotPath != "/api/v3/order" {
		t.Fatalf("path = %q, want /api/v3/order", gotPath)
	}
	if got.Get("type") != "LIMIT_MAKER" {
		t.Fatalf("type = %q, want LIMIT_MAKER", got.Get("type"))
	}
	if got.Get("timeInForce") != "" {
		t.Fatalf("timeInForce = %q, want empty for LIMIT_MAKER", got.Get("timeInForce"))
	}
	if got.Get("reduceOnly") != "" {
		t.Fatalf("reduceOnly = %q, want empty for spot", got.Get("reduceOnly"))
	}
	if got.Get("newOrderRespType") != "FULL" {
		t.Fatalf("newOrderRespType = %q, want FULL", got.Get("newOrderRespType"))
	}
	if result.Status != "FILLED" || result.ExchangeOrderID != "1001" || result.AvgPrice != 2500 {
		t.Fatalf("result = %+v, want filled spot result", result)
	}
	if len(result.Fills) != 1 || result.Fills[0].ExchangeTradeID != "9001" || result.Fills[0].FeeAsset != "USDT" {
		t.Fatalf("fills = %+v, want parsed spot fill", result.Fills)
	}
}

func TestBinanceSpotGTDRejectedBeforeHTTP(t *testing.T) {
	hit := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit = true
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	price := 2500.0
	gtd := time.Unix(1893456000, 0).UTC()
	exec := orderExecutor{
		route: adapter.Route{
			Exchange:    domain.ExchangeBinance,
			Environment: domain.EnvironmentDemo,
			Market:      domain.MarketSpot,
		},
		baseURL:    srv.URL,
		httpClient: srv.Client(),
	}
	result, err := exec.PlaceOrder(context.Background(), adapter.OrderRequest{
		Market:       domain.MarketSpot,
		Symbol:       "ETHUSDT",
		Side:         "BUY",
		OrderType:    "LIMIT",
		TimeInForce:  "GTD",
		GoodTillDate: &gtd,
		Qty:          0.2,
		Price:        &price,
		Credential:   testParsedCredential(),
	})
	if err != nil {
		t.Fatalf("PlaceOrder() error = %v", err)
	}
	if result.Status != "FAILED" {
		t.Fatalf("status = %q, want FAILED", result.Status)
	}
	if !strings.Contains(result.ErrorMessage, "time_in_force=GTD") {
		t.Fatalf("error = %q, want time_in_force=GTD", result.ErrorMessage)
	}
	if hit {
		t.Fatal("unexpected outbound HTTP request for spot GTD")
	}
}

func TestBinanceSpotExecutedOrderWithMissingFillsStaysFillPending(t *testing.T) {
	result := placeSpotOrderWithResponse(t, `{
		"symbol":"ETHUSDT",
		"orderId":1002,
		"clientOrderId":"spot-missing-fills",
		"price":"2500.00",
		"origQty":"0.2",
		"executedQty":"0.2",
		"cummulativeQuoteQty":"500.00",
		"status":"FILLED",
		"type":"LIMIT",
		"side":"BUY"
	}`)

	assertSpotFillPending(t, result, "missing spot fills")
}

func TestBinanceSpotExecutedOrderWithMalformedFeeStaysFillPending(t *testing.T) {
	result := placeSpotOrderWithResponse(t, `{
		"symbol":"ETHUSDT",
		"orderId":1003,
		"clientOrderId":"spot-bad-fee",
		"price":"2500.00",
		"origQty":"0.2",
		"executedQty":"0.2",
		"cummulativeQuoteQty":"500.00",
		"status":"FILLED",
		"type":"LIMIT",
		"side":"BUY",
		"fills":[{"price":"2500.00","qty":"0.2","commission":"bad-fee","commissionAsset":"USDT","tradeId":9003}]
	}`)

	assertSpotFillPending(t, result, "invalid spot fill commission")
}

func TestBinanceSpotExecutedOrderWithMissingTradeIDStaysFillPending(t *testing.T) {
	result := placeSpotOrderWithResponse(t, `{
		"symbol":"ETHUSDT",
		"orderId":1004,
		"clientOrderId":"spot-missing-trade-id",
		"price":"2500.00",
		"origQty":"0.2",
		"executedQty":"0.2",
		"cummulativeQuoteQty":"500.00",
		"status":"FILLED",
		"type":"LIMIT",
		"side":"BUY",
		"fills":[{"price":"2500.00","qty":"0.2","commission":"0.25","commissionAsset":"USDT"}]
	}`)

	assertSpotFillPending(t, result, "invalid spot fill trade_id")
}

func TestBinanceSpotExecutedOrderWithMalformedTradeIDTypeStaysFillPending(t *testing.T) {
	result := placeSpotOrderWithResponse(t, `{
		"symbol":"ETHUSDT",
		"orderId":1009,
		"clientOrderId":"spot-bad-trade-id-type",
		"price":"2500.00",
		"origQty":"0.2",
		"executedQty":"0.2",
		"cummulativeQuoteQty":"500.00",
		"status":"FILLED",
		"type":"LIMIT",
		"side":"BUY",
		"fills":[{"price":"2500.00","qty":"0.2","commission":"0.25","commissionAsset":"USDT","tradeId":"bad"}]
	}`)

	assertSpotFillPending(t, result, "invalid spot fill trade_id")
}

func TestBinanceSpotExecutedOrderParsesStringTradeID(t *testing.T) {
	result := placeSpotOrderWithResponse(t, `{
		"symbol":"ETHUSDT",
		"orderId":1010,
		"clientOrderId":"spot-string-trade-id",
		"price":"2500.00",
		"origQty":"0.2",
		"executedQty":"0.2",
		"cummulativeQuoteQty":"500.00",
		"status":"FILLED",
		"type":"LIMIT",
		"side":"BUY",
		"fills":[{"price":"2500.00","qty":"0.2","commission":"0.25","commissionAsset":"USDT","tradeId":"9010"}]
	}`)

	if result.FillPending {
		t.Fatalf("FillPending = true: %s", result.ErrorMessage)
	}
	if len(result.Fills) != 1 || result.Fills[0].ExchangeTradeID != "9010" {
		t.Fatalf("fills = %+v, want string trade id parsed", result.Fills)
	}
}

func TestBinanceSpotExecutedOrderWithIncompleteFillQtyStaysFillPending(t *testing.T) {
	result := placeSpotOrderWithResponse(t, `{
		"symbol":"ETHUSDT",
		"orderId":1006,
		"clientOrderId":"spot-incomplete-fill-qty",
		"price":"2500.00",
		"origQty":"0.2",
		"executedQty":"0.2",
		"cummulativeQuoteQty":"500.00",
		"status":"FILLED",
		"type":"LIMIT",
		"side":"BUY",
		"fills":[{"price":"2500.00","qty":"0.1","commission":"0.125","commissionAsset":"USDT","tradeId":9006}]
	}`)

	assertSpotFillPending(t, result, "incomplete spot fills")
}

func TestBinanceSpotExecutedOrderWithExcessiveFillQtyStaysFillPending(t *testing.T) {
	result := placeSpotOrderWithResponse(t, `{
		"symbol":"ETHUSDT",
		"orderId":1007,
		"clientOrderId":"spot-excessive-fill-qty",
		"price":"2500.00",
		"origQty":"0.2",
		"executedQty":"0.2",
		"cummulativeQuoteQty":"500.00",
		"status":"FILLED",
		"type":"LIMIT",
		"side":"BUY",
		"fills":[{"price":"2500.00","qty":"0.3","commission":"0.375","commissionAsset":"USDT","tradeId":9007}]
	}`)

	assertSpotFillPending(t, result, "excessive spot fills")
}

func TestBinanceSpotExecutedOrderWithMalformedExecutedQtyStaysFillPending(t *testing.T) {
	result := placeSpotOrderWithResponse(t, `{
		"symbol":"ETHUSDT",
		"orderId":1008,
		"clientOrderId":"spot-bad-executed-qty",
		"price":"2500.00",
		"origQty":"0.2",
		"executedQty":"bad",
		"status":"FILLED",
		"type":"LIMIT",
		"side":"BUY",
		"fills":[{"price":"2500.00","qty":"0.2","commission":"0.25","commissionAsset":"USDT","tradeId":9008}]
	}`)

	assertSpotFillPending(t, result, "invalid spot executed_qty")
}

func TestBinanceSpotReduceOnlyBuyRejectedBeforeHTTP(t *testing.T) {
	hit := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit = true
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	exec := newTestSpotOrderExecutor(srv)
	result, err := exec.PlaceOrder(context.Background(), adapter.OrderRequest{
		Market:        domain.MarketSpot,
		Symbol:        "ETHUSDT",
		Side:          "BUY",
		OrderType:     "MARKET",
		ReduceOnly:    true,
		Qty:           0.2,
		Credential:    testParsedCredential(),
		ClientOrderID: "spot-reduce-only-buy",
	})
	if err != nil {
		t.Fatalf("PlaceOrder() error = %v", err)
	}
	if result.Status != "FAILED" {
		t.Fatalf("status = %q, want FAILED", result.Status)
	}
	if !strings.Contains(result.ErrorMessage, "spot reduce_only BUY is unsupported") {
		t.Fatalf("error = %q, want spot reduce_only BUY rejection", result.ErrorMessage)
	}
	if hit {
		t.Fatal("unexpected outbound HTTP request for spot reduce_only BUY")
	}
}

func TestBinanceSpotReduceOnlySellDoesNotSendReduceOnly(t *testing.T) {
	var got url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		got, err = url.ParseQuery(string(body))
		if err != nil {
			t.Fatalf("parse query: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"symbol":"ETHUSDT",
			"orderId":1005,
			"clientOrderId":"spot-reduce-only-sell",
			"origQty":"0.2",
			"executedQty":"0",
			"status":"NEW",
			"type":"MARKET",
			"side":"SELL"
		}`))
	}))
	defer srv.Close()

	exec := newTestSpotOrderExecutor(srv)
	result, err := exec.PlaceOrder(context.Background(), adapter.OrderRequest{
		Market:        domain.MarketSpot,
		Symbol:        "ETHUSDT",
		Side:          "SELL",
		OrderType:     "MARKET",
		ReduceOnly:    true,
		Qty:           0.2,
		Credential:    testParsedCredential(),
		ClientOrderID: "spot-reduce-only-sell",
	})
	if err != nil {
		t.Fatalf("PlaceOrder() error = %v", err)
	}
	if result.Status == "FAILED" {
		t.Fatalf("status = FAILED: %s", result.ErrorMessage)
	}
	if got.Get("reduceOnly") != "" {
		t.Fatalf("reduceOnly = %q, want empty for spot platform semantic", got.Get("reduceOnly"))
	}
}

func TestBinanceRejectsUnsupportedOrderCombinationsBeforeHTTP(t *testing.T) {
	price := 2500.0
	gtd := time.Unix(1893456000, 0).UTC()
	cases := []struct {
		name    string
		req     adapter.OrderRequest
		wantMsg string
	}{
		{
			name: "spot market price",
			req: adapter.OrderRequest{
				Market:    domain.MarketSpot,
				Symbol:    "ETHUSDT",
				Side:      "BUY",
				OrderType: "MARKET",
				Qty:       0.2,
				Price:     &price,
			},
			wantMsg: "market order must not set price",
		},
		{
			name: "futures market price",
			req: adapter.OrderRequest{
				Market:    domain.MarketPerpetualFutures,
				Symbol:    "ETHUSDT",
				Side:      "BUY",
				OrderType: "MARKET",
				Qty:       0.2,
				Price:     &price,
			},
			wantMsg: "market order must not set price",
		},
		{
			name: "market post-only",
			req: adapter.OrderRequest{
				Market:    domain.MarketSpot,
				Symbol:    "ETHUSDT",
				Side:      "BUY",
				OrderType: "MARKET",
				PostOnly:  true,
				Qty:       0.2,
			},
			wantMsg: "market order must not set post_only",
		},
		{
			name: "market time-in-force",
			req: adapter.OrderRequest{
				Market:      domain.MarketSpot,
				Symbol:      "ETHUSDT",
				Side:        "BUY",
				OrderType:   "MARKET",
				TimeInForce: "GTC",
				Qty:         0.2,
			},
			wantMsg: "market order must not set time_in_force",
		},
		{
			name: "market good-till-date",
			req: adapter.OrderRequest{
				Market:       domain.MarketSpot,
				Symbol:       "ETHUSDT",
				Side:         "BUY",
				OrderType:    "MARKET",
				GoodTillDate: &gtd,
				Qty:          0.2,
			},
			wantMsg: "market order must not set good_till_date",
		},
		{
			name: "ioc post-only",
			req: adapter.OrderRequest{
				Market:      domain.MarketSpot,
				Symbol:      "ETHUSDT",
				Side:        "BUY",
				OrderType:   "LIMIT",
				TimeInForce: "IOC",
				PostOnly:    true,
				Qty:         0.2,
				Price:       &price,
			},
			wantMsg: "post_only cannot be combined with time_in_force=IOC",
		},
		{
			name: "futures gtd too near",
			req: adapter.OrderRequest{
				Market:       domain.MarketPerpetualFutures,
				Symbol:       "ETHUSDT",
				Side:         "BUY",
				OrderType:    "LIMIT",
				TimeInForce:  "GTD",
				GoodTillDate: ptrTime(time.Now().Add(599 * time.Second)),
				Qty:          0.2,
				Price:        &price,
			},
			wantMsg: "good_till_date must be at least 600s in the future",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hit := false
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hit = true
				w.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()

			tc.req.Credential = testParsedCredential()
			exec := orderExecutor{
				route: adapter.Route{
					Exchange:    domain.ExchangeBinance,
					Environment: domain.EnvironmentDemo,
					Market:      tc.req.Market,
				},
				baseURL:    srv.URL,
				httpClient: srv.Client(),
			}
			result, err := exec.PlaceOrder(context.Background(), tc.req)
			if err != nil {
				t.Fatalf("PlaceOrder() error = %v", err)
			}
			if result.Status != "FAILED" {
				t.Fatalf("status = %q, want FAILED", result.Status)
			}
			if !strings.Contains(result.ErrorMessage, tc.wantMsg) {
				t.Fatalf("error = %q, want to contain %q", result.ErrorMessage, tc.wantMsg)
			}
			if hit {
				t.Fatal("unexpected outbound HTTP request for unsupported order combination")
			}
		})
	}
}

func placeSpotOrderWithResponse(t *testing.T, response string) adapter.OrderResult {
	t.Helper()

	var hit bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit = true
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(response))
	}))
	defer srv.Close()

	price := 2500.0
	exec := newTestSpotOrderExecutor(srv)
	result, err := exec.PlaceOrder(context.Background(), adapter.OrderRequest{
		Market:        domain.MarketSpot,
		Symbol:        "ETHUSDT",
		Side:          "BUY",
		OrderType:     "LIMIT",
		TimeInForce:   "GTC",
		Qty:           0.2,
		Price:         &price,
		ClientOrderID: "spot-fill-pending",
		Credential:    testParsedCredential(),
	})
	if err != nil {
		t.Fatalf("PlaceOrder() error = %v", err)
	}
	if !hit {
		t.Fatal("expected outbound HTTP request")
	}
	return result
}

func assertSpotFillPending(t *testing.T, result adapter.OrderResult, wantMsg string) {
	t.Helper()
	if result.Status != "FILLED" {
		t.Fatalf("status = %q, want FILLED with fill pending", result.Status)
	}
	if !result.FillPending {
		t.Fatal("FillPending = false, want true")
	}
	if len(result.Fills) != 0 {
		t.Fatalf("fills = %+v, want no settleable fake fills", result.Fills)
	}
	if !strings.Contains(result.ErrorMessage, wantMsg) {
		t.Fatalf("error = %q, want to contain %q", result.ErrorMessage, wantMsg)
	}
}

func newTestSpotOrderExecutor(srv *httptest.Server) orderExecutor {
	return orderExecutor{
		route: adapter.Route{
			Exchange:    domain.ExchangeBinance,
			Environment: domain.EnvironmentDemo,
			Market:      domain.MarketSpot,
		},
		baseURL:    srv.URL,
		httpClient: srv.Client(),
	}
}

func ptrTime(t time.Time) *time.Time {
	return &t
}

func testParsedCredential() adapter.ParsedCredential {
	return adapter.ParsedCredential{
		Metadata: map[string]string{
			"api_key":    "key",
			"api_secret": "secret",
		},
	}
}
