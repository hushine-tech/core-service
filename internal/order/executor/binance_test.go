package executor

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/internal/order/accountmeta"
	"github.com/hushine-tech/golang-lib/middleware/httpclient"
	elog "github.com/hushine-tech/golang-lib/pkg/log"
)

type noopExtAPILogger struct{}

func (noopExtAPILogger) ExtAPI(context.Context, elog.ExtAPILogEntry) {}

func TestBinanceExecutor_OneWayBuyPassesThrough(t *testing.T) {
	t.Parallel()

	var got url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/fapi/v1/userTrades" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"id":11,"orderId":1,"price":"2500","qty":"1.065","commission":"0.1"}]`))
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		got, err = url.ParseQuery(string(body))
		if err != nil {
			t.Fatalf("parse query: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"orderId":1,"symbol":"ETHUSDT","side":"BUY","origQty":"1.065","avgPrice":"2500","executedQty":"1.065","status":"FILLED"}`))
	}))
	defer srv.Close()

	exec := &BinanceExecutor{
		baseURL:    srv.URL,
		httpClient: httpclient.New(&http.Client{}, noopExtAPILogger{}, "binance_test"),
	}

	res, err := exec.Execute(context.Background(), OrderRequest{
		Symbol:    "ETHUSDT",
		Side:      "BUY",
		Qty:       1.065,
		MarkPrice: 2500,
	}, accountmeta.Meta{
		APIKey:       "key",
		APISecret:    "secret",
		PositionMode: "one_way",
	})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if res.Status != "FILLED" {
		t.Fatalf("status = %q, want FILLED (error=%s)", res.Status, res.ErrorMessage)
	}
	if res.Side != "BUY" {
		t.Fatalf("result side = %q, want BUY", res.Side)
	}
	if len(res.Fills) != 1 {
		t.Fatalf("fills = %d, want 1", len(res.Fills))
	}
	if res.Fills[0].ExchangeTradeID != "11" || res.Fills[0].Fee != 0.1 || res.Fills[0].FeeMissing {
		t.Fatalf("fill fee backfill = %+v, want trade_id=11 fee=0.1 confirmed", res.Fills[0])
	}
	if got.Get("side") != "BUY" {
		t.Fatalf("side = %q, want BUY", got.Get("side"))
	}
	if got.Get("newOrderRespType") != "RESULT" {
		t.Fatalf("newOrderRespType = %q, want RESULT", got.Get("newOrderRespType"))
	}
	if got.Get("positionSide") != "" {
		t.Fatalf("positionSide = %q, want empty", got.Get("positionSide"))
	}
}

func TestBinanceExecutor_OneWaySellPassesThrough(t *testing.T) {
	t.Parallel()

	var got url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/fapi/v1/userTrades" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"id":12,"orderId":2,"price":"2400","qty":"0.5","commission":"0.05"}]`))
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		got, err = url.ParseQuery(string(body))
		if err != nil {
			t.Fatalf("parse query: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"orderId":2,"symbol":"ETHUSDT","side":"SELL","origQty":"0.5","avgPrice":"2400","executedQty":"0.5","status":"FILLED"}`))
	}))
	defer srv.Close()

	exec := &BinanceExecutor{
		baseURL:    srv.URL,
		httpClient: httpclient.New(&http.Client{}, noopExtAPILogger{}, "binance_test"),
	}

	res, err := exec.Execute(context.Background(), OrderRequest{
		Symbol:    "ETHUSDT",
		Side:      "SELL",
		Qty:       0.5,
		MarkPrice: 2400,
	}, accountmeta.Meta{
		APIKey:       "key",
		APISecret:    "secret",
		PositionMode: "one_way",
	})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if res.Status != "FILLED" {
		t.Fatalf("status = %q, want FILLED (error=%s)", res.Status, res.ErrorMessage)
	}
	if res.Side != "SELL" {
		t.Fatalf("result side = %q, want SELL", res.Side)
	}
	if got.Get("side") != "SELL" {
		t.Fatalf("side = %q, want SELL", got.Get("side"))
	}
}

func TestBinanceExecutor_OneWayRejectsDirectionSide(t *testing.T) {
	t.Parallel()

	hit := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit = true
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	exec := &BinanceExecutor{
		baseURL:    srv.URL,
		httpClient: httpclient.New(&http.Client{}, noopExtAPILogger{}, "binance_test"),
	}

	res, err := exec.Execute(context.Background(), OrderRequest{
		Symbol:    "ETHUSDT",
		Side:      "LONG",
		Qty:       1,
		MarkPrice: 2500,
	}, accountmeta.Meta{
		APIKey:       "key",
		APISecret:    "secret",
		PositionMode: "one_way",
	})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if res.Status != "FAILED" {
		t.Fatalf("status = %q, want FAILED", res.Status)
	}
	if res.ErrorMessage == "" {
		t.Fatal("expected error message for rejected direction side")
	}
	if hit {
		t.Fatal("unexpected outbound HTTP request for rejected direction side")
	}
}

func TestBinanceFuturesPostOnlyMapsToGTX(t *testing.T) {
	t.Parallel()

	price := 2500.0
	got := executeAndCaptureBinanceOrderParams(t, OrderRequest{
		Symbol:      "ETHUSDT",
		Side:        "BUY",
		OrderType:   "LIMIT",
		TimeInForce: "GTC",
		PostOnly:    true,
		Qty:         1,
		Price:       &price,
		MarkPrice:   2500,
	})

	if got.Get("type") != "LIMIT" {
		t.Fatalf("type = %q, want LIMIT", got.Get("type"))
	}
	if got.Get("timeInForce") != "GTX" {
		t.Fatalf("timeInForce = %q, want GTX", got.Get("timeInForce"))
	}
	if got.Get("price") != "2500" {
		t.Fatalf("price = %q, want 2500", got.Get("price"))
	}
}

func TestBinanceFuturesGTDIncludesGoodTillDate(t *testing.T) {
	t.Parallel()

	price := 2500.0
	gtd := time.Now().Add(20 * time.Minute).UTC()
	got := executeAndCaptureBinanceOrderParams(t, OrderRequest{
		Symbol:       "ETHUSDT",
		Side:         "BUY",
		OrderType:    "LIMIT",
		TimeInForce:  "GTD",
		GoodTillDate: &gtd,
		Qty:          1,
		Price:        &price,
		MarkPrice:    2500,
	})

	if got.Get("timeInForce") != "GTD" {
		t.Fatalf("timeInForce = %q, want GTD", got.Get("timeInForce"))
	}
	wantGTD := strconv.FormatInt(gtd.UnixMilli(), 10)
	if got.Get("goodTillDate") != wantGTD {
		t.Fatalf("goodTillDate = %q, want unix milliseconds %s", got.Get("goodTillDate"), wantGTD)
	}
}

func TestBinanceFuturesReduceOnlyParam(t *testing.T) {
	t.Parallel()

	got := executeAndCaptureBinanceOrderParams(t, OrderRequest{
		Symbol:     "ETHUSDT",
		Side:       "SELL",
		OrderType:  "MARKET",
		ReduceOnly: true,
		Qty:        1,
		MarkPrice:  2500,
	})

	if got.Get("type") != "MARKET" {
		t.Fatalf("type = %q, want MARKET", got.Get("type"))
	}
	if got.Get("reduceOnly") != "true" {
		t.Fatalf("reduceOnly = %q, want true", got.Get("reduceOnly"))
	}
	if got.Get("timeInForce") != "" || got.Get("price") != "" {
		t.Fatalf("market params timeInForce=%q price=%q, want both empty", got.Get("timeInForce"), got.Get("price"))
	}
}

func TestBinanceExecutor_UnsupportedOrderCombinationsFailBeforeHTTP(t *testing.T) {
	price := 2500.0
	gtd := time.Unix(1893456000, 0).UTC()
	cases := []struct {
		name    string
		req     OrderRequest
		wantMsg string
	}{
		{
			name: "market price",
			req: OrderRequest{
				Symbol:    "ETHUSDT",
				Side:      "BUY",
				OrderType: "MARKET",
				Qty:       1,
				Price:     &price,
			},
			wantMsg: "market order must not set price",
		},
		{
			name: "market post-only",
			req: OrderRequest{
				Symbol:    "ETHUSDT",
				Side:      "BUY",
				OrderType: "MARKET",
				PostOnly:  true,
				Qty:       1,
			},
			wantMsg: "market order must not set post_only",
		},
		{
			name: "market time-in-force",
			req: OrderRequest{
				Symbol:      "ETHUSDT",
				Side:        "BUY",
				OrderType:   "MARKET",
				TimeInForce: "GTC",
				Qty:         1,
			},
			wantMsg: "market order must not set time_in_force",
		},
		{
			name: "market good-till-date",
			req: OrderRequest{
				Symbol:       "ETHUSDT",
				Side:         "BUY",
				OrderType:    "MARKET",
				GoodTillDate: &gtd,
				Qty:          1,
			},
			wantMsg: "market order must not set good_till_date",
		},
		{
			name: "ioc post-only",
			req: OrderRequest{
				Symbol:      "ETHUSDT",
				Side:        "BUY",
				OrderType:   "LIMIT",
				TimeInForce: "IOC",
				PostOnly:    true,
				Qty:         1,
				Price:       &price,
			},
			wantMsg: "post_only cannot be combined with time_in_force=IOC",
		},
		{
			name: "gtd too near",
			req: OrderRequest{
				Symbol:       "ETHUSDT",
				Side:         "BUY",
				OrderType:    "LIMIT",
				TimeInForce:  "GTD",
				GoodTillDate: ptrTime(time.Now().Add(599 * time.Second)),
				Qty:          1,
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
				_, _ = w.Write([]byte(`{}`))
			}))
			defer srv.Close()

			exec := &BinanceExecutor{
				baseURL:    srv.URL,
				httpClient: httpclient.New(&http.Client{}, noopExtAPILogger{}, "binance_test"),
			}
			res, err := exec.Execute(context.Background(), tc.req, accountmeta.Meta{
				APIKey:       "key",
				APISecret:    "secret",
				PositionMode: "one_way",
			})
			if err != nil {
				t.Fatalf("Execute returned error: %v", err)
			}
			if res.Status != "FAILED" {
				t.Fatalf("status = %q, want FAILED", res.Status)
			}
			if !strings.Contains(res.ErrorMessage, tc.wantMsg) {
				t.Fatalf("error = %q, want to contain %q", res.ErrorMessage, tc.wantMsg)
			}
			if hit {
				t.Fatal("unexpected outbound HTTP request for unsupported order contract")
			}
		})
	}
}

func executeAndCaptureBinanceOrderParams(t *testing.T, req OrderRequest) url.Values {
	t.Helper()

	var got url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/fapi/v1/order" {
			t.Fatalf("path = %q, want /fapi/v1/order", r.URL.Path)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		got, err = url.ParseQuery(string(body))
		if err != nil {
			t.Fatalf("parse query: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"orderId":9,"symbol":"ETHUSDT","side":"BUY","origQty":"1","avgPrice":"0","executedQty":"0","status":"NEW"}`))
	}))
	defer srv.Close()

	exec := &BinanceExecutor{
		baseURL:    srv.URL,
		httpClient: httpclient.New(&http.Client{}, noopExtAPILogger{}, "binance_test"),
	}
	res, err := exec.Execute(context.Background(), req, accountmeta.Meta{
		APIKey:       "key",
		APISecret:    "secret",
		PositionMode: "one_way",
	})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if res.Status == "FAILED" {
		t.Fatalf("status = FAILED: %s", res.ErrorMessage)
	}
	return got
}

func ptrTime(t time.Time) *time.Time {
	return &t
}

func TestBinanceExecutor_ConfirmedFillMissingTradeFeeIsObservable(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet && r.URL.Path == "/fapi/v1/userTrades" {
			_, _ = w.Write([]byte(`[]`))
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		_, _ = w.Write([]byte(`{"orderId":3,"symbol":"ETHUSDT","side":"BUY","origQty":"0.5","avgPrice":"2400","executedQty":"0.5","status":"FILLED"}`))
	}))
	defer srv.Close()

	exec := &BinanceExecutor{
		baseURL:             srv.URL,
		httpClient:          httpclient.New(&http.Client{}, noopExtAPILogger{}, "binance_test"),
		tradeLookupAttempts: 2,
		tradeLookupDelay:    0,
	}

	res, err := exec.Execute(context.Background(), OrderRequest{
		Symbol:    "ETHUSDT",
		Side:      "BUY",
		Qty:       0.5,
		MarkPrice: 2400,
	}, accountmeta.Meta{
		APIKey:       "key",
		APISecret:    "secret",
		PositionMode: "one_way",
	})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if res.Status != "FILLED" {
		t.Fatalf("status = %q, want FILLED", res.Status)
	}
	if res.ErrorMessage == "" {
		t.Fatal("expected missing fee message to be observable on order result")
	}
	if !res.FillPending {
		t.Fatalf("fill_pending = false, want true")
	}
	if len(res.Fills) != 0 {
		t.Fatalf("fills = %d, want no settleable fallback fill", len(res.Fills))
	}
}

func TestBinanceExecutor_PartialTradeBackfillStaysFillPending(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet && r.URL.Path == "/fapi/v1/userTrades" {
			_, _ = w.Write([]byte(`[{"id":31,"orderId":3,"price":"2400","qty":"0.2","commission":"0.0192"}]`))
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		_, _ = w.Write([]byte(`{"orderId":3,"symbol":"ETHUSDT","side":"BUY","origQty":"0.5","avgPrice":"2400","executedQty":"0.5","status":"FILLED"}`))
	}))
	defer srv.Close()

	exec := &BinanceExecutor{
		baseURL:             srv.URL,
		httpClient:          httpclient.New(&http.Client{}, noopExtAPILogger{}, "binance_test"),
		tradeLookupAttempts: 2,
		tradeLookupDelay:    0,
	}

	res, err := exec.Execute(context.Background(), OrderRequest{
		Symbol:    "ETHUSDT",
		Side:      "BUY",
		Qty:       0.5,
		MarkPrice: 2400,
	}, accountmeta.Meta{
		APIKey:       "key",
		APISecret:    "secret",
		PositionMode: "one_way",
	})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if !res.FillPending {
		t.Fatalf("fill_pending = false, want true")
	}
	if len(res.Fills) != 0 {
		t.Fatalf("fills = %d, want no settleable partial fill while details incomplete", len(res.Fills))
	}
}

func TestBinanceExecutor_TradeLookupRetryEventuallySucceeds(t *testing.T) {
	t.Parallel()

	var tradeCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet && r.URL.Path == "/fapi/v1/userTrades" {
			if tradeCalls.Add(1) == 1 {
				_, _ = w.Write([]byte(`[]`))
				return
			}
			_, _ = w.Write([]byte(`[{"id":41,"orderId":4,"price":"2400","qty":"0.5","commission":"0.08"}]`))
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		_, _ = w.Write([]byte(`{"orderId":4,"symbol":"ETHUSDT","side":"BUY","origQty":"0.5","avgPrice":"2400","executedQty":"0.5","status":"FILLED"}`))
	}))
	defer srv.Close()

	exec := &BinanceExecutor{
		baseURL:             srv.URL,
		httpClient:          httpclient.New(&http.Client{}, noopExtAPILogger{}, "binance_test"),
		tradeLookupAttempts: 2,
		tradeLookupDelay:    0,
	}

	res, err := exec.Execute(context.Background(), OrderRequest{
		Symbol:    "ETHUSDT",
		Side:      "BUY",
		Qty:       0.5,
		MarkPrice: 2400,
	}, accountmeta.Meta{
		APIKey:       "key",
		APISecret:    "secret",
		PositionMode: "one_way",
	})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if res.FillPending {
		t.Fatalf("fill_pending = true, want false")
	}
	if got := tradeCalls.Load(); got != 2 {
		t.Fatalf("userTrades calls = %d, want 2", got)
	}
	if len(res.Fills) != 1 || res.Fills[0].ExchangeTradeID != "41" || res.Fills[0].Fee != 0.08 {
		t.Fatalf("fills = %+v, want confirmed retried fill", res.Fills)
	}
}

func TestBinanceExecutor_ResolveUsesTradeLookupRetry(t *testing.T) {
	t.Parallel()

	var tradeCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet && r.URL.Path == "/fapi/v1/userTrades" {
			if tradeCalls.Add(1) == 1 {
				_, _ = w.Write([]byte(`[]`))
				return
			}
			_, _ = w.Write([]byte(`[{"id":51,"orderId":5,"price":"2400","qty":"0.5","commission":"0.08"}]`))
			return
		}
		_, _ = w.Write([]byte(`{"orderId":5,"clientOrderId":"coid-5","symbol":"ETHUSDT","side":"BUY","origQty":"0.5","avgPrice":"2400","executedQty":"0.5","status":"FILLED"}`))
	}))
	defer srv.Close()

	exec := &BinanceExecutor{
		baseURL:             srv.URL,
		httpClient:          httpclient.New(&http.Client{}, noopExtAPILogger{}, "binance_test"),
		tradeLookupAttempts: 2,
		tradeLookupDelay:    0,
	}

	res, err := exec.Resolve(context.Background(), RecoveryRequest{
		AccountID:     13,
		Symbol:        "ETHUSDT",
		ClientOrderID: "coid-5",
	}, accountmeta.Meta{
		APIKey:       "key",
		APISecret:    "secret",
		PositionMode: "one_way",
	})
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if res.FillPending {
		t.Fatalf("fill_pending = true, want false")
	}
	if got := tradeCalls.Load(); got != 2 {
		t.Fatalf("userTrades calls = %d, want 2", got)
	}
	if len(res.Fills) != 1 || res.Fills[0].ExchangeTradeID != "51" {
		t.Fatalf("fills = %+v, want confirmed retried fill", res.Fills)
	}
}

func TestBinanceExecutor_HedgeModeRejected(t *testing.T) {
	t.Parallel()

	hit := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit = true
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	exec := &BinanceExecutor{
		baseURL:    srv.URL,
		httpClient: httpclient.New(&http.Client{}, noopExtAPILogger{}, "binance_test"),
	}

	res, err := exec.Execute(context.Background(), OrderRequest{
		Symbol:    "ETHUSDT",
		Side:      "BUY",
		Qty:       1,
		MarkPrice: 2500,
	}, accountmeta.Meta{
		APIKey:       "key",
		APISecret:    "secret",
		PositionMode: "hedge",
	})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if res.Status != "FAILED" {
		t.Fatalf("status = %q, want FAILED", res.Status)
	}
	if res.ErrorMessage == "" {
		t.Fatal("expected error message for hedge-mode rejection")
	}
	if hit {
		t.Fatal("unexpected outbound HTTP request for hedge-mode order")
	}
}
