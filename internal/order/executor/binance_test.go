package executor

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/hushine-tech/golang-lib/middleware/httpclient"
	elog "github.com/hushine-tech/golang-lib/pkg/log"
	"github.com/hushine-tech/core-service/internal/order/accountmeta"
)

type noopExtAPILogger struct{}

func (noopExtAPILogger) ExtAPI(context.Context, elog.ExtAPILogEntry) {}

func TestBinanceExecutor_OneWayLongMapsToBuy(t *testing.T) {
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
		Side:      "LONG",
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

func TestBinanceExecutor_OneWayShortMapsToSell(t *testing.T) {
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
		Side:      "SHORT",
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
		Side:      "LONG",
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
