package binance

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/internal/exchange/adapter"
)

func TestBinanceSpotOrderStateReaderQueriesOrderAndTrades(t *testing.T) {
	var orderPathHit bool
	var tradesPathHit bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v3/order":
			orderPathHit = true
			if r.URL.Query().Get("origClientOrderId") != "spot-client-1" {
				t.Fatalf("origClientOrderId = %q, want spot-client-1", r.URL.Query().Get("origClientOrderId"))
			}
			_, _ = w.Write([]byte(`{
				"symbol":"ETHUSDT",
				"orderId":2001,
				"clientOrderId":"spot-client-1",
				"price":"2500.00",
				"origQty":"0.2",
				"executedQty":"0.2",
				"cummulativeQuoteQty":"500.00",
				"status":"FILLED",
				"type":"LIMIT",
				"side":"BUY",
				"updateTime":1710000000000
			}`))
		case "/api/v3/myTrades":
			tradesPathHit = true
			if r.URL.Query().Get("orderId") != "2001" {
				t.Fatalf("orderId = %q, want 2001", r.URL.Query().Get("orderId"))
			}
			_, _ = w.Write([]byte(`[{
				"id":3001,
				"orderId":2001,
				"price":"2500.00",
				"qty":"0.2",
				"commission":"0.25",
				"commissionAsset":"USDT",
				"time":1710000001000
			}]`))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	reader := spotOrderStateReader{baseURL: srv.URL, httpClient: srv.Client()}
	state, err := reader.QueryOrder(context.Background(), adapter.QueryOrderRequest{
		Symbol:        "ETHUSDT",
		ClientOrderID: "spot-client-1",
		Credential:    testParsedCredential(),
	})
	if err != nil {
		t.Fatalf("QueryOrder() error = %v", err)
	}
	if !orderPathHit {
		t.Fatal("expected /api/v3/order to be queried")
	}
	if state.ExchangeOrderID != "2001" || state.ClientOrderID != "spot-client-1" || state.Status != "FILLED" {
		t.Fatalf("state = %+v, want parsed spot order state", state)
	}
	if state.ExecutedQty != 0.2 || state.RemainingQty != 0 || state.AvgPrice != 2500 {
		t.Fatalf("state quantities = %+v, want executed spot state", state)
	}
	if want := time.UnixMilli(1710000000000).UTC(); !state.UpdatedAt.Equal(want) {
		t.Fatalf("UpdatedAt = %s, want %s", state.UpdatedAt, want)
	}

	trades, err := reader.QueryTrades(context.Background(), adapter.QueryTradesRequest{
		Symbol:          state.Symbol,
		ExchangeOrderID: state.ExchangeOrderID,
		Credential:      testParsedCredential(),
	})
	if err != nil {
		t.Fatalf("QueryTrades() error = %v", err)
	}
	if !tradesPathHit {
		t.Fatal("expected /api/v3/myTrades to be queried")
	}
	if len(trades) != 1 || trades[0].ExchangeTradeID != "3001" || trades[0].FeeAsset != "USDT" {
		t.Fatalf("trades = %+v, want parsed spot trade", trades)
	}
	if trades[0].Qty != 0.2 || trades[0].FillPrice != 2500 || trades[0].Fee != 0.25 {
		t.Fatalf("trade values = %+v, want parsed spot trade values", trades[0])
	}
}
