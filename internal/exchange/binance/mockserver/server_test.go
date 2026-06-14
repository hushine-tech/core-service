package mockserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hushine-tech/core-service/internal/domain"
)

func TestMockServerFuturesOrderPartialThenTradesComplete(t *testing.T) {
	mock := New()
	server := mock.StartHTTP(t)
	defer server.Close()

	mock.EnqueueScenario(BinanceScenario{
		Market:       domain.MarketPerpetualFutures,
		Symbol:       "ETHUSDT",
		Side:         "BUY",
		PositionSide: "LONG",
		OrderType:    "LIMIT",
		TimeInForce:  "GTC",
		OrigQty:      1,
		Price:        2000,
		Events: []BinanceOrderEventStep{
			{Kind: EventAcceptNew},
			{Kind: EventRESTTradesComplete, Fills: []BinanceFill{{TradeID: "9001", Qty: 0.4, Price: 2000, Fee: 0.32, FeeAsset: "USDT"}}},
		},
	})

	form := url.Values{}
	form.Set("symbol", "ETHUSDT")
	form.Set("side", "BUY")
	form.Set("positionSide", "LONG")
	form.Set("type", "LIMIT")
	form.Set("timeInForce", "GTC")
	form.Set("quantity", "1")
	form.Set("price", "2000")
	form.Set("newClientOrderId", "cid-1")
	form.Set("timestamp", "1700000000000")
	form.Set("signature", "sig")
	req, err := http.NewRequest(http.MethodPost, server.URL+"/fapi/v1/order?"+form.Encode(), nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-MBX-APIKEY", "key")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var order map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&order); err != nil {
		t.Fatal(err)
	}
	if order["status"] != "PARTIALLY_FILLED" || order["clientOrderId"] != "cid-1" {
		t.Fatalf("unexpected order response: %+v", order)
	}

	tradeReq, err := http.NewRequest(http.MethodGet, server.URL+"/fapi/v1/userTrades?symbol=ETHUSDT&orderId=1001&timestamp=1700000000001&signature=sig", nil)
	if err != nil {
		t.Fatal(err)
	}
	tradeReq.Header.Set("X-MBX-APIKEY", "key")
	tradeResp, err := server.Client().Do(tradeReq)
	if err != nil {
		t.Fatal(err)
	}
	defer tradeResp.Body.Close()
	var trades []map[string]any
	if err := json.NewDecoder(tradeResp.Body).Decode(&trades); err != nil {
		t.Fatal(err)
	}
	if len(trades) != 1 || trades[0]["id"] != float64(9001) {
		t.Fatalf("unexpected trades: %+v", trades)
	}
}

func TestMockServerResetClearsOrders(t *testing.T) {
	mock := New()
	mock.EnqueueScenario(BinanceScenario{Market: domain.MarketPerpetualFutures, Symbol: "ETHUSDT", Side: "BUY", OrderType: "MARKET", OrigQty: 1})
	params := url.Values{"symbol": []string{"ETHUSDT"}, "quantity": []string{"1"}}
	mock.allocateOrder(domain.MarketPerpetualFutures, params, mock.nextScenario())
	if err := mock.Reset(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := mock.OrderCount(); got != 0 {
		t.Fatalf("OrderCount = %d, want 0", got)
	}
}

func TestMockServerFuturesExchangeInfoSupportsRiskGateSymbolRules(t *testing.T) {
	mock := New()
	server := mock.StartHTTP(t)
	defer server.Close()

	resp, err := server.Client().Get(server.URL + "/fapi/v1/exchangeInfo")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var payload struct {
		Symbols []struct {
			Symbol  string `json:"symbol"`
			Filters []struct {
				FilterType string `json:"filterType"`
				TickSize   string `json:"tickSize"`
				StepSize   string `json:"stepSize"`
				MinQty     string `json:"minQty"`
				Notional   string `json:"notional"`
			} `json:"filters"`
		} `json:"symbols"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	bySymbol := make(map[string]struct {
		Symbol  string `json:"symbol"`
		Filters []struct {
			FilterType string `json:"filterType"`
			TickSize   string `json:"tickSize"`
			StepSize   string `json:"stepSize"`
			MinQty     string `json:"minQty"`
			Notional   string `json:"notional"`
		} `json:"filters"`
	}, len(payload.Symbols))
	for _, symbol := range payload.Symbols {
		bySymbol[symbol.Symbol] = symbol
	}
	if _, ok := bySymbol["ETHUSDT"]; !ok {
		t.Fatalf("unexpected exchangeInfo payload: %+v", payload)
	}
	zec, ok := bySymbol["ZECUSDT"]
	if !ok {
		t.Fatalf("exchangeInfo missing ZECUSDT: %+v", payload)
	}
	hasMinNotional := false
	for _, filter := range zec.Filters {
		if filter.FilterType == "MIN_NOTIONAL" && filter.Notional != "" {
			hasMinNotional = true
		}
	}
	if !hasMinNotional {
		t.Fatalf("exchangeInfo missing MIN_NOTIONAL filter: %+v", zec.Filters)
	}
}

func TestMockServerSceneAPISetGetAndReset(t *testing.T) {
	mock := New()
	server := mock.StartHTTP(t)
	defer server.Close()

	resp, err := server.Client().Post(server.URL+"/mock/scene?scene=2", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /mock/scene status = %d, want 202", resp.StatusCode)
	}

	sceneResp, err := server.Client().Get(server.URL + "/mock/scene")
	if err != nil {
		t.Fatal(err)
	}
	defer sceneResp.Body.Close()
	var scene map[string]any
	if err := json.NewDecoder(sceneResp.Body).Decode(&scene); err != nil {
		t.Fatal(err)
	}
	if scene["scene"] != float64(2) {
		t.Fatalf("scene = %+v, want 2", scene)
	}

	resetResp, err := server.Client().Post(server.URL+"/mock/reset", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = resetResp.Body.Close()

	sceneResp, err = server.Client().Get(server.URL + "/mock/scene")
	if err != nil {
		t.Fatal(err)
	}
	defer sceneResp.Body.Close()
	scene = map[string]any{}
	if err := json.NewDecoder(sceneResp.Body).Decode(&scene); err != nil {
		t.Fatal(err)
	}
	if scene["scene"] != float64(1) {
		t.Fatalf("scene after reset = %+v, want 1", scene)
	}
}

func TestMockServerSceneMatrixFuturesOrders(t *testing.T) {
	tests := []struct {
		name       string
		scene      int
		orderType  string
		tif        string
		wantStatus string
		wantExec   string
		wantHTTP   int
	}{
		{name: "scene1_market_filled", scene: 1, orderType: "MARKET", wantStatus: "FILLED", wantExec: "1", wantHTTP: http.StatusOK},
		{name: "scene1_fok_filled", scene: 1, orderType: "LIMIT", tif: "FOK", wantStatus: "FILLED", wantExec: "1", wantHTTP: http.StatusOK},
		{name: "scene1_ioc_filled", scene: 1, orderType: "LIMIT", tif: "IOC", wantStatus: "FILLED", wantExec: "1", wantHTTP: http.StatusOK},
		{name: "scene1_gtc_filled", scene: 1, orderType: "LIMIT", tif: "GTC", wantStatus: "FILLED", wantExec: "1", wantHTTP: http.StatusOK},
		{name: "scene1_gtx_posts", scene: 1, orderType: "LIMIT", tif: "GTX", wantStatus: "NEW", wantExec: "0", wantHTTP: http.StatusOK},
		{name: "scene2_gtc_partial_open", scene: 2, orderType: "LIMIT", tif: "GTC", wantStatus: "PARTIALLY_FILLED", wantExec: "0.2", wantHTTP: http.StatusOK},
		{name: "scene2_gtd_partial_open", scene: 2, orderType: "LIMIT", tif: "GTD", wantStatus: "PARTIALLY_FILLED", wantExec: "0.2", wantHTTP: http.StatusOK},
		{name: "scene2_fok_expires_without_fill", scene: 2, orderType: "LIMIT", tif: "FOK", wantStatus: "EXPIRED", wantExec: "0", wantHTTP: http.StatusOK},
		{name: "scene2_ioc_partial_expires", scene: 2, orderType: "LIMIT", tif: "IOC", wantStatus: "EXPIRED", wantExec: "0.2", wantHTTP: http.StatusOK},
		{name: "scene2_market_partial_expires", scene: 2, orderType: "MARKET", wantStatus: "EXPIRED", wantExec: "0.2", wantHTTP: http.StatusOK},
		{name: "scene3_gtc_partial_then_scheduled", scene: 3, orderType: "LIMIT", tif: "GTC", wantStatus: "PARTIALLY_FILLED", wantExec: "0.2", wantHTTP: http.StatusOK},
		{name: "scene4_gtc_resting", scene: 4, orderType: "LIMIT", tif: "GTC", wantStatus: "NEW", wantExec: "0", wantHTTP: http.StatusOK},
		{name: "scene4_market_expires", scene: 4, orderType: "MARKET", wantStatus: "EXPIRED", wantExec: "0", wantHTTP: http.StatusOK},
		{name: "scene5_post_only_expires", scene: 5, orderType: "LIMIT", tif: "GTX", wantStatus: "EXPIRED", wantExec: "0", wantHTTP: http.StatusOK},
		{name: "scene5_non_post_only_fills", scene: 5, orderType: "LIMIT", tif: "GTC", wantStatus: "FILLED", wantExec: "1", wantHTTP: http.StatusOK},
		{name: "scene6_gtc_no_liquidity_rests", scene: 6, orderType: "LIMIT", tif: "GTC", wantStatus: "NEW", wantExec: "0", wantHTTP: http.StatusOK},
		{name: "scene6_ioc_no_liquidity_expires", scene: 6, orderType: "LIMIT", tif: "IOC", wantStatus: "EXPIRED", wantExec: "0", wantHTTP: http.StatusOK},
		{name: "scene7_rejected", scene: 7, orderType: "LIMIT", tif: "GTC", wantHTTP: http.StatusBadRequest},
		{name: "scene8_rate_limited", scene: 8, orderType: "LIMIT", tif: "GTC", wantHTTP: http.StatusTooManyRequests},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := NewWithConfig(Config{Scene3Delay: time.Hour})
			server := mock.StartHTTP(t)
			defer server.Close()
			setMockScene(t, server.URL, tt.scene)

			resp, order := placeMockFuturesOrder(t, server.URL, server.Client(), tt.orderType, tt.tif)
			defer resp.Body.Close()
			if resp.StatusCode != tt.wantHTTP {
				t.Fatalf("HTTP status = %d, want %d; order=%+v", resp.StatusCode, tt.wantHTTP, order)
			}
			if tt.wantHTTP != http.StatusOK {
				return
			}
			if order["status"] != tt.wantStatus {
				t.Fatalf("status = %v, want %s; order=%+v", order["status"], tt.wantStatus, order)
			}
			if order["executedQty"] != tt.wantExec {
				t.Fatalf("executedQty = %v, want %s; order=%+v", order["executedQty"], tt.wantExec, order)
			}
		})
	}
}

func TestMockServerScene3EmitsDelayedWebsocketFinalFill(t *testing.T) {
	mock := NewWithConfig(Config{Scene3Delay: 10 * time.Millisecond})
	server := mock.StartHTTP(t)
	defer server.Close()
	setMockScene(t, server.URL, 3)

	listenReq, err := http.NewRequest(http.MethodPost, server.URL+"/fapi/v1/listenKey", nil)
	if err != nil {
		t.Fatal(err)
	}
	listenReq.Header.Set("X-MBX-APIKEY", "key")
	listenResp, err := server.Client().Do(listenReq)
	if err != nil {
		t.Fatal(err)
	}
	defer listenResp.Body.Close()
	var listenPayload map[string]string
	if err := json.NewDecoder(listenResp.Body).Decode(&listenPayload); err != nil {
		t.Fatal(err)
	}
	wsURL := "ws" + server.URL[len("http"):] + "/ws/" + listenPayload["listenKey"]
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	resp, order := placeMockFuturesOrder(t, server.URL, server.Client(), "LIMIT", "GTC")
	defer resp.Body.Close()
	if order["status"] != "PARTIALLY_FILLED" {
		t.Fatalf("initial order = %+v, want PARTIALLY_FILLED", order)
	}

	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	var event map[string]any
	if err := json.Unmarshal(payload, &event); err != nil {
		t.Fatal(err)
	}
	orderEvent := event["o"].(map[string]any)
	if orderEvent["X"] != "FILLED" || orderEvent["l"] != "0.8" || orderEvent["z"] != "1" {
		t.Fatalf("unexpected ws final fill: %s", string(payload))
	}
}

func TestMockServerScene9DelaysLongEnoughForClientTimeout(t *testing.T) {
	mock := NewWithConfig(Config{Scene9Delay: 50 * time.Millisecond})
	server := mock.StartHTTP(t)
	defer server.Close()
	setMockScene(t, server.URL, 9)

	client := &http.Client{Timeout: 5 * time.Millisecond}
	resp, _ := placeMockFuturesOrder(t, server.URL, client, "LIMIT", "GTC")
	if resp != nil {
		_ = resp.Body.Close()
		t.Fatalf("scene 9 response status = %d, want client timeout", resp.StatusCode)
	}
}

func setMockScene(t *testing.T, baseURL string, scene int) {
	t.Helper()
	resp, err := http.Post(baseURL+"/mock/scene?scene="+formatIntForTest(scene), "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("set scene status = %d, want 202", resp.StatusCode)
	}
}

func placeMockFuturesOrder(t *testing.T, baseURL string, client *http.Client, orderType, tif string) (*http.Response, map[string]any) {
	t.Helper()
	form := url.Values{}
	form.Set("symbol", "ETHUSDT")
	form.Set("side", "BUY")
	form.Set("positionSide", "LONG")
	form.Set("type", orderType)
	if tif != "" {
		form.Set("timeInForce", tif)
	}
	form.Set("quantity", "1")
	form.Set("price", "2000")
	form.Set("newClientOrderId", "cid-"+formatIntForTest(time.Now().Nanosecond()))
	form.Set("timestamp", "1700000000000")
	form.Set("signature", "sig")
	req, err := http.NewRequest(http.MethodPost, baseURL+"/fapi/v1/order?"+form.Encode(), nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-MBX-APIKEY", "key")
	resp, err := client.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, nil
		}
		var netErr interface{ Timeout() bool }
		if errors.As(err, &netErr) && netErr.Timeout() {
			return nil, nil
		}
		t.Fatal(err)
	}
	var order map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&order); err != nil {
		t.Fatal(err)
	}
	return resp, order
}

func formatIntForTest(value int) string {
	return strconv.Itoa(value)
}
