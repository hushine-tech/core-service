package mockserver

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestMockServerBroadcastsFuturesPartialFill(t *testing.T) {
	mock := New()
	server := mock.StartHTTP(t)
	defer server.Close()

	req, err := http.NewRequest(http.MethodPost, server.URL+"/fapi/v1/listenKey", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-MBX-APIKEY", "key")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws/" + body["listenKey"]
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	mock.EmitFuturesOrderEvent(BinanceOrderEvent{
		Symbol: "ETHUSDT", ClientOrderID: "cid-1", ExchangeOrderID: "1001", ExchangeTradeID: "9001",
		Side: "BUY", PositionSide: "LONG", OrderType: "LIMIT", TimeInForce: "GTC",
		ExecutionType: "TRADE", OrderStatus: "PARTIALLY_FILLED",
		LastFilledQty: 0.2, LastFilledPrice: 2000, AccumulatedFilledQty: 0.2,
		Fee: 0.08, FeeAsset: "USDT", EventTime: time.UnixMilli(1700000000000).UTC(),
	})

	_, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(payload), `"e":"ORDER_TRADE_UPDATE"`) || !strings.Contains(string(payload), `"X":"PARTIALLY_FILLED"`) {
		t.Fatalf("unexpected ws payload: %s", payload)
	}
}
