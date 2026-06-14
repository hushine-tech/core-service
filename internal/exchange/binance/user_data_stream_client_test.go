package binance

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/exchange/adapter"
)

func TestBinanceUserDataStreamClientReceivesFuturesPartialFill(t *testing.T) {
	upgrader := websocket.Upgrader{}
	mux := http.NewServeMux()
	mux.HandleFunc("/fapi/v1/listenKey", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("listenKey method = %s", r.Method)
		}
		if r.Header.Get("X-MBX-APIKEY") == "" {
			t.Fatal("missing X-MBX-APIKEY")
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"listenKey": "listen-1"})
	})
	mux.HandleFunc("/ws/listen-1", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		payload := `{"e":"ORDER_TRADE_UPDATE","E":1700000000000,"o":{"s":"ETHUSDT","c":"cid-1","S":"BUY","ps":"LONG","o":"LIMIT","f":"GTC","x":"TRADE","X":"PARTIALLY_FILLED","i":1001,"t":9001,"l":"0.2","L":"2000","z":"0.2","n":"0.08","N":"USDT","R":false}}`
		if err := conn.WriteMessage(websocket.TextMessage, []byte(payload)); err != nil {
			t.Fatalf("write ws: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client := binanceUserDataStreamClient{
		route: adapter.Route{Market: domain.MarketPerpetualFutures},
		endpoints: Endpoints{
			RESTBaseURL: server.URL,
			WSBaseURL:   "ws" + strings.TrimPrefix(server.URL, "http"),
		},
		httpClient: server.Client(),
		dialer:     websocket.DefaultDialer,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var got adapter.UserDataOrderEvent
	err := client.Listen(ctx, adapter.UserDataStreamRequest{
		Credential: adapter.ParsedCredential{Metadata: map[string]string{"api_key": "key", "api_secret": "secret"}},
	}, func(_ context.Context, event adapter.UserDataOrderEvent) error {
		got = event
		cancel()
		return nil
	})
	if err != nil && ctx.Err() == nil {
		t.Fatalf("Listen returned error: %v", err)
	}
	if got.OrderStatus != "PARTIALLY_FILLED" || got.ExchangeTradeID != "9001" || got.LastFilledQty != 0.2 {
		t.Fatalf("unexpected event: %+v", got)
	}
}
