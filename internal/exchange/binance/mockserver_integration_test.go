package binance

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/exchange/adapter"
	"github.com/hushine-tech/core-service/internal/exchange/binance/mockserver"
	"github.com/hushine-tech/core-service/internal/logger"
)

func TestBinanceFactoryMockCapturesOrderConditionCombinations(t *testing.T) {
	futureGoodTill := time.Now().UTC().Add(time.Hour)
	limitPrice := 2000.0
	cases := []struct {
		name            string
		market          domain.Market
		side            string
		orderType       string
		timeInForce     string
		postOnly        bool
		reduceOnly      bool
		price           *float64
		goodTillDate    *time.Time
		path            string
		wantType        string
		wantTimeInForce string
		wantReduceOnly  string
		wantGTD         bool
	}{
		{
			name:        "spot post-only maps to LIMIT_MAKER",
			market:      domain.MarketSpot,
			side:        "SELL",
			orderType:   "LIMIT",
			timeInForce: "GTC",
			postOnly:    true,
			price:       &limitPrice,
			path:        "/api/v3/order",
			wantType:    "LIMIT_MAKER",
		},
		{
			name:            "futures post-only maps to GTX",
			market:          domain.MarketPerpetualFutures,
			side:            "BUY",
			orderType:       "LIMIT",
			timeInForce:     "GTC",
			postOnly:        true,
			price:           &limitPrice,
			path:            "/fapi/v1/order",
			wantType:        "LIMIT",
			wantTimeInForce: "GTX",
		},
		{
			name:           "futures reduce-only passes reduceOnly",
			market:         domain.MarketPerpetualFutures,
			side:           "SELL",
			orderType:      "MARKET",
			reduceOnly:     true,
			path:           "/fapi/v1/order",
			wantType:       "MARKET",
			wantReduceOnly: "true",
		},
		{
			name:       "spot reduce-only sell is platform-only",
			market:     domain.MarketSpot,
			side:       "SELL",
			orderType:  "MARKET",
			reduceOnly: true,
			path:       "/api/v3/order",
			wantType:   "MARKET",
		},
		{
			name:            "futures GTD passes goodTillDate",
			market:          domain.MarketPerpetualFutures,
			side:            "BUY",
			orderType:       "LIMIT",
			timeInForce:     "GTD",
			price:           &limitPrice,
			goodTillDate:    &futureGoodTill,
			path:            "/fapi/v1/order",
			wantType:        "LIMIT",
			wantTimeInForce: "GTD",
			wantGTD:         true,
		},
		{
			name:            "futures IOC passes IOC",
			market:          domain.MarketPerpetualFutures,
			side:            "BUY",
			orderType:       "LIMIT",
			timeInForce:     "IOC",
			price:           &limitPrice,
			path:            "/fapi/v1/order",
			wantType:        "LIMIT",
			wantTimeInForce: "IOC",
		},
		{
			name:            "futures FOK passes FOK",
			market:          domain.MarketPerpetualFutures,
			side:            "BUY",
			orderType:       "LIMIT",
			timeInForce:     "FOK",
			price:           &limitPrice,
			path:            "/fapi/v1/order",
			wantType:        "LIMIT",
			wantTimeInForce: "FOK",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := mockserver.New()
			server := mock.StartHTTP(t)
			defer server.Close()
			mock.EnqueueScenario(mockserver.BinanceScenario{Status: "NEW"})
			route := adapter.Route{
				Exchange:    domain.ExchangeBinance,
				Environment: domain.EnvironmentDemo,
				Market:      tc.market,
			}
			factory := NewFactoryWithEndpoints(route, logger.Instance(), Endpoints{
				RESTBaseURL: server.URL,
				WSBaseURL:   "ws" + strings.TrimPrefix(server.URL, "http"),
			})
			exec, err := factory.OrderExecutor()
			if err != nil {
				t.Fatal(err)
			}
			result, err := exec.PlaceOrder(context.Background(), adapter.OrderRequest{
				AccountID:    1,
				VenueID:      2,
				Exchange:     domain.ExchangeBinance,
				Environment:  domain.EnvironmentDemo,
				Market:       tc.market,
				Symbol:       "ETHUSDT",
				Side:         tc.side,
				OrderType:    tc.orderType,
				TimeInForce:  tc.timeInForce,
				PostOnly:     tc.postOnly,
				ReduceOnly:   tc.reduceOnly,
				GoodTillDate: tc.goodTillDate,
				Qty:          1,
				Price:        tc.price,
				Credential: adapter.ParsedCredential{Metadata: map[string]string{
					"api_key":    "key",
					"api_secret": "secret",
				}},
			})
			if err != nil {
				t.Fatal(err)
			}
			if result.Status != "NEW" {
				t.Fatalf("result status = %s, want NEW: %+v", result.Status, result)
			}
			record, ok := mock.LastRequest(tc.path)
			if !ok {
				t.Fatalf("missing mock request for %s", tc.path)
			}
			if got := record.Params.Get("type"); got != tc.wantType {
				t.Fatalf("type = %s, want %s; params=%v", got, tc.wantType, record.Params)
			}
			if got := record.Params.Get("timeInForce"); got != tc.wantTimeInForce {
				t.Fatalf("timeInForce = %s, want %s; params=%v", got, tc.wantTimeInForce, record.Params)
			}
			if got := record.Params.Get("reduceOnly"); got != tc.wantReduceOnly {
				t.Fatalf("reduceOnly = %s, want %s; params=%v", got, tc.wantReduceOnly, record.Params)
			}
			if tc.wantGTD && record.Params.Get("goodTillDate") == "" {
				t.Fatalf("goodTillDate missing; params=%v", record.Params)
			}
		})
	}
}

func TestBinanceFactoryPlacesOrderAndReceivesMockWSPartialFill(t *testing.T) {
	mock := mockserver.New()
	server := mock.StartHTTP(t)
	defer server.Close()

	route := adapter.Route{
		Exchange:    domain.ExchangeBinance,
		Environment: domain.EnvironmentDemo,
		Market:      domain.MarketPerpetualFutures,
	}
	factory := NewFactoryWithEndpoints(route, logger.Instance(), Endpoints{
		RESTBaseURL: server.URL,
		WSBaseURL:   "ws" + strings.TrimPrefix(server.URL, "http"),
	})
	mock.EnqueueScenario(mockserver.BinanceScenario{
		Market:       domain.MarketPerpetualFutures,
		Symbol:       "ETHUSDT",
		Side:         "BUY",
		PositionSide: "LONG",
		OrderType:    "LIMIT",
		TimeInForce:  "GTC",
		OrigQty:      1,
		Price:        2000,
		Status:       "NEW",
	})

	exec, err := factory.OrderExecutor()
	if err != nil {
		t.Fatal(err)
	}
	price := 2000.0
	result, err := exec.PlaceOrder(context.Background(), adapter.OrderRequest{
		AccountID:     1,
		VenueID:       2,
		Exchange:      domain.ExchangeBinance,
		Environment:   domain.EnvironmentDemo,
		Market:        domain.MarketPerpetualFutures,
		Symbol:        "ETHUSDT",
		Side:          "BUY",
		PositionSide:  "LONG",
		OrderType:     "LIMIT",
		TimeInForce:   "GTC",
		Qty:           1,
		Price:         &price,
		ClientOrderID: "cid-1",
		Credential: adapter.ParsedCredential{Metadata: map[string]string{
			"api_key":    "key",
			"api_secret": "secret",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "NEW" || result.ExchangeOrderID != "1001" {
		t.Fatalf("unexpected order result: %+v", result)
	}

	stream, err := factory.UserDataStream()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	events := make(chan adapter.UserDataOrderEvent, 1)
	errs := make(chan error, 1)
	go func() {
		errs <- stream.Listen(ctx, adapter.UserDataStreamRequest{
			AccountID: 1,
			VenueID:   2,
			Credential: adapter.ParsedCredential{Metadata: map[string]string{
				"api_key":    "key",
				"api_secret": "secret",
			}},
		}, func(_ context.Context, event adapter.UserDataOrderEvent) error {
			events <- event
			cancel()
			return nil
		})
	}()
	waitForSubscribers(t, mock, 1)
	mock.EmitFuturesOrderEvent(mockserver.BinanceOrderEvent{
		Symbol:               "ETHUSDT",
		ClientOrderID:        "cid-1",
		ExchangeOrderID:      "1001",
		ExchangeTradeID:      "9001",
		Side:                 "BUY",
		PositionSide:         "LONG",
		OrderType:            "LIMIT",
		TimeInForce:          "GTC",
		ExecutionType:        "TRADE",
		OrderStatus:          "PARTIALLY_FILLED",
		LastFilledQty:        0.2,
		LastFilledPrice:      2000,
		AccumulatedFilledQty: 0.2,
		Fee:                  0.08,
		FeeAsset:             "USDT",
		EventTime:            time.UnixMilli(1700000000000).UTC(),
	})

	select {
	case got := <-events:
		if got.OrderStatus != "PARTIALLY_FILLED" || got.ExchangeTradeID != "9001" {
			t.Fatalf("unexpected stream event: %+v", got)
		}
	case err := <-errs:
		t.Fatalf("stream exited before event: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for partial fill event")
	}
}

func waitForSubscribers(t *testing.T, mock *mockserver.Server, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if mock.SubscriberCount() >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("subscriber count = %d, want at least %d", mock.SubscriberCount(), want)
}
