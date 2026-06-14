package binance

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
)

func TestParseSpotUserDataOrderEventTrade(t *testing.T) {
	event, err := ParseSpotUserDataOrderEvent([]byte(`{
		"e": "executionReport",
		"E": 1710000000123,
		"s": "ETHUSDT",
		"c": "spot-client-1",
		"S": "BUY",
		"o": "LIMIT",
		"f": "GTC",
		"x": "TRADE",
		"X": "PARTIALLY_FILLED",
		"i": 987654321,
		"t": 12345,
		"l": "0.1",
		"L": "3000",
		"z": "0.3",
		"n": "0.03",
		"N": "USDT"
	}`))
	if err != nil {
		t.Fatalf("ParseSpotUserDataOrderEvent: %v", err)
	}

	if event.EventSource != "websocket" ||
		event.Symbol != "ETHUSDT" ||
		event.ClientOrderID != "spot-client-1" ||
		event.ExchangeOrderID != "987654321" ||
		event.ExchangeTradeID != "12345" ||
		event.Side != "BUY" ||
		event.PositionSide != "" ||
		event.OrderType != "LIMIT" ||
		event.TimeInForce != "GTC" ||
		event.ExecutionType != "TRADE" ||
		event.OrderStatus != "PARTIALLY_FILLED" ||
		event.LastFilledQty != 0.1 ||
		event.LastFilledPrice != 3000 ||
		event.AccumulatedFilledQty != 0.3 ||
		event.Fee != 0.03 ||
		event.FeeAsset != "USDT" ||
		event.ReduceOnly {
		t.Fatalf("event = %+v", event)
	}
	wantTime := time.UnixMilli(1710000000123).UTC()
	if !event.EventTime.Equal(wantTime) {
		t.Fatalf("event time = %s, want %s", event.EventTime, wantTime)
	}
}

func TestParseFuturesUserDataOrderEventTrade(t *testing.T) {
	event, err := ParseFuturesUserDataOrderEvent([]byte(`{
		"e": "ORDER_TRADE_UPDATE",
		"E": 1710000000456,
		"o": {
			"s": "ETHUSDT",
			"c": "futures-client-1",
			"S": "SELL",
			"ps": "BOTH",
			"o": "MARKET",
			"f": "GTC",
			"x": "TRADE",
			"X": "FILLED",
			"i": 123456789,
			"t": 67890,
			"l": "0.2",
			"L": "3100",
			"z": "0.2",
			"n": "0.04",
			"N": "USDT",
			"R": true
		}
	}`))
	if err != nil {
		t.Fatalf("ParseFuturesUserDataOrderEvent: %v", err)
	}

	if event.EventSource != "websocket" ||
		event.Symbol != "ETHUSDT" ||
		event.ClientOrderID != "futures-client-1" ||
		event.ExchangeOrderID != "123456789" ||
		event.ExchangeTradeID != "67890" ||
		event.Side != "SELL" ||
		event.PositionSide != "BOTH" ||
		event.OrderType != "MARKET" ||
		event.TimeInForce != "GTC" ||
		event.ExecutionType != "TRADE" ||
		event.OrderStatus != "FILLED" ||
		event.LastFilledQty != 0.2 ||
		event.LastFilledPrice != 3100 ||
		event.AccumulatedFilledQty != 0.2 ||
		event.Fee != 0.04 ||
		event.FeeAsset != "USDT" ||
		!event.ReduceOnly {
		t.Fatalf("event = %+v", event)
	}
	wantTime := time.UnixMilli(1710000000456).UTC()
	if !event.EventTime.Equal(wantTime) {
		t.Fatalf("event time = %s, want %s", event.EventTime, wantTime)
	}
}

func TestUserDataStreamParserDispatchesByMarket(t *testing.T) {
	spot, err := NewUserDataStreamParser(domain.MarketSpot).ParseOrderEvent([]byte(`{
		"e": "executionReport",
		"E": 1710000000123,
		"s": "ETHUSDT",
		"x": "TRADE",
		"X": "FILLED",
		"i": 1,
		"t": 2,
		"l": "0.1",
		"L": "3000",
		"z": "0.1",
		"n": "0.03",
		"N": "USDT"
	}`))
	if err != nil {
		t.Fatalf("ParseOrderEvent(spot): %v", err)
	}
	if spot.ExchangeOrderID != "1" || spot.ExchangeTradeID != "2" {
		t.Fatalf("spot event = %+v", spot)
	}

	futures, err := NewUserDataStreamParser(domain.MarketPerpetualFutures).ParseOrderEvent([]byte(`{
		"e": "ORDER_TRADE_UPDATE",
		"E": 1710000000456,
		"o": {
			"s": "ETHUSDT",
			"x": "TRADE",
			"X": "FILLED",
			"i": 3,
			"t": 4,
			"l": "0.2",
			"L": "3100",
			"z": "0.2",
			"n": "0.04",
			"N": "USDT"
		}
	}`))
	if err != nil {
		t.Fatalf("ParseOrderEvent(futures): %v", err)
	}
	if futures.ExchangeOrderID != "3" || futures.ExchangeTradeID != "4" {
		t.Fatalf("futures event = %+v", futures)
	}
}

func TestMockBinanceUserDataStreamPartialFillOrderConditionCombinations(t *testing.T) {
	cases := []struct {
		name             string
		market           domain.Market
		payload          string
		wantSymbol       string
		wantClientID     string
		wantOrderID      string
		wantTradeID      string
		wantSide         string
		wantPositionSide string
		wantOrderType    string
		wantTIF          string
		wantLastQty      float64
		wantAccumQty     float64
		wantFee          float64
		wantReduceOnly   bool
	}{
		{
			name:          "spot limit gtc partial fill",
			market:        domain.MarketSpot,
			payload:       mockSpotUserDataPayload("spot-gtc-1", "BUY", "LIMIT", "GTC", "PARTIALLY_FILLED", 10001, 20001, "0.12", "0.4", "0.036"),
			wantSymbol:    "ETHUSDT",
			wantClientID:  "spot-gtc-1",
			wantOrderID:   "10001",
			wantTradeID:   "20001",
			wantSide:      "BUY",
			wantOrderType: "LIMIT",
			wantTIF:       "GTC",
			wantLastQty:   0.12,
			wantAccumQty:  0.4,
			wantFee:       0.036,
		},
		{
			name:          "spot post-only limit maker partial fill",
			market:        domain.MarketSpot,
			payload:       mockSpotUserDataPayload("spot-maker-1", "SELL", "LIMIT_MAKER", "GTC", "PARTIALLY_FILLED", 10002, 20002, "0.05", "0.15", "0.015"),
			wantSymbol:    "ETHUSDT",
			wantClientID:  "spot-maker-1",
			wantOrderID:   "10002",
			wantTradeID:   "20002",
			wantSide:      "SELL",
			wantOrderType: "LIMIT_MAKER",
			wantTIF:       "GTC",
			wantLastQty:   0.05,
			wantAccumQty:  0.15,
			wantFee:       0.015,
		},
		{
			name:             "futures ioc limit partial fill",
			market:           domain.MarketPerpetualFutures,
			payload:          mockFuturesUserDataPayload("futures-ioc-1", "BUY", "BOTH", "LIMIT", "IOC", "PARTIALLY_FILLED", false, 11001, 21001, "0.2", "0.2", "0.12"),
			wantSymbol:       "ETHUSDT",
			wantClientID:     "futures-ioc-1",
			wantOrderID:      "11001",
			wantTradeID:      "21001",
			wantSide:         "BUY",
			wantPositionSide: "BOTH",
			wantOrderType:    "LIMIT",
			wantTIF:          "IOC",
			wantLastQty:      0.2,
			wantAccumQty:     0.2,
			wantFee:          0.12,
		},
		{
			name:             "futures post-only gtx partial fill",
			market:           domain.MarketPerpetualFutures,
			payload:          mockFuturesUserDataPayload("futures-gtx-1", "SELL", "BOTH", "LIMIT", "GTX", "PARTIALLY_FILLED", false, 11002, 21002, "0.15", "0.45", "0.09"),
			wantSymbol:       "ETHUSDT",
			wantClientID:     "futures-gtx-1",
			wantOrderID:      "11002",
			wantTradeID:      "21002",
			wantSide:         "SELL",
			wantPositionSide: "BOTH",
			wantOrderType:    "LIMIT",
			wantTIF:          "GTX",
			wantLastQty:      0.15,
			wantAccumQty:     0.45,
			wantFee:          0.09,
		},
		{
			name:             "futures gtd reduce-only partial fill",
			market:           domain.MarketPerpetualFutures,
			payload:          mockFuturesUserDataPayload("futures-gtd-1", "BUY", "SHORT", "LIMIT", "GTD", "PARTIALLY_FILLED", true, 11003, 21003, "0.3", "0.3", "0.18"),
			wantSymbol:       "ETHUSDT",
			wantClientID:     "futures-gtd-1",
			wantOrderID:      "11003",
			wantTradeID:      "21003",
			wantSide:         "BUY",
			wantPositionSide: "SHORT",
			wantOrderType:    "LIMIT",
			wantTIF:          "GTD",
			wantLastQty:      0.3,
			wantAccumQty:     0.3,
			wantFee:          0.18,
			wantReduceOnly:   true,
		},
		{
			name:             "futures market reduce-only partial fill",
			market:           domain.MarketPerpetualFutures,
			payload:          mockFuturesUserDataPayload("futures-market-1", "SELL", "BOTH", "MARKET", "GTC", "PARTIALLY_FILLED", true, 11004, 21004, "0.4", "0.4", "0.24"),
			wantSymbol:       "ETHUSDT",
			wantClientID:     "futures-market-1",
			wantOrderID:      "11004",
			wantTradeID:      "21004",
			wantSide:         "SELL",
			wantPositionSide: "BOTH",
			wantOrderType:    "MARKET",
			wantTIF:          "GTC",
			wantLastQty:      0.4,
			wantAccumQty:     0.4,
			wantFee:          0.24,
			wantReduceOnly:   true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			event, err := NewUserDataStreamParser(tc.market).ParseOrderEvent([]byte(tc.payload))
			if err != nil {
				t.Fatalf("ParseOrderEvent: %v", err)
			}
			assertMockPartialFillEvent(t, event, tc.wantSymbol, tc.wantClientID, tc.wantOrderID, tc.wantTradeID, tc.wantSide, tc.wantPositionSide, tc.wantOrderType, tc.wantTIF, tc.wantLastQty, tc.wantAccumQty, tc.wantFee, tc.wantReduceOnly)
		})
	}
}

func TestMockBinanceUserDataStreamRejectsFOKPartialFill(t *testing.T) {
	cases := []struct {
		name    string
		market  domain.Market
		payload string
	}{
		{
			name:    "spot fok partial fill",
			market:  domain.MarketSpot,
			payload: mockSpotUserDataPayload("spot-fok-1", "BUY", "LIMIT", "FOK", "PARTIALLY_FILLED", 12000, 22000, "0.1", "0.1", "0.06"),
		},
		{
			name:    "futures fok partial fill",
			market:  domain.MarketPerpetualFutures,
			payload: mockFuturesUserDataPayload("futures-fok-1", "BUY", "BOTH", "LIMIT", "FOK", "PARTIALLY_FILLED", false, 12001, 22001, "0.1", "0.1", "0.06"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewUserDataStreamParser(tc.market).ParseOrderEvent([]byte(tc.payload))
			if err == nil {
				t.Fatal("expected FOK partial-fill websocket payload to be rejected")
			}
			if !strings.Contains(err.Error(), "FOK") || !strings.Contains(err.Error(), "PARTIALLY_FILLED") {
				t.Fatalf("error = %q, want FOK/PARTIALLY_FILLED context", err.Error())
			}
		})
	}
}

func mockSpotUserDataPayload(clientID, side, orderType, tif, status string, orderID, tradeID int64, lastQty, accumQty, fee string) string {
	return `{
		"e": "executionReport",
		"E": 1710000000123,
		"s": "ETHUSDT",
		"c": "` + clientID + `",
		"S": "` + side + `",
		"o": "` + orderType + `",
		"f": "` + tif + `",
		"x": "TRADE",
		"X": "` + status + `",
		"i": ` + int64Text(orderID) + `,
		"t": ` + int64Text(tradeID) + `,
		"l": "` + lastQty + `",
		"L": "3000",
		"z": "` + accumQty + `",
		"n": "` + fee + `",
		"N": "USDT"
	}`
}

func mockFuturesUserDataPayload(clientID, side, positionSide, orderType, tif, status string, reduceOnly bool, orderID, tradeID int64, lastQty, accumQty, fee string) string {
	reduceOnlyText := "false"
	if reduceOnly {
		reduceOnlyText = "true"
	}
	return `{
		"e": "ORDER_TRADE_UPDATE",
		"E": 1710000000456,
		"o": {
			"s": "ETHUSDT",
			"c": "` + clientID + `",
			"S": "` + side + `",
			"ps": "` + positionSide + `",
			"o": "` + orderType + `",
			"f": "` + tif + `",
			"x": "TRADE",
			"X": "` + status + `",
			"i": ` + int64Text(orderID) + `,
			"t": ` + int64Text(tradeID) + `,
			"l": "` + lastQty + `",
			"L": "3000",
			"z": "` + accumQty + `",
			"n": "` + fee + `",
			"N": "USDT",
			"R": ` + reduceOnlyText + `
		}
	}`
}

func assertMockPartialFillEvent(
	t *testing.T,
	event UserDataOrderEvent,
	wantSymbol, wantClientID, wantOrderID, wantTradeID, wantSide, wantPositionSide, wantOrderType, wantTIF string,
	wantLastQty, wantAccumQty, wantFee float64,
	wantReduceOnly bool,
) {
	t.Helper()
	if event.EventSource != "websocket" ||
		event.Symbol != wantSymbol ||
		event.ClientOrderID != wantClientID ||
		event.ExchangeOrderID != wantOrderID ||
		event.ExchangeTradeID != wantTradeID ||
		event.Side != wantSide ||
		event.PositionSide != wantPositionSide ||
		event.OrderType != wantOrderType ||
		event.TimeInForce != wantTIF ||
		event.ExecutionType != "TRADE" ||
		event.OrderStatus != "PARTIALLY_FILLED" ||
		event.LastFilledQty != wantLastQty ||
		event.AccumulatedFilledQty != wantAccumQty ||
		event.LastFilledPrice != 3000 ||
		event.Fee != wantFee ||
		event.FeeAsset != "USDT" ||
		event.ReduceOnly != wantReduceOnly {
		t.Fatalf("event = %+v", event)
	}
}

func int64Text(value int64) string {
	return strconv.FormatInt(value, 10)
}
