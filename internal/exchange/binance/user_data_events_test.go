package binance

import (
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
