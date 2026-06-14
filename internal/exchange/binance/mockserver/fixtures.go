package mockserver

import (
	"encoding/json"
)

func FuturesOrderTradeUpdate(event BinanceOrderEvent) []byte {
	payload := map[string]any{
		"e": "ORDER_TRADE_UPDATE",
		"E": nowMillis(event.EventTime),
		"o": map[string]any{
			"s":  event.Symbol,
			"c":  event.ClientOrderID,
			"S":  event.Side,
			"ps": event.PositionSide,
			"o":  event.OrderType,
			"f":  event.TimeInForce,
			"x":  event.ExecutionType,
			"X":  event.OrderStatus,
			"i":  formatOrderID(event.ExchangeOrderID),
			"t":  formatTradeID(event.ExchangeTradeID),
			"l":  formatFloat(event.LastFilledQty),
			"L":  formatFloat(event.LastFilledPrice),
			"z":  formatFloat(event.AccumulatedFilledQty),
			"n":  formatFloat(event.Fee),
			"N":  event.FeeAsset,
			"R":  event.ReduceOnly,
		},
	}
	encoded, _ := json.Marshal(payload)
	return encoded
}

func SpotExecutionReport(event BinanceOrderEvent) []byte {
	payload := map[string]any{
		"e": "executionReport",
		"E": nowMillis(event.EventTime),
		"s": event.Symbol,
		"c": event.ClientOrderID,
		"S": event.Side,
		"o": event.OrderType,
		"f": event.TimeInForce,
		"x": event.ExecutionType,
		"X": event.OrderStatus,
		"i": formatOrderID(event.ExchangeOrderID),
		"t": formatTradeID(event.ExchangeTradeID),
		"l": formatFloat(event.LastFilledQty),
		"L": formatFloat(event.LastFilledPrice),
		"z": formatFloat(event.AccumulatedFilledQty),
		"n": formatFloat(event.Fee),
		"N": event.FeeAsset,
	}
	encoded, _ := json.Marshal(payload)
	return encoded
}
