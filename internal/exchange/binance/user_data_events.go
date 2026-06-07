package binance

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const userDataEventSourceWebsocket = "websocket"

type UserDataOrderEvent struct {
	EventSource          string
	EventTime            time.Time
	Symbol               string
	ClientOrderID        string
	ExchangeOrderID      string
	ExchangeTradeID      string
	Side                 string
	PositionSide         string
	OrderType            string
	TimeInForce          string
	OrderStatus          string
	ExecutionType        string
	LastFilledQty        float64
	LastFilledPrice      float64
	AccumulatedFilledQty float64
	Fee                  float64
	FeeAsset             string
	ReduceOnly           bool
}

type spotUserDataOrderPayload struct {
	EventType            string `json:"e"`
	EventTime            int64  `json:"E"`
	Symbol               string `json:"s"`
	ClientOrderID        string `json:"c"`
	Side                 string `json:"S"`
	OrderType            string `json:"o"`
	TimeInForce          string `json:"f"`
	ExecutionType        string `json:"x"`
	OrderStatus          string `json:"X"`
	ExchangeOrderID      int64  `json:"i"`
	ExchangeTradeID      int64  `json:"t"`
	LastFilledQty        string `json:"l"`
	LastFilledPrice      string `json:"L"`
	AccumulatedFilledQty string `json:"z"`
	Fee                  string `json:"n"`
	FeeAsset             string `json:"N"`
}

type futuresUserDataOrderEnvelope struct {
	EventType string                      `json:"e"`
	EventTime int64                       `json:"E"`
	Order     futuresUserDataOrderPayload `json:"o"`
}

type futuresUserDataOrderPayload struct {
	Symbol               string `json:"s"`
	ClientOrderID        string `json:"c"`
	Side                 string `json:"S"`
	PositionSide         string `json:"ps"`
	OrderType            string `json:"o"`
	TimeInForce          string `json:"f"`
	ExecutionType        string `json:"x"`
	OrderStatus          string `json:"X"`
	ExchangeOrderID      int64  `json:"i"`
	ExchangeTradeID      int64  `json:"t"`
	LastFilledQty        string `json:"l"`
	LastFilledPrice      string `json:"L"`
	AccumulatedFilledQty string `json:"z"`
	Fee                  string `json:"n"`
	FeeAsset             string `json:"N"`
	ReduceOnly           bool   `json:"R"`
}

func ParseSpotUserDataOrderEvent(payload []byte) (UserDataOrderEvent, error) {
	var raw spotUserDataOrderPayload
	if err := json.Unmarshal(payload, &raw); err != nil {
		return UserDataOrderEvent{}, fmt.Errorf("decode spot user data order event: %w", err)
	}
	if strings.TrimSpace(raw.EventType) != "executionReport" {
		return UserDataOrderEvent{}, fmt.Errorf("unsupported spot user data event type: %s", raw.EventType)
	}
	return normalizeSpotUserDataOrder(raw)
}

func ParseFuturesUserDataOrderEvent(payload []byte) (UserDataOrderEvent, error) {
	var raw futuresUserDataOrderEnvelope
	if err := json.Unmarshal(payload, &raw); err != nil {
		return UserDataOrderEvent{}, fmt.Errorf("decode futures user data order event: %w", err)
	}
	if strings.TrimSpace(raw.EventType) != "ORDER_TRADE_UPDATE" {
		return UserDataOrderEvent{}, fmt.Errorf("unsupported futures user data event type: %s", raw.EventType)
	}
	return normalizeFuturesUserDataOrder(raw.EventTime, raw.Order)
}

func normalizeSpotUserDataOrder(raw spotUserDataOrderPayload) (UserDataOrderEvent, error) {
	lastFilledQty, err := parseUserDataFloat(raw.LastFilledQty, "last filled qty")
	if err != nil {
		return UserDataOrderEvent{}, err
	}
	lastFilledPrice, err := parseUserDataFloat(raw.LastFilledPrice, "last filled price")
	if err != nil {
		return UserDataOrderEvent{}, err
	}
	accumulatedFilledQty, err := parseUserDataFloat(raw.AccumulatedFilledQty, "accumulated filled qty")
	if err != nil {
		return UserDataOrderEvent{}, err
	}
	fee, err := parseUserDataFloat(raw.Fee, "fee")
	if err != nil {
		return UserDataOrderEvent{}, err
	}
	return UserDataOrderEvent{
		EventSource:          userDataEventSourceWebsocket,
		EventTime:            time.UnixMilli(raw.EventTime).UTC(),
		Symbol:               normalizeUserDataText(raw.Symbol),
		ClientOrderID:        strings.TrimSpace(raw.ClientOrderID),
		ExchangeOrderID:      formatUserDataID(raw.ExchangeOrderID),
		ExchangeTradeID:      formatUserDataID(raw.ExchangeTradeID),
		Side:                 normalizeUserDataText(raw.Side),
		OrderType:            normalizeUserDataText(raw.OrderType),
		TimeInForce:          normalizeUserDataText(raw.TimeInForce),
		OrderStatus:          normalizeUserDataText(raw.OrderStatus),
		ExecutionType:        normalizeUserDataText(raw.ExecutionType),
		LastFilledQty:        lastFilledQty,
		LastFilledPrice:      lastFilledPrice,
		AccumulatedFilledQty: accumulatedFilledQty,
		Fee:                  fee,
		FeeAsset:             normalizeUserDataText(raw.FeeAsset),
	}, nil
}

func normalizeFuturesUserDataOrder(eventTime int64, raw futuresUserDataOrderPayload) (UserDataOrderEvent, error) {
	lastFilledQty, err := parseUserDataFloat(raw.LastFilledQty, "last filled qty")
	if err != nil {
		return UserDataOrderEvent{}, err
	}
	lastFilledPrice, err := parseUserDataFloat(raw.LastFilledPrice, "last filled price")
	if err != nil {
		return UserDataOrderEvent{}, err
	}
	accumulatedFilledQty, err := parseUserDataFloat(raw.AccumulatedFilledQty, "accumulated filled qty")
	if err != nil {
		return UserDataOrderEvent{}, err
	}
	fee, err := parseUserDataFloat(raw.Fee, "fee")
	if err != nil {
		return UserDataOrderEvent{}, err
	}
	return UserDataOrderEvent{
		EventSource:          userDataEventSourceWebsocket,
		EventTime:            time.UnixMilli(eventTime).UTC(),
		Symbol:               normalizeUserDataText(raw.Symbol),
		ClientOrderID:        strings.TrimSpace(raw.ClientOrderID),
		ExchangeOrderID:      formatUserDataID(raw.ExchangeOrderID),
		ExchangeTradeID:      formatUserDataID(raw.ExchangeTradeID),
		Side:                 normalizeUserDataText(raw.Side),
		PositionSide:         normalizeUserDataText(raw.PositionSide),
		OrderType:            normalizeUserDataText(raw.OrderType),
		TimeInForce:          normalizeUserDataText(raw.TimeInForce),
		OrderStatus:          normalizeUserDataText(raw.OrderStatus),
		ExecutionType:        normalizeUserDataText(raw.ExecutionType),
		LastFilledQty:        lastFilledQty,
		LastFilledPrice:      lastFilledPrice,
		AccumulatedFilledQty: accumulatedFilledQty,
		Fee:                  fee,
		FeeAsset:             normalizeUserDataText(raw.FeeAsset),
		ReduceOnly:           raw.ReduceOnly,
	}, nil
}

func parseUserDataFloat(raw, label string) (float64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %q", label, raw)
	}
	return value, nil
}

func normalizeUserDataText(value string) string {
	return strings.ToUpper(strings.TrimSpace(value))
}

func formatUserDataID(value int64) string {
	if value <= 0 {
		return ""
	}
	return strconv.FormatInt(value, 10)
}
