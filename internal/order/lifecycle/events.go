package lifecycle

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type FillDelta struct {
	ExchangeTradeID string    `json:"exchange_trade_id,omitempty"`
	ExchangeOrderID string    `json:"exchange_order_id,omitempty"`
	Symbol          string    `json:"symbol,omitempty"`
	Qty             float64   `json:"qty,omitempty"`
	FillPrice       float64   `json:"fill_price,omitempty"`
	Fee             float64   `json:"fee,omitempty"`
	FeeAsset        string    `json:"fee_asset,omitempty"`
	FeeMissing      bool      `json:"fee_missing,omitempty"`
	TradeTime       time.Time `json:"trade_time,omitempty"`
}

type OrderState struct {
	ExchangeOrderID string    `json:"exchange_order_id,omitempty"`
	ClientOrderID   string    `json:"client_order_id,omitempty"`
	Symbol          string    `json:"symbol,omitempty"`
	Status          string    `json:"status,omitempty"`
	OrigQty         float64   `json:"orig_qty,omitempty"`
	ExecutedQty     float64   `json:"executed_qty,omitempty"`
	RemainingQty    float64   `json:"remaining_qty,omitempty"`
	AvgPrice        float64   `json:"avg_price,omitempty"`
	UpdatedAt       time.Time `json:"updated_at,omitempty"`
}

type Event struct {
	EventID         int64
	SessionID       string
	AccountID       int64
	VenueID         int64
	Environment     int32
	Exchange        int32
	Market          int32
	PositionSide    int32
	Side            string
	IntentID        string
	AttemptID       string
	OrderID         string
	ExchangeOrderID string
	ExchangeTradeID string
	EventType       string
	EventSource     string
	OrderStatus     string
	FillDelta       FillDelta
	OrderState      OrderState
	OccurredAt      time.Time
	CreatedAt       time.Time
}

const (
	EventSourcePlaceOrder   = "place_order"
	EventSourceWebsocket    = "websocket"
	EventSourceRESTRecovery = "rest_recovery"
	EventSourceForceClose   = "force_close"
)

func ValidateEventRouteFacts(event Event) error {
	if strings.TrimSpace(event.EventType) == "" {
		return fmt.Errorf("event_type is required")
	}
	if !isAllowedEventSource(event.EventSource) {
		return fmt.Errorf("unsupported event_source: %s", event.EventSource)
	}
	if event.AccountID <= 0 {
		return fmt.Errorf("account_id is required")
	}
	if event.VenueID <= 0 {
		return fmt.Errorf("venue_id is required")
	}
	if event.Exchange <= 0 {
		return fmt.Errorf("exchange is required")
	}
	if event.Market <= 0 {
		return fmt.Errorf("market is required")
	}
	switch event.PositionSide {
	case 0, 1, 2:
	default:
		return fmt.Errorf("unsupported position_side: %d", event.PositionSide)
	}
	if strings.TrimSpace(event.Side) == "" {
		return fmt.Errorf("side is required")
	}
	if strings.TrimSpace(firstNonEmpty(event.FillDelta.Symbol, event.OrderState.Symbol)) == "" {
		return fmt.Errorf("symbol is required")
	}
	return nil
}

func isAllowedEventSource(source string) bool {
	switch strings.TrimSpace(source) {
	case EventSourcePlaceOrder, EventSourceWebsocket, EventSourceRESTRecovery, EventSourceForceClose:
		return true
	default:
		return false
	}
}

func EventIdentityKey(event Event) string {
	orderID := strings.TrimSpace(firstNonEmpty(
		event.ExchangeOrderID,
		event.FillDelta.ExchangeOrderID,
		event.OrderState.ExchangeOrderID,
	))
	tradeID := strings.TrimSpace(firstNonEmpty(event.ExchangeTradeID, event.FillDelta.ExchangeTradeID))
	if orderID == "" {
		return ""
	}
	if tradeID != "" {
		return strings.Join([]string{
			"trade",
			strings.ToUpper(orderID),
			strings.ToUpper(tradeID),
		}, "|")
	}

	status := strings.TrimSpace(firstNonEmpty(event.OrderStatus, event.OrderState.Status))
	identityTime := event.OrderState.UpdatedAt
	if identityTime.IsZero() {
		identityTime = event.OccurredAt
	}
	if status == "" || identityTime.IsZero() {
		return ""
	}
	return strings.Join([]string{
		"state",
		strings.ToUpper(orderID),
		strings.ToUpper(status),
		strconv.FormatFloat(event.OrderState.ExecutedQty, 'f', -1, 64),
		identityTime.UTC().Format(time.RFC3339Nano),
	}, "|")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
