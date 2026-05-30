package lifecycle

import "time"

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
	OrderStatus     string
	FillDelta       FillDelta
	OrderState      OrderState
	OccurredAt      time.Time
	CreatedAt       time.Time
}
