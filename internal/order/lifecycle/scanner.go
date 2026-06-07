package lifecycle

import (
	"context"
	"time"
)

type OpenOrder struct {
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
	ClientOrderID   string
	Symbol          string
}

type ScannerStore interface {
	EventStore
	ListOpenOrders(ctx context.Context, limit int) ([]OpenOrder, error)
}

type OrderStateReader interface {
	QueryOrder(ctx context.Context, order OpenOrder) (OrderState, error)
	QueryTrades(ctx context.Context, order OpenOrder) ([]FillDelta, error)
}

type ScannerConfig struct {
	BatchSize      int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

type Scanner struct {
	store  ScannerStore
	reader OrderStateReader
	cfg    ScannerConfig

	venueBackoff map[int64]time.Time
}

func NewScanner(store ScannerStore, reader OrderStateReader, cfg ScannerConfig) *Scanner {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 50
	}
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = 5 * time.Second
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 60 * time.Second
	}
	return &Scanner{
		store:        store,
		reader:       reader,
		cfg:          cfg,
		venueBackoff: make(map[int64]time.Time),
	}
}

func (s *Scanner) ScanOnce(ctx context.Context, now time.Time) (int, error) {
	orders, err := s.store.ListOpenOrders(ctx, s.cfg.BatchSize)
	if err != nil {
		return 0, err
	}
	written := 0
	for _, order := range orders {
		if until, ok := s.venueBackoff[order.VenueID]; ok && now.Before(until) {
			continue
		}
		state, err := s.reader.QueryOrder(ctx, order)
		if err != nil {
			s.backoffVenue(order.VenueID, now)
			continue
		}
		if !hasFillDelta(state) {
			continue
		}
		trades, err := s.reader.QueryTrades(ctx, order)
		if err != nil {
			s.backoffVenue(order.VenueID, now)
			continue
		}
		for _, fill := range trades {
			if fill.TradeTime.IsZero() {
				fill.TradeTime = now
			}
			event := Event{
				SessionID:       order.SessionID,
				AccountID:       order.AccountID,
				VenueID:         order.VenueID,
				Environment:     order.Environment,
				Exchange:        order.Exchange,
				Market:          order.Market,
				PositionSide:    order.PositionSide,
				Side:            order.Side,
				IntentID:        order.IntentID,
				AttemptID:       order.AttemptID,
				OrderID:         order.OrderID,
				ExchangeOrderID: firstNonEmpty(fill.ExchangeOrderID, state.ExchangeOrderID, order.ExchangeOrderID),
				ExchangeTradeID: fill.ExchangeTradeID,
				EventType:       "fill",
				EventSource:     EventSourceRESTRecovery,
				OrderStatus:     state.Status,
				FillDelta:       fill,
				OrderState:      state,
				OccurredAt:      fill.TradeTime,
			}
			if err := ValidateEventRouteFacts(event); err != nil {
				return written, err
			}
			_, err := s.store.SaveLifecycleEvent(ctx, event)
			if err != nil {
				return written, err
			}
			written++
		}
	}
	return written, nil
}

func (s *Scanner) backoffVenue(venueID int64, now time.Time) {
	delay := s.cfg.InitialBackoff
	if delay > s.cfg.MaxBackoff {
		delay = s.cfg.MaxBackoff
	}
	s.venueBackoff[venueID] = now.Add(delay)
}

func hasFillDelta(state OrderState) bool {
	switch state.Status {
	case "FILLED", "PARTIALLY_FILLED":
		return state.ExecutedQty > 0
	default:
		return false
	}
}
