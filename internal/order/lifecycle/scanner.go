package lifecycle

import (
	"context"
	"errors"
	"strings"
	"time"
)

var ErrRecoveryCancellerUnavailable = errors.New("order recovery canceller unavailable")

type OpenOrder struct {
	SessionID          string
	AccountID          int64
	VenueID            int64
	Environment        int32
	Exchange           int32
	Market             int32
	PositionSide       int32
	Side               string
	IntentID           string
	AttemptID          string
	OrderID            string
	ExchangeOrderID    string
	ClientOrderID      string
	Symbol             string
	RecoveryStatus     string
	RecoveryStartedAt  time.Time
	NextCheckAt        time.Time
	RecoveryDeadlineAt time.Time
	LastRecoveryError  string
}

type ScannerStore interface {
	EventStore
	ListDueOpenOrders(ctx context.Context, limit int) ([]OpenOrder, error)
	MarkRecoveryExpired(ctx context.Context, orderID string, forceClosedAt time.Time, lastError string) error
}

type OrderStateReader interface {
	QueryOrder(ctx context.Context, order OpenOrder) (OrderState, error)
	QueryTrades(ctx context.Context, order OpenOrder) ([]FillDelta, error)
}

type OrderRecoveryCanceller interface {
	CancelOrder(ctx context.Context, order OpenOrder) (CancelResult, error)
}

type ScannerConfig struct {
	BatchSize        int
	InitialBackoff   time.Duration
	MaxBackoff       time.Duration
	RecoveryDeadline time.Duration
}

type Scanner struct {
	store     ScannerStore
	reader    OrderStateReader
	canceller OrderRecoveryCanceller
	cfg       ScannerConfig

	venueBackoff map[int64]time.Time
}

func NewScanner(store ScannerStore, reader OrderStateReader, cfg ScannerConfig) *Scanner {
	var canceller OrderRecoveryCanceller
	if c, ok := reader.(OrderRecoveryCanceller); ok {
		canceller = c
	}
	return NewScannerWithCanceller(store, reader, canceller, cfg)
}

func NewScannerWithCanceller(store ScannerStore, reader OrderStateReader, canceller OrderRecoveryCanceller, cfg ScannerConfig) *Scanner {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 50
	}
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = 5 * time.Second
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 60 * time.Second
	}
	if cfg.RecoveryDeadline <= 0 {
		cfg.RecoveryDeadline = 14 * 24 * time.Hour
	}
	return &Scanner{
		store:        store,
		reader:       reader,
		canceller:    canceller,
		cfg:          cfg,
		venueBackoff: make(map[int64]time.Time),
	}
}

func (s *Scanner) ScanOnce(ctx context.Context, now time.Time) (int, error) {
	orders, err := s.store.ListDueOpenOrders(ctx, s.cfg.BatchSize)
	if err != nil {
		return 0, err
	}
	written := 0
	for _, order := range orders {
		if until, ok := s.venueBackoff[order.VenueID]; ok && now.Before(until) {
			continue
		}
		if deadline := s.orderRecoveryDeadline(order); !deadline.IsZero() && !now.Before(deadline) {
			writtenForOrder, err := s.forceClose(ctx, order, now)
			written += writtenForOrder
			if err != nil {
				s.backoffVenue(order.VenueID, now)
				continue
			}
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
		tradeOrder := enrichOpenOrderFromState(order, state)
		trades, err := s.reader.QueryTrades(ctx, tradeOrder)
		if err != nil {
			s.backoffVenue(order.VenueID, now)
			continue
		}
		writtenForOrder, err := s.writeFillEvents(ctx, tradeOrder, state, trades, EventSourceRESTRecovery, now)
		written += writtenForOrder
		if err != nil {
			return written, err
		}
	}
	return written, nil
}

func (s *Scanner) orderRecoveryDeadline(order OpenOrder) time.Time {
	if !order.RecoveryDeadlineAt.IsZero() {
		return order.RecoveryDeadlineAt
	}
	if !order.RecoveryStartedAt.IsZero() {
		return order.RecoveryStartedAt.Add(s.cfg.RecoveryDeadline)
	}
	return time.Time{}
}

func (s *Scanner) forceClose(ctx context.Context, order OpenOrder, now time.Time) (int, error) {
	if s.canceller == nil {
		return 0, ErrRecoveryCancellerUnavailable
	}
	cancelResult, cancelErr := s.canceller.CancelOrder(ctx, order)
	state, err := s.reader.QueryOrder(ctx, order)
	if err != nil {
		if cancelErr != nil {
			return 0, cancelErr
		}
		return 0, err
	}
	if cancelErr != nil && !isTerminalOrderState(state) {
		return 0, cancelErr
	}
	state.ExchangeOrderID = firstNonEmpty(state.ExchangeOrderID, cancelResult.ExchangeOrderID, order.ExchangeOrderID)
	state.ClientOrderID = firstNonEmpty(state.ClientOrderID, cancelResult.ClientOrderID, order.ClientOrderID)
	state.Symbol = firstNonEmpty(state.Symbol, order.Symbol)
	state.Status = firstNonEmpty(state.Status, cancelResult.Status)
	if state.UpdatedAt.IsZero() {
		state.UpdatedAt = now
	}
	tradeOrder := enrichOpenOrderFromState(order, state)
	trades, err := s.reader.QueryTrades(ctx, tradeOrder)
	if err != nil {
		if cancelErr != nil {
			return 0, cancelErr
		}
		return 0, err
	}
	written, err := s.writeFillEvents(ctx, tradeOrder, state, trades, EventSourceForceClose, now)
	if err != nil {
		return written, err
	}
	terminal := Event{
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
		ExchangeOrderID: firstNonEmpty(state.ExchangeOrderID, order.ExchangeOrderID),
		EventType:       "terminal",
		EventSource:     EventSourceForceClose,
		OrderStatus:     "RECOVERY_EXPIRED",
		OrderState:      state,
		OccurredAt:      now,
	}
	if err := ValidateEventRouteFacts(terminal); err != nil {
		return written, err
	}
	if _, err := s.store.SaveLifecycleEvent(ctx, terminal); err != nil {
		return written, err
	}
	if err := s.store.MarkRecoveryExpired(ctx, order.OrderID, now, cancelErrorText(cancelErr)); err != nil {
		return written + 1, err
	}
	return written + 1, nil
}

func cancelErrorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func isTerminalOrderState(state OrderState) bool {
	switch strings.ToUpper(strings.TrimSpace(state.Status)) {
	case "FILLED", "CANCELED", "EXPIRED", "REJECTED", "RECOVERY_EXPIRED", "FORCE_CLOSED":
		return true
	default:
		return state.OrigQty > 0 && state.RemainingQty <= 0
	}
}

func enrichOpenOrderFromState(order OpenOrder, state OrderState) OpenOrder {
	order.ExchangeOrderID = firstNonEmpty(order.ExchangeOrderID, state.ExchangeOrderID)
	order.ClientOrderID = firstNonEmpty(order.ClientOrderID, state.ClientOrderID)
	order.Symbol = firstNonEmpty(order.Symbol, state.Symbol)
	return order
}

func (s *Scanner) writeFillEvents(ctx context.Context, order OpenOrder, state OrderState, trades []FillDelta, source string, now time.Time) (int, error) {
	written := 0
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
			EventSource:     source,
			OrderStatus:     state.Status,
			FillDelta:       fill,
			OrderState:      state,
			OccurredAt:      fill.TradeTime,
		}
		if err := ValidateEventRouteFacts(event); err != nil {
			return written, err
		}
		if _, err := s.store.SaveLifecycleEvent(ctx, event); err != nil {
			return written, err
		}
		written++
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
