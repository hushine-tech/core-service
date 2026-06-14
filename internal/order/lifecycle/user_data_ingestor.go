package lifecycle

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/hushine-tech/core-service/internal/exchange/adapter"
)

var ErrOpenOrderNotFound = errors.New("open order not found")

type UserDataOrderResolver interface {
	ResolveOpenOrderByExchangeRef(ctx context.Context, venueID int64, exchangeOrderID, clientOrderID string) (OpenOrder, error)
}

type UserDataIngestStore interface {
	EventStore
	UserDataOrderResolver
}

type UserDataIngestor struct {
	store UserDataIngestStore
}

func NewUserDataIngestor(store UserDataIngestStore) *UserDataIngestor {
	return &UserDataIngestor{store: store}
}

func (i *UserDataIngestor) Ingest(ctx context.Context, venueID int64, event adapter.UserDataOrderEvent) error {
	execType := strings.ToUpper(strings.TrimSpace(event.ExecutionType))
	status := strings.ToUpper(strings.TrimSpace(event.OrderStatus))
	if execType != "TRADE" && !isTerminalUserDataStatus(status) {
		return nil
	}
	order, err := i.store.ResolveOpenOrderByExchangeRef(ctx, venueID, event.ExchangeOrderID, event.ClientOrderID)
	if err != nil {
		return err
	}
	occurredAt := event.EventTime
	if occurredAt.IsZero() {
		occurredAt = time.Now().UTC()
	}

	lifecycleEvent := Event{
		SessionID:       order.SessionID,
		AccountID:       order.AccountID,
		VenueID:         order.VenueID,
		Environment:     order.Environment,
		Exchange:        order.Exchange,
		Market:          order.Market,
		PositionSide:    order.PositionSide,
		Side:            firstNonEmpty(event.Side, order.Side),
		IntentID:        order.IntentID,
		AttemptID:       order.AttemptID,
		OrderID:         order.OrderID,
		ExchangeOrderID: firstNonEmpty(event.ExchangeOrderID, order.ExchangeOrderID),
		ExchangeTradeID: event.ExchangeTradeID,
		EventSource:     EventSourceWebsocket,
		OrderStatus:     status,
		OrderState: OrderState{
			ExchangeOrderID: firstNonEmpty(event.ExchangeOrderID, order.ExchangeOrderID),
			ClientOrderID:   firstNonEmpty(event.ClientOrderID, order.ClientOrderID),
			Symbol:          firstNonEmpty(event.Symbol, order.Symbol),
			Status:          status,
			ExecutedQty:     event.AccumulatedFilledQty,
			AvgPrice:        event.LastFilledPrice,
			UpdatedAt:       occurredAt,
		},
		OccurredAt: occurredAt,
	}
	if execType == "TRADE" && event.LastFilledQty > 0 {
		lifecycleEvent.EventType = "fill"
		lifecycleEvent.FillDelta = FillDelta{
			ExchangeTradeID: event.ExchangeTradeID,
			ExchangeOrderID: firstNonEmpty(event.ExchangeOrderID, order.ExchangeOrderID),
			Symbol:          firstNonEmpty(event.Symbol, order.Symbol),
			Qty:             event.LastFilledQty,
			FillPrice:       event.LastFilledPrice,
			Fee:             event.Fee,
			FeeAsset:        event.FeeAsset,
			TradeTime:       occurredAt,
		}
	} else {
		lifecycleEvent.EventType = "terminal"
	}
	if err := ValidateEventRouteFacts(lifecycleEvent); err != nil {
		return err
	}
	_, err = i.store.SaveLifecycleEvent(ctx, lifecycleEvent)
	return err
}

func isTerminalUserDataStatus(status string) bool {
	switch status {
	case "FILLED", "CANCELED", "EXPIRED", "REJECTED":
		return true
	default:
		return false
	}
}
