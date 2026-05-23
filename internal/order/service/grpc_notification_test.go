package service

import (
	"context"
	"errors"
	"testing"

	"github.com/hushine-tech/core-service/gen/orderv1"
	"github.com/hushine-tech/core-service/internal/order/accountmeta"
	"github.com/hushine-tech/core-service/internal/order/executor"
	ordernotify "github.com/hushine-tech/core-service/internal/order/notification"
)

type recordingNotificationPublisher struct {
	err    error
	events []ordernotify.Event
}

func (p *recordingNotificationPublisher) Publish(_ context.Context, event ordernotify.Event) error {
	p.events = append(p.events, event)
	return p.err
}

func TestPlaceOrderPublishesSucceededNotification(t *testing.T) {
	pub := &recordingNotificationPublisher{}
	repo := &stubRepo{}
	svc := NewOrderGRPCService(
		&stubMetaGetter{meta: accountmeta.Meta{AccountID: 7, UserID: 42, Mode: 2}},
		&stubRouterExec{result: executor.OrderResult{
			ExchangeOrderID: "ex-1",
			ClientOrderID:   "client-1",
			Symbol:          "ETHUSDT",
			Side:            "BUY",
			OrigQty:         1,
			ExecutedQty:     1,
			AvgPrice:        2500,
			Status:          "FILLED",
		}},
		repo,
		pub,
	)

	resp, err := svc.PlaceOrder(context.Background(), &orderv1.PlaceOrderRequest{
		AccountId:  7,
		StrategyId: 9,
		SessionId:  "sess-1",
		Market:     "futures",
		IntentId:   "intent-1",
		Symbol:     "ETHUSDT",
		Side:       "BUY",
		Qty:        1,
	})
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if resp.GetAttemptStatus() != attemptStatusAccepted {
		t.Fatalf("attempt status = %s, want %s", resp.GetAttemptStatus(), attemptStatusAccepted)
	}
	if len(pub.events) != 1 {
		t.Fatalf("events = %d, want 1", len(pub.events))
	}
	event := pub.events[0]
	if event.UserID != 42 ||
		event.AccountID != 7 ||
		event.StrategyID != 9 ||
		event.SessionID != "sess-1" ||
		event.Category != ordernotify.CategoryStrategy ||
		event.EventType != ordernotify.EventOrderAccepted ||
		event.Severity != ordernotify.SeverityInfo {
		t.Fatalf("event = %+v", event)
	}
}

func TestPlaceOrderPublishesFailedNotification(t *testing.T) {
	pub := &recordingNotificationPublisher{}
	repo := &stubRepo{}
	svc := NewOrderGRPCService(
		&stubMetaGetter{meta: accountmeta.Meta{AccountID: 7, UserID: 42, Mode: 2}},
		&stubRouterExec{result: executor.OrderResult{ErrorMessage: "exchange rejected"}},
		repo,
		pub,
	)

	resp, err := svc.PlaceOrder(context.Background(), &orderv1.PlaceOrderRequest{
		AccountId:  7,
		StrategyId: 9,
		SessionId:  "sess-1",
		Market:     "futures",
		IntentId:   "intent-1",
		Symbol:     "ETHUSDT",
		Side:       "BUY",
		Qty:        1,
	})
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if resp.GetAttemptStatus() != attemptStatusFailed {
		t.Fatalf("attempt status = %s, want %s", resp.GetAttemptStatus(), attemptStatusFailed)
	}
	if len(pub.events) != 1 || pub.events[0].EventType != ordernotify.EventOrderFailed || pub.events[0].Severity != ordernotify.SeverityError {
		t.Fatalf("events = %+v, want order.failed", pub.events)
	}
}

func TestPlaceOrderSkipsOrderNotificationForBacktestMode(t *testing.T) {
	cases := []struct {
		name   string
		result executor.OrderResult
		want   string
	}{
		{
			name: "accepted",
			result: executor.OrderResult{
				ExchangeOrderID: "ex-1",
				ClientOrderID:   "client-1",
				Symbol:          "ETHUSDT",
				Side:            "BUY",
				OrigQty:         1,
				ExecutedQty:     1,
				AvgPrice:        2500,
				Status:          "FILLED",
			},
			want: attemptStatusAccepted,
		},
		{
			name:   "failed",
			result: executor.OrderResult{ErrorMessage: "mock rejected"},
			want:   attemptStatusFailed,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pub := &recordingNotificationPublisher{}
			svc := NewOrderGRPCService(
				&stubMetaGetter{meta: accountmeta.Meta{AccountID: 7, UserID: 42, Mode: 0}},
				&stubRouterExec{result: tc.result},
				&stubRepo{},
				pub,
			)

			resp, err := svc.PlaceOrder(context.Background(), &orderv1.PlaceOrderRequest{
				AccountId:  7,
				StrategyId: 9,
				SessionId:  "sess-backtest",
				Market:     "futures",
				IntentId:   "intent-backtest-" + tc.name,
				Symbol:     "ETHUSDT",
				Side:       "BUY",
				Qty:        1,
			})
			if err != nil {
				t.Fatalf("PlaceOrder: %v", err)
			}
			if resp.GetAttemptStatus() != tc.want {
				t.Fatalf("attempt status = %s, want %s", resp.GetAttemptStatus(), tc.want)
			}
			if len(pub.events) != 0 {
				t.Fatalf("events = %+v, want none for backtest mode", pub.events)
			}
		})
	}
}

func TestPlaceOrderNotificationFailureDoesNotBlockOrder(t *testing.T) {
	pub := &recordingNotificationPublisher{err: errors.New("kafka down")}
	repo := &stubRepo{}
	svc := NewOrderGRPCService(
		&stubMetaGetter{meta: accountmeta.Meta{AccountID: 7, UserID: 42, Mode: 2}},
		&stubRouterExec{result: executor.OrderResult{
			ExchangeOrderID: "ex-1",
			Symbol:          "ETHUSDT",
			Side:            "BUY",
			OrigQty:         1,
			ExecutedQty:     1,
			AvgPrice:        2500,
			Status:          "FILLED",
		}},
		repo,
		pub,
	)

	resp, err := svc.PlaceOrder(context.Background(), &orderv1.PlaceOrderRequest{
		AccountId:  7,
		StrategyId: 9,
		SessionId:  "sess-1",
		Market:     "futures",
		IntentId:   "intent-1",
		Symbol:     "ETHUSDT",
		Side:       "BUY",
		Qty:        1,
	})
	if err != nil {
		t.Fatalf("PlaceOrder should not fail on notification error: %v", err)
	}
	if resp.GetAttemptStatus() != attemptStatusAccepted {
		t.Fatalf("attempt status = %s, want %s", resp.GetAttemptStatus(), attemptStatusAccepted)
	}
}
