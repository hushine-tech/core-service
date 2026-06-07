package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/gen/orderv1"
	"github.com/hushine-tech/core-service/internal/order/accountmeta"
	"github.com/hushine-tech/core-service/internal/order/executor"
	ordernotify "github.com/hushine-tech/core-service/internal/order/notification"
	orderrisk "github.com/hushine-tech/core-service/internal/order/risk"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type recordingNotificationPublisher struct {
	err    error
	events []ordernotify.Event
}

func (p *recordingNotificationPublisher) Publish(_ context.Context, event ordernotify.Event) error {
	p.events = append(p.events, event)
	return p.err
}

func notificationMeta(environment int32) accountmeta.Meta {
	meta := testOrderMeta(environment)
	meta.AccountID = 7
	meta.VenueID = 70
	meta.UserID = 42
	return meta
}

func notificationRequest() *orderv1.PlaceOrderRequest {
	req := testPlaceOrderRequest()
	req.AccountId = 7
	req.Symbol = "ETHUSDT"
	req.Side = "BUY"
	req.Qty = 1
	return req
}

func TestPlaceOrderPublishesSucceededNotification(t *testing.T) {
	pub := &recordingNotificationPublisher{}
	repo := &stubRepo{}
	svc := NewOrderGRPCService(
		&stubMetaGetter{meta: notificationMeta(environmentDemo)},
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

	req := notificationRequest()
	req.StrategyId = 9
	req.SessionId = "sess-1"
	req.IntentId = "intent-1"
	req.MarketTime = timestamppb.New(time.Date(2026, 6, 3, 15, 44, 0, 0, time.UTC))
	resp, err := svc.PlaceOrder(context.Background(), req)
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
	for _, want := range []string{
		"order_id=" + event.OrderID,
		"exchange_order_id=ex-1",
		"attempt_id=" + event.AttemptID,
		"trigger_time=2026-06-03T15:44:00Z",
	} {
		if !strings.Contains(event.Message, want) {
			t.Fatalf("message %q does not include %q", event.Message, want)
		}
	}
}

func TestPlaceOrderPublishesFailedNotification(t *testing.T) {
	pub := &recordingNotificationPublisher{}
	repo := &stubRepo{}
	svc := NewOrderGRPCService(
		&stubMetaGetter{meta: notificationMeta(environmentDemo)},
		&stubRouterExec{result: executor.OrderResult{ErrorMessage: "exchange rejected"}},
		repo,
		pub,
	)

	req := notificationRequest()
	req.StrategyId = 9
	req.SessionId = "sess-1"
	req.IntentId = "intent-1"
	resp, err := svc.PlaceOrder(context.Background(), req)
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

func TestPlaceOrderPublishesRiskRejectedNotification(t *testing.T) {
	pub := &recordingNotificationPublisher{}
	repo := &stubRepo{}
	router := &stubRouterExec{}
	svc := NewOrderGRPCService(
		&stubMetaGetter{meta: notificationMeta(environmentDemo)},
		router,
		repo,
		pub,
	)
	svc.SetRiskGate(&stubRiskGate{decision: orderrisk.Decision{
		Status:     orderrisk.DecisionReject,
		ReasonCode: "ROUTE_PENDING_EXECUTION",
		Violations: []orderrisk.Violation{{
			Code:    "ROUTE_PENDING_EXECUTION",
			Message: "route has pending execution",
		}},
	}})

	req := notificationRequest()
	req.StrategyId = 9
	req.SessionId = "sess-risk"
	req.IntentId = "intent-risk"
	resp, err := svc.PlaceOrder(context.Background(), req)
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if resp.GetAttemptStatus() != attemptStatusRiskRejected {
		t.Fatalf("attempt status = %s, want %s", resp.GetAttemptStatus(), attemptStatusRiskRejected)
	}
	if router.executeCalls != 0 {
		t.Fatalf("execute calls = %d, want 0", router.executeCalls)
	}
	if len(pub.events) != 1 {
		t.Fatalf("events = %d, want 1", len(pub.events))
	}
	event := pub.events[0]
	if event.EventType != ordernotify.EventOrderFailed ||
		event.Severity != ordernotify.SeverityError ||
		event.Title != "Order risk rejected" ||
		event.Metadata["attempt_status"] != attemptStatusRiskRejected {
		t.Fatalf("event = %+v, want risk rejected order.failed notification", event)
	}
	if !strings.Contains(event.Message, "ROUTE_PENDING_EXECUTION") {
		t.Fatalf("message %q does not include risk reject code", event.Message)
	}
}

func TestPlaceOrderSkipsOrderNotificationForBacktestEnvironment(t *testing.T) {
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
				&stubMetaGetter{meta: notificationMeta(environmentBacktest)},
				&stubRouterExec{result: tc.result},
				&stubRepo{},
				pub,
			)

			req := notificationRequest()
			req.StrategyId = 9
			req.SessionId = "sess-backtest"
			req.IntentId = "intent-backtest-" + tc.name
			resp, err := svc.PlaceOrder(context.Background(), req)
			if err != nil {
				t.Fatalf("PlaceOrder: %v", err)
			}
			if resp.GetAttemptStatus() != tc.want {
				t.Fatalf("attempt status = %s, want %s", resp.GetAttemptStatus(), tc.want)
			}
			if len(pub.events) != 0 {
				t.Fatalf("events = %+v, want none for backtest environment", pub.events)
			}
		})
	}
}

func TestPlaceOrderNotificationFailureDoesNotBlockOrder(t *testing.T) {
	pub := &recordingNotificationPublisher{err: errors.New("kafka down")}
	repo := &stubRepo{}
	svc := NewOrderGRPCService(
		&stubMetaGetter{meta: notificationMeta(environmentDemo)},
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

	req := notificationRequest()
	req.StrategyId = 9
	req.SessionId = "sess-1"
	req.IntentId = "intent-1"
	resp, err := svc.PlaceOrder(context.Background(), req)
	if err != nil {
		t.Fatalf("PlaceOrder should not fail on notification error: %v", err)
	}
	if resp.GetAttemptStatus() != attemptStatusAccepted {
		t.Fatalf("attempt status = %s, want %s", resp.GetAttemptStatus(), attemptStatusAccepted)
	}
}
