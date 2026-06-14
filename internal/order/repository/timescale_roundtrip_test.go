package repository

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/internal/order/lifecycle"
)

func TestOrderRepositoryPersistsRiskRecoveryContract(t *testing.T) {
	repo, ctx := lifecycleTestRepo(t)

	seed := time.Now().UnixNano()
	accountID := int64(910000000000 + seed%100000000)
	userID := int64(920000000000 + seed%100000000)
	venueID := int64(930000000000 + seed%100000000)
	strategyID := int64(940000000000 + seed%100000000)
	sessionID := fmt.Sprintf("risk-recovery-%d", seed)
	intentID := fmt.Sprintf("intent-%d", seed)
	attemptID := fmt.Sprintf("attempt-%d", seed)
	orderID := fmt.Sprintf("order-%d", seed)
	exchangeOrderID := fmt.Sprintf("exchange-%d", seed)
	clientOrderID := fmt.Sprintf("client-%d", seed)

	baseTime := time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)
	intentGoodTillDate := baseTime.Add(2 * time.Hour)
	attemptGoodTillDate := baseTime.Add(3 * time.Hour)
	orderGoodTillDate := baseTime.Add(4 * time.Hour)
	recoveryStartedAt := baseTime.Add(5 * time.Minute)
	nextCheckAt := baseTime.Add(10 * time.Minute)
	recoveryDeadlineAt := baseTime.Add(24 * time.Hour)
	forceClosedAt := baseTime.Add(48 * time.Hour)

	intent := OrderIntent{
		IntentID:       intentID,
		Time:           baseTime,
		AccountID:      accountID,
		VenueID:        venueID,
		UserID:         userID,
		StrategyID:     strategyID,
		SessionID:      sessionID,
		Environment:    2,
		Exchange:       1,
		Market:         2,
		PositionSide:   0,
		OrderType:      1,
		Symbol:         "ETHUSDT",
		Side:           "BUY",
		RequestedQty:   0.5,
		RequestedPrice: 3000,
		PostOnly:       true,
		GoodTillDate:   &intentGoodTillDate,
		ReduceOnly:     true,
		Status:         "REQUESTED",
	}
	if err := repo.UpsertOrderIntent(ctx, intent); err != nil {
		t.Fatalf("UpsertOrderIntent: %v", err)
	}

	attempt := OrderAttempt{
		AttemptID:       attemptID,
		IntentID:        intentID,
		Time:            baseTime.Add(time.Second),
		AccountID:       accountID,
		VenueID:         venueID,
		UserID:          userID,
		StrategyID:      strategyID,
		SessionID:       sessionID,
		Environment:     2,
		Exchange:        1,
		Market:          2,
		PositionSide:    0,
		OrderType:       1,
		Symbol:          "ETHUSDT",
		Side:            "BUY",
		RequestedQty:    0.5,
		RequestedPrice:  3000,
		PostOnly:        true,
		GoodTillDate:    &attemptGoodTillDate,
		ReduceOnly:      true,
		MarkPrice:       3010,
		Status:          "PENDING",
		ClientOrderID:   clientOrderID,
		RiskStatus:      "APPROVED",
		RiskReasonsJSON: `["balance_ok","leverage_ok"]`,
	}
	if err := repo.CreateOrderAttempt(ctx, attempt); err != nil {
		t.Fatalf("CreateOrderAttempt: %v", err)
	}

	attempt.Status = "ACCEPTED"
	attempt.OrderID = orderID
	attempt.ExchangeOrderID = exchangeOrderID
	order := &Order{
		OrderID:            orderID,
		ExchangeOrderID:    exchangeOrderID,
		ClientOrderID:      clientOrderID,
		AttemptID:          attemptID,
		IntentID:           intentID,
		Time:               baseTime.Add(2 * time.Second),
		AccountID:          accountID,
		VenueID:            venueID,
		UserID:             userID,
		StrategyID:         strategyID,
		SessionID:          sessionID,
		Environment:        2,
		Exchange:           1,
		Market:             2,
		PositionSide:       0,
		Symbol:             "ETHUSDT",
		Side:               "BUY",
		OrigQty:            0.5,
		ExecutedQty:        0.25,
		RemainingQty:       0.25,
		AvgPrice:           3012,
		Price:              3010,
		PostOnly:           true,
		GoodTillDate:       &orderGoodTillDate,
		ReduceOnly:         true,
		Status:             "PARTIALLY_FILLED",
		RecoveryStatus:     "FILL_PENDING",
		RecoveryStartedAt:  &recoveryStartedAt,
		NextCheckAt:        &nextCheckAt,
		RecoveryDeadlineAt: &recoveryDeadlineAt,
		LastRecoveryError:  "fee pending",
		ForceClosedAt:      &forceClosedAt,
	}
	if err := repo.FinalizeOrderAttempt(ctx, attempt, order, nil); err != nil {
		t.Fatalf("FinalizeOrderAttempt: %v", err)
	}

	intents, total, err := repo.QueryOrderIntentsPaginated(ctx, userID, accountID, 0, sessionID, 10, 0)
	if err != nil {
		t.Fatalf("QueryOrderIntentsPaginated: %v", err)
	}
	if total != 1 || len(intents) != 1 {
		t.Fatalf("intents total=%d len=%d, want 1/1", total, len(intents))
	}
	assertOrderSemanticFields(t, "intent", intents[0].PostOnly, intents[0].GoodTillDate, intents[0].ReduceOnly, intentGoodTillDate)

	gotAttempt, err := repo.FindOrderAttempt(ctx, userID, accountID, "", attemptID, "")
	if err != nil {
		t.Fatalf("FindOrderAttempt: %v", err)
	}
	assertAttemptRiskFields(t, "find attempt", gotAttempt, attemptGoodTillDate, attempt.RiskReasonsJSON)

	attempts, total, err := repo.QueryOrderAttemptsPaginated(ctx, userID, accountID, 0, sessionID, intentID, 10, 0)
	if err != nil {
		t.Fatalf("QueryOrderAttemptsPaginated: %v", err)
	}
	if total != 1 || len(attempts) != 1 {
		t.Fatalf("attempts total=%d len=%d, want 1/1", total, len(attempts))
	}
	assertAttemptRiskFields(t, "query attempt", attempts[0], attemptGoodTillDate, attempt.RiskReasonsJSON)

	gotOrder, err := repo.FindOrderByAttempt(ctx, attemptID)
	if err != nil {
		t.Fatalf("FindOrderByAttempt: %v", err)
	}
	assertOrderRecoveryFields(t, "find order", gotOrder, *order)

	orders, total, err := repo.QueryOrdersPaginated(ctx, userID, accountID, 0, sessionID, intentID, attemptID, 10, 0)
	if err != nil {
		t.Fatalf("QueryOrdersPaginated: %v", err)
	}
	if total != 1 || len(orders) != 1 {
		t.Fatalf("orders total=%d len=%d, want 1/1", total, len(orders))
	}
	assertOrderRecoveryFields(t, "query order", orders[0], *order)
}

func TestOrderRepositoryPersistsRiskRejectedAttempt(t *testing.T) {
	repo, ctx := lifecycleTestRepo(t)

	seed := time.Now().UnixNano()
	accountID := int64(950000000000 + seed%100000000)
	userID := int64(960000000000 + seed%100000000)
	venueID := int64(970000000000 + seed%100000000)
	sessionID := fmt.Sprintf("risk-rejected-%d", seed)
	intentID := fmt.Sprintf("risk-intent-%d", seed)
	attemptID := fmt.Sprintf("risk-attempt-%d", seed)
	baseTime := time.Date(2026, 6, 7, 13, 0, 0, 0, time.UTC)

	intent := OrderIntent{
		IntentID:       intentID,
		Time:           baseTime,
		AccountID:      accountID,
		VenueID:        venueID,
		UserID:         userID,
		SessionID:      sessionID,
		Environment:    2,
		Exchange:       1,
		Market:         2,
		PositionSide:   0,
		OrderType:      1,
		Symbol:         "ETHUSDT",
		Side:           "BUY",
		RequestedQty:   0.5,
		RequestedPrice: 3000,
		Status:         "REQUESTED",
	}
	if err := repo.UpsertOrderIntent(ctx, intent); err != nil {
		t.Fatalf("UpsertOrderIntent: %v", err)
	}

	attempt := OrderAttempt{
		AttemptID:      attemptID,
		IntentID:       intentID,
		Time:           baseTime.Add(time.Second),
		AccountID:      accountID,
		VenueID:        venueID,
		UserID:         userID,
		SessionID:      sessionID,
		Environment:    2,
		Exchange:       1,
		Market:         2,
		PositionSide:   0,
		OrderType:      1,
		Symbol:         "ETHUSDT",
		Side:           "BUY",
		RequestedQty:   0.5,
		RequestedPrice: 3000,
		MarkPrice:      3000,
		Status:         "PENDING",
		ClientOrderID:  fmt.Sprintf("risk-client-%d", seed),
	}
	if err := repo.CreateOrderAttempt(ctx, attempt); err != nil {
		t.Fatalf("CreateOrderAttempt: %v", err)
	}

	intent.Status = "REJECTED"
	intent.RejectCode = "ROUTE_PENDING_EXECUTION"
	intent.RejectMessage = "route has pending execution"
	attempt.Status = "RISK_REJECTED"
	attempt.ErrorMessage = "ROUTE_PENDING_EXECUTION"
	attempt.RiskStatus = "REJECT"
	attempt.RiskReasonsJSON = `[{"code":"ROUTE_PENDING_EXECUTION","message":"route has pending execution"}]`
	if err := repo.FinalizeRiskRejectedAttempt(ctx, intent, attempt); err != nil {
		t.Fatalf("FinalizeRiskRejectedAttempt: %v", err)
	}

	intents, total, err := repo.QueryOrderIntentsPaginated(ctx, userID, accountID, 0, sessionID, 10, 0)
	if err != nil {
		t.Fatalf("QueryOrderIntentsPaginated: %v", err)
	}
	if total != 1 || len(intents) != 1 {
		t.Fatalf("intents total=%d len=%d, want 1/1", total, len(intents))
	}
	if intents[0].Status != "REJECTED" || intents[0].RejectCode != intent.RejectCode || intents[0].RejectMessage != intent.RejectMessage {
		t.Fatalf("intent reject fields = %+v", intents[0])
	}

	gotAttempt, err := repo.FindOrderAttempt(ctx, userID, accountID, "", attemptID, "")
	if err != nil {
		t.Fatalf("FindOrderAttempt: %v", err)
	}
	if gotAttempt.Status != "RISK_REJECTED" || gotAttempt.RiskStatus != "REJECT" {
		t.Fatalf("attempt risk fields = %+v", gotAttempt)
	}
	if !strings.Contains(gotAttempt.RiskReasonsJSON, "ROUTE_PENDING_EXECUTION") {
		t.Fatalf("risk_reasons_json = %s, want route pending code", gotAttempt.RiskReasonsJSON)
	}
}

func TestListOpenOrdersReturnsDueRecoveryFields(t *testing.T) {
	repo, ctx := lifecycleTestRepo(t)

	seed := time.Now().UnixNano()
	accountID := int64(950000000000 + seed%100000000)
	userID := int64(960000000000 + seed%100000000)
	venueID := int64(970000000000 + seed%100000000)
	strategyID := int64(980000000000 + seed%100000000)
	sessionID := fmt.Sprintf("open-recovery-%d", seed)
	intentID := fmt.Sprintf("intent-open-%d", seed)
	attemptID := fmt.Sprintf("attempt-open-%d", seed)
	orderID := fmt.Sprintf("order-open-%d", seed)
	exchangeOrderID := fmt.Sprintf("exchange-open-%d", seed)
	clientOrderID := fmt.Sprintf("client-open-%d", seed)
	now := time.Now().UTC().Truncate(time.Microsecond)
	recoveryStartedAt := now.Add(-time.Hour)
	nextCheckAt := now.Add(-time.Minute)
	recoveryDeadlineAt := now.Add(13 * 24 * time.Hour)

	intent := OrderIntent{
		IntentID:     intentID,
		Time:         now.Add(-2 * time.Hour),
		AccountID:    accountID,
		VenueID:      venueID,
		UserID:       userID,
		StrategyID:   strategyID,
		SessionID:    sessionID,
		Environment:  2,
		Exchange:     1,
		Market:       2,
		PositionSide: 0,
		OrderType:    1,
		Symbol:       "ETHUSDT",
		Side:         "BUY",
		RequestedQty: 0.5,
		Status:       "REQUESTED",
	}
	if err := repo.UpsertOrderIntent(ctx, intent); err != nil {
		t.Fatalf("UpsertOrderIntent: %v", err)
	}
	attempt := OrderAttempt{
		AttemptID:       attemptID,
		IntentID:        intentID,
		Time:            now.Add(-90 * time.Minute),
		AccountID:       accountID,
		VenueID:         venueID,
		UserID:          userID,
		StrategyID:      strategyID,
		SessionID:       sessionID,
		Environment:     2,
		Exchange:        1,
		Market:          2,
		PositionSide:    0,
		OrderType:       1,
		Symbol:          "ETHUSDT",
		Side:            "BUY",
		RequestedQty:    0.5,
		Status:          "PENDING",
		ClientOrderID:   clientOrderID,
		OrderID:         orderID,
		ExchangeOrderID: exchangeOrderID,
	}
	if err := repo.CreateOrderAttempt(ctx, attempt); err != nil {
		t.Fatalf("CreateOrderAttempt: %v", err)
	}
	attempt.Status = "ACCEPTED"
	order := &Order{
		OrderID:            orderID,
		ExchangeOrderID:    exchangeOrderID,
		ClientOrderID:      clientOrderID,
		AttemptID:          attemptID,
		IntentID:           intentID,
		Time:               now.Add(-time.Hour),
		AccountID:          accountID,
		VenueID:            venueID,
		UserID:             userID,
		StrategyID:         strategyID,
		SessionID:          sessionID,
		Environment:        2,
		Exchange:           1,
		Market:             2,
		PositionSide:       0,
		Symbol:             "ETHUSDT",
		Side:               "BUY",
		OrigQty:            0.5,
		ExecutedQty:        0.2,
		RemainingQty:       0.3,
		Status:             "PARTIALLY_FILLED",
		RecoveryStatus:     "PARTIALLY_FILLED",
		RecoveryStartedAt:  &recoveryStartedAt,
		NextCheckAt:        &nextCheckAt,
		RecoveryDeadlineAt: &recoveryDeadlineAt,
		LastRecoveryError:  "fee pending",
	}
	if err := repo.FinalizeOrderAttempt(ctx, attempt, order, nil); err != nil {
		t.Fatalf("FinalizeOrderAttempt: %v", err)
	}

	orders, err := repo.ListDueOpenOrders(ctx, 500)
	if err != nil {
		t.Fatalf("ListDueOpenOrders: %v", err)
	}
	found := false
	for _, got := range orders {
		if got.OrderID != orderID {
			continue
		}
		found = true
		if got.RecoveryStatus != "PARTIALLY_FILLED" || got.LastRecoveryError != "fee pending" {
			t.Fatalf("open order recovery fields = %+v", got)
		}
		if !got.RecoveryStartedAt.Equal(recoveryStartedAt) || !got.NextCheckAt.Equal(nextCheckAt) || !got.RecoveryDeadlineAt.Equal(recoveryDeadlineAt) {
			t.Fatalf("open order recovery times = %+v", got)
		}
	}
	if !found {
		t.Fatalf("ListDueOpenOrders did not include due recovery order %s", orderID)
	}

	forceClosedAt := now.Add(time.Minute)
	if err := repo.MarkRecoveryExpired(ctx, orderID, forceClosedAt, "deadline exceeded"); err != nil {
		t.Fatalf("MarkRecoveryExpired: %v", err)
	}
	orders, err = repo.ListDueOpenOrders(ctx, 500)
	if err != nil {
		t.Fatalf("ListDueOpenOrders after mark expired: %v", err)
	}
	for _, got := range orders {
		if got.OrderID == orderID {
			t.Fatalf("expired recovery order should not remain due: %+v", got)
		}
	}
}

func TestResolveOpenOrderByExchangeRef(t *testing.T) {
	repo, ctx := lifecycleTestRepo(t)

	seed := time.Now().UnixNano()
	accountID := int64(952000000000 + seed%100000000)
	userID := int64(962000000000 + seed%100000000)
	venueID := int64(972000000000 + seed%100000000)
	strategyID := int64(982000000000 + seed%100000000)
	sessionID := fmt.Sprintf("resolve-open-%d", seed)
	intentID := fmt.Sprintf("intent-resolve-%d", seed)
	attemptID := fmt.Sprintf("attempt-resolve-%d", seed)
	orderID := fmt.Sprintf("order-resolve-%d", seed)
	exchangeOrderID := fmt.Sprintf("exchange-resolve-%d", seed)
	clientOrderID := fmt.Sprintf("client-resolve-%d", seed)
	now := time.Now().UTC().Truncate(time.Microsecond)
	recoveryStartedAt := now.Add(-time.Hour)
	nextCheckAt := now.Add(time.Minute)
	recoveryDeadlineAt := now.Add(14 * 24 * time.Hour)

	if err := repo.UpsertOrderIntent(ctx, OrderIntent{
		IntentID:     intentID,
		Time:         now.Add(-2 * time.Hour),
		AccountID:    accountID,
		VenueID:      venueID,
		UserID:       userID,
		StrategyID:   strategyID,
		SessionID:    sessionID,
		Environment:  2,
		Exchange:     1,
		Market:       2,
		PositionSide: 1,
		OrderType:    2,
		Symbol:       "ETHUSDT",
		Side:         "BUY",
		RequestedQty: 0.5,
		Status:       "REQUESTED",
	}); err != nil {
		t.Fatalf("UpsertOrderIntent: %v", err)
	}
	attempt := OrderAttempt{
		AttemptID:       attemptID,
		IntentID:        intentID,
		Time:            now.Add(-90 * time.Minute),
		AccountID:       accountID,
		VenueID:         venueID,
		UserID:          userID,
		StrategyID:      strategyID,
		SessionID:       sessionID,
		Environment:     2,
		Exchange:        1,
		Market:          2,
		PositionSide:    1,
		OrderType:       2,
		Symbol:          "ETHUSDT",
		Side:            "BUY",
		RequestedQty:    0.5,
		Status:          "PENDING",
		ClientOrderID:   clientOrderID,
		OrderID:         orderID,
		ExchangeOrderID: exchangeOrderID,
	}
	if err := repo.CreateOrderAttempt(ctx, attempt); err != nil {
		t.Fatalf("CreateOrderAttempt: %v", err)
	}
	attempt.Status = "ACCEPTED"
	if err := repo.FinalizeOrderAttempt(ctx, attempt, &Order{
		OrderID:            orderID,
		ExchangeOrderID:    exchangeOrderID,
		ClientOrderID:      clientOrderID,
		AttemptID:          attemptID,
		IntentID:           intentID,
		Time:               now.Add(-time.Hour),
		AccountID:          accountID,
		VenueID:            venueID,
		UserID:             userID,
		StrategyID:         strategyID,
		SessionID:          sessionID,
		Environment:        2,
		Exchange:           1,
		Market:             2,
		PositionSide:       1,
		Symbol:             "ETHUSDT",
		Side:               "BUY",
		OrigQty:            0.5,
		ExecutedQty:        0.2,
		RemainingQty:       0.3,
		Status:             "PARTIALLY_FILLED",
		RecoveryStatus:     "PARTIALLY_FILLED",
		RecoveryStartedAt:  &recoveryStartedAt,
		NextCheckAt:        &nextCheckAt,
		RecoveryDeadlineAt: &recoveryDeadlineAt,
	}, nil); err != nil {
		t.Fatalf("FinalizeOrderAttempt: %v", err)
	}

	byExchange, err := repo.ResolveOpenOrderByExchangeRef(ctx, venueID, exchangeOrderID, "")
	if err != nil {
		t.Fatalf("ResolveOpenOrderByExchangeRef by exchange id: %v", err)
	}
	if byExchange.OrderID != orderID || byExchange.ClientOrderID != clientOrderID || byExchange.PositionSide != 1 {
		t.Fatalf("unexpected exchange lookup result: %+v", byExchange)
	}
	byClient, err := repo.ResolveOpenOrderByExchangeRef(ctx, venueID, "", clientOrderID)
	if err != nil {
		t.Fatalf("ResolveOpenOrderByExchangeRef by client id: %v", err)
	}
	if byClient.OrderID != orderID || byClient.ExchangeOrderID != exchangeOrderID {
		t.Fatalf("unexpected client lookup result: %+v", byClient)
	}
	_, err = repo.ResolveOpenOrderByExchangeRef(ctx, venueID, "missing", "")
	if !errors.Is(err, lifecycle.ErrOpenOrderNotFound) {
		t.Fatalf("missing lookup err = %v, want ErrOpenOrderNotFound", err)
	}
}

func TestMarkRecoveryResolvedClearsDueOpenOrder(t *testing.T) {
	repo, ctx := lifecycleTestRepo(t)

	seed := time.Now().UnixNano()
	accountID := int64(951000000000 + seed%100000000)
	userID := int64(961000000000 + seed%100000000)
	venueID := int64(971000000000 + seed%100000000)
	strategyID := int64(981000000000 + seed%100000000)
	sessionID := fmt.Sprintf("resolved-recovery-%d", seed)
	intentID := fmt.Sprintf("intent-resolved-%d", seed)
	attemptID := fmt.Sprintf("attempt-resolved-%d", seed)
	orderID := fmt.Sprintf("order-resolved-%d", seed)
	exchangeOrderID := fmt.Sprintf("exchange-resolved-%d", seed)
	clientOrderID := fmt.Sprintf("client-resolved-%d", seed)
	now := time.Now().UTC().Truncate(time.Microsecond)
	recoveryStartedAt := now.Add(-time.Hour)
	nextCheckAt := now.Add(-time.Minute)
	recoveryDeadlineAt := now.Add(13 * 24 * time.Hour)

	if err := repo.UpsertOrderIntent(ctx, OrderIntent{
		IntentID:     intentID,
		Time:         now.Add(-2 * time.Hour),
		AccountID:    accountID,
		VenueID:      venueID,
		UserID:       userID,
		StrategyID:   strategyID,
		SessionID:    sessionID,
		Environment:  2,
		Exchange:     1,
		Market:       2,
		PositionSide: 0,
		OrderType:    1,
		Symbol:       "ETHUSDT",
		Side:         "BUY",
		RequestedQty: 0.5,
		Status:       "REQUESTED",
	}); err != nil {
		t.Fatalf("UpsertOrderIntent: %v", err)
	}
	attempt := OrderAttempt{
		AttemptID:       attemptID,
		IntentID:        intentID,
		Time:            now.Add(-90 * time.Minute),
		AccountID:       accountID,
		VenueID:         venueID,
		UserID:          userID,
		StrategyID:      strategyID,
		SessionID:       sessionID,
		Environment:     2,
		Exchange:        1,
		Market:          2,
		PositionSide:    0,
		OrderType:       1,
		Symbol:          "ETHUSDT",
		Side:            "BUY",
		RequestedQty:    0.5,
		Status:          "PENDING",
		ClientOrderID:   clientOrderID,
		OrderID:         orderID,
		ExchangeOrderID: exchangeOrderID,
	}
	if err := repo.CreateOrderAttempt(ctx, attempt); err != nil {
		t.Fatalf("CreateOrderAttempt: %v", err)
	}
	attempt.Status = "ACCEPTED"
	if err := repo.FinalizeOrderAttempt(ctx, attempt, &Order{
		OrderID:            orderID,
		ExchangeOrderID:    exchangeOrderID,
		ClientOrderID:      clientOrderID,
		AttemptID:          attemptID,
		IntentID:           intentID,
		Time:               now.Add(-time.Hour),
		AccountID:          accountID,
		VenueID:            venueID,
		UserID:             userID,
		StrategyID:         strategyID,
		SessionID:          sessionID,
		Environment:        2,
		Exchange:           1,
		Market:             2,
		PositionSide:       0,
		Symbol:             "ETHUSDT",
		Side:               "BUY",
		OrigQty:            0.5,
		ExecutedQty:        0.2,
		RemainingQty:       0.3,
		Status:             "PARTIALLY_FILLED",
		RecoveryStatus:     "PARTIALLY_FILLED",
		RecoveryStartedAt:  &recoveryStartedAt,
		NextCheckAt:        &nextCheckAt,
		RecoveryDeadlineAt: &recoveryDeadlineAt,
		LastRecoveryError:  "fee pending",
	}, nil); err != nil {
		t.Fatalf("FinalizeOrderAttempt: %v", err)
	}

	resolvedAt := now.Add(2 * time.Minute)
	if err := repo.MarkRecoveryResolved(ctx, orderID, lifecycle.OrderState{
		ExchangeOrderID: exchangeOrderID,
		ClientOrderID:   clientOrderID,
		Symbol:          "ETHUSDT",
		Status:          "FILLED",
		OrigQty:         0.5,
		ExecutedQty:     0.5,
		RemainingQty:    0,
		AvgPrice:        3006,
		UpdatedAt:       resolvedAt,
	}, resolvedAt); err != nil {
		t.Fatalf("MarkRecoveryResolved: %v", err)
	}

	orders, err := repo.ListDueOpenOrders(ctx, 500)
	if err != nil {
		t.Fatalf("ListDueOpenOrders after resolved: %v", err)
	}
	for _, got := range orders {
		if got.OrderID == orderID {
			t.Fatalf("resolved recovery order should not remain due: %+v", got)
		}
	}

	got, err := repo.FindOrderByAttempt(ctx, attemptID)
	if err != nil {
		t.Fatalf("FindOrderByAttempt: %v", err)
	}
	if got.Status != "FILLED" || got.RecoveryStatus != "FILLED" || got.NextCheckAt != nil || got.LastRecoveryError != "" {
		t.Fatalf("resolved order fields = %+v, want FILLED recovery with no next check/error", got)
	}
	if got.OrigQty != 0.5 || got.ExecutedQty != 0.5 || got.RemainingQty != 0 || got.AvgPrice != 3006 {
		t.Fatalf("resolved order quantities = %+v", got)
	}
}

func assertAttemptRiskFields(t *testing.T, label string, got OrderAttempt, wantGoodTillDate time.Time, wantRiskReasons string) {
	t.Helper()
	assertOrderSemanticFields(t, label, got.PostOnly, got.GoodTillDate, got.ReduceOnly, wantGoodTillDate)
	if got.RiskStatus != "APPROVED" {
		t.Fatalf("%s risk_status = %q, want APPROVED", label, got.RiskStatus)
	}
	assertJSONStringArray(t, label+" risk_reasons_json", got.RiskReasonsJSON, wantRiskReasons)
}

func assertOrderRecoveryFields(t *testing.T, label string, got, want Order) {
	t.Helper()
	assertOrderSemanticFields(t, label, got.PostOnly, got.GoodTillDate, got.ReduceOnly, *want.GoodTillDate)
	if got.RecoveryStatus != want.RecoveryStatus {
		t.Fatalf("%s recovery_status = %q, want %q", label, got.RecoveryStatus, want.RecoveryStatus)
	}
	assertTimePtrEqual(t, label+" recovery_started_at", got.RecoveryStartedAt, want.RecoveryStartedAt)
	assertTimePtrEqual(t, label+" next_check_at", got.NextCheckAt, want.NextCheckAt)
	assertTimePtrEqual(t, label+" recovery_deadline_at", got.RecoveryDeadlineAt, want.RecoveryDeadlineAt)
	if got.LastRecoveryError != want.LastRecoveryError {
		t.Fatalf("%s last_recovery_error = %q, want %q", label, got.LastRecoveryError, want.LastRecoveryError)
	}
	assertTimePtrEqual(t, label+" force_closed_at", got.ForceClosedAt, want.ForceClosedAt)
}

func assertOrderSemanticFields(t *testing.T, label string, gotPostOnly bool, gotGoodTillDate *time.Time, gotReduceOnly bool, wantGoodTillDate time.Time) {
	t.Helper()
	if !gotPostOnly {
		t.Fatalf("%s post_only = false, want true", label)
	}
	assertTimePtrEqual(t, label+" good_till_date", gotGoodTillDate, &wantGoodTillDate)
	if !gotReduceOnly {
		t.Fatalf("%s reduce_only = false, want true", label)
	}
}

func assertTimePtrEqual(t *testing.T, label string, got, want *time.Time) {
	t.Helper()
	if got == nil || want == nil {
		if got != want {
			t.Fatalf("%s = %v, want %v", label, got, want)
		}
		return
	}
	if !got.Equal(*want) {
		t.Fatalf("%s = %s, want %s", label, got.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
	}
}

func assertJSONStringArray(t *testing.T, label string, got, want string) {
	t.Helper()
	var gotItems []string
	if err := json.Unmarshal([]byte(got), &gotItems); err != nil {
		t.Fatalf("%s got invalid JSON %q: %v", label, got, err)
	}
	var wantItems []string
	if err := json.Unmarshal([]byte(want), &wantItems); err != nil {
		t.Fatalf("%s want invalid JSON %q: %v", label, want, err)
	}
	if !reflect.DeepEqual(gotItems, wantItems) {
		t.Fatalf("%s = %v, want %v", label, gotItems, wantItems)
	}
}
