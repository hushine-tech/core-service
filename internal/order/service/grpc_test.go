package service

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/gen/orderv1"
	"github.com/hushine-tech/core-service/internal/domain"
	exchangeadapter "github.com/hushine-tech/core-service/internal/exchange/adapter"
	exchangebinance "github.com/hushine-tech/core-service/internal/exchange/binance"
	"github.com/hushine-tech/core-service/internal/order/accountmeta"
	"github.com/hushine-tech/core-service/internal/order/executor"
	"github.com/hushine-tech/core-service/internal/order/lifecycle"
	"github.com/hushine-tech/core-service/internal/order/repository"
	orderrisk "github.com/hushine-tech/core-service/internal/order/risk"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type stubMetaGetter struct {
	meta              accountmeta.Meta
	err               error
	validateErr       error
	validatedSessions []string
}

func (s *stubMetaGetter) Get(_ context.Context, _ int64, _ int32, _ int32) (accountmeta.Meta, error) {
	return s.meta, s.err
}

func (s *stubMetaGetter) ValidateActiveSession(_ context.Context, _ accountmeta.Meta, _ int64, sessionID string) error {
	s.validatedSessions = append(s.validatedSessions, sessionID)
	return s.validateErr
}

type stubRouterExec struct {
	result        executor.OrderResult
	err           error
	resolveResult executor.OrderResult
	resolveErr    error
	executeCalls  int
	resolveCalls  int
	lastReq       executor.OrderRequest
}

func (r *stubRouterExec) Execute(_ context.Context, req executor.OrderRequest, _ accountmeta.Meta) (executor.OrderResult, error) {
	r.executeCalls++
	r.lastReq = req
	return r.result, r.err
}

func (r *stubRouterExec) Resolve(_ context.Context, _ executor.RecoveryRequest, _ accountmeta.Meta) (executor.OrderResult, error) {
	r.resolveCalls++
	return r.resolveResult, r.resolveErr
}

type stubRiskGate struct {
	decision orderrisk.Decision
	err      error
	calls    int
	lastReq  orderrisk.ReviewRequest
}

func (g *stubRiskGate) Review(_ context.Context, req orderrisk.ReviewRequest) (orderrisk.Decision, error) {
	g.calls++
	g.lastReq = req
	return g.decision, g.err
}

type stubRepo struct {
	intents     []repository.OrderIntent
	attempts    []repository.OrderAttempt
	orders      []repository.Order
	fills       []repository.OrderFill
	events      []lifecycle.Event
	finalizeErr error
}

func (s *stubRepo) UpsertOrderIntent(_ context.Context, intent repository.OrderIntent) error {
	for i := range s.intents {
		if s.intents[i].IntentID == intent.IntentID {
			s.intents[i] = intent
			return nil
		}
	}
	s.intents = append(s.intents, intent)
	return nil
}

func (s *stubRepo) CreateOrderAttempt(_ context.Context, attempt repository.OrderAttempt) error {
	s.attempts = append(s.attempts, attempt)
	return nil
}

func (s *stubRepo) FinalizeOrderAttempt(_ context.Context, attempt repository.OrderAttempt, order *repository.Order, fills []repository.OrderFill) error {
	if s.finalizeErr != nil {
		return s.finalizeErr
	}
	for i := range s.attempts {
		if s.attempts[i].AttemptID == attempt.AttemptID {
			s.attempts[i] = attempt
			break
		}
	}
	if order != nil {
		s.orders = append(s.orders, *order)
	}
	s.fills = append(s.fills, fills...)
	return nil
}

func (s *stubRepo) FinalizeRiskRejectedAttempt(ctx context.Context, intent repository.OrderIntent, attempt repository.OrderAttempt) error {
	if err := s.UpsertOrderIntent(ctx, intent); err != nil {
		return err
	}
	return s.FinalizeOrderAttempt(ctx, attempt, nil, nil)
}

func paginate[T any](items []T, limit, offset int) ([]T, int64) {
	total := int64(len(items))
	if offset >= len(items) {
		return nil, total
	}
	end := len(items)
	if limit > 0 && offset+limit < end {
		end = offset + limit
	}
	return items[offset:end], total
}

func (s *stubRepo) QueryOrderIntentsPaginated(_ context.Context, _ int64, _ int64, _ int64, _ string, limit, offset int) ([]repository.OrderIntent, int64, error) {
	items, total := paginate(s.intents, limit, offset)
	return items, total, nil
}

func (s *stubRepo) QueryOrderAttemptsPaginated(_ context.Context, _ int64, _ int64, _ int64, _ string, intentID string, limit, offset int) ([]repository.OrderAttempt, int64, error) {
	filtered := s.attempts
	if intentID != "" {
		filtered = filtered[:0:0]
		for _, a := range s.attempts {
			if a.IntentID == intentID {
				filtered = append(filtered, a)
			}
		}
	}
	items, total := paginate(filtered, limit, offset)
	return items, total, nil
}

func (s *stubRepo) QueryOrdersPaginated(_ context.Context, _ int64, _ int64, _ int64, _ string, intentID, attemptID string, limit, offset int) ([]repository.Order, int64, error) {
	filtered := s.orders
	if intentID != "" || attemptID != "" {
		filtered = filtered[:0:0]
		for _, o := range s.orders {
			if intentID != "" && o.IntentID != intentID {
				continue
			}
			if attemptID != "" && o.AttemptID != attemptID {
				continue
			}
			filtered = append(filtered, o)
		}
	}
	items, total := paginate(filtered, limit, offset)
	return items, total, nil
}

func (s *stubRepo) QueryOrderFillsPaginated(_ context.Context, _ int64, _ int64, _ int64, _ string, intentID, attemptID, orderID string, limit, offset int) ([]repository.OrderFill, int64, error) {
	filtered := s.fills
	if intentID != "" || attemptID != "" || orderID != "" {
		filtered = filtered[:0:0]
		for _, f := range s.fills {
			if intentID != "" && f.IntentID != intentID {
				continue
			}
			if attemptID != "" && f.AttemptID != attemptID {
				continue
			}
			if orderID != "" && f.OrderID != orderID {
				continue
			}
			filtered = append(filtered, f)
		}
	}
	items, total := paginate(filtered, limit, offset)
	return items, total, nil
}

func (s *stubRepo) FindOrderAttempt(_ context.Context, _ int64, _ int64, intentID, attemptID, clientOrderID string) (repository.OrderAttempt, error) {
	for _, attempt := range s.attempts {
		if attemptID != "" && attempt.AttemptID == attemptID {
			return attempt, nil
		}
		if clientOrderID != "" && attempt.ClientOrderID == clientOrderID {
			return attempt, nil
		}
		if intentID != "" && attempt.IntentID == intentID {
			return attempt, nil
		}
	}
	return repository.OrderAttempt{}, repository.ErrNotFound
}

func (s *stubRepo) FindOrderByAttempt(_ context.Context, attemptID string) (repository.Order, error) {
	for _, item := range s.orders {
		if item.AttemptID == attemptID {
			return item, nil
		}
	}
	return repository.Order{}, repository.ErrNotFound
}

func (s *stubRepo) ListOrderFillsByAttempt(_ context.Context, attemptID string) ([]repository.OrderFill, error) {
	out := make([]repository.OrderFill, 0)
	for _, item := range s.fills {
		if item.AttemptID == attemptID {
			out = append(out, item)
		}
	}
	return out, nil
}

func (s *stubRepo) ListOpenOrders(_ context.Context, _ int) ([]lifecycle.OpenOrder, error) {
	return nil, nil
}

func (s *stubRepo) ListDueOpenOrders(_ context.Context, _ int) ([]lifecycle.OpenOrder, error) {
	return nil, nil
}

func (s *stubRepo) MarkRecoveryExpired(_ context.Context, _ string, _ time.Time, _ string) error {
	return nil
}

func (s *stubRepo) SaveLifecycleEvent(_ context.Context, event lifecycle.Event) (lifecycle.Event, error) {
	event.EventID = int64(len(s.events) + 1)
	s.events = append(s.events, event)
	return event, nil
}

func (s *stubRepo) ListLifecycleEvents(_ context.Context, sessionID string, afterEventID int64, limit int) ([]lifecycle.Event, error) {
	out := make([]lifecycle.Event, 0)
	for _, event := range s.events {
		if event.SessionID != sessionID || event.EventID <= afterEventID {
			continue
		}
		out = append(out, event)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func newTestSvc(meta accountmeta.Meta, metaErr error, result executor.OrderResult, execErr error) (*OrderGRPCService, *stubRepo) {
	repo := &stubRepo{}
	if meta.Environment == 0 && meta.Exchange == 0 && meta.Market == 0 {
		meta = testOrderMeta(environmentBacktest)
	}
	svc := NewOrderGRPCService(&stubMetaGetter{meta: meta, err: metaErr}, &stubRouterExec{result: result, err: execErr}, repo)
	return svc, repo
}

func testOrderMeta(environment int32) accountmeta.Meta {
	return accountmeta.Meta{
		AccountID:      1,
		VenueID:        10,
		UserID:         77,
		Environment:    environment,
		Exchange:       exchangeBinance,
		Market:         marketPerpetualFutures,
		PositionMode:   "one_way",
		DefaultFeeRate: 0.0004,
		SlippageBps:    2.5,
	}
}

func testPlaceOrderRequest() *orderv1.PlaceOrderRequest {
	return &orderv1.PlaceOrderRequest{
		AccountId:    1,
		Exchange:     exchangeBinance,
		Market:       marketPerpetualFutures,
		PositionSide: positionSideBoth,
		Symbol:       "ETHUSDT",
		Side:         "BUY",
		Qty:          1,
		MarkPrice:    2500,
	}
}

func TestPlaceOrder_validationErrors(t *testing.T) {
	svc, _ := newTestSvc(testOrderMeta(environmentBacktest), nil, executor.OrderResult{}, nil)

	cases := []struct {
		req  *orderv1.PlaceOrderRequest
		code codes.Code
	}{
		{&orderv1.PlaceOrderRequest{AccountId: 0, Symbol: "BTCUSDT", Qty: 1}, codes.InvalidArgument},
		{&orderv1.PlaceOrderRequest{AccountId: 1, Symbol: "", Qty: 1}, codes.InvalidArgument},
		{&orderv1.PlaceOrderRequest{AccountId: 1, Symbol: "BTCUSDT", Qty: 0}, codes.InvalidArgument},
		{&orderv1.PlaceOrderRequest{AccountId: 1, Exchange: exchangeBinance, Market: marketPerpetualFutures, PositionSide: positionSideBoth, Symbol: "BTCUSDT", Side: "LONG", Qty: 1}, codes.InvalidArgument},
	}
	for _, tc := range cases {
		_, err := svc.PlaceOrder(context.Background(), tc.req)
		if err == nil {
			t.Errorf("expected error for %+v", tc.req)
			continue
		}
		if s, _ := status.FromError(err); s.Code() != tc.code {
			t.Errorf("want %v, got %v", tc.code, s.Code())
		}
	}
}

func TestMarketOrderRejectsPrice(t *testing.T) {
	svc, _ := newTestSvc(testOrderMeta(environmentBacktest), nil, executor.OrderResult{}, nil)
	req := testPlaceOrderRequest()
	req.OrderType = "MARKET"
	price := 2500.0
	req.Price = &price

	_, err := svc.PlaceOrder(context.Background(), req)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument (err=%v)", status.Code(err), err)
	}
}

func TestLimitOrderRequiresPrice(t *testing.T) {
	svc, _ := newTestSvc(testOrderMeta(environmentBacktest), nil, executor.OrderResult{}, nil)
	req := testPlaceOrderRequest()
	req.OrderType = "LIMIT"
	req.Price = nil

	_, err := svc.PlaceOrder(context.Background(), req)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument (err=%v)", status.Code(err), err)
	}
}

func TestLimitOrderDefaultsGTC(t *testing.T) {
	meta := testOrderMeta(environmentBacktest)
	router := &stubRouterExec{result: executor.OrderResult{
		ExchangeOrderID: "limit-order-1",
		Symbol:          "ETHUSDT",
		Side:            "BUY",
		Status:          "NEW",
		OrigQty:         1,
		ExecutedQty:     0,
		RemainingQty:    1,
	}}
	repo := &stubRepo{}
	svc := NewOrderGRPCService(&stubMetaGetter{meta: meta}, router, repo)

	req := testPlaceOrderRequest()
	price := 2499.0
	req.Price = &price
	req.OrderType = "LIMIT"
	req.TimeInForce = ""
	resp, err := svc.PlaceOrder(context.Background(), req)
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if resp.GetAttemptStatus() != attemptStatusAccepted {
		t.Fatalf("attempt status = %s, want %s", resp.GetAttemptStatus(), attemptStatusAccepted)
	}
	if router.lastReq.OrderType != "LIMIT" || router.lastReq.TimeInForce != "GTC" {
		t.Fatalf("order contract = %s/%s, want LIMIT/GTC", router.lastReq.OrderType, router.lastReq.TimeInForce)
	}
	if len(repo.intents) != 1 || repo.intents[0].OrderType != orderTypeLimit {
		t.Fatalf("persisted intent order_type = %+v, want LIMIT", repo.intents)
	}
}

func TestPlaceOrder_PassesAdvancedOrderContractToExecutor(t *testing.T) {
	meta := testOrderMeta(environmentBacktest)
	router := &stubRouterExec{result: executor.OrderResult{
		ExchangeOrderID: "gtd-order-1",
		Symbol:          "ETHUSDT",
		Side:            "BUY",
		Status:          "NEW",
		OrigQty:         1,
		ExecutedQty:     0,
		RemainingQty:    1,
	}}
	repo := &stubRepo{}
	svc := NewOrderGRPCService(&stubMetaGetter{meta: meta}, router, repo)

	req := testPlaceOrderRequest()
	price := 2499.0
	req.Price = &price
	req.OrderType = "LIMIT"
	req.TimeInForce = "GTD"
	req.PostOnly = false
	req.ReduceOnly = true
	req.GoodTillDate = timestamppb.New(time.Unix(1893456000, 0).UTC())

	if _, err := svc.PlaceOrder(context.Background(), req); err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if !router.lastReq.ReduceOnly {
		t.Fatalf("reduce_only was not forwarded")
	}
	if router.lastReq.GoodTillDate == nil || router.lastReq.GoodTillDate.Unix() != 1893456000 {
		t.Fatalf("good_till_date = %v, want 1893456000", router.lastReq.GoodTillDate)
	}
	if len(repo.intents) != 1 || !repo.intents[0].ReduceOnly || repo.intents[0].GoodTillDate == nil {
		t.Fatalf("intent advanced fields = %+v, want reduce_only/good_till_date persisted", repo.intents)
	}
	if len(repo.attempts) != 1 || !repo.attempts[0].ReduceOnly || repo.attempts[0].GoodTillDate == nil {
		t.Fatalf("attempt advanced fields = %+v, want reduce_only/good_till_date persisted", repo.attempts)
	}
}

func TestPlaceOrder_RiskRejectFinalizesAttemptBeforeExecuting(t *testing.T) {
	meta := testOrderMeta(environmentDemo)
	router := &stubRouterExec{result: executor.OrderResult{
		ExchangeOrderID: "should-not-execute",
		Symbol:          "ETHUSDT",
		Status:          "FILLED",
		OrigQty:         1,
		ExecutedQty:     1,
	}}
	repo := &stubRepo{}
	gate := &stubRiskGate{decision: orderrisk.Decision{
		Status:     orderrisk.DecisionReject,
		ReasonCode: "ROUTE_PENDING_EXECUTION",
		Violations: []orderrisk.Violation{{
			Code:    "ROUTE_PENDING_EXECUTION",
			Message: "route has pending execution",
		}},
		ReviewedAt: time.Unix(1893456000, 0).UTC(),
	}}
	svc := NewOrderGRPCService(&stubMetaGetter{meta: meta}, router, repo)
	svc.SetRiskGate(gate)

	req := testPlaceOrderRequest()
	req.StrategyId = 9
	req.SessionId = "sess-risk-reject"
	resp, err := svc.PlaceOrder(context.Background(), req)
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if resp.GetAttemptStatus() != attemptStatusRiskRejected {
		t.Fatalf("attempt status = %s, want %s", resp.GetAttemptStatus(), attemptStatusRiskRejected)
	}
	if router.executeCalls != 0 {
		t.Fatalf("executor called %d time(s), want 0", router.executeCalls)
	}
	if gate.calls != 1 {
		t.Fatalf("risk gate calls = %d, want 1", gate.calls)
	}
	if gate.lastReq.Symbol != "ETHUSDT" || gate.lastReq.OrderType != "MARKET" {
		t.Fatalf("risk request = %+v, want normalized request", gate.lastReq)
	}
	if len(repo.attempts) != 1 {
		t.Fatalf("attempts = %d, want 1", len(repo.attempts))
	}
	if len(repo.intents) != 1 {
		t.Fatalf("intents = %d, want 1", len(repo.intents))
	}
	if repo.intents[0].Status != "REJECTED" {
		t.Fatalf("intent status = %q, want REJECTED", repo.intents[0].Status)
	}
	if repo.intents[0].RejectCode != "ROUTE_PENDING_EXECUTION" || !strings.Contains(repo.intents[0].RejectMessage, "route has pending execution") {
		t.Fatalf("intent reject fields = %+v, want risk reason", repo.intents[0])
	}
	attempt := repo.attempts[0]
	if attempt.Status != attemptStatusRiskRejected {
		t.Fatalf("persisted attempt status = %s, want %s", attempt.Status, attemptStatusRiskRejected)
	}
	if attempt.RiskStatus != string(orderrisk.DecisionReject) {
		t.Fatalf("risk_status = %q, want REJECT", attempt.RiskStatus)
	}
	if !strings.Contains(attempt.RiskReasonsJSON, "ROUTE_PENDING_EXECUTION") {
		t.Fatalf("risk_reasons_json = %s, want route pending code", attempt.RiskReasonsJSON)
	}
	if attempt.ErrorMessage != "ROUTE_PENDING_EXECUTION" {
		t.Fatalf("error_message = %q, want reason code", attempt.ErrorMessage)
	}
}

func TestPlaceOrderBacktestLimitRemainsOpenWhenAdapterMarkDoesNotTouch(t *testing.T) {
	meta := testOrderMeta(environmentBacktest)
	registry := exchangeadapter.NewRegistry()
	route := exchangeadapter.Route{
		Exchange:    domain.ExchangeBinance,
		Environment: domain.EnvironmentBacktest,
		Market:      domain.MarketPerpetualFutures,
	}
	registry.Register(route, exchangebinance.NewBacktestFactory(route))
	repo := &stubRepo{}
	svc := NewOrderGRPCService(&stubMetaGetter{meta: meta}, executor.NewAdapterRouter(registry), repo)

	price := 19.885
	req := testPlaceOrderRequest()
	req.Price = &price
	req.OrderType = "LIMIT"
	req.TimeInForce = "GTC"
	req.MarkPrice = 1988.5
	req.Qty = 0.004
	req.StrategyId = 29
	req.SessionId = "sess-limit-open"

	resp, err := svc.PlaceOrder(context.Background(), req)
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if resp.GetAttemptStatus() != attemptStatusAccepted {
		t.Fatalf("attempt status = %s, want %s", resp.GetAttemptStatus(), attemptStatusAccepted)
	}
	order := resp.GetOrder()
	if order == nil {
		t.Fatal("response order is nil")
	}
	if order.GetStatus() != "NEW" || order.GetExecutedQty() != 0 || order.GetRemainingQty() != 0.004 {
		t.Fatalf("order = %+v, want open limit order", order)
	}
	if len(resp.GetFillDeltas()) != 0 || len(repo.fills) != 0 || len(repo.events) != 0 {
		t.Fatalf("unexpected fills/events: resp=%d repo_fills=%d events=%d", len(resp.GetFillDeltas()), len(repo.fills), len(repo.events))
	}
}

func TestUnsupportedTimeInForceFailsClosed(t *testing.T) {
	svc, _ := newTestSvc(testOrderMeta(environmentBacktest), nil, executor.OrderResult{}, nil)
	req := testPlaceOrderRequest()
	price := 2500.0
	req.Price = &price
	req.OrderType = "LIMIT"
	req.TimeInForce = "GTX"

	_, err := svc.PlaceOrder(context.Background(), req)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
}

func TestPlaceOrderRejectsTerminalSessionBeforePersistingOrExecuting(t *testing.T) {
	metaGetter := &stubMetaGetter{
		meta:        testOrderMeta(environmentDemo),
		validateErr: status.Error(codes.FailedPrecondition, "session is not active"),
	}
	router := &stubRouterExec{
		result: executor.OrderResult{
			ExchangeOrderID: "ex-order-1",
			Symbol:          "ETHUSDT",
			Side:            "BUY",
			Status:          "FILLED",
			OrigQty:         0.1,
			ExecutedQty:     0.1,
		},
	}
	repo := &stubRepo{}
	svc := NewOrderGRPCService(metaGetter, router, repo)

	req := testPlaceOrderRequest()
	req.Qty = 0.1
	req.StrategyId = 9
	req.SessionId = "sess-terminal"
	_, err := svc.PlaceOrder(context.Background(), req)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
	if len(metaGetter.validatedSessions) != 1 || metaGetter.validatedSessions[0] != "sess-terminal" {
		t.Fatalf("validated sessions = %+v, want sess-terminal", metaGetter.validatedSessions)
	}
	if router.executeCalls != 0 {
		t.Fatalf("executor called %d time(s), want 0", router.executeCalls)
	}
	if len(repo.intents) != 0 || len(repo.attempts) != 0 || len(repo.orders) != 0 || len(repo.fills) != 0 {
		t.Fatalf("order state persisted despite rejected session: intents=%d attempts=%d orders=%d fills=%d",
			len(repo.intents), len(repo.attempts), len(repo.orders), len(repo.fills))
	}
}

func TestPlaceOrderRejectsUnsupportedHedgeBeforePersistingOrExecuting(t *testing.T) {
	meta := testOrderMeta(environmentDemo)
	meta.PositionMode = "hedge"
	metaGetter := &stubMetaGetter{meta: meta}
	router := &stubRouterExec{}
	repo := &stubRepo{}
	svc := NewOrderGRPCService(metaGetter, router, repo)

	req := testPlaceOrderRequest()
	req.PositionSide = positionSideLong
	req.StrategyId = 9
	req.SessionId = "sess-hedge"

	_, err := svc.PlaceOrder(context.Background(), req)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
	if router.executeCalls != 0 {
		t.Fatalf("executor called %d time(s), want 0", router.executeCalls)
	}
	if len(repo.intents) != 0 || len(repo.attempts) != 0 || len(repo.orders) != 0 || len(repo.fills) != 0 {
		t.Fatalf("order state persisted despite unsupported hedge mode: intents=%d attempts=%d orders=%d fills=%d",
			len(repo.intents), len(repo.attempts), len(repo.orders), len(repo.fills))
	}
}

func TestPlaceOrder_filledOrderCreatesAttemptOrderAndFill(t *testing.T) {
	meta := testOrderMeta(environmentBacktest)
	result := executor.OrderResult{
		ExchangeOrderID: "ex-order-1",
		Symbol:          "BTCUSDT",
		Side:            "BUY",
		Status:          "FILLED",
		OrigQty:         0.1,
		ExecutedQty:     0.1,
		RemainingQty:    0,
		AvgPrice:        50025,
		Fills: []executor.FillResult{{
			Qty:       0.1,
			FillPrice: 50025,
			Fee:       2.001,
		}},
	}
	svc, repo := newTestSvc(meta, nil, result, nil)

	req := testPlaceOrderRequest()
	req.Symbol = "BTCUSDT"
	req.Qty = 0.1
	req.MarkPrice = 50000
	req.StrategyId = 9
	req.SessionId = "sess-1"
	req.IntentId = "intent-1"
	resp, err := svc.PlaceOrder(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetAttemptStatus() != "ACCEPTED" {
		t.Fatalf("attempt_status = %q, want ACCEPTED", resp.GetAttemptStatus())
	}
	if resp.GetIntentId() != "intent-1" {
		t.Fatalf("intent_id = %q, want intent-1", resp.GetIntentId())
	}
	if resp.GetOrder() == nil {
		t.Fatal("expected exchange order in response")
	}
	if got := len(resp.GetFillDeltas()); got != 1 {
		t.Fatalf("fill_deltas = %d, want 1", got)
	}
	if len(repo.intents) != 1 || len(repo.attempts) != 1 || len(repo.orders) != 1 || len(repo.fills) != 1 {
		t.Fatalf("persisted counts intents=%d attempts=%d orders=%d fills=%d", len(repo.intents), len(repo.attempts), len(repo.orders), len(repo.fills))
	}
	if repo.orders[0].ExchangeOrderID != "ex-order-1" {
		t.Fatalf("exchange_order_id = %q", repo.orders[0].ExchangeOrderID)
	}
	if repo.fills[0].Qty != 0.1 {
		t.Fatalf("fill qty = %.4f", repo.fills[0].Qty)
	}
}

func TestPlaceOrderBacktestUsesMarketTimeAsExecutionEventTime(t *testing.T) {
	meta := testOrderMeta(environmentBacktest)
	result := executor.OrderResult{
		ExchangeOrderID: "ex-order-1",
		Symbol:          "BTCUSDT",
		Side:            "BUY",
		Status:          "FILLED",
		OrigQty:         0.1,
		ExecutedQty:     0.1,
		AvgPrice:        50025,
		Fills: []executor.FillResult{{
			Qty:       0.1,
			FillPrice: 50025,
			Fee:       2.001,
		}},
	}
	svc, repo := newTestSvc(meta, nil, result, nil)
	marketTime := time.Date(2026, 6, 1, 0, 43, 0, 0, time.UTC)

	req := testPlaceOrderRequest()
	req.Symbol = "BTCUSDT"
	req.Qty = 0.1
	req.MarkPrice = 50000
	req.MarketTime = timestamppb.New(marketTime)

	if _, err := svc.PlaceOrder(context.Background(), req); err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if !repo.intents[0].Time.Equal(marketTime) {
		t.Fatalf("intent time = %s, want %s", repo.intents[0].Time, marketTime)
	}
	if !repo.orders[0].Time.Equal(marketTime) {
		t.Fatalf("order time = %s, want %s", repo.orders[0].Time, marketTime)
	}
	if !repo.fills[0].Time.Equal(marketTime) {
		t.Fatalf("fill time = %s, want %s", repo.fills[0].Time, marketTime)
	}
}

func TestPlaceOrder_fillPendingPersistsOrderWithoutSettleableFill(t *testing.T) {
	meta := testOrderMeta(environmentDemo)
	result := executor.OrderResult{
		ExchangeOrderID: "ex-order-missing-fee",
		Symbol:          "ETHUSDT",
		Side:            "BUY",
		Status:          "FILLED",
		OrigQty:         0.5,
		ExecutedQty:     0.5,
		RemainingQty:    0,
		AvgPrice:        2400,
		ErrorMessage:    "binance trade fee data not available after confirmed execution",
		FillPending:     true,
	}
	svc, repo := newTestSvc(meta, nil, result, nil)

	req := testPlaceOrderRequest()
	req.Qty = 0.5
	req.MarkPrice = 2400
	req.StrategyId = 9
	req.SessionId = "sess-1"
	req.IntentId = "intent-missing-fee"
	resp, err := svc.PlaceOrder(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetAttemptStatus() != attemptStatusRecovering {
		t.Fatalf("attempt_status = %q, want %s", resp.GetAttemptStatus(), attemptStatusRecovering)
	}
	if len(repo.orders) != 1 {
		t.Fatalf("persisted orders = %d, want 1", len(repo.orders))
	}
	if len(repo.fills) != 0 {
		t.Fatalf("persisted fills = %d, want 0", len(repo.fills))
	}
	if len(resp.GetFillDeltas()) != 0 {
		t.Fatalf("response fill_deltas = %d, want 0", len(resp.GetFillDeltas()))
	}
	if repo.orders[0].ErrorMessage == "" || repo.attempts[0].RecoveryError == "" {
		t.Fatalf("order/attempt should carry fill-pending observability, orders=%+v attempts=%+v", repo.orders, repo.attempts)
	}
	if repo.orders[0].RecoveryStatus != "FILL_PENDING" {
		t.Fatalf("recovery_status = %q, want FILL_PENDING", repo.orders[0].RecoveryStatus)
	}
	if repo.orders[0].RecoveryStartedAt == nil || repo.orders[0].NextCheckAt == nil || repo.orders[0].RecoveryDeadlineAt == nil {
		t.Fatalf("recoverable order should carry started/next/deadline timestamps: %+v", repo.orders[0])
	}
	wantDeadline := repo.orders[0].Time.Add(14 * 24 * time.Hour)
	if !repo.orders[0].RecoveryDeadlineAt.Equal(wantDeadline) {
		t.Fatalf("recovery_deadline_at = %s, want %s", repo.orders[0].RecoveryDeadlineAt, wantDeadline)
	}
}

func TestBuildPersistedExecutionSetsRecoveryDeadlineForPartialOrder(t *testing.T) {
	meta := testOrderMeta(environmentDemo)
	attempt := repository.OrderAttempt{
		AttemptID:       "attempt-partial",
		IntentID:        "intent-partial",
		AccountID:       1,
		VenueID:         meta.VenueID,
		UserID:          meta.UserID,
		StrategyID:      9,
		SessionID:       "sess-partial",
		Environment:     environmentDemo,
		Exchange:        int32(domain.ExchangeBinance),
		Market:          int32(domain.MarketPerpetualFutures),
		PositionSide:    0,
		Symbol:          "ETHUSDT",
		Side:            "BUY",
		RequestedQty:    0.5,
		ClientOrderID:   "client-partial",
		ExchangeOrderID: "ex-partial",
	}
	eventTime := time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)
	order, _ := buildPersistedExecution(meta, attempt, 9, "sess-partial", int32(domain.MarketPerpetualFutures), executor.OrderResult{
		ExchangeOrderID: "ex-partial",
		ClientOrderID:   "client-partial",
		Symbol:          "ETHUSDT",
		Side:            "BUY",
		Status:          "PARTIALLY_FILLED",
		OrigQty:         0.5,
		ExecutedQty:     0.2,
		RemainingQty:    0.3,
		AvgPrice:        3000,
	}, eventTime)

	if order.RecoveryStatus != "PARTIALLY_FILLED" {
		t.Fatalf("recovery_status = %q, want PARTIALLY_FILLED", order.RecoveryStatus)
	}
	if order.RecoveryStartedAt == nil || !order.RecoveryStartedAt.Equal(eventTime) {
		t.Fatalf("recovery_started_at = %v, want %s", order.RecoveryStartedAt, eventTime)
	}
	if order.NextCheckAt == nil || order.NextCheckAt.Before(eventTime) || !order.NextCheckAt.Before(eventTime.Add(time.Minute)) {
		t.Fatalf("next_check_at = %v, want shortly after %s", order.NextCheckAt, eventTime)
	}
	wantDeadline := eventTime.Add(14 * 24 * time.Hour)
	if order.RecoveryDeadlineAt == nil || !order.RecoveryDeadlineAt.Equal(wantDeadline) {
		t.Fatalf("recovery_deadline_at = %v, want %s", order.RecoveryDeadlineAt, wantDeadline)
	}
}

func TestPlaceOrder_failedMetaLookup(t *testing.T) {
	svc, _ := newTestSvc(accountmeta.Meta{}, fmt.Errorf("account not found"), executor.OrderResult{}, nil)
	req := testPlaceOrderRequest()
	req.AccountId = 99
	req.Symbol = "BTCUSDT"
	req.MarkPrice = 50000
	_, err := svc.PlaceOrder(context.Background(), req)
	if err == nil {
		t.Fatal("expected error")
	}
	if s, _ := status.FromError(err); s.Code() != codes.Unknown {
		t.Errorf("want Unknown, got %v", s.Code())
	}
}

func TestPlaceOrder_failedAttemptDoesNotCreateOrderOrFill(t *testing.T) {
	meta := testOrderMeta(environmentLive)
	meta.UserID = 55
	result := executor.OrderResult{Status: "FAILED", ErrorMessage: "exchange error", OrigQty: 1, RemainingQty: 1}
	svc, repo := newTestSvc(meta, nil, result, nil)

	req := testPlaceOrderRequest()
	req.Side = "SELL"
	req.MarkPrice = 3000
	resp, err := svc.PlaceOrder(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected gRPC error: %v", err)
	}
	if resp.GetAttemptStatus() != "FAILED" {
		t.Fatalf("attempt_status = %q, want FAILED", resp.GetAttemptStatus())
	}
	if resp.GetOrder() != nil {
		t.Fatal("failed attempt must not create order")
	}
	if got := len(resp.GetFillDeltas()); got != 0 {
		t.Fatalf("fill_deltas = %d, want 0", got)
	}
	if len(repo.attempts) != 1 || len(repo.orders) != 0 || len(repo.fills) != 0 {
		t.Fatalf("persisted counts attempts=%d orders=%d fills=%d", len(repo.attempts), len(repo.orders), len(repo.fills))
	}
}

func TestPlaceOrder_finalizeFailureAfterExchangeAcceptReturnsRecoverableState(t *testing.T) {
	meta := testOrderMeta(environmentBacktest)
	result := executor.OrderResult{
		ExchangeOrderID: "ex-order-1",
		Symbol:          "BTCUSDT",
		Side:            "BUY",
		Status:          "FILLED",
		OrigQty:         0.1,
		ExecutedQty:     0.1,
		RemainingQty:    0,
		AvgPrice:        50025,
		Fills: []executor.FillResult{{
			Qty:       0.1,
			FillPrice: 50025,
			Fee:       2.001,
		}},
	}
	svc, repo := newTestSvc(meta, nil, result, nil)
	repo.finalizeErr = fmt.Errorf("db unavailable")

	req := testPlaceOrderRequest()
	req.Symbol = "BTCUSDT"
	req.Qty = 0.1
	req.MarkPrice = 50000
	req.StrategyId = 9
	req.SessionId = "sess-1"
	req.IntentId = "intent-1"
	resp, err := svc.PlaceOrder(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected gRPC error: %v", err)
	}
	if resp.GetAttemptStatus() != attemptStatusRecoveryFailed {
		t.Fatalf("attempt_status = %q, want %s", resp.GetAttemptStatus(), attemptStatusRecoveryFailed)
	}
	if resp.GetOrder() != nil {
		t.Fatalf("expected unresolved response without local order, got %+v", resp.GetOrder())
	}
	if resp.GetErrorMessage() == "" {
		t.Fatal("expected recovery failure message")
	}
}

func TestPlaceOrder_executeErrorResolvesRecoveredOrder(t *testing.T) {
	meta := testOrderMeta(environmentDemo)
	svc, repo := newTestSvc(meta, nil, executor.OrderResult{}, fmt.Errorf("rpc timeout"))
	router := svc.routerExec.(*stubRouterExec)
	router.resolveResult = executor.OrderResult{
		ExchangeOrderID: "ex-order-2",
		ClientOrderID:   "coid-2",
		Symbol:          "BTCUSDT",
		Side:            "BUY",
		Status:          "FILLED",
		OrigQty:         0.1,
		ExecutedQty:     0.1,
		RemainingQty:    0,
		AvgPrice:        50025,
		Fills: []executor.FillResult{{
			ExchangeTradeID: "trade-1",
			Qty:             0.1,
			FillPrice:       50025,
			Fee:             1.2,
		}},
	}

	req := testPlaceOrderRequest()
	req.Symbol = "BTCUSDT"
	req.Qty = 0.1
	req.MarkPrice = 50000
	req.StrategyId = 9
	req.SessionId = "sess-1"
	req.IntentId = "intent-1"
	resp, err := svc.PlaceOrder(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetAttemptStatus() != attemptStatusRecovered {
		t.Fatalf("attempt_status = %q, want %s", resp.GetAttemptStatus(), attemptStatusRecovered)
	}
	if resp.GetOrder() == nil || resp.GetOrder().GetExchangeOrderId() != "ex-order-2" {
		t.Fatalf("expected recovered order, got %+v", resp.GetOrder())
	}
	if len(repo.orders) != 1 || len(repo.fills) != 1 {
		t.Fatalf("persisted counts orders=%d fills=%d", len(repo.orders), len(repo.fills))
	}
}

func TestResolveOrderAttempt_notFoundReturnsFailed(t *testing.T) {
	meta := testOrderMeta(environmentDemo)
	svc, _ := newTestSvc(meta, nil, executor.OrderResult{}, nil)

	resp, err := svc.ResolveOrderAttempt(context.Background(), &orderv1.ResolveOrderAttemptRequest{
		AccountId: 1,
		IntentId:  "missing-intent",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetAttemptStatus() != attemptStatusFailed {
		t.Fatalf("attempt_status = %q, want %s", resp.GetAttemptStatus(), attemptStatusFailed)
	}
}

func TestResolveOrderAttempt_riskRejectedIsTerminal(t *testing.T) {
	meta := testOrderMeta(environmentDemo)
	svc, repo := newTestSvc(meta, nil, executor.OrderResult{}, nil)
	router := svc.routerExec.(*stubRouterExec)
	repo.attempts = append(repo.attempts, repository.OrderAttempt{
		IntentID:        "intent-risk",
		AttemptID:       "attempt-risk",
		AccountID:       1,
		UserID:          7,
		VenueID:         10,
		StrategyID:      9,
		SessionID:       "sess-risk",
		ClientOrderID:   "coid-risk",
		Exchange:        int32(domain.ExchangeBinance),
		Market:          int32(domain.MarketPerpetualFutures),
		Environment:     environmentDemo,
		Symbol:          "BTCUSDT",
		Status:          attemptStatusRiskRejected,
		ErrorMessage:    "route has pending execution",
		RiskStatus:      "REJECT",
		RiskReasonsJSON: `[{"code":"ROUTE_PENDING_EXECUTION","message":"route has pending execution"}]`,
	})

	resp, err := svc.ResolveOrderAttempt(context.Background(), &orderv1.ResolveOrderAttemptRequest{
		AccountId: 1,
		IntentId:  "intent-risk",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetAttemptStatus() != attemptStatusRiskRejected {
		t.Fatalf("attempt_status = %q, want %s", resp.GetAttemptStatus(), attemptStatusRiskRejected)
	}
	if resp.GetErrorMessage() != "route has pending execution" {
		t.Fatalf("error_message = %q", resp.GetErrorMessage())
	}
	if router.resolveCalls != 0 {
		t.Fatalf("router resolve calls = %d, want 0", router.resolveCalls)
	}
}

func TestQueryOrderAttempts_requiresUserID(t *testing.T) {
	svc, _ := newTestSvc(accountmeta.Meta{}, nil, executor.OrderResult{}, nil)
	_, err := svc.QueryOrderAttempts(context.Background(), &orderv1.QueryOrderAttemptsRequest{})
	if err == nil {
		t.Fatal("expected error")
	}
	if s, _ := status.FromError(err); s.Code() != codes.InvalidArgument {
		t.Errorf("want InvalidArgument, got %v", s.Code())
	}
}

func TestQueryOrderAttemptsAndOrdersAndFills(t *testing.T) {
	svc, repo := newTestSvc(accountmeta.Meta{}, nil, executor.OrderResult{}, nil)
	goodTillDate := time.Unix(1893456000, 0).UTC()
	nextCheckAt := time.Unix(1893456060, 0).UTC()
	recoveryDeadlineAt := time.Unix(1894665600, 0).UTC()
	forceClosedAt := time.Unix(1894665660, 0).UTC()
	repo.attempts = append(repo.attempts, repository.OrderAttempt{
		AttemptID:       "a1",
		IntentID:        "i1",
		Status:          "RISK_REJECTED",
		PostOnly:        true,
		GoodTillDate:    &goodTillDate,
		ReduceOnly:      true,
		RiskStatus:      "REJECT",
		RiskReasonsJSON: `[{"code":"ROUTE_PENDING_EXECUTION"}]`,
	})
	repo.orders = append(repo.orders, repository.Order{
		OrderID:            "o1",
		IntentID:           "i1",
		Status:             "NEW",
		PostOnly:           true,
		GoodTillDate:       &goodTillDate,
		ReduceOnly:         true,
		RecoveryStatus:     "PARTIALLY_FILLED",
		NextCheckAt:        &nextCheckAt,
		RecoveryDeadlineAt: &recoveryDeadlineAt,
		LastRecoveryError:  "trade fee pending",
		ForceClosedAt:      &forceClosedAt,
	})
	repo.fills = append(repo.fills, repository.OrderFill{FillID: "f1", OrderID: "o1", IntentID: "i1"})

	attemptsResp, err := svc.QueryOrderAttempts(context.Background(), &orderv1.QueryOrderAttemptsRequest{UserId: 1, Limit: 10})
	if err != nil {
		t.Fatalf("QueryOrderAttempts err: %v", err)
	}
	if attemptsResp.GetTotal() != 1 || len(attemptsResp.GetAttempts()) != 1 {
		t.Fatalf("attempts total/items = %d/%d", attemptsResp.GetTotal(), len(attemptsResp.GetAttempts()))
	}
	if attemptsResp.GetAttempts()[0].GetRiskStatus() != "REJECT" || !strings.Contains(attemptsResp.GetAttempts()[0].GetRiskReasonsJson(), "ROUTE_PENDING_EXECUTION") {
		t.Fatalf("attempt risk fields = %+v", attemptsResp.GetAttempts()[0])
	}
	attempt := attemptsResp.GetAttempts()[0]
	if !attempt.GetPostOnly() || !attempt.GetReduceOnly() || attempt.GetGoodTillDate().AsTime().Unix() != goodTillDate.Unix() {
		t.Fatalf("attempt order semantics = %+v, want post_only/reduce_only/good_till_date", attempt)
	}

	ordersResp, err := svc.QueryOrders(context.Background(), &orderv1.QueryOrdersRequest{UserId: 1, Limit: 10})
	if err != nil {
		t.Fatalf("QueryOrders err: %v", err)
	}
	if ordersResp.GetTotal() != 1 || len(ordersResp.GetOrders()) != 1 {
		t.Fatalf("orders total/items = %d/%d", ordersResp.GetTotal(), len(ordersResp.GetOrders()))
	}
	order := ordersResp.GetOrders()[0]
	if !order.GetPostOnly() || !order.GetReduceOnly() || order.GetGoodTillDate().AsTime().Unix() != goodTillDate.Unix() {
		t.Fatalf("order semantics = %+v, want post_only/reduce_only/good_till_date", order)
	}
	if order.GetRecoveryStatus() != "PARTIALLY_FILLED" ||
		order.GetNextCheckAt().AsTime().Unix() != nextCheckAt.Unix() ||
		order.GetRecoveryDeadlineAt().AsTime().Unix() != recoveryDeadlineAt.Unix() ||
		order.GetLastRecoveryError() != "trade fee pending" ||
		order.GetForceClosedAt().AsTime().Unix() != forceClosedAt.Unix() {
		t.Fatalf("order recovery fields = %+v", order)
	}

	fillsResp, err := svc.QueryOrderFills(context.Background(), &orderv1.QueryOrderFillsRequest{UserId: 1, Limit: 10})
	if err != nil {
		t.Fatalf("QueryOrderFills err: %v", err)
	}
	if fillsResp.GetTotal() != 1 || len(fillsResp.GetFills()) != 1 {
		t.Fatalf("fills total/items = %d/%d", fillsResp.GetTotal(), len(fillsResp.GetFills()))
	}
}

func TestQueryOrderIntents_requiresUserID(t *testing.T) {
	svc, _ := newTestSvc(accountmeta.Meta{}, nil, executor.OrderResult{}, nil)
	_, err := svc.QueryOrderIntents(context.Background(), &orderv1.QueryOrderIntentsRequest{})
	if err == nil {
		t.Fatal("expected error")
	}
	if s, _ := status.FromError(err); s.Code() != codes.InvalidArgument {
		t.Errorf("want InvalidArgument, got %v", s.Code())
	}
}

func TestQueryOrderIntents_returnsItemsAndTotal(t *testing.T) {
	svc, repo := newTestSvc(accountmeta.Meta{}, nil, executor.OrderResult{}, nil)
	goodTillDate := time.Unix(1893456000, 0).UTC()
	repo.intents = append(repo.intents,
		repository.OrderIntent{
			IntentID:      "i1",
			Symbol:        "BTCUSDT",
			Side:          "BUY",
			RequestedQty:  1,
			PostOnly:      true,
			GoodTillDate:  &goodTillDate,
			ReduceOnly:    true,
			SessionID:     "sess-1",
			Status:        "REJECTED",
			RejectCode:    "ROUTE_PENDING_EXECUTION",
			RejectMessage: "route has pending execution",
		},
		repository.OrderIntent{IntentID: "i2", Symbol: "ETHUSDT", Side: "SELL", RequestedQty: 2, SessionID: "sess-1"},
	)
	resp, err := svc.QueryOrderIntents(context.Background(), &orderv1.QueryOrderIntentsRequest{UserId: 1, Limit: 10})
	if err != nil {
		t.Fatalf("QueryOrderIntents err: %v", err)
	}
	if resp.GetTotal() != 2 || len(resp.GetIntents()) != 2 {
		t.Fatalf("intents total/items = %d/%d", resp.GetTotal(), len(resp.GetIntents()))
	}
	if resp.GetIntents()[0].GetStatus() != "REJECTED" || resp.GetIntents()[0].GetRejectCode() != "ROUTE_PENDING_EXECUTION" {
		t.Fatalf("intent reject fields = %+v", resp.GetIntents()[0])
	}
	if !resp.GetIntents()[0].GetPostOnly() || !resp.GetIntents()[0].GetReduceOnly() || resp.GetIntents()[0].GetGoodTillDate().AsTime().Unix() != goodTillDate.Unix() {
		t.Fatalf("intent order semantics = %+v, want post_only/reduce_only/good_till_date", resp.GetIntents()[0])
	}
}

func TestQueryOrderAttempts_filtersByIntent(t *testing.T) {
	svc, repo := newTestSvc(accountmeta.Meta{}, nil, executor.OrderResult{}, nil)
	repo.attempts = append(repo.attempts,
		repository.OrderAttempt{AttemptID: "a1", IntentID: "i1"},
		repository.OrderAttempt{AttemptID: "a2", IntentID: "i1"},
		repository.OrderAttempt{AttemptID: "a3", IntentID: "i2"},
	)
	resp, err := svc.QueryOrderAttempts(context.Background(), &orderv1.QueryOrderAttemptsRequest{UserId: 1, IntentId: "i1", Limit: 10})
	if err != nil {
		t.Fatalf("QueryOrderAttempts err: %v", err)
	}
	if resp.GetTotal() != 2 || len(resp.GetAttempts()) != 2 {
		t.Fatalf("attempts total/items = %d/%d", resp.GetTotal(), len(resp.GetAttempts()))
	}
}

func TestQueryOrders_filtersByAttempt(t *testing.T) {
	svc, repo := newTestSvc(accountmeta.Meta{}, nil, executor.OrderResult{}, nil)
	repo.orders = append(repo.orders,
		repository.Order{OrderID: "o1", AttemptID: "a1", IntentID: "i1"},
		repository.Order{OrderID: "o2", AttemptID: "a2", IntentID: "i1"},
	)
	resp, err := svc.QueryOrders(context.Background(), &orderv1.QueryOrdersRequest{UserId: 1, AttemptId: "a2", Limit: 10})
	if err != nil {
		t.Fatalf("QueryOrders err: %v", err)
	}
	if resp.GetTotal() != 1 || len(resp.GetOrders()) != 1 || resp.GetOrders()[0].GetOrderId() != "o2" {
		t.Fatalf("orders total=%d items=%d first=%v", resp.GetTotal(), len(resp.GetOrders()), resp.GetOrders())
	}
}

func TestQueryOrderFills_filtersByOrder(t *testing.T) {
	svc, repo := newTestSvc(accountmeta.Meta{}, nil, executor.OrderResult{}, nil)
	repo.fills = append(repo.fills,
		repository.OrderFill{FillID: "f1", OrderID: "o1", AttemptID: "a1", IntentID: "i1"},
		repository.OrderFill{FillID: "f2", OrderID: "o1", AttemptID: "a1", IntentID: "i1"},
		repository.OrderFill{FillID: "f3", OrderID: "o2", AttemptID: "a2", IntentID: "i1"},
	)
	resp, err := svc.QueryOrderFills(context.Background(), &orderv1.QueryOrderFillsRequest{UserId: 1, OrderId: "o1", Limit: 10})
	if err != nil {
		t.Fatalf("QueryOrderFills err: %v", err)
	}
	if resp.GetTotal() != 2 || len(resp.GetFills()) != 2 {
		t.Fatalf("fills total/items = %d/%d", resp.GetTotal(), len(resp.GetFills()))
	}
}

func TestListOrderLifecycleEventsAfterCursor(t *testing.T) {
	svc, repo := newTestSvc(accountmeta.Meta{}, nil, executor.OrderResult{}, nil)
	repo.events = append(repo.events,
		lifecycle.Event{EventID: 1, SessionID: "sess-1", EventType: "fill", EventSource: lifecycle.EventSourceRESTRecovery, OrderStatus: "PARTIALLY_FILLED"},
		lifecycle.Event{
			EventID:      2,
			SessionID:    "sess-1",
			Environment:  environmentDemo,
			Exchange:     exchangeBinance,
			Market:       marketPerpetualFutures,
			PositionSide: positionSideBoth,
			Side:         "BUY",
			EventType:    "fill",
			EventSource:  lifecycle.EventSourceWebsocket,
			OrderStatus:  "FILLED",
			FillDelta:    lifecycle.FillDelta{Symbol: "ETHUSDT", Qty: 0.2, FillPrice: 3000},
		},
		lifecycle.Event{EventID: 3, SessionID: "other", EventType: "fill", EventSource: lifecycle.EventSourceRESTRecovery},
	)

	resp, err := svc.ListOrderLifecycleEvents(context.Background(), &orderv1.ListOrderLifecycleEventsRequest{
		SessionId:    "sess-1",
		AfterEventId: 1,
		Limit:        10,
	})
	if err != nil {
		t.Fatalf("ListOrderLifecycleEvents err: %v", err)
	}
	if len(resp.GetEvents()) != 1 {
		t.Fatalf("events len = %d, want 1", len(resp.GetEvents()))
	}
	event := resp.GetEvents()[0]
	if event.GetEventId() != 2 || event.GetOrderStatus() != "FILLED" || event.GetFillDelta().GetSymbol() != "ETHUSDT" {
		t.Fatalf("event = %+v, want cursor event 2", event)
	}
	if event.GetEventSource() != lifecycle.EventSourceWebsocket {
		t.Fatalf("event_source = %q, want %s", event.GetEventSource(), lifecycle.EventSourceWebsocket)
	}
	if event.GetEnvironment() != environmentDemo || event.GetExchange() != exchangeBinance ||
		event.GetMarket() != marketPerpetualFutures || event.GetPositionSide() != positionSideBoth || event.GetSide() != "BUY" {
		t.Fatalf("event route facts = %+v, want binance futures BUY", event)
	}
}

func TestEmitLifecycleEventsSkipsAmbiguousRouteFacts(t *testing.T) {
	svc, repo := newTestSvc(accountmeta.Meta{}, nil, executor.OrderResult{}, nil)
	order := &repository.Order{
		OrderID:         "order-ambiguous",
		ExchangeOrderID: "exchange-ambiguous",
		Symbol:          "ETHUSDT",
		Status:          "FILLED",
		OrigQty:         0.1,
		ExecutedQty:     0.1,
	}
	fills := []repository.OrderFill{{
		FillID:          "fill-ambiguous",
		OrderID:         "order-ambiguous",
		ExchangeOrderID: "exchange-ambiguous",
		Symbol:          "ETHUSDT",
		Side:            "BUY",
		Qty:             0.1,
		FillPrice:       2500,
		Status:          "FILLED",
	}}

	svc.emitLifecycleEvents(context.Background(), order, fills)

	if len(repo.events) != 0 {
		t.Fatalf("events = %+v, want no ambiguous lifecycle event", repo.events)
	}
}

func TestEmitLifecycleEventsAllowsPositionSideBoth(t *testing.T) {
	svc, repo := newTestSvc(accountmeta.Meta{}, nil, executor.OrderResult{}, nil)
	order := &repository.Order{
		OrderID:         "order-1",
		ExchangeOrderID: "exchange-1",
		ClientOrderID:   "client-1",
		AccountID:       1,
		VenueID:         10,
		UserID:          77,
		Environment:     environmentBacktest,
		Exchange:        exchangeBinance,
		Market:          marketPerpetualFutures,
		PositionSide:    positionSideBoth,
		Symbol:          "ETHUSDT",
		Side:            "BUY",
		Status:          "FILLED",
		OrigQty:         0.1,
		ExecutedQty:     0.1,
	}
	fills := []repository.OrderFill{{
		FillID:          "fill-1",
		ExchangeTradeID: "trade-1",
		OrderID:         "order-1",
		ExchangeOrderID: "exchange-1",
		AccountID:       1,
		VenueID:         10,
		UserID:          77,
		Environment:     environmentBacktest,
		Exchange:        exchangeBinance,
		Market:          marketPerpetualFutures,
		PositionSide:    positionSideBoth,
		Symbol:          "ETHUSDT",
		Side:            "BUY",
		Qty:             0.1,
		FillPrice:       2500,
		Status:          "FILLED",
	}}

	svc.emitLifecycleEvents(context.Background(), order, fills)

	if len(repo.events) != 1 {
		t.Fatalf("events = %+v, want one valid lifecycle event", repo.events)
	}
	if repo.events[0].PositionSide != positionSideBoth {
		t.Fatalf("position_side = %d, want BOTH/0", repo.events[0].PositionSide)
	}
	if repo.events[0].EventSource != lifecycle.EventSourcePlaceOrder {
		t.Fatalf("event_source = %q, want %s", repo.events[0].EventSource, lifecycle.EventSourcePlaceOrder)
	}
}
