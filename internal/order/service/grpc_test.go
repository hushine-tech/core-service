package service

import (
	"context"
	"fmt"
	"testing"

	"github.com/hushine-tech/core-service/gen/orderv1"
	"github.com/hushine-tech/core-service/internal/order/accountmeta"
	"github.com/hushine-tech/core-service/internal/order/executor"
	"github.com/hushine-tech/core-service/internal/order/lifecycle"
	"github.com/hushine-tech/core-service/internal/order/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	lastReq       executor.OrderRequest
}

func (r *stubRouterExec) Execute(_ context.Context, req executor.OrderRequest, _ accountmeta.Meta) (executor.OrderResult, error) {
	r.executeCalls++
	r.lastReq = req
	return r.result, r.err
}

func (r *stubRouterExec) Resolve(_ context.Context, _ executor.RecoveryRequest, _ accountmeta.Meta) (executor.OrderResult, error) {
	return r.resolveResult, r.resolveErr
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

func TestUnsupportedTimeInForceFailsClosed(t *testing.T) {
	svc, _ := newTestSvc(testOrderMeta(environmentBacktest), nil, executor.OrderResult{}, nil)
	req := testPlaceOrderRequest()
	price := 2500.0
	req.Price = &price
	req.OrderType = "LIMIT"
	req.TimeInForce = "IOC"

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
	repo.attempts = append(repo.attempts, repository.OrderAttempt{AttemptID: "a1", IntentID: "i1", Status: "FAILED"})
	repo.orders = append(repo.orders, repository.Order{OrderID: "o1", IntentID: "i1", Status: "NEW"})
	repo.fills = append(repo.fills, repository.OrderFill{FillID: "f1", OrderID: "o1", IntentID: "i1"})

	attemptsResp, err := svc.QueryOrderAttempts(context.Background(), &orderv1.QueryOrderAttemptsRequest{UserId: 1, Limit: 10})
	if err != nil {
		t.Fatalf("QueryOrderAttempts err: %v", err)
	}
	if attemptsResp.GetTotal() != 1 || len(attemptsResp.GetAttempts()) != 1 {
		t.Fatalf("attempts total/items = %d/%d", attemptsResp.GetTotal(), len(attemptsResp.GetAttempts()))
	}

	ordersResp, err := svc.QueryOrders(context.Background(), &orderv1.QueryOrdersRequest{UserId: 1, Limit: 10})
	if err != nil {
		t.Fatalf("QueryOrders err: %v", err)
	}
	if ordersResp.GetTotal() != 1 || len(ordersResp.GetOrders()) != 1 {
		t.Fatalf("orders total/items = %d/%d", ordersResp.GetTotal(), len(ordersResp.GetOrders()))
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
	repo.intents = append(repo.intents,
		repository.OrderIntent{IntentID: "i1", Symbol: "BTCUSDT", Side: "BUY", RequestedQty: 1, SessionID: "sess-1"},
		repository.OrderIntent{IntentID: "i2", Symbol: "ETHUSDT", Side: "SELL", RequestedQty: 2, SessionID: "sess-1"},
	)
	resp, err := svc.QueryOrderIntents(context.Background(), &orderv1.QueryOrderIntentsRequest{UserId: 1, Limit: 10})
	if err != nil {
		t.Fatalf("QueryOrderIntents err: %v", err)
	}
	if resp.GetTotal() != 2 || len(resp.GetIntents()) != 2 {
		t.Fatalf("intents total/items = %d/%d", resp.GetTotal(), len(resp.GetIntents()))
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
		lifecycle.Event{EventID: 1, SessionID: "sess-1", EventType: "fill", OrderStatus: "PARTIALLY_FILLED"},
		lifecycle.Event{
			EventID:      2,
			SessionID:    "sess-1",
			Environment:  environmentDemo,
			Exchange:     exchangeBinance,
			Market:       marketPerpetualFutures,
			PositionSide: positionSideBoth,
			Side:         "BUY",
			EventType:    "fill",
			OrderStatus:  "FILLED",
			FillDelta:    lifecycle.FillDelta{Symbol: "ETHUSDT", Qty: 0.2, FillPrice: 3000},
		},
		lifecycle.Event{EventID: 3, SessionID: "other", EventType: "fill"},
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
	if event.GetEnvironment() != environmentDemo || event.GetExchange() != exchangeBinance ||
		event.GetMarket() != marketPerpetualFutures || event.GetPositionSide() != positionSideBoth || event.GetSide() != "BUY" {
		t.Fatalf("event route facts = %+v, want binance futures BUY", event)
	}
}
