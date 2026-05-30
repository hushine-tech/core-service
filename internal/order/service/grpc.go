package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/hushine-tech/core-service/gen/orderv1"
	"github.com/hushine-tech/core-service/internal/logger"
	"github.com/hushine-tech/core-service/internal/order/accountmeta"
	"github.com/hushine-tech/core-service/internal/order/executor"
	"github.com/hushine-tech/core-service/internal/order/lifecycle"
	ordernotify "github.com/hushine-tech/core-service/internal/order/notification"
	"github.com/hushine-tech/core-service/internal/order/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// MetaGetter fetches account metadata (allows test injection).
type MetaGetter interface {
	Get(ctx context.Context, accountID int64, exchange int32, market int32) (accountmeta.Meta, error)
	ValidateActiveSession(ctx context.Context, meta accountmeta.Meta, strategyID int64, sessionID string) error
}

// RouterExecutor routes and executes orders (allows test injection).
type RouterExecutor interface {
	Execute(ctx context.Context, req executor.OrderRequest, meta accountmeta.Meta) (executor.OrderResult, error)
	Resolve(ctx context.Context, req executor.RecoveryRequest, meta accountmeta.Meta) (executor.OrderResult, error)
}

// OrderGRPCService implements the OrderService gRPC interface.
type OrderGRPCService struct {
	orderv1.UnimplementedOrderServiceServer
	metaGetter MetaGetter
	routerExec RouterExecutor
	repo       repository.Repository
	notifier   ordernotify.Publisher
}

const (
	attemptStatusPending        = "PENDING"
	attemptStatusAccepted       = "ACCEPTED"
	attemptStatusFailed         = "FAILED"
	attemptStatusUnknown        = "UNKNOWN"
	attemptStatusRecovering     = "RECOVERING"
	attemptStatusRecovered      = "RECOVERED"
	attemptStatusRecoveryFailed = "RECOVERY_FAILED"

	environmentBacktest = int32(0)
	environmentDemo     = int32(1)
	environmentLive     = int32(2)

	exchangeBinance = int32(1)
	exchangeOKX     = int32(2)

	marketSpot             = int32(1)
	marketPerpetualFutures = int32(2)
	marketDeliveryFutures  = int32(3)

	positionSideBoth  = int32(0)
	positionSideLong  = int32(1)
	positionSideShort = int32(2)

	orderTypeMarket = int32(1)
	orderTypeLimit  = int32(2)
)

func NewOrderGRPCService(meta MetaGetter, router RouterExecutor, repo repository.Repository, notifierOpt ...ordernotify.Publisher) *OrderGRPCService {
	var notifier ordernotify.Publisher = ordernotify.NoopPublisher{}
	if len(notifierOpt) > 0 && notifierOpt[0] != nil {
		notifier = notifierOpt[0]
	}
	return &OrderGRPCService{metaGetter: meta, routerExec: router, repo: repo, notifier: notifier}
}

func (s *OrderGRPCService) PlaceOrder(ctx context.Context, req *orderv1.PlaceOrderRequest) (*orderv1.PlaceOrderResponse, error) {
	accountID := req.GetAccountId()
	if accountID == 0 {
		return nil, status.Error(codes.InvalidArgument, "account_id is required")
	}
	if req.GetSymbol() == "" {
		return nil, status.Error(codes.InvalidArgument, "symbol is required")
	}
	if req.GetQty() == 0 {
		return nil, status.Error(codes.InvalidArgument, "qty must be non-zero")
	}
	if err := validateRouteRequest(req.GetExchange(), req.GetMarket(), req.GetPositionSide()); err != nil {
		return nil, err
	}
	orderTypeCode, orderTypeText, timeInForce, err := normalizeOrderContract(req)
	if err != nil {
		return nil, err
	}

	meta, err := s.metaGetter.Get(ctx, accountID, req.GetExchange(), req.GetMarket())
	if err != nil {
		return nil, err
	}
	if err := validatePositionSide(meta, req.GetPositionSide()); err != nil {
		return nil, err
	}
	if err := s.metaGetter.ValidateActiveSession(ctx, meta, req.GetStrategyId(), req.GetSessionId()); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	intentID := strings.TrimSpace(req.GetIntentId())
	if intentID == "" {
		intentID = uuid.New().String()
	}
	attemptID := uuid.New().String()
	clientOrderID := buildClientOrderID(intentID, attemptID)
	market := meta.Market

	intent := repository.OrderIntent{
		IntentID:       intentID,
		Time:           now,
		AccountID:      accountID,
		VenueID:        meta.VenueID,
		UserID:         meta.UserID,
		StrategyID:     req.GetStrategyId(),
		SessionID:      req.GetSessionId(),
		Environment:    meta.Environment,
		Exchange:       meta.Exchange,
		Market:         market,
		PositionSide:   req.GetPositionSide(),
		OrderType:      orderTypeCode,
		Symbol:         req.GetSymbol(),
		Side:           req.GetSide(),
		RequestedQty:   req.GetQty(),
		RequestedPrice: req.GetPrice(),
		Status:         "REQUESTED",
	}
	if err := s.repo.UpsertOrderIntent(ctx, intent); err != nil {
		return nil, status.Errorf(codes.Internal, "upsert order intent: %v", err)
	}

	attempt := repository.OrderAttempt{
		AttemptID:      attemptID,
		IntentID:       intentID,
		Time:           now,
		AccountID:      accountID,
		VenueID:        meta.VenueID,
		UserID:         meta.UserID,
		StrategyID:     req.GetStrategyId(),
		SessionID:      req.GetSessionId(),
		Environment:    meta.Environment,
		Exchange:       meta.Exchange,
		Market:         market,
		PositionSide:   req.GetPositionSide(),
		OrderType:      orderTypeCode,
		Symbol:         req.GetSymbol(),
		Side:           req.GetSide(),
		RequestedQty:   req.GetQty(),
		RequestedPrice: req.GetPrice(),
		MarkPrice:      req.GetMarkPrice(),
		Status:         attemptStatusPending,
		ClientOrderID:  clientOrderID,
	}
	if err := s.repo.CreateOrderAttempt(ctx, attempt); err != nil {
		return nil, status.Errorf(codes.Internal, "create order attempt: %v", err)
	}

	var price *float64
	if req.Price != nil {
		p := req.GetPrice()
		price = &p
	}
	orderReq := executor.OrderRequest{
		AccountID:     accountID,
		Symbol:        req.GetSymbol(),
		Side:          req.GetSide(),
		Qty:           req.GetQty(),
		Price:         price,
		MarkPrice:     req.GetMarkPrice(),
		ClientOrderID: clientOrderID,
		Exchange:      meta.Exchange,
		Market:        meta.Market,
		PositionSide:  req.GetPositionSide(),
		OrderType:     orderTypeText,
		TimeInForce:   timeInForce,
	}

	result, execErr := s.routerExec.Execute(ctx, orderReq, meta)
	if execErr != nil {
		attempt.Status = attemptStatusUnknown
		attempt.ErrorMessage = fmt.Sprintf("execution result unknown: %v", execErr)
		attempt.RecoveryError = execErr.Error()
		attempt.Time = time.Now().UTC()
		if err := s.repo.FinalizeOrderAttempt(ctx, attempt, nil, nil); err != nil {
			logger.Error(ctx, "system", fmt.Sprintf("record unknown attempt failed: attempt_id=%s err=%v", attemptID, err))
		}
		return s.resolveAttempt(ctx, meta, attempt, req.GetStrategyId(), req.GetSessionId(), market)
	}

	var persistedOrder *repository.Order
	var persistedFills []repository.OrderFill

	if strings.TrimSpace(result.ExchangeOrderID) == "" {
		attempt.Status = attemptStatusFailed
		attempt.ErrorMessage = nonEmpty(result.ErrorMessage, "order attempt failed")
		attempt.RecoveryError = ""
	} else {
		persistedOrder, persistedFills = buildPersistedExecution(meta, attempt, req.GetStrategyId(), req.GetSessionId(), market, result)
		attempt.OrderID = persistedOrder.OrderID
		attempt.ExchangeOrderID = persistedOrder.ExchangeOrderID
		if result.FillPending {
			attempt.Status = attemptStatusRecovering
			attempt.ErrorMessage = nonEmpty(result.ErrorMessage, "exchange order filled but trade details are pending")
			attempt.RecoveryError = attempt.ErrorMessage
		} else {
			attempt.Status = attemptStatusAccepted
			attempt.ErrorMessage = ""
			attempt.RecoveryError = ""
		}
	}

	attempt.Time = time.Now().UTC()
	if err := s.repo.FinalizeOrderAttempt(ctx, attempt, persistedOrder, persistedFills); err != nil {
		if persistedOrder != nil {
			logger.Error(ctx, "system", fmt.Sprintf(
				"finalize accepted order attempt failed: attempt_id=%s intent_id=%s order_id=%s exchange_order_id=%s client_order_id=%s err=%v",
				attemptID, intentID, persistedOrder.OrderID, persistedOrder.ExchangeOrderID, persistedOrder.ClientOrderID, err,
			))
			attempt.Status = attemptStatusUnknown
			attempt.OrderID = ""
			attempt.ExchangeOrderID = ""
			attempt.ErrorMessage = fmt.Sprintf("local finalize failed after exchange acceptance: %v", err)
			attempt.RecoveryError = attempt.ErrorMessage
			attempt.Time = time.Now().UTC()
			if markErr := s.repo.FinalizeOrderAttempt(ctx, attempt, nil, nil); markErr != nil {
				logger.Error(ctx, "system", fmt.Sprintf("mark unknown after finalize failure failed: attempt_id=%s err=%v", attemptID, markErr))
			}
			return s.resolveAttempt(ctx, meta, attempt, req.GetStrategyId(), req.GetSessionId(), market)
		}
		return nil, status.Errorf(codes.Internal, "finalize order attempt: %v", err)
	}

	s.emitLifecycleEvents(ctx, persistedOrder, persistedFills)
	s.publishOrderNotification(ctx, attempt, persistedOrder)
	return buildPlaceOrderResponse(attempt, persistedOrder, persistedFills), nil
}

func (s *OrderGRPCService) QueryOrderIntents(ctx context.Context, req *orderv1.QueryOrderIntentsRequest) (*orderv1.QueryOrderIntentsResponse, error) {
	if req.GetUserId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}
	limit, offset := normalizePage(req.GetLimit(), req.GetOffset())
	items, total, err := s.repo.QueryOrderIntentsPaginated(ctx, req.GetUserId(), req.GetAccountId(), req.GetStrategyId(), req.GetSessionId(), limit, offset)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query order intents: %v", err)
	}
	out := make([]*orderv1.OrderIntentEntry, 0, len(items))
	for _, item := range items {
		out = append(out, toProtoIntent(item))
	}
	return &orderv1.QueryOrderIntentsResponse{Intents: out, Total: total}, nil
}

func (s *OrderGRPCService) QueryOrderAttempts(ctx context.Context, req *orderv1.QueryOrderAttemptsRequest) (*orderv1.QueryOrderAttemptsResponse, error) {
	if req.GetUserId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}
	limit, offset := normalizePage(req.GetLimit(), req.GetOffset())
	items, total, err := s.repo.QueryOrderAttemptsPaginated(ctx, req.GetUserId(), req.GetAccountId(), req.GetStrategyId(), req.GetSessionId(), req.GetIntentId(), limit, offset)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query order attempts: %v", err)
	}
	out := make([]*orderv1.OrderAttemptEntry, 0, len(items))
	for _, item := range items {
		out = append(out, toProtoAttempt(item))
	}
	return &orderv1.QueryOrderAttemptsResponse{Attempts: out, Total: total}, nil
}

func (s *OrderGRPCService) QueryOrders(ctx context.Context, req *orderv1.QueryOrdersRequest) (*orderv1.QueryOrdersResponse, error) {
	if req.GetUserId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}
	limit, offset := normalizePage(req.GetLimit(), req.GetOffset())
	items, total, err := s.repo.QueryOrdersPaginated(ctx, req.GetUserId(), req.GetAccountId(), req.GetStrategyId(), req.GetSessionId(), req.GetIntentId(), req.GetAttemptId(), limit, offset)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query orders: %v", err)
	}
	out := make([]*orderv1.ExchangeOrderEntry, 0, len(items))
	for _, item := range items {
		out = append(out, toProtoOrder(item))
	}
	return &orderv1.QueryOrdersResponse{Orders: out, Total: total}, nil
}

func (s *OrderGRPCService) QueryOrderFills(ctx context.Context, req *orderv1.QueryOrderFillsRequest) (*orderv1.QueryOrderFillsResponse, error) {
	if req.GetUserId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}
	limit, offset := normalizePage(req.GetLimit(), req.GetOffset())
	items, total, err := s.repo.QueryOrderFillsPaginated(ctx, req.GetUserId(), req.GetAccountId(), req.GetStrategyId(), req.GetSessionId(), req.GetIntentId(), req.GetAttemptId(), req.GetOrderId(), limit, offset)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query order fills: %v", err)
	}
	out := make([]*orderv1.OrderFillEntry, 0, len(items))
	for _, item := range items {
		out = append(out, toProtoFill(item))
	}
	return &orderv1.QueryOrderFillsResponse{Fills: out, Total: total}, nil
}

func (s *OrderGRPCService) ListOrderLifecycleEvents(ctx context.Context, req *orderv1.ListOrderLifecycleEventsRequest) (*orderv1.ListOrderLifecycleEventsResponse, error) {
	sessionID := strings.TrimSpace(req.GetSessionId())
	if sessionID == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}
	limit, _ := normalizePage(req.GetLimit(), 0)
	items, err := s.repo.ListLifecycleEvents(ctx, sessionID, req.GetAfterEventId(), limit)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list order lifecycle events: %v", err)
	}
	out := make([]*orderv1.OrderLifecycleEventEntry, 0, len(items))
	for _, item := range items {
		out = append(out, toProtoLifecycleEvent(item))
	}
	return &orderv1.ListOrderLifecycleEventsResponse{Events: out}, nil
}

func (s *OrderGRPCService) ResolveOrderAttempt(ctx context.Context, req *orderv1.ResolveOrderAttemptRequest) (*orderv1.ResolveOrderAttemptResponse, error) {
	accountID := req.GetAccountId()
	if accountID == 0 {
		return nil, status.Error(codes.InvalidArgument, "account_id is required")
	}
	if strings.TrimSpace(req.GetIntentId()) == "" && strings.TrimSpace(req.GetAttemptId()) == "" && strings.TrimSpace(req.GetClientOrderId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "intent_id, attempt_id, or client_order_id is required")
	}

	attempt, err := s.repo.FindOrderAttempt(ctx, 0, accountID, req.GetIntentId(), req.GetAttemptId(), req.GetClientOrderId())
	if err != nil {
		if err == repository.ErrNotFound {
			return &orderv1.ResolveOrderAttemptResponse{
				IntentId:      req.GetIntentId(),
				AttemptStatus: attemptStatusFailed,
				ErrorMessage:  "attempt not found; no local execution record exists",
				ClientOrderId: req.GetClientOrderId(),
			}, nil
		}
		return nil, status.Errorf(codes.Internal, "find order attempt: %v", err)
	}

	if attempt.Status == attemptStatusFailed || attempt.Status == attemptStatusAccepted || attempt.Status == attemptStatusRecovered {
		order, fills := s.loadPersistedExecution(ctx, attempt.AttemptID)
		resp := buildPlaceOrderResponse(attempt, order, fills)
		return &orderv1.ResolveOrderAttemptResponse{
			IntentId:      resp.GetIntentId(),
			AttemptId:     resp.GetAttemptId(),
			AttemptStatus: resp.GetAttemptStatus(),
			ErrorMessage:  resp.GetErrorMessage(),
			Order:         resp.GetOrder(),
			FillDeltas:    resp.GetFillDeltas(),
			ClientOrderId: resp.GetClientOrderId(),
		}, nil
	}

	meta, err := s.metaGetter.Get(ctx, accountID, attempt.Exchange, attempt.Market)
	if err != nil {
		return nil, err
	}

	resp, err := s.resolveAttempt(ctx, meta, attempt, attempt.StrategyID, attempt.SessionID, attempt.Market)
	if err != nil {
		return nil, err
	}
	return &orderv1.ResolveOrderAttemptResponse{
		IntentId:      resp.GetIntentId(),
		AttemptId:     resp.GetAttemptId(),
		AttemptStatus: resp.GetAttemptStatus(),
		ErrorMessage:  resp.GetErrorMessage(),
		Order:         resp.GetOrder(),
		FillDeltas:    resp.GetFillDeltas(),
		ClientOrderId: resp.GetClientOrderId(),
	}, nil
}

func normalizePage(limitRaw, offsetRaw int32) (int, int) {
	limit := int(limitRaw)
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	offset := int(offsetRaw)
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}

func (s *OrderGRPCService) resolveAttempt(
	ctx context.Context,
	meta accountmeta.Meta,
	attempt repository.OrderAttempt,
	strategyID int64,
	sessionID string,
	market int32,
) (*orderv1.PlaceOrderResponse, error) {
	resolveReq := executor.RecoveryRequest{
		AccountID:       attempt.AccountID,
		Symbol:          attempt.Symbol,
		ClientOrderID:   attempt.ClientOrderID,
		ExchangeOrderID: attempt.ExchangeOrderID,
	}
	logger.Warn(ctx, "system", fmt.Sprintf(
		"order recovery start: intent_id=%s attempt_id=%s client_order_id=%s exchange_order_id=%s symbol=%s market=%d",
		attempt.IntentID, attempt.AttemptID, attempt.ClientOrderID, attempt.ExchangeOrderID, attempt.Symbol, market,
	))

	attempt.Status = attemptStatusRecovering
	attempt.Time = time.Now().UTC()
	if err := s.repo.FinalizeOrderAttempt(ctx, attempt, nil, nil); err != nil {
		logger.Error(ctx, "system", fmt.Sprintf("mark recovering failed: attempt_id=%s err=%v", attempt.AttemptID, err))
	}

	result, err := s.routerExec.Resolve(ctx, resolveReq, meta)
	if err != nil {
		if err == executor.ErrOrderNotFound {
			logger.Warn(ctx, "system", fmt.Sprintf(
				"order recovery confirmed absent: intent_id=%s attempt_id=%s client_order_id=%s",
				attempt.IntentID, attempt.AttemptID, attempt.ClientOrderID,
			))
			attempt.Status = attemptStatusFailed
			attempt.ErrorMessage = nonEmpty(attempt.ErrorMessage, "attempt confirmed absent after recovery query")
			attempt.RecoveryError = ""
			attempt.Time = time.Now().UTC()
			if finalizeErr := s.repo.FinalizeOrderAttempt(ctx, attempt, nil, nil); finalizeErr != nil {
				return buildRecoveryFailedResponse(attempt, fmt.Sprintf("failed to persist confirmed absence: %v", finalizeErr)), nil
			}
			s.publishOrderNotification(ctx, attempt, nil)
			return buildPlaceOrderResponse(attempt, nil, nil), nil
		}
		attempt.Status = attemptStatusRecoveryFailed
		attempt.RecoveryError = err.Error()
		attempt.ErrorMessage = nonEmpty(attempt.ErrorMessage, "recovery query failed")
		logger.Error(ctx, "system", fmt.Sprintf(
			"order recovery query failed: intent_id=%s attempt_id=%s client_order_id=%s err=%v",
			attempt.IntentID, attempt.AttemptID, attempt.ClientOrderID, err,
		))
		attempt.Time = time.Now().UTC()
		if finalizeErr := s.repo.FinalizeOrderAttempt(ctx, attempt, nil, nil); finalizeErr != nil {
			logger.Error(ctx, "system", fmt.Sprintf("persist recovery failure failed: attempt_id=%s err=%v", attempt.AttemptID, finalizeErr))
		}
		s.publishOrderNotification(ctx, attempt, nil)
		return buildRecoveryFailedResponse(attempt, err.Error()), nil
	}

	if strings.TrimSpace(result.ExchangeOrderID) == "" {
		attempt.Status = attemptStatusFailed
		attempt.ErrorMessage = nonEmpty(attempt.ErrorMessage, "attempt confirmed absent after recovery query")
		attempt.RecoveryError = ""
		attempt.Time = time.Now().UTC()
		if finalizeErr := s.repo.FinalizeOrderAttempt(ctx, attempt, nil, nil); finalizeErr != nil {
			return buildRecoveryFailedResponse(attempt, fmt.Sprintf("failed to persist confirmed absence: %v", finalizeErr)), nil
		}
		s.publishOrderNotification(ctx, attempt, nil)
		return buildPlaceOrderResponse(attempt, nil, nil), nil
	}

	persistedOrder, persistedFills := buildPersistedExecution(meta, attempt, strategyID, sessionID, market, result)
	attempt.OrderID = persistedOrder.OrderID
	attempt.ExchangeOrderID = persistedOrder.ExchangeOrderID
	if result.FillPending {
		attempt.Status = attemptStatusRecovering
		attempt.ErrorMessage = nonEmpty(result.ErrorMessage, "exchange order filled but trade details are pending")
		attempt.RecoveryError = attempt.ErrorMessage
	} else {
		attempt.Status = attemptStatusRecovered
		attempt.ErrorMessage = ""
		attempt.RecoveryError = ""
	}
	attempt.Time = time.Now().UTC()
	if finalizeErr := s.repo.FinalizeOrderAttempt(ctx, attempt, persistedOrder, persistedFills); finalizeErr != nil {
		logger.Error(ctx, "system", fmt.Sprintf(
			"order recovery persist failed: intent_id=%s attempt_id=%s client_order_id=%s err=%v",
			attempt.IntentID, attempt.AttemptID, attempt.ClientOrderID, finalizeErr,
		))
		return buildRecoveryFailedResponse(attempt, fmt.Sprintf("failed to persist recovered execution: %v", finalizeErr)), nil
	}
	s.emitLifecycleEvents(ctx, persistedOrder, persistedFills)
	if result.FillPending {
		logger.Warn(ctx, "system", fmt.Sprintf(
			"order recovery still pending fill details: intent_id=%s attempt_id=%s client_order_id=%s exchange_order_id=%s err=%s",
			attempt.IntentID, attempt.AttemptID, attempt.ClientOrderID, persistedOrder.ExchangeOrderID, attempt.ErrorMessage,
		))
		s.publishOrderNotification(ctx, attempt, persistedOrder)
		return buildPlaceOrderResponse(attempt, persistedOrder, persistedFills), nil
	}
	logger.Info(ctx, "system", fmt.Sprintf(
		"order recovery completed: intent_id=%s attempt_id=%s client_order_id=%s exchange_order_id=%s",
		attempt.IntentID, attempt.AttemptID, attempt.ClientOrderID, persistedOrder.ExchangeOrderID,
	))
	s.publishOrderNotification(ctx, attempt, persistedOrder)
	return buildPlaceOrderResponse(attempt, persistedOrder, persistedFills), nil
}

func (s *OrderGRPCService) emitLifecycleEvents(ctx context.Context, order *repository.Order, fills []repository.OrderFill) {
	if order == nil || len(fills) == 0 {
		return
	}
	for _, fill := range fills {
		event := lifecycle.Event{
			SessionID:       fill.SessionID,
			AccountID:       fill.AccountID,
			VenueID:         fill.VenueID,
			Environment:     fill.Environment,
			Exchange:        fill.Exchange,
			Market:          fill.Market,
			PositionSide:    fill.PositionSide,
			Side:            fill.Side,
			IntentID:        fill.IntentID,
			AttemptID:       fill.AttemptID,
			OrderID:         fill.OrderID,
			ExchangeOrderID: fill.ExchangeOrderID,
			ExchangeTradeID: fill.ExchangeTradeID,
			EventType:       "fill",
			OrderStatus:     order.Status,
			FillDelta: lifecycle.FillDelta{
				ExchangeTradeID: fill.ExchangeTradeID,
				ExchangeOrderID: fill.ExchangeOrderID,
				Symbol:          fill.Symbol,
				Qty:             fill.Qty,
				FillPrice:       fill.FillPrice,
				Fee:             fill.Fee,
				FeeMissing:      strings.EqualFold(fill.Status, "FEE_MISSING"),
				TradeTime:       fill.Time,
			},
			OrderState: lifecycle.OrderState{
				ExchangeOrderID: order.ExchangeOrderID,
				ClientOrderID:   order.ClientOrderID,
				Symbol:          order.Symbol,
				Status:          order.Status,
				OrigQty:         order.OrigQty,
				ExecutedQty:     order.ExecutedQty,
				RemainingQty:    order.RemainingQty,
				AvgPrice:        order.AvgPrice,
				UpdatedAt:       order.Time,
			},
			OccurredAt: fill.Time,
		}
		if err := lifecycle.ValidateEventRouteFacts(event); err != nil {
			logger.Error(ctx, "system", fmt.Sprintf("skip invalid lifecycle event: order_id=%s fill_id=%s err=%v", fill.OrderID, fill.FillID, err))
			continue
		}
		if _, err := s.repo.SaveLifecycleEvent(ctx, event); err != nil {
			logger.Error(ctx, "system", fmt.Sprintf("save lifecycle event failed: order_id=%s fill_id=%s err=%v", fill.OrderID, fill.FillID, err))
		}
	}
}

func (s *OrderGRPCService) publishOrderNotification(ctx context.Context, attempt repository.OrderAttempt, order *repository.Order) {
	if attempt.Environment == environmentBacktest {
		return
	}
	eventType, severity, title, ok := orderNotificationClass(attempt.Status)
	if !ok {
		return
	}
	event := ordernotify.Event{
		SchemaVersion: ordernotify.SchemaVersion,
		UserID:        attempt.UserID,
		Category:      ordernotify.CategoryStrategy,
		EventType:     eventType,
		Severity:      severity,
		AccountID:     attempt.AccountID,
		StrategyID:    attempt.StrategyID,
		SessionID:     attempt.SessionID,
		AttemptID:     attempt.AttemptID,
		Title:         title,
		Message:       orderNotificationMessage(attempt),
		DedupeKey:     fmt.Sprintf("order:%s:%s", attempt.AttemptID, eventType),
		Metadata: map[string]string{
			"attempt_id":      attempt.AttemptID,
			"intent_id":       attempt.IntentID,
			"symbol":          attempt.Symbol,
			"side":            attempt.Side,
			"attempt_status":  attempt.Status,
			"client_order_id": attempt.ClientOrderID,
		},
	}
	if order != nil {
		event.OrderID = order.OrderID
		event.Metadata["order_id"] = order.OrderID
		event.Metadata["exchange_order_id"] = order.ExchangeOrderID
		event.Metadata["order_status"] = order.Status
	}
	if err := s.notifier.Publish(ctx, event); err != nil {
		logger.Warn(ctx, "system", fmt.Sprintf("publish order notification failed: attempt_id=%s err=%v", attempt.AttemptID, err))
	}
}

func orderNotificationClass(status string) (eventType, severity, title string, ok bool) {
	switch status {
	case attemptStatusAccepted, attemptStatusRecovered:
		if status == attemptStatusRecovered {
			return ordernotify.EventOrderRecovered, ordernotify.SeverityInfo, "Order recovered", true
		}
		return ordernotify.EventOrderAccepted, ordernotify.SeverityInfo, "Order accepted", true
	case attemptStatusFailed, attemptStatusRecoveryFailed:
		if status == attemptStatusRecoveryFailed {
			return ordernotify.EventOrderRecoveryFailed, ordernotify.SeverityError, "Order recovery failed", true
		}
		return ordernotify.EventOrderFailed, ordernotify.SeverityError, "Order failed", true
	case attemptStatusRecovering, attemptStatusUnknown:
		return ordernotify.EventOrderRecovering, ordernotify.SeverityWarn, "Order recovering", true
	default:
		return "", "", "", false
	}
}

func orderNotificationMessage(attempt repository.OrderAttempt) string {
	base := fmt.Sprintf("%s %s qty=%g status=%s", attempt.Side, attempt.Symbol, attempt.RequestedQty, attempt.Status)
	if msg := strings.TrimSpace(nonEmpty(attempt.ErrorMessage, attempt.RecoveryError)); msg != "" {
		return fmt.Sprintf("%s: %s", base, msg)
	}
	return base
}

func buildPersistedExecution(
	meta accountmeta.Meta,
	attempt repository.OrderAttempt,
	strategyID int64,
	sessionID string,
	market int32,
	result executor.OrderResult,
) (*repository.Order, []repository.OrderFill) {
	now := time.Now().UTC()
	localOrderID := nonEmpty(attempt.OrderID, uuid.New().String())
	order := &repository.Order{
		OrderID:         localOrderID,
		ExchangeOrderID: result.ExchangeOrderID,
		ClientOrderID:   nonEmpty(result.ClientOrderID, attempt.ClientOrderID),
		AttemptID:       attempt.AttemptID,
		IntentID:        attempt.IntentID,
		Time:            now,
		AccountID:       attempt.AccountID,
		VenueID:         attempt.VenueID,
		UserID:          meta.UserID,
		StrategyID:      strategyID,
		SessionID:       sessionID,
		Environment:     attempt.Environment,
		Exchange:        attempt.Exchange,
		Market:          market,
		PositionSide:    attempt.PositionSide,
		Symbol:          nonEmpty(result.Symbol, attempt.Symbol),
		Side:            nonEmpty(result.Side, attempt.Side),
		OrigQty:         fallbackPositive(result.OrigQty, abs(attempt.RequestedQty)),
		ExecutedQty:     result.ExecutedQty,
		RemainingQty:    fallbackRemaining(result.OrigQty, result.ExecutedQty, result.RemainingQty, abs(attempt.RequestedQty)),
		AvgPrice:        result.AvgPrice,
		Price:           result.Price,
		Status:          nonEmpty(result.Status, "NEW"),
		ErrorMessage:    result.ErrorMessage,
	}

	fills := make([]repository.OrderFill, 0, len(result.Fills))
	for _, fill := range result.Fills {
		fillStatus := nonEmpty(result.Status, "FILLED")
		if fill.FeeMissing {
			fillStatus = "FEE_MISSING"
		}
		fills = append(fills, repository.OrderFill{
			FillID:          uuid.New().String(),
			ExchangeTradeID: fill.ExchangeTradeID,
			OrderID:         localOrderID,
			ExchangeOrderID: result.ExchangeOrderID,
			AttemptID:       attempt.AttemptID,
			IntentID:        attempt.IntentID,
			Time:            now,
			AccountID:       attempt.AccountID,
			VenueID:         attempt.VenueID,
			UserID:          meta.UserID,
			Environment:     attempt.Environment,
			Exchange:        attempt.Exchange,
			Market:          market,
			PositionSide:    attempt.PositionSide,
			Symbol:          order.Symbol,
			Side:            order.Side,
			Qty:             fill.Qty,
			FillPrice:       fill.FillPrice,
			Fee:             fill.Fee,
			Status:          fillStatus,
			StrategyID:      strategyID,
			SessionID:       sessionID,
		})
	}
	return order, fills
}

func (s *OrderGRPCService) loadPersistedExecution(ctx context.Context, attemptID string) (*repository.Order, []repository.OrderFill) {
	order, err := s.repo.FindOrderByAttempt(ctx, attemptID)
	if err != nil && err != repository.ErrNotFound {
		logger.Error(ctx, "system", fmt.Sprintf("load order by attempt failed: attempt_id=%s err=%v", attemptID, err))
	}
	var orderPtr *repository.Order
	if err == nil {
		orderCopy := order
		orderPtr = &orderCopy
	}
	fills, fillErr := s.repo.ListOrderFillsByAttempt(ctx, attemptID)
	if fillErr != nil {
		logger.Error(ctx, "system", fmt.Sprintf("list fills by attempt failed: attempt_id=%s err=%v", attemptID, fillErr))
		return orderPtr, nil
	}
	return orderPtr, fills
}

func buildPlaceOrderResponse(attempt repository.OrderAttempt, order *repository.Order, fills []repository.OrderFill) *orderv1.PlaceOrderResponse {
	resp := &orderv1.PlaceOrderResponse{
		IntentId:      attempt.IntentID,
		AttemptId:     attempt.AttemptID,
		AttemptStatus: attempt.Status,
		ErrorMessage:  nonEmpty(attempt.ErrorMessage, attempt.RecoveryError),
		ClientOrderId: attempt.ClientOrderID,
	}
	if order != nil {
		resp.Order = toProtoOrder(*order)
	}
	if len(fills) > 0 {
		resp.FillDeltas = make([]*orderv1.OrderFillEntry, 0, len(fills))
		for _, f := range fills {
			resp.FillDeltas = append(resp.FillDeltas, toProtoFill(f))
		}
	}
	return resp
}

func buildRecoveryFailedResponse(attempt repository.OrderAttempt, recoveryErr string) *orderv1.PlaceOrderResponse {
	attempt.Status = attemptStatusRecoveryFailed
	attempt.RecoveryError = recoveryErr
	if strings.TrimSpace(attempt.ErrorMessage) == "" {
		attempt.ErrorMessage = "recovery failed"
	}
	return buildPlaceOrderResponse(attempt, nil, nil)
}

func buildClientOrderID(intentID, attemptID string) string {
	intentPart := strings.ReplaceAll(strings.TrimSpace(intentID), "-", "")
	attemptPart := strings.ReplaceAll(strings.TrimSpace(attemptID), "-", "")
	if len(intentPart) > 12 {
		intentPart = intentPart[:12]
	}
	if len(attemptPart) > 12 {
		attemptPart = attemptPart[:12]
	}
	return fmt.Sprintf("h-%s-%s", intentPart, attemptPart)
}

func toProtoIntent(item repository.OrderIntent) *orderv1.OrderIntentEntry {
	return &orderv1.OrderIntentEntry{
		Time:           timestamppb.New(item.Time),
		IntentId:       item.IntentID,
		AccountId:      item.AccountID,
		VenueId:        item.VenueID,
		Symbol:         item.Symbol,
		Side:           item.Side,
		RequestedQty:   item.RequestedQty,
		RequestedPrice: item.RequestedPrice,
		StrategyId:     item.StrategyID,
		Environment:    item.Environment,
		Exchange:       item.Exchange,
		Market:         item.Market,
		PositionSide:   item.PositionSide,
		SessionId:      item.SessionID,
	}
}

func toProtoAttempt(item repository.OrderAttempt) *orderv1.OrderAttemptEntry {
	return &orderv1.OrderAttemptEntry{
		Time:            timestamppb.New(item.Time),
		AttemptId:       item.AttemptID,
		IntentId:        item.IntentID,
		OrderId:         item.OrderID,
		ExchangeOrderId: item.ExchangeOrderID,
		AccountId:       item.AccountID,
		Symbol:          item.Symbol,
		Side:            item.Side,
		RequestedQty:    item.RequestedQty,
		RequestedPrice:  item.RequestedPrice,
		MarkPrice:       item.MarkPrice,
		Status:          item.Status,
		Environment:     item.Environment,
		ErrorMessage:    item.ErrorMessage,
		StrategyId:      item.StrategyID,
		Market:          item.Market,
		SessionId:       item.SessionID,
		ClientOrderId:   item.ClientOrderID,
		RecoveryError:   item.RecoveryError,
		VenueId:         item.VenueID,
		Exchange:        item.Exchange,
		PositionSide:    item.PositionSide,
	}
}

func toProtoOrder(item repository.Order) *orderv1.ExchangeOrderEntry {
	return &orderv1.ExchangeOrderEntry{
		Time:            timestamppb.New(item.Time),
		OrderId:         item.OrderID,
		ExchangeOrderId: item.ExchangeOrderID,
		ClientOrderId:   item.ClientOrderID,
		AttemptId:       item.AttemptID,
		IntentId:        item.IntentID,
		AccountId:       item.AccountID,
		Symbol:          item.Symbol,
		Side:            item.Side,
		OrigQty:         item.OrigQty,
		ExecutedQty:     item.ExecutedQty,
		RemainingQty:    item.RemainingQty,
		AvgPrice:        item.AvgPrice,
		Status:          item.Status,
		Environment:     item.Environment,
		ErrorMessage:    item.ErrorMessage,
		StrategyId:      item.StrategyID,
		Market:          item.Market,
		SessionId:       item.SessionID,
		Price:           item.Price,
		VenueId:         item.VenueID,
		Exchange:        item.Exchange,
		PositionSide:    item.PositionSide,
	}
}

func toProtoFill(item repository.OrderFill) *orderv1.OrderFillEntry {
	return &orderv1.OrderFillEntry{
		Time:            timestamppb.New(item.Time),
		FillId:          item.FillID,
		ExchangeTradeId: item.ExchangeTradeID,
		OrderId:         item.OrderID,
		ExchangeOrderId: item.ExchangeOrderID,
		AttemptId:       item.AttemptID,
		IntentId:        item.IntentID,
		AccountId:       item.AccountID,
		Symbol:          item.Symbol,
		Side:            item.Side,
		Qty:             item.Qty,
		FillPrice:       item.FillPrice,
		Fee:             item.Fee,
		Status:          item.Status,
		Environment:     item.Environment,
		StrategyId:      item.StrategyID,
		Market:          item.Market,
		SessionId:       item.SessionID,
		VenueId:         item.VenueID,
		Exchange:        item.Exchange,
		PositionSide:    item.PositionSide,
	}
}

func toProtoLifecycleEvent(item lifecycle.Event) *orderv1.OrderLifecycleEventEntry {
	return &orderv1.OrderLifecycleEventEntry{
		EventId:         item.EventID,
		SessionId:       item.SessionID,
		AccountId:       item.AccountID,
		VenueId:         item.VenueID,
		Environment:     item.Environment,
		Exchange:        item.Exchange,
		Market:          item.Market,
		PositionSide:    item.PositionSide,
		Side:            item.Side,
		IntentId:        item.IntentID,
		AttemptId:       item.AttemptID,
		OrderId:         item.OrderID,
		ExchangeOrderId: item.ExchangeOrderID,
		ExchangeTradeId: item.ExchangeTradeID,
		EventType:       item.EventType,
		OrderStatus:     item.OrderStatus,
		FillDelta: &orderv1.FillDeltaEntry{
			ExchangeTradeId: item.FillDelta.ExchangeTradeID,
			ExchangeOrderId: item.FillDelta.ExchangeOrderID,
			Symbol:          item.FillDelta.Symbol,
			Qty:             item.FillDelta.Qty,
			FillPrice:       item.FillDelta.FillPrice,
			Fee:             item.FillDelta.Fee,
			FeeAsset:        item.FillDelta.FeeAsset,
			FeeMissing:      item.FillDelta.FeeMissing,
			TradeTime:       timestampOrNil(item.FillDelta.TradeTime),
		},
		OrderState: &orderv1.OrderStateEntry{
			ExchangeOrderId: item.OrderState.ExchangeOrderID,
			ClientOrderId:   item.OrderState.ClientOrderID,
			Symbol:          item.OrderState.Symbol,
			Status:          item.OrderState.Status,
			OrigQty:         item.OrderState.OrigQty,
			ExecutedQty:     item.OrderState.ExecutedQty,
			RemainingQty:    item.OrderState.RemainingQty,
			AvgPrice:        item.OrderState.AvgPrice,
			UpdatedAt:       timestampOrNil(item.OrderState.UpdatedAt),
		},
		OccurredAt: timestampOrNil(item.OccurredAt),
		CreatedAt:  timestampOrNil(item.CreatedAt),
	}
}

func timestampOrNil(value time.Time) *timestamppb.Timestamp {
	if value.IsZero() {
		return nil
	}
	return timestamppb.New(value)
}

func fallbackPositive(primary, fallback float64) float64 {
	if primary > 0 {
		return primary
	}
	return fallback
}

func fallbackRemaining(origQty, executedQty, remainingQty, fallbackOrig float64) float64 {
	if remainingQty >= 0 {
		return remainingQty
	}
	orig := fallbackPositive(origQty, fallbackOrig)
	if orig <= executedQty {
		return 0
	}
	return orig - executedQty
}

func nonEmpty(v, fallback string) string {
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	return v
}

func abs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

func logSaveFailure(ctx context.Context, msg string, err error) {
	logger.Error(ctx, "system", fmt.Sprintf("%s: %v", msg, err))
}

func validateRouteRequest(exchange, market, positionSide int32) error {
	switch exchange {
	case exchangeBinance, exchangeOKX:
	default:
		return status.Errorf(codes.InvalidArgument, "unsupported exchange: %d", exchange)
	}
	switch market {
	case marketSpot, marketPerpetualFutures, marketDeliveryFutures:
	default:
		return status.Errorf(codes.InvalidArgument, "unsupported market: %d", market)
	}
	switch positionSide {
	case positionSideBoth, positionSideLong, positionSideShort:
		return nil
	default:
		return status.Errorf(codes.InvalidArgument, "unsupported position_side: %d", positionSide)
	}
}

func normalizeOrderContract(req *orderv1.PlaceOrderRequest) (int32, string, string, error) {
	orderType := strings.ToUpper(strings.TrimSpace(req.GetOrderType()))
	if orderType == "" {
		if req.Price != nil {
			orderType = "LIMIT"
		} else {
			orderType = "MARKET"
		}
	}
	switch orderType {
	case "MARKET":
		if req.Price != nil {
			return 0, "", "", status.Error(codes.InvalidArgument, "market order must not set price")
		}
		return orderTypeMarket, "MARKET", "", nil
	case "LIMIT":
		if req.Price == nil || req.GetPrice() <= 0 {
			return 0, "", "", status.Error(codes.InvalidArgument, "limit order requires positive price")
		}
		tif := strings.ToUpper(strings.TrimSpace(req.GetTimeInForce()))
		if tif == "" {
			tif = "GTC"
		}
		if tif != "GTC" {
			return 0, "", "", status.Errorf(codes.FailedPrecondition, "unsupported time_in_force: %s", tif)
		}
		return orderTypeLimit, "LIMIT", tif, nil
	default:
		return 0, "", "", status.Errorf(codes.FailedPrecondition, "unsupported order_type: %s", orderType)
	}
}

func validatePositionSide(meta accountmeta.Meta, positionSide int32) error {
	if meta.Market == marketSpot {
		if positionSide != positionSideBoth {
			return status.Error(codes.InvalidArgument, "spot orders must not set position_side")
		}
		return nil
	}
	if strings.EqualFold(meta.PositionMode, "hedge") {
		return status.Error(codes.FailedPrecondition, "hedge futures orders are not supported yet")
	}
	if positionSide != positionSideBoth {
		return status.Error(codes.InvalidArgument, "one-way futures orders must use position_side=BOTH")
	}
	return nil
}
