package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/hushine-tech/core-service/internal/domain"
	exchangeadapter "github.com/hushine-tech/core-service/internal/exchange/adapter"
	"github.com/hushine-tech/core-service/internal/order/accountmeta"
)

const adapterRecoveredFillQtyEpsilon = 1e-12

// AdapterRouter dispatches order operations through the exchange capability registry.
type AdapterRouter struct {
	registry *exchangeadapter.Registry
}

func NewAdapterRouter(registry *exchangeadapter.Registry) *AdapterRouter {
	return &AdapterRouter{registry: registry}
}

func (r *AdapterRouter) Execute(ctx context.Context, req OrderRequest, meta accountmeta.Meta) (OrderResult, error) {
	route := routeFromMeta(meta)
	exec, err := r.registry.OrderExecutor(route)
	if err != nil {
		return failedOrderResult(err), nil
	}
	credential, err := r.parseCredential(ctx, route, meta)
	if err != nil {
		return failedOrderResult(err), nil
	}
	result, err := exec.PlaceOrder(ctx, toAdapterOrderRequest(req, meta, credential))
	if err != nil {
		return OrderResult{}, err
	}
	return fromAdapterOrderResult(result), nil
}

func (r *AdapterRouter) Resolve(ctx context.Context, req RecoveryRequest, meta accountmeta.Meta) (OrderResult, error) {
	route := routeFromMeta(meta)
	reader, err := r.registry.OrderStateReader(route)
	if err != nil {
		return OrderResult{}, err
	}
	credential, err := r.parseCredential(ctx, route, meta)
	if err != nil {
		return OrderResult{}, err
	}

	query := exchangeadapter.QueryOrderRequest{
		AccountID:       req.AccountID,
		VenueID:         meta.VenueID,
		Symbol:          req.Symbol,
		ClientOrderID:   req.ClientOrderID,
		ExchangeOrderID: req.ExchangeOrderID,
		Credential:      credential,
	}
	state, err := reader.QueryOrder(ctx, query)
	if err != nil {
		return OrderResult{}, err
	}
	trades, err := reader.QueryTrades(ctx, exchangeadapter.QueryTradesRequest{
		AccountID:       req.AccountID,
		VenueID:         meta.VenueID,
		Symbol:          firstNonEmpty(req.Symbol, state.Symbol),
		ExchangeOrderID: firstNonEmpty(req.ExchangeOrderID, state.ExchangeOrderID),
		Credential:      credential,
	})
	if err != nil {
		out := fromAdapterOrderState(state, nil)
		out.FillPending = true
		out.ErrorMessage = err.Error()
		return out, nil
	}
	if pendingMsg := recoveredFillPendingMessage(state, trades); pendingMsg != "" {
		out := fromAdapterOrderState(state, nil)
		out.FillPending = true
		out.ErrorMessage = pendingMsg
		return out, nil
	}
	return fromAdapterOrderState(state, trades), nil
}

func (r *AdapterRouter) parseCredential(ctx context.Context, route exchangeadapter.Route, meta accountmeta.Meta) (exchangeadapter.ParsedCredential, error) {
	if route.Environment == domain.EnvironmentBacktest {
		return exchangeadapter.ParsedCredential{Exchange: route.Exchange, Environment: route.Environment}, nil
	}
	raw, metadata, err := credentialPayload(meta)
	if err != nil {
		return exchangeadapter.ParsedCredential{}, err
	}
	validator, err := r.registry.CredentialValidator(route)
	if err != nil {
		return exchangeadapter.ParsedCredential{}, err
	}
	parsed, err := validator.ValidateCredential(ctx, raw)
	if err != nil {
		return exchangeadapter.ParsedCredential{}, err
	}
	if parsed.Metadata == nil {
		parsed.Metadata = metadata
	}
	for key, value := range metadata {
		if strings.TrimSpace(parsed.Metadata[key]) == "" {
			parsed.Metadata[key] = value
		}
	}
	return parsed, nil
}

func credentialPayload(meta accountmeta.Meta) (json.RawMessage, map[string]string, error) {
	payload := map[string]any{}
	if raw := strings.TrimSpace(meta.CredentialJSON); raw != "" {
		if err := json.Unmarshal([]byte(raw), &payload); err != nil {
			return nil, nil, fmt.Errorf("invalid credential json: %w", err)
		}
	}
	if strings.TrimSpace(meta.APIKey) != "" {
		payload["api_key"] = strings.TrimSpace(meta.APIKey)
	}
	if strings.TrimSpace(meta.APISecret) != "" {
		payload["api_secret"] = strings.TrimSpace(meta.APISecret)
	}
	apiKey, _ := payload["api_key"].(string)
	apiSecret, _ := payload["api_secret"].(string)
	if strings.TrimSpace(apiKey) == "" || strings.TrimSpace(apiSecret) == "" {
		return nil, nil, fmt.Errorf("credential requires api_key and api_secret")
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal credential json: %w", err)
	}
	return raw, map[string]string{
		"api_key":    strings.TrimSpace(apiKey),
		"api_secret": strings.TrimSpace(apiSecret),
	}, nil
}

func routeFromMeta(meta accountmeta.Meta) exchangeadapter.Route {
	return exchangeadapter.Route{
		Exchange:    domain.Exchange(meta.Exchange),
		Environment: domain.Environment(meta.Environment),
		Market:      domain.Market(meta.Market),
	}
}

func toAdapterOrderRequest(req OrderRequest, meta accountmeta.Meta, credential exchangeadapter.ParsedCredential) exchangeadapter.OrderRequest {
	return exchangeadapter.OrderRequest{
		UserID:         meta.UserID,
		AccountID:      req.AccountID,
		VenueID:        meta.VenueID,
		Exchange:       domain.Exchange(meta.Exchange),
		Environment:    domain.Environment(meta.Environment),
		Market:         domain.Market(meta.Market),
		Symbol:         req.Symbol,
		Side:           req.Side,
		PositionSide:   positionSideText(req.PositionSide),
		MarginMode:     marginModeDomain(meta.MarginMode),
		PositionMode:   positionModeDomain(meta.PositionMode),
		OrderType:      req.OrderType,
		TimeInForce:    req.TimeInForce,
		PostOnly:       req.PostOnly,
		GoodTillDate:   req.GoodTillDate,
		ReduceOnly:     req.ReduceOnly,
		Qty:            req.Qty,
		Price:          req.Price,
		MarkPrice:      req.MarkPrice,
		DefaultFeeRate: meta.DefaultFeeRate,
		SlippageBps:    meta.SlippageBps,
		ClientOrderID:  req.ClientOrderID,
		Credential:     credential,
	}
}

func fromAdapterOrderResult(result exchangeadapter.OrderResult) OrderResult {
	fills := make([]FillResult, 0, len(result.Fills))
	for _, fill := range result.Fills {
		fills = append(fills, FillResult{
			ExchangeTradeID: fill.ExchangeTradeID,
			Qty:             fill.Qty,
			FillPrice:       fill.FillPrice,
			Fee:             fill.Fee,
			FeeMissing:      fill.FeeMissing,
		})
	}
	return OrderResult{
		ExchangeOrderID: result.ExchangeOrderID,
		ClientOrderID:   result.ClientOrderID,
		Symbol:          result.Symbol,
		Side:            result.Side,
		OrderType:       result.OrderType,
		TimeInForce:     result.TimeInForce,
		Status:          result.Status,
		OrigQty:         result.OrigQty,
		ExecutedQty:     result.ExecutedQty,
		RemainingQty:    result.RemainingQty,
		AvgPrice:        result.AvgPrice,
		Price:           result.Price,
		Fills:           fills,
		ErrorMessage:    result.ErrorMessage,
		FillPending:     result.FillPending,
	}
}

func fromAdapterOrderState(state exchangeadapter.OrderState, trades []exchangeadapter.FillDelta) OrderResult {
	fills := make([]FillResult, 0, len(trades))
	for _, fill := range trades {
		fills = append(fills, FillResult{
			ExchangeTradeID: fill.ExchangeTradeID,
			Qty:             fill.Qty,
			FillPrice:       fill.FillPrice,
			Fee:             fill.Fee,
			FeeMissing:      fill.FeeMissing,
		})
	}
	return OrderResult{
		ExchangeOrderID: state.ExchangeOrderID,
		ClientOrderID:   state.ClientOrderID,
		Symbol:          state.Symbol,
		Status:          state.Status,
		OrigQty:         state.OrigQty,
		ExecutedQty:     state.ExecutedQty,
		RemainingQty:    state.RemainingQty,
		AvgPrice:        state.AvgPrice,
		Fills:           fills,
	}
}

func recoveredFillPendingMessage(state exchangeadapter.OrderState, trades []exchangeadapter.FillDelta) string {
	if state.ExecutedQty <= 0 {
		return ""
	}
	if len(trades) == 0 {
		return fmt.Sprintf("order trades pending: executed_qty=%g trade_qty=0", state.ExecutedQty)
	}
	tradeQty := 0.0
	for _, fill := range trades {
		tradeQty += math.Abs(fill.Qty)
	}
	if math.Abs(tradeQty-state.ExecutedQty) > adapterRecoveredFillQtyEpsilon {
		return fmt.Sprintf("order trades inconsistent: executed_qty=%g trade_qty=%g", state.ExecutedQty, tradeQty)
	}
	return ""
}

func failedOrderResult(err error) OrderResult {
	return OrderResult{Status: "FAILED", ErrorMessage: err.Error()}
}

func positionSideText(positionSide int32) string {
	switch positionSide {
	case 1:
		return "LONG"
	case 2:
		return "SHORT"
	default:
		return "BOTH"
	}
}

func marginModeDomain(value string) domain.MarginMode {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "cross":
		return domain.MarginModeCross
	case "isolated":
		return domain.MarginModeIsolated
	default:
		return domain.MarginModeNone
	}
}

func positionModeDomain(value string) domain.PositionMode {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "one_way":
		return domain.PositionModeOneWay
	case "hedge":
		return domain.PositionModeHedge
	default:
		return domain.PositionModeNone
	}
}
