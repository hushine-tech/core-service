package binance

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/exchange/adapter"
	"github.com/hushine-tech/core-service/internal/order/accountmeta"
	orderexecutor "github.com/hushine-tech/core-service/internal/order/executor"
	"github.com/hushine-tech/core-service/internal/order/lifecycle"
)

type orderExecutor struct {
	exec interface {
		Execute(context.Context, orderexecutor.OrderRequest, accountmeta.Meta) (orderexecutor.OrderResult, error)
	}
}

func (e orderExecutor) PlaceOrder(ctx context.Context, req adapter.OrderRequest) (adapter.OrderResult, error) {
	result, err := e.exec.Execute(ctx, toLegacyOrderRequest(req), toAccountMeta(req))
	if err != nil {
		return adapter.OrderResult{}, err
	}
	return fromLegacyOrderResult(result), nil
}

type simulatedOrderExecutor struct{}

func (simulatedOrderExecutor) PlaceOrder(_ context.Context, req adapter.OrderRequest) (adapter.OrderResult, error) {
	orderType := firstNonEmpty(req.OrderType, "MARKET")
	if strings.EqualFold(orderType, "LIMIT") {
		return placeSimulatedLimitOrder(req, orderType)
	}

	price := req.MarkPrice
	if req.Price != nil {
		price = *req.Price
	}
	if price <= 0 {
		return adapter.OrderResult{}, fmt.Errorf("simulated order requires positive mark or limit price")
	}
	qty := math.Abs(req.Qty)
	return adapter.OrderResult{
		ExchangeOrderID: fmt.Sprintf("sim-%d", time.Now().UnixNano()),
		ClientOrderID:   strings.TrimSpace(req.ClientOrderID),
		Symbol:          strings.ToUpper(strings.TrimSpace(req.Symbol)),
		Side:            strings.ToUpper(strings.TrimSpace(req.Side)),
		PositionSide:    req.PositionSide,
		OrderType:       orderType,
		TimeInForce:     req.TimeInForce,
		Status:          "FILLED",
		OrigQty:         qty,
		ExecutedQty:     qty,
		RemainingQty:    0,
		AvgPrice:        price,
		Price:           price,
		Fills: []adapter.FillDelta{
			{
				ExchangeTradeID: fmt.Sprintf("sim-trade-%d", time.Now().UnixNano()),
				Symbol:          strings.ToUpper(strings.TrimSpace(req.Symbol)),
				Qty:             qty,
				FillPrice:       price,
				TradeTime:       time.Now().UTC(),
			},
		},
	}, nil
}

func placeSimulatedLimitOrder(req adapter.OrderRequest, orderType string) (adapter.OrderResult, error) {
	limitPrice := 0.0
	if req.Price != nil {
		limitPrice = *req.Price
	}
	if limitPrice <= 0 {
		return adapter.OrderResult{}, fmt.Errorf("simulated limit order requires positive limit price")
	}

	exchangeOrderID := fmt.Sprintf("sim-%d", time.Now().UnixNano())
	timeInForce := firstNonEmpty(req.TimeInForce, "GTC")
	now := time.Now().UTC()
	match := lifecycle.MatchBacktestLimitGTC(lifecycle.BacktestLimitOrder{
		AccountID:       req.AccountID,
		VenueID:         req.VenueID,
		Environment:     int32(req.Environment),
		Exchange:        int32(req.Exchange),
		Market:          int32(req.Market),
		PositionSide:    positionSideCode(req.PositionSide),
		ExchangeOrderID: exchangeOrderID,
		ClientOrderID:   strings.TrimSpace(req.ClientOrderID),
		Symbol:          strings.ToUpper(strings.TrimSpace(req.Symbol)),
		Side:            strings.ToUpper(strings.TrimSpace(req.Side)),
		Qty:             req.Qty,
		LimitPrice:      limitPrice,
	}, lifecycle.BacktestBar{
		Symbol: strings.ToUpper(strings.TrimSpace(req.Symbol)),
		Time:   now,
		Open:   req.MarkPrice,
		High:   req.MarkPrice,
		Low:    req.MarkPrice,
		Close:  req.MarkPrice,
	})

	out := adapter.OrderResult{
		ExchangeOrderID: exchangeOrderID,
		ClientOrderID:   strings.TrimSpace(req.ClientOrderID),
		Symbol:          match.State.Symbol,
		Side:            strings.ToUpper(strings.TrimSpace(req.Side)),
		PositionSide:    req.PositionSide,
		OrderType:       orderType,
		TimeInForce:     timeInForce,
		Status:          match.State.Status,
		OrigQty:         match.State.OrigQty,
		ExecutedQty:     match.State.ExecutedQty,
		RemainingQty:    match.State.RemainingQty,
		AvgPrice:        match.State.AvgPrice,
		Price:           limitPrice,
	}
	if match.Event != nil {
		out.Fills = []adapter.FillDelta{{
			ExchangeTradeID: fmt.Sprintf("sim-trade-%d", time.Now().UnixNano()),
			ExchangeOrderID: exchangeOrderID,
			Symbol:          out.Symbol,
			Qty:             match.Event.FillDelta.Qty,
			FillPrice:       match.Event.FillDelta.FillPrice,
			Fee:             match.Event.FillDelta.Fee,
			TradeTime:       now,
		}}
	}
	return out, nil
}

func toLegacyOrderRequest(req adapter.OrderRequest) orderexecutor.OrderRequest {
	return orderexecutor.OrderRequest{
		AccountID:     req.AccountID,
		Exchange:      int32(req.Exchange),
		Market:        int32(req.Market),
		Symbol:        req.Symbol,
		Side:          req.Side,
		PositionSide:  positionSideCode(req.PositionSide),
		OrderType:     req.OrderType,
		TimeInForce:   req.TimeInForce,
		Qty:           req.Qty,
		Price:         req.Price,
		MarkPrice:     req.MarkPrice,
		ClientOrderID: req.ClientOrderID,
	}
}

func toAccountMeta(req adapter.OrderRequest) accountmeta.Meta {
	return accountmeta.Meta{
		AccountID:      req.AccountID,
		VenueID:        req.VenueID,
		UserID:         req.UserID,
		Environment:    int32(req.Environment),
		Exchange:       int32(req.Exchange),
		Market:         int32(req.Market),
		MarginMode:     marginModeText(req.MarginMode),
		PositionMode:   positionModeText(req.PositionMode),
		APIKey:         req.Credential.Metadata["api_key"],
		APISecret:      req.Credential.Metadata["api_secret"],
		CredentialJSON: string(req.Credential.Raw),
	}
}

func fromLegacyOrderResult(result orderexecutor.OrderResult) adapter.OrderResult {
	fills := make([]adapter.FillDelta, 0, len(result.Fills))
	for _, fill := range result.Fills {
		fills = append(fills, adapter.FillDelta{
			ExchangeTradeID: fill.ExchangeTradeID,
			Symbol:          result.Symbol,
			Qty:             fill.Qty,
			FillPrice:       fill.FillPrice,
			Fee:             fill.Fee,
			FeeMissing:      fill.FeeMissing,
		})
	}
	return adapter.OrderResult{
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

func positionSideCode(positionSide string) int32 {
	switch strings.ToUpper(strings.TrimSpace(positionSide)) {
	case "LONG":
		return 1
	case "SHORT":
		return 2
	default:
		return 0
	}
}

func marginModeText(mode domain.MarginMode) string {
	switch mode {
	case domain.MarginModeCross:
		return "cross"
	case domain.MarginModeIsolated:
		return "isolated"
	default:
		return ""
	}
}

func positionModeText(mode domain.PositionMode) string {
	switch mode {
	case domain.PositionModeOneWay:
		return "one_way"
	case domain.PositionModeHedge:
		return "hedge"
	default:
		return "one_way"
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}
