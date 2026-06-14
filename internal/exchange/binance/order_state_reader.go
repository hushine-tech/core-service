package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/hushine-tech/core-service/internal/exchange/adapter"
	"github.com/hushine-tech/core-service/internal/order/accountmeta"
	orderexecutor "github.com/hushine-tech/core-service/internal/order/executor"
)

type orderStateReader struct {
	exec interface {
		Resolve(context.Context, orderexecutor.RecoveryRequest, accountmeta.Meta) (orderexecutor.OrderResult, error)
	}
}

func (r orderStateReader) QueryOrder(ctx context.Context, req adapter.QueryOrderRequest) (adapter.OrderState, error) {
	result, err := r.exec.Resolve(ctx, orderexecutor.RecoveryRequest{
		AccountID:       req.AccountID,
		Symbol:          req.Symbol,
		ClientOrderID:   req.ClientOrderID,
		ExchangeOrderID: req.ExchangeOrderID,
	}, accountmeta.Meta{
		AccountID:      req.AccountID,
		VenueID:        req.VenueID,
		APIKey:         req.Credential.Metadata["api_key"],
		APISecret:      req.Credential.Metadata["api_secret"],
		CredentialJSON: string(req.Credential.Raw),
	})
	if err != nil {
		return adapter.OrderState{}, err
	}
	return adapter.OrderState{
		ExchangeOrderID: result.ExchangeOrderID,
		ClientOrderID:   result.ClientOrderID,
		Symbol:          result.Symbol,
		Status:          result.Status,
		OrigQty:         result.OrigQty,
		ExecutedQty:     result.ExecutedQty,
		RemainingQty:    result.RemainingQty,
		AvgPrice:        result.AvgPrice,
	}, nil
}

func (r orderStateReader) QueryTrades(ctx context.Context, req adapter.QueryTradesRequest) ([]adapter.FillDelta, error) {
	result, err := r.exec.Resolve(ctx, orderexecutor.RecoveryRequest{
		AccountID:       req.AccountID,
		Symbol:          req.Symbol,
		ExchangeOrderID: req.ExchangeOrderID,
	}, accountmeta.Meta{
		AccountID:      req.AccountID,
		VenueID:        req.VenueID,
		APIKey:         req.Credential.Metadata["api_key"],
		APISecret:      req.Credential.Metadata["api_secret"],
		CredentialJSON: string(req.Credential.Raw),
	})
	if err != nil {
		return nil, err
	}
	if result.FillPending {
		return nil, fmt.Errorf("order fills pending: %s", result.ErrorMessage)
	}
	return fromLegacyOrderResult(result).Fills, nil
}

type spotOrderStateReader struct {
	baseURL    string
	httpClient *http.Client
}

type spotTradeResponse struct {
	ID              int64  `json:"id"`
	OrderID         int64  `json:"orderId"`
	Price           string `json:"price"`
	Qty             string `json:"qty"`
	Commission      string `json:"commission"`
	CommissionAsset string `json:"commissionAsset"`
	Time            int64  `json:"time"`
}

func (r spotOrderStateReader) QueryOrder(ctx context.Context, req adapter.QueryOrderRequest) (adapter.OrderState, error) {
	params := url.Values{}
	symbol := strings.ToUpper(strings.TrimSpace(req.Symbol))
	if symbol == "" {
		return adapter.OrderState{}, fmt.Errorf("symbol is required")
	}
	params.Set("symbol", symbol)
	if strings.TrimSpace(req.ClientOrderID) != "" {
		params.Set("origClientOrderId", strings.TrimSpace(req.ClientOrderID))
	} else if strings.TrimSpace(req.ExchangeOrderID) != "" {
		params.Set("orderId", strings.TrimSpace(req.ExchangeOrderID))
	} else {
		return adapter.OrderState{}, orderexecutor.ErrOrderNotFound
	}
	params.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))

	body, statusCode, err := r.signedRequest(ctx, http.MethodGet, "/api/v3/order", params, req.Credential)
	if err != nil {
		return adapter.OrderState{}, err
	}
	if statusCode != http.StatusOK {
		if isSpotOrderNotFound(body) {
			return adapter.OrderState{}, orderexecutor.ErrOrderNotFound
		}
		return adapter.OrderState{}, fmt.Errorf("HTTP %d: %s", statusCode, string(body))
	}

	var raw spotOrderResponse
	if err := json.Unmarshal(body, &raw); err != nil {
		return adapter.OrderState{}, fmt.Errorf("decode binance spot order query response: %w", err)
	}
	if raw.Code != 0 {
		if isSpotOrderNotFound(body) {
			return adapter.OrderState{}, orderexecutor.ErrOrderNotFound
		}
		return adapter.OrderState{}, fmt.Errorf("binance error %d: %s", raw.Code, raw.Msg)
	}
	return spotOrderStateFromResponse(raw, symbol)
}

func (r spotOrderStateReader) QueryTrades(ctx context.Context, req adapter.QueryTradesRequest) ([]adapter.FillDelta, error) {
	symbol := strings.ToUpper(strings.TrimSpace(req.Symbol))
	if symbol == "" {
		return nil, fmt.Errorf("symbol is required")
	}
	orderID := strings.TrimSpace(req.ExchangeOrderID)
	if orderID == "" {
		return nil, fmt.Errorf("exchange_order_id is required")
	}
	params := url.Values{}
	params.Set("symbol", symbol)
	params.Set("orderId", orderID)
	params.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))

	body, statusCode, err := r.signedRequest(ctx, http.MethodGet, "/api/v3/myTrades", params, req.Credential)
	if err != nil {
		return nil, err
	}
	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", statusCode, string(body))
	}
	var trades []spotTradeResponse
	if err := json.Unmarshal(body, &trades); err != nil {
		return nil, fmt.Errorf("decode binance spot trades response: %w", err)
	}
	out := make([]adapter.FillDelta, 0, len(trades))
	for _, trade := range trades {
		if trade.ID <= 0 {
			return nil, fmt.Errorf("invalid spot trade id")
		}
		qty, err := parseStrictPositiveFloat(trade.Qty, "spot trade qty")
		if err != nil {
			return nil, err
		}
		price, err := parseStrictPositiveFloat(trade.Price, "spot trade price")
		if err != nil {
			return nil, err
		}
		fee, err := parseStrictNonNegativeFloat(trade.Commission, "spot trade commission")
		if err != nil {
			return nil, err
		}
		tradeTime := time.Now().UTC()
		if trade.Time > 0 {
			tradeTime = time.UnixMilli(trade.Time).UTC()
		}
		out = append(out, adapter.FillDelta{
			ExchangeTradeID: strconv.FormatInt(trade.ID, 10),
			ExchangeOrderID: orderID,
			Symbol:          symbol,
			Qty:             qty,
			FillPrice:       price,
			Fee:             fee,
			FeeAsset:        trade.CommissionAsset,
			TradeTime:       tradeTime,
		})
	}
	return out, nil
}

func (r spotOrderStateReader) signedRequest(ctx context.Context, method, path string, params url.Values, credential adapter.ParsedCredential) ([]byte, int, error) {
	exec := orderExecutor{baseURL: r.baseURL, httpClient: r.httpClient}
	return exec.signedRequest(ctx, method, path, params, credential)
}

func spotOrderStateFromResponse(raw spotOrderResponse, fallbackSymbol string) (adapter.OrderState, error) {
	origQty, err := parseStrictPositiveFloat(raw.OrigQty, "spot order orig_qty")
	if err != nil {
		return adapter.OrderState{}, err
	}
	executedQty, err := parseStrictNonNegativeFloat(raw.ExecutedQty, "spot order executed_qty")
	if err != nil {
		return adapter.OrderState{}, err
	}
	remainingQty := origQty - executedQty
	if remainingQty < 0 {
		remainingQty = 0
	}
	avgPrice := spotAveragePrice(raw, executedQty, 0, 0)
	exchangeOrderID := ""
	if raw.OrderID != 0 {
		exchangeOrderID = strconv.FormatInt(raw.OrderID, 10)
	}
	updatedAt := time.Now().UTC()
	if raw.UpdateTime > 0 {
		updatedAt = time.UnixMilli(raw.UpdateTime).UTC()
	} else if raw.Time > 0 {
		updatedAt = time.UnixMilli(raw.Time).UTC()
	}
	return adapter.OrderState{
		ExchangeOrderID: exchangeOrderID,
		ClientOrderID:   raw.ClientOrderID,
		Symbol:          firstNonEmpty(raw.Symbol, fallbackSymbol),
		Status:          strings.ToUpper(firstNonEmpty(raw.Status, "NEW")),
		OrigQty:         origQty,
		ExecutedQty:     executedQty,
		RemainingQty:    remainingQty,
		AvgPrice:        avgPrice,
		UpdatedAt:       updatedAt,
	}, nil
}

func isSpotOrderNotFound(body []byte) bool {
	text := strings.ToLower(string(body))
	return strings.Contains(text, "order does not exist") || strings.Contains(text, "\"code\":-2013")
}

type simulatedOrderStateReader struct{}

func (simulatedOrderStateReader) QueryOrder(context.Context, adapter.QueryOrderRequest) (adapter.OrderState, error) {
	return adapter.OrderState{}, adapter.CapabilityUnsupported("order_state_reader")
}

func (simulatedOrderStateReader) QueryTrades(context.Context, adapter.QueryTradesRequest) ([]adapter.FillDelta, error) {
	return nil, adapter.CapabilityUnsupported("order_state_reader")
}
