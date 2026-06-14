package binance

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/exchange/adapter"
)

type orderCanceller struct {
	route      adapter.Route
	baseURL    string
	httpClient *http.Client
}

func (c orderCanceller) CancelOrder(ctx context.Context, req adapter.CancelOrderRequest) (adapter.CancelOrderResult, error) {
	if c.route.Environment != domain.EnvironmentBacktest {
		return c.cancelRemote(ctx, req)
	}
	return adapter.CancelOrderResult{
		ExchangeOrderID: strings.TrimSpace(req.ExchangeOrderID),
		ClientOrderID:   strings.TrimSpace(req.ClientOrderID),
		Symbol:          strings.ToUpper(strings.TrimSpace(req.Symbol)),
		Status:          "CANCELED",
		CancelledAt:     time.Now().UTC(),
	}, nil
}

type cancelOrderResponse struct {
	OrderID       int64  `json:"orderId"`
	ClientOrderID string `json:"clientOrderId"`
	Symbol        string `json:"symbol"`
	Status        string `json:"status"`
	Code          int    `json:"code"`
	Msg           string `json:"msg"`
}

func (c orderCanceller) cancelRemote(ctx context.Context, req adapter.CancelOrderRequest) (adapter.CancelOrderResult, error) {
	symbol := strings.ToUpper(strings.TrimSpace(req.Symbol))
	if symbol == "" {
		return adapter.CancelOrderResult{}, fmt.Errorf("cancel order requires symbol")
	}
	params := url.Values{}
	params.Set("symbol", symbol)
	if clientOrderID := strings.TrimSpace(req.ClientOrderID); clientOrderID != "" {
		params.Set("origClientOrderId", clientOrderID)
	} else if exchangeOrderID := strings.TrimSpace(req.ExchangeOrderID); exchangeOrderID != "" {
		params.Set("orderId", exchangeOrderID)
	} else {
		return adapter.CancelOrderResult{}, fmt.Errorf("cancel order requires client_order_id or exchange_order_id")
	}
	params.Set("timestamp", fmt.Sprintf("%d", time.Now().UnixMilli()))

	apiKey := req.Credential.Metadata["api_key"]
	apiSecret := req.Credential.Metadata["api_secret"]
	if strings.TrimSpace(apiKey) == "" || strings.TrimSpace(apiSecret) == "" {
		return adapter.CancelOrderResult{}, fmt.Errorf("%w: missing api_key or api_secret", ErrInvalidCredential)
	}
	query := params.Encode()
	params.Set("signature", signQuery(query, apiSecret))

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, strings.TrimRight(c.baseURL, "/")+c.orderPath()+"?"+params.Encode(), nil)
	if err != nil {
		return adapter.CancelOrderResult{}, err
	}
	httpReq.Header.Set("X-MBX-APIKEY", apiKey)

	httpClient := c.httpClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	resp, err := httpClient.Do(httpReq)
	if err != nil {
		return adapter.CancelOrderResult{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return adapter.CancelOrderResult{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return adapter.CancelOrderResult{}, fmt.Errorf("binance cancel HTTP %d: %s", resp.StatusCode, string(body))
	}

	var raw cancelOrderResponse
	if err := json.Unmarshal(body, &raw); err != nil {
		return adapter.CancelOrderResult{}, fmt.Errorf("decode binance cancel response: %w", err)
	}
	if raw.Code != 0 {
		return adapter.CancelOrderResult{}, fmt.Errorf("binance cancel error %d: %s", raw.Code, raw.Msg)
	}

	return adapter.CancelOrderResult{
		ExchangeOrderID: fmt.Sprintf("%d", raw.OrderID),
		ClientOrderID:   raw.ClientOrderID,
		Symbol:          raw.Symbol,
		Status:          raw.Status,
		CancelledAt:     time.Now().UTC(),
	}, nil
}

func (c orderCanceller) orderPath() string {
	if c.route.Market == domain.MarketSpot {
		return "/api/v3/order"
	}
	return "/fapi/v1/order"
}

func signQuery(query, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(query))
	return hex.EncodeToString(mac.Sum(nil))
}
