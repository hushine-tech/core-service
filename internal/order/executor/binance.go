package executor

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/hushine-tech/golang-lib/middleware/httpclient"
	elog "github.com/hushine-tech/golang-lib/pkg/log"
	"github.com/hushine-tech/core-service/internal/order/accountmeta"
)

const (
	binanceLiveBaseURL    = "https://fapi.binance.com"
	binanceTestnetBaseURL = "https://testnet.binancefuture.com"

	defaultTradeLookupAttempts = 5
	defaultTradeLookupDelay    = 250 * time.Millisecond
	tradeQtyEpsilon            = 1e-12
)

// BinanceExecutor places real orders on Binance USDT-M futures via REST API.
type BinanceExecutor struct {
	baseURL             string
	httpClient          *httpclient.Client
	tradeLookupAttempts int
	tradeLookupDelay    time.Duration
}

func NewBinanceLiveExecutor(logger elog.Logger) *BinanceExecutor {
	return &BinanceExecutor{
		baseURL:             binanceLiveBaseURL,
		httpClient:          httpclient.New(&http.Client{Timeout: 10 * time.Second}, logger, "binance_order_live"),
		tradeLookupAttempts: defaultTradeLookupAttempts,
		tradeLookupDelay:    defaultTradeLookupDelay,
	}
}

func NewBinanceTestnetExecutor(logger elog.Logger) *BinanceExecutor {
	return &BinanceExecutor{
		baseURL:             binanceTestnetBaseURL,
		httpClient:          httpclient.New(&http.Client{Timeout: 10 * time.Second}, logger, "binance_order_testnet"),
		tradeLookupAttempts: defaultTradeLookupAttempts,
		tradeLookupDelay:    defaultTradeLookupDelay,
	}
}

type binanceOrderResponse struct {
	OrderID       int64  `json:"orderId"`
	ClientOrderID string `json:"clientOrderId"`
	Symbol        string `json:"symbol"`
	Side          string `json:"side"`
	OrigQty       string `json:"origQty"`
	AvgPrice      string `json:"avgPrice"`
	ExecutedQty   string `json:"executedQty"`
	Status        string `json:"status"` // "FILLED", "NEW", "REJECTED", ...
	Code          int    `json:"code"`   // non-zero on error
	Msg           string `json:"msg"`    // error message
}

type binanceTradeResponse struct {
	ID         int64  `json:"id"`
	OrderID    int64  `json:"orderId"`
	Price      string `json:"price"`
	Qty        string `json:"qty"`
	Commission string `json:"commission"`
}

func (e *BinanceExecutor) Execute(ctx context.Context, req OrderRequest, meta accountmeta.Meta) (OrderResult, error) {
	side, positionSide, err := normalizeBinanceOrderSide(req.Side, meta.PositionMode)
	if err != nil {
		return failed(req, err.Error()), nil
	}

	params := url.Values{}
	params.Set("symbol", strings.ToUpper(req.Symbol))
	params.Set("side", side)
	if positionSide != "" {
		params.Set("positionSide", positionSide)
	}
	params.Set("type", "MARKET")
	params.Set("newOrderRespType", "RESULT")
	params.Set("quantity", strconv.FormatFloat(req.Qty, 'f', -1, 64))
	if strings.TrimSpace(req.ClientOrderID) != "" {
		params.Set("newClientOrderId", strings.TrimSpace(req.ClientOrderID))
	}
	params.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
	body, statusCode, err := e.signedRequest(ctx, http.MethodPost, "/fapi/v1/order", params, meta)
	if err != nil {
		return OrderResult{}, err
	}
	if statusCode != http.StatusOK {
		return failed(req, fmt.Sprintf("HTTP %d: %s", statusCode, string(body))), nil
	}

	var orderResp binanceOrderResponse
	if err := json.Unmarshal(body, &orderResp); err != nil {
		return OrderResult{}, fmt.Errorf("parse response: %w", err)
	}

	// Binance may return error details in JSON body with non-zero Code even on 200.
	if orderResp.Code != 0 {
		return failed(req, fmt.Sprintf("binance error %d: %s", orderResp.Code, orderResp.Msg)), nil
	}

	result, err := e.orderResultFromResponse(ctx, orderResp, req, meta)
	if err != nil {
		return failed(req, err.Error()), nil
	}
	return result, nil
}

func (e *BinanceExecutor) Resolve(ctx context.Context, req RecoveryRequest, meta accountmeta.Meta) (OrderResult, error) {
	params := url.Values{}
	params.Set("symbol", strings.ToUpper(req.Symbol))
	if strings.TrimSpace(req.ClientOrderID) != "" {
		params.Set("origClientOrderId", strings.TrimSpace(req.ClientOrderID))
	} else if strings.TrimSpace(req.ExchangeOrderID) != "" {
		params.Set("orderId", strings.TrimSpace(req.ExchangeOrderID))
	} else {
		return OrderResult{}, ErrOrderNotFound
	}
	params.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))

	body, statusCode, err := e.signedRequest(ctx, http.MethodGet, "/fapi/v1/order", params, meta)
	if err != nil {
		return OrderResult{}, err
	}
	if statusCode != http.StatusOK {
		if isBinanceOrderNotFound(body) {
			return OrderResult{}, ErrOrderNotFound
		}
		return OrderResult{}, fmt.Errorf("HTTP %d: %s", statusCode, string(body))
	}

	var orderResp binanceOrderResponse
	if err := json.Unmarshal(body, &orderResp); err != nil {
		return OrderResult{}, fmt.Errorf("parse response: %w", err)
	}
	if orderResp.Code != 0 {
		if isBinanceOrderNotFound(body) {
			return OrderResult{}, ErrOrderNotFound
		}
		return OrderResult{}, fmt.Errorf("binance error %d: %s", orderResp.Code, orderResp.Msg)
	}
	return e.orderResultFromResponse(ctx, orderResp, OrderRequest{
		AccountID:     req.AccountID,
		Symbol:        req.Symbol,
		ClientOrderID: req.ClientOrderID,
	}, meta)
}

func normalizeBinanceOrderSide(side string, positionMode string) (string, string, error) {
	sideUpper := strings.ToUpper(strings.TrimSpace(side))
	mode := strings.ToLower(strings.TrimSpace(positionMode))
	if mode == "" {
		mode = "one_way"
	}

	switch mode {
	case "one_way":
		switch sideUpper {
		case "BUY", "SELL":
			return sideUpper, "", nil
		case "LONG":
			return "BUY", "", nil
		case "SHORT":
			return "SELL", "", nil
		default:
			return "", "", fmt.Errorf("unsupported order side for one-way mode: %q", side)
		}
	case "hedge":
		return "", "", fmt.Errorf("hedge position mode is not supported by binance executor: order side %q lacks explicit open/close intent", side)
	default:
		return "", "", fmt.Errorf("unsupported position mode: %q", positionMode)
	}
}

func sign(payload, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}

func failed(req OrderRequest, msg string) OrderResult {
	return OrderResult{
		Symbol:        req.Symbol,
		Side:          req.Side,
		Status:        "FAILED",
		ClientOrderID: req.ClientOrderID,
		OrigQty:       math.Abs(req.Qty),
		ExecutedQty:   0,
		RemainingQty:  math.Abs(req.Qty),
		ErrorMessage:  msg,
		Price: func() float64 {
			if req.Price != nil {
				return *req.Price
			}
			return 0
		}(),
	}
}

func (e *BinanceExecutor) orderResultFromResponse(ctx context.Context, orderResp binanceOrderResponse, req OrderRequest, meta accountmeta.Meta) (OrderResult, error) {
	fillPrice, err := strconv.ParseFloat(orderResp.AvgPrice, 64)
	if err != nil || fillPrice == 0 {
		fillPrice = req.MarkPrice
	}
	executedQty, err := strconv.ParseFloat(orderResp.ExecutedQty, 64)
	if err != nil {
		return OrderResult{}, fmt.Errorf("invalid executed quantity: %q", orderResp.ExecutedQty)
	}
	origQty, err := strconv.ParseFloat(orderResp.OrigQty, 64)
	if err != nil || origQty <= 0 {
		origQty = math.Abs(req.Qty)
	}
	if executedQty < 0 {
		return OrderResult{}, fmt.Errorf("invalid executed quantity: %q", orderResp.ExecutedQty)
	}
	remainingQty := origQty - executedQty
	if remainingQty < 0 {
		remainingQty = 0
	}

	status := strings.ToUpper(strings.TrimSpace(orderResp.Status))
	if status == "" {
		status = "NEW"
	}

	var fills []FillResult
	missingFeeMessage := ""
	fillPending := false
	if executedQty > 0 {
		var tradeErr error
		fills, tradeErr = e.queryTradesUntilComplete(ctx, strings.ToUpper(req.Symbol), orderResp.OrderID, executedQty, meta)
		if tradeErr != nil {
			missingFeeMessage = fmt.Sprintf("binance trade fee data not available after confirmed execution: %v", tradeErr)
			fillPending = true
			fills = nil
		}
	}

	return OrderResult{
		ExchangeOrderID: strconv.FormatInt(orderResp.OrderID, 10),
		ClientOrderID:   nonEmptyStr(orderResp.ClientOrderID, req.ClientOrderID),
		Symbol:          nonEmptyStr(orderResp.Symbol, req.Symbol),
		Side:            nonEmptyStr(orderResp.Side, req.Side),
		Status:          status,
		OrigQty:         origQty,
		ExecutedQty:     executedQty,
		RemainingQty:    remainingQty,
		AvgPrice:        fillPrice,
		Price: func() float64 {
			if req.Price != nil {
				return *req.Price
			}
			return 0
		}(),
		Fills:        fills,
		ErrorMessage: missingFeeMessage,
		FillPending:  fillPending,
	}, nil
}

func (e *BinanceExecutor) queryTradesUntilComplete(
	ctx context.Context,
	symbol string,
	orderID int64,
	executedQty float64,
	meta accountmeta.Meta,
) ([]FillResult, error) {
	attempts, delay := e.tradeLookupRetry()
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		fills, err := e.queryTrades(ctx, symbol, orderID, meta)
		if err == nil {
			tradeQty := totalFillQty(fills)
			if tradeQty+tradeQtyEpsilon >= executedQty {
				return fills, nil
			}
			lastErr = fmt.Errorf("incomplete trade details: executed_qty=%g trade_qty=%g", executedQty, tradeQty)
		} else {
			lastErr = err
		}

		if attempt < attempts && delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("incomplete trade details: executed_qty=%g trade_qty=0", executedQty)
	}
	return nil, lastErr
}

func (e *BinanceExecutor) tradeLookupRetry() (int, time.Duration) {
	attempts := e.tradeLookupAttempts
	if attempts <= 0 {
		attempts = defaultTradeLookupAttempts
	}
	delay := e.tradeLookupDelay
	if delay < 0 {
		delay = 0
	}
	return attempts, delay
}

func totalFillQty(fills []FillResult) float64 {
	total := 0.0
	for _, fill := range fills {
		total += math.Abs(fill.Qty)
	}
	return total
}

func (e *BinanceExecutor) queryTrades(ctx context.Context, symbol string, orderID int64, meta accountmeta.Meta) ([]FillResult, error) {
	if orderID == 0 {
		return nil, nil
	}
	params := url.Values{}
	params.Set("symbol", symbol)
	params.Set("orderId", strconv.FormatInt(orderID, 10))
	params.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
	body, statusCode, err := e.signedRequest(ctx, http.MethodGet, "/fapi/v1/userTrades", params, meta)
	if err != nil {
		return nil, err
	}
	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", statusCode, string(body))
	}
	var trades []binanceTradeResponse
	if err := json.Unmarshal(body, &trades); err != nil {
		return nil, fmt.Errorf("parse trades response: %w", err)
	}
	out := make([]FillResult, 0, len(trades))
	for _, trade := range trades {
		qty, err := strconv.ParseFloat(trade.Qty, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid trade qty: %q", trade.Qty)
		}
		price, err := strconv.ParseFloat(trade.Price, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid trade price: %q", trade.Price)
		}
		fee, err := strconv.ParseFloat(trade.Commission, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid trade commission: %q", trade.Commission)
		}
		out = append(out, FillResult{
			ExchangeTradeID: strconv.FormatInt(trade.ID, 10),
			Qty:             qty,
			FillPrice:       price,
			Fee:             fee,
		})
	}
	return out, nil
}

func (e *BinanceExecutor) signedRequest(ctx context.Context, method, path string, params url.Values, meta accountmeta.Meta) ([]byte, int, error) {
	sig := sign(params.Encode(), meta.APISecret)
	params.Set("signature", sig)

	endpoint := e.baseURL + path
	var body io.Reader
	if method == http.MethodGet {
		endpoint += "?" + params.Encode()
	} else {
		body = strings.NewReader(params.Encode())
	}
	httpReq, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return nil, 0, fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("X-MBX-APIKEY", meta.APIKey)
	httpReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := e.httpClient.Do(ctx, httpReq)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, fmt.Errorf("read body: %w", err)
	}
	return payload, resp.StatusCode, nil
}

func isBinanceOrderNotFound(body []byte) bool {
	text := strings.ToLower(string(body))
	return strings.Contains(text, "order does not exist") || strings.Contains(text, "\"code\":-2013")
}

func nonEmptyStr(primary, fallback string) string {
	if strings.TrimSpace(primary) != "" {
		return primary
	}
	return fallback
}
