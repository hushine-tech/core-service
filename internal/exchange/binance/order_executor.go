package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/exchange/adapter"
	"github.com/hushine-tech/core-service/internal/order/accountmeta"
	orderexecutor "github.com/hushine-tech/core-service/internal/order/executor"
	"github.com/hushine-tech/core-service/internal/order/lifecycle"
)

const (
	binanceGTDMinLead  = 600 * time.Second
	spotFillQtyEpsilon = 1e-12
)

type orderExecutor struct {
	route      adapter.Route
	baseURL    string
	httpClient *http.Client
	exec       interface {
		Execute(context.Context, orderexecutor.OrderRequest, accountmeta.Meta) (orderexecutor.OrderResult, error)
	}
}

func (e orderExecutor) PlaceOrder(ctx context.Context, req adapter.OrderRequest) (adapter.OrderResult, error) {
	market := firstMarket(req.Market, e.route.Market)
	contract, err := normalizeBinanceOrderContract(req, market, e.supportsGTD(market))
	if err != nil {
		return failedAdapterOrder(req, err.Error()), nil
	}
	if market == domain.MarketSpot {
		return e.placeSpotOrder(ctx, req, contract)
	}
	if e.exec == nil {
		return failedAdapterOrder(req, "binance futures order executor is not configured"), nil
	}
	result, err := e.exec.Execute(ctx, toLegacyOrderRequest(req), toAccountMeta(req))
	if err != nil {
		return adapter.OrderResult{}, err
	}
	return fromLegacyOrderResult(result), nil
}

type simulatedOrderExecutor struct {
	route adapter.Route
}

func (e simulatedOrderExecutor) PlaceOrder(_ context.Context, req adapter.OrderRequest) (adapter.OrderResult, error) {
	market := firstMarket(req.Market, e.route.Market)
	if _, err := normalizeBinanceOrderContract(req, market, false); err != nil {
		return failedAdapterOrder(req, err.Error()), nil
	}
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
	slippageFactor := req.SlippageBps / 10000.0
	side, err := normalizeOrderSide(req.Side)
	if err != nil {
		return adapter.OrderResult{}, err
	}
	fillPrice := price
	if side == "BUY" {
		fillPrice = price * (1 + slippageFactor)
	} else {
		fillPrice = price * (1 - slippageFactor)
	}
	fillPrice = math.Round(fillPrice*1e8) / 1e8
	fee := qty * fillPrice * req.DefaultFeeRate
	return adapter.OrderResult{
		ExchangeOrderID: fmt.Sprintf("sim-%d", time.Now().UnixNano()),
		ClientOrderID:   strings.TrimSpace(req.ClientOrderID),
		Symbol:          strings.ToUpper(strings.TrimSpace(req.Symbol)),
		Side:            side,
		PositionSide:    req.PositionSide,
		OrderType:       orderType,
		TimeInForce:     req.TimeInForce,
		Status:          "FILLED",
		OrigQty:         qty,
		ExecutedQty:     qty,
		RemainingQty:    0,
		AvgPrice:        fillPrice,
		Price:           price,
		Fills: []adapter.FillDelta{
			{
				ExchangeTradeID: fmt.Sprintf("sim-trade-%d", time.Now().UnixNano()),
				Symbol:          strings.ToUpper(strings.TrimSpace(req.Symbol)),
				Qty:             qty,
				FillPrice:       fillPrice,
				Fee:             fee,
				TradeTime:       time.Now().UTC(),
			},
		},
	}, nil
}

func placeSimulatedLimitOrder(req adapter.OrderRequest, orderType string) (adapter.OrderResult, error) {
	side, err := normalizeOrderSide(req.Side)
	if err != nil {
		return adapter.OrderResult{}, err
	}
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
		Side:            side,
		Qty:             req.Qty,
		LimitPrice:      limitPrice,
		FeeRate:         req.DefaultFeeRate,
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
		Side:            side,
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

type binanceOrderContract struct {
	OrderType      string
	TimeInForce    string
	Price          *float64
	GoodTillDateMS string
	ReduceOnly     bool
}

func normalizeBinanceOrderContract(req adapter.OrderRequest, market domain.Market, supportsGTD bool) (binanceOrderContract, error) {
	orderType := strings.ToUpper(strings.TrimSpace(req.OrderType))
	if orderType == "" {
		if req.Price != nil {
			orderType = "LIMIT"
		} else {
			orderType = "MARKET"
		}
	}

	if market == domain.MarketSpot && req.ReduceOnly {
		// Spot reduce_only 是平台语义；SELL 的 unlocked quantity 校验留给 Task 4 RiskGate。
		if strings.EqualFold(strings.TrimSpace(req.Side), "BUY") {
			return binanceOrderContract{}, fmt.Errorf("spot reduce_only BUY is unsupported")
		}
	}

	switch orderType {
	case "MARKET":
		if req.Price != nil {
			return binanceOrderContract{}, fmt.Errorf("market order must not set price")
		}
		if req.PostOnly {
			return binanceOrderContract{}, fmt.Errorf("market order must not set post_only")
		}
		if strings.TrimSpace(req.TimeInForce) != "" {
			return binanceOrderContract{}, fmt.Errorf("market order must not set time_in_force")
		}
		if req.GoodTillDate != nil {
			return binanceOrderContract{}, fmt.Errorf("market order must not set good_till_date")
		}
		return binanceOrderContract{
			OrderType:  "MARKET",
			ReduceOnly: market == domain.MarketPerpetualFutures && req.ReduceOnly,
		}, nil
	case "LIMIT":
		if req.Price == nil || *req.Price <= 0 {
			return binanceOrderContract{}, fmt.Errorf("limit order requires positive price")
		}
		tif := strings.ToUpper(strings.TrimSpace(req.TimeInForce))
		if tif == "" {
			tif = "GTC"
		}
		if req.PostOnly {
			if tif == "IOC" || tif == "FOK" || tif == "GTD" {
				return binanceOrderContract{}, fmt.Errorf("post_only cannot be combined with time_in_force=%s", tif)
			}
			if req.GoodTillDate != nil {
				return binanceOrderContract{}, fmt.Errorf("post_only cannot be combined with good_till_date")
			}
			if market == domain.MarketSpot {
				return binanceOrderContract{
					OrderType:  "LIMIT_MAKER",
					Price:      req.Price,
					ReduceOnly: false,
				}, nil
			}
			return binanceOrderContract{
				OrderType:   "LIMIT",
				TimeInForce: "GTX",
				Price:       req.Price,
				ReduceOnly:  market == domain.MarketPerpetualFutures && req.ReduceOnly,
			}, nil
		}
		if req.GoodTillDate != nil && tif != "GTD" {
			return binanceOrderContract{}, fmt.Errorf("good_till_date requires time_in_force=GTD")
		}
		switch tif {
		case "GTC", "IOC", "FOK":
			return binanceOrderContract{
				OrderType:   "LIMIT",
				TimeInForce: tif,
				Price:       req.Price,
				ReduceOnly:  market == domain.MarketPerpetualFutures && req.ReduceOnly,
			}, nil
		case "GTD":
			if market == domain.MarketSpot || !supportsGTD {
				return binanceOrderContract{}, fmt.Errorf("binance %s does not support time_in_force=GTD", market.String())
			}
			if req.GoodTillDate == nil {
				return binanceOrderContract{}, fmt.Errorf("gtd limit order requires good_till_date")
			}
			if err := validateBinanceGoodTillDate(req.GoodTillDate, time.Now().UTC()); err != nil {
				return binanceOrderContract{}, err
			}
			return binanceOrderContract{
				OrderType:      "LIMIT",
				TimeInForce:    "GTD",
				Price:          req.Price,
				GoodTillDateMS: strconv.FormatInt(req.GoodTillDate.UTC().UnixMilli(), 10),
				ReduceOnly:     req.ReduceOnly,
			}, nil
		default:
			return binanceOrderContract{}, fmt.Errorf("unsupported time_in_force: %s", tif)
		}
	default:
		return binanceOrderContract{}, fmt.Errorf("unsupported order_type: %s", orderType)
	}
}

func failedAdapterOrder(req adapter.OrderRequest, message string) adapter.OrderResult {
	return adapter.OrderResult{
		ClientOrderID: strings.TrimSpace(req.ClientOrderID),
		Symbol:        strings.ToUpper(strings.TrimSpace(req.Symbol)),
		Side:          strings.ToUpper(strings.TrimSpace(req.Side)),
		PositionSide:  req.PositionSide,
		OrderType:     strings.ToUpper(strings.TrimSpace(req.OrderType)),
		TimeInForce:   strings.ToUpper(strings.TrimSpace(req.TimeInForce)),
		Status:        "FAILED",
		OrigQty:       math.Abs(req.Qty),
		RemainingQty:  math.Abs(req.Qty),
		Price:         orderPrice(req.Price),
		ErrorMessage:  message,
	}
}

func orderPrice(price *float64) float64 {
	if price == nil {
		return 0
	}
	return *price
}

func validateBinanceGoodTillDate(goodTillDate *time.Time, now time.Time) error {
	if goodTillDate == nil {
		return fmt.Errorf("gtd limit order requires good_till_date")
	}
	if goodTillDate.UTC().Before(now.UTC().Add(binanceGTDMinLead)) {
		return fmt.Errorf("good_till_date must be at least 600s in the future")
	}
	return nil
}

type spotOrderResponse struct {
	OrderID             int64      `json:"orderId"`
	ClientOrderID       string     `json:"clientOrderId"`
	Symbol              string     `json:"symbol"`
	Side                string     `json:"side"`
	Type                string     `json:"type"`
	TimeInForce         string     `json:"timeInForce"`
	Price               string     `json:"price"`
	OrigQty             string     `json:"origQty"`
	ExecutedQty         string     `json:"executedQty"`
	CummulativeQuoteQty string     `json:"cummulativeQuoteQty"`
	Status              string     `json:"status"`
	Time                int64      `json:"time"`
	UpdateTime          int64      `json:"updateTime"`
	Code                int        `json:"code"`
	Msg                 string     `json:"msg"`
	Fills               []spotFill `json:"fills"`
}

type spotFill struct {
	Price           string          `json:"price"`
	Qty             string          `json:"qty"`
	Commission      string          `json:"commission"`
	CommissionAsset string          `json:"commissionAsset"`
	TradeID         json.RawMessage `json:"tradeId"`
}

func (e orderExecutor) placeSpotOrder(ctx context.Context, req adapter.OrderRequest, contract binanceOrderContract) (adapter.OrderResult, error) {
	side, err := normalizeOrderSide(req.Side)
	if err != nil {
		return failedAdapterOrder(req, err.Error()), nil
	}
	params := url.Values{}
	params.Set("symbol", strings.ToUpper(strings.TrimSpace(req.Symbol)))
	params.Set("side", side)
	params.Set("type", contract.OrderType)
	params.Set("quantity", strconv.FormatFloat(math.Abs(req.Qty), 'f', -1, 64))
	if contract.TimeInForce != "" {
		params.Set("timeInForce", contract.TimeInForce)
	}
	if contract.Price != nil {
		params.Set("price", strconv.FormatFloat(*contract.Price, 'f', -1, 64))
	}
	if clientOrderID := strings.TrimSpace(req.ClientOrderID); clientOrderID != "" {
		params.Set("newClientOrderId", clientOrderID)
	}
	params.Set("newOrderRespType", "FULL")
	params.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))

	body, statusCode, err := e.signedRequest(ctx, http.MethodPost, "/api/v3/order", params, req.Credential)
	if err != nil {
		return adapter.OrderResult{}, err
	}
	if statusCode != http.StatusOK {
		return failedAdapterOrder(req, fmt.Sprintf("HTTP %d: %s", statusCode, string(body))), nil
	}

	var raw spotOrderResponse
	if err := json.Unmarshal(body, &raw); err != nil {
		return adapter.OrderResult{}, fmt.Errorf("decode binance spot order response: %w", err)
	}
	if raw.Code != 0 {
		return failedAdapterOrder(req, fmt.Sprintf("binance error %d: %s", raw.Code, raw.Msg)), nil
	}
	return spotOrderResultFromResponse(req, raw), nil
}

func (e orderExecutor) signedRequest(ctx context.Context, method, path string, params url.Values, credential adapter.ParsedCredential) ([]byte, int, error) {
	apiKey := credential.Metadata["api_key"]
	apiSecret := credential.Metadata["api_secret"]
	if strings.TrimSpace(apiKey) == "" || strings.TrimSpace(apiSecret) == "" {
		return nil, 0, fmt.Errorf("%w: missing api_key or api_secret", ErrInvalidCredential)
	}
	query := params.Encode()
	params.Set("signature", signQuery(query, apiSecret))

	endpoint := strings.TrimRight(e.baseURL, "/") + path
	var body io.Reader
	if method == http.MethodGet {
		endpoint += "?" + params.Encode()
	} else {
		body = strings.NewReader(params.Encode())
	}
	httpReq, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return nil, 0, err
	}
	httpReq.Header.Set("X-MBX-APIKEY", apiKey)
	httpReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := e.httpClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}
	return payload, resp.StatusCode, nil
}

func spotOrderResultFromResponse(req adapter.OrderRequest, raw spotOrderResponse) adapter.OrderResult {
	origQty := parseFloatOrDefault(raw.OrigQty, math.Abs(req.Qty))
	executedQty, executedQtyErr := parseStrictNonNegativeFloat(raw.ExecutedQty, "spot executed_qty")
	if executedQtyErr != nil {
		executedQty = 0
	}
	remainingQty := origQty - executedQty
	if remainingQty < 0 {
		remainingQty = 0
	}
	price := parseFloatOrDefault(raw.Price, orderPrice(req.Price))
	avgPrice := spotAveragePrice(raw, executedQty, price, req.MarkPrice)
	exchangeOrderID := ""
	if raw.OrderID != 0 {
		exchangeOrderID = strconv.FormatInt(raw.OrderID, 10)
	}
	fills, fillPending, fillErrMessage := spotFillResult(raw, exchangeOrderID, executedQty)
	if executedQtyErr != nil && spotOrderLooksExecuted(raw) {
		fills = nil
		fillPending = true
		fillErrMessage = executedQtyErr.Error()
	}
	return adapter.OrderResult{
		ExchangeOrderID: exchangeOrderID,
		ClientOrderID:   firstNonEmpty(raw.ClientOrderID, req.ClientOrderID),
		Symbol:          firstNonEmpty(raw.Symbol, req.Symbol),
		Side:            firstNonEmpty(raw.Side, req.Side),
		OrderType:       strings.ToUpper(firstNonEmpty(raw.Type, req.OrderType)),
		TimeInForce:     strings.ToUpper(firstNonEmpty(raw.TimeInForce, req.TimeInForce)),
		Status:          strings.ToUpper(firstNonEmpty(raw.Status, "NEW")),
		OrigQty:         origQty,
		ExecutedQty:     executedQty,
		RemainingQty:    remainingQty,
		AvgPrice:        avgPrice,
		Price:           price,
		Fills:           fills,
		ErrorMessage:    fillErrMessage,
		FillPending:     fillPending,
	}
}

func spotOrderLooksExecuted(raw spotOrderResponse) bool {
	status := strings.ToUpper(strings.TrimSpace(raw.Status))
	return status == "FILLED" || status == "PARTIALLY_FILLED" || len(raw.Fills) > 0
}

func spotAveragePrice(raw spotOrderResponse, executedQty, fallbackPrice, markPrice float64) float64 {
	quoteQty := parseFloatOrDefault(raw.CummulativeQuoteQty, 0)
	if executedQty > 0 && quoteQty > 0 {
		return quoteQty / executedQty
	}
	totalQty := 0.0
	totalQuote := 0.0
	for _, fill := range raw.Fills {
		qty := parseFloatOrDefault(fill.Qty, 0)
		price := parseFloatOrDefault(fill.Price, 0)
		totalQty += qty
		totalQuote += qty * price
	}
	if totalQty > 0 {
		return totalQuote / totalQty
	}
	if fallbackPrice > 0 {
		return fallbackPrice
	}
	return markPrice
}

func spotFillResult(raw spotOrderResponse, exchangeOrderID string, executedQty float64) ([]adapter.FillDelta, bool, string) {
	if executedQty <= 0 {
		return nil, false, ""
	}
	if len(raw.Fills) == 0 {
		return nil, true, "missing spot fills for executed order"
	}
	fills, err := spotFills(raw, exchangeOrderID)
	if err != nil {
		return nil, true, err.Error()
	}
	fillQty := 0.0
	for _, fill := range fills {
		fillQty += math.Abs(fill.Qty)
	}
	if fillQty+spotFillQtyEpsilon < executedQty {
		return nil, true, fmt.Sprintf("incomplete spot fills: executed_qty=%g fill_qty=%g", executedQty, fillQty)
	}
	if fillQty > executedQty+spotFillQtyEpsilon {
		return nil, true, fmt.Sprintf("excessive spot fills: executed_qty=%g fill_qty=%g", executedQty, fillQty)
	}
	return fills, false, ""
}

func spotFills(raw spotOrderResponse, exchangeOrderID string) ([]adapter.FillDelta, error) {
	out := make([]adapter.FillDelta, 0, len(raw.Fills))
	now := time.Now().UTC()
	for _, fill := range raw.Fills {
		tradeID, err := parseSpotTradeID(fill.TradeID)
		if err != nil {
			return nil, err
		}
		qty, err := parseStrictPositiveFloat(fill.Qty, "spot fill qty")
		if err != nil {
			return nil, err
		}
		price, err := parseStrictPositiveFloat(fill.Price, "spot fill price")
		if err != nil {
			return nil, err
		}
		fee, err := parseStrictNonNegativeFloat(fill.Commission, "spot fill commission")
		if err != nil {
			return nil, err
		}
		out = append(out, adapter.FillDelta{
			ExchangeTradeID: strconv.FormatInt(tradeID, 10),
			ExchangeOrderID: exchangeOrderID,
			Symbol:          raw.Symbol,
			Qty:             qty,
			FillPrice:       price,
			Fee:             fee,
			FeeAsset:        fill.CommissionAsset,
			TradeTime:       now,
		})
	}
	return out, nil
}

func parseSpotTradeID(raw json.RawMessage) (int64, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, fmt.Errorf("invalid spot fill trade_id")
	}
	var numeric int64
	if err := json.Unmarshal(raw, &numeric); err == nil {
		if numeric > 0 {
			return numeric, nil
		}
		return 0, fmt.Errorf("invalid spot fill trade_id")
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		parsed, parseErr := strconv.ParseInt(strings.TrimSpace(text), 10, 64)
		if parseErr == nil && parsed > 0 {
			return parsed, nil
		}
	}
	return 0, fmt.Errorf("invalid spot fill trade_id")
}

func parseFloatOrDefault(raw string, fallback float64) float64 {
	value, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return fallback
	}
	return value
}

func parseStrictPositiveFloat(raw string, field string) (float64, error) {
	value, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("invalid %s", field)
	}
	return value, nil
}

func parseStrictNonNegativeFloat(raw string, field string) (float64, error) {
	value, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("invalid %s", field)
	}
	return value, nil
}

func (e orderExecutor) supportsGTD(market domain.Market) bool {
	return market == domain.MarketPerpetualFutures && e.route.Environment != domain.EnvironmentBacktest
}

func firstMarket(values ...domain.Market) domain.Market {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return domain.MarketPerpetualFutures
}

func normalizeOrderSide(side string) (string, error) {
	normalized := strings.ToUpper(strings.TrimSpace(side))
	switch normalized {
	case "BUY", "SELL":
		return normalized, nil
	default:
		return "", fmt.Errorf("unsupported order side: %q", side)
	}
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
		PostOnly:      req.PostOnly,
		GoodTillDate:  req.GoodTillDate,
		ReduceOnly:    req.ReduceOnly,
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
