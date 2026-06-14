package mockserver

import (
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
)

func (s *Server) registerFuturesRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/fapi/v1/order", s.handleFuturesOrder)
	mux.HandleFunc("/fapi/v1/userTrades", s.handleFuturesUserTrades)
	mux.HandleFunc("/fapi/v1/listenKey", s.handleFuturesListenKey)
	mux.HandleFunc("/fapi/v1/exchangeInfo", s.handleFuturesExchangeInfo)
}

func (s *Server) handleFuturesOrder(w http.ResponseWriter, r *http.Request) {
	params, err := s.requestParams(r)
	if err != nil {
		writeBinanceError(w, http.StatusBadRequest, -1100, "Illegal characters found in parameter.")
		return
	}
	s.recordRequest(r, params)
	if !s.requireSigned(w, r, params) {
		return
	}
	switch r.Method {
	case http.MethodPost:
		if s.handleOrderScenePreflight(w, r) {
			return
		}
		scenario := s.nextScenario()
		if scenario.Market == 0 {
			scenario.Market = domain.MarketPerpetualFutures
		}
		order := s.allocateOrder(domain.MarketPerpetualFutures, params, scenario)
		writeJSON(w, http.StatusOK, futuresOrderResponse(order))
	case http.MethodGet:
		order, ok := s.findOrder(params)
		if !ok {
			writeBinanceError(w, http.StatusBadRequest, -2013, "Order does not exist.")
			return
		}
		writeJSON(w, http.StatusOK, futuresOrderResponse(order))
	case http.MethodDelete:
		order, ok := s.cancelOrder(params)
		if !ok {
			writeBinanceError(w, http.StatusBadRequest, -2013, "Order does not exist.")
			return
		}
		writeJSON(w, http.StatusOK, futuresOrderResponse(order))
	default:
		unsupportedMethod(w, r.Method)
	}
}

func (s *Server) handleFuturesUserTrades(w http.ResponseWriter, r *http.Request) {
	params, err := s.requestParams(r)
	if err != nil {
		writeBinanceError(w, http.StatusBadRequest, -1100, "Illegal characters found in parameter.")
		return
	}
	s.recordRequest(r, params)
	if !s.requireSigned(w, r, params) {
		return
	}
	if r.Method != http.MethodGet {
		unsupportedMethod(w, r.Method)
		return
	}
	order, ok := s.findOrder(params)
	if !ok {
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	trades := make([]map[string]any, 0, len(order.Fills))
	for _, fill := range order.Fills {
		tradeTime := fill.Time
		if tradeTime.IsZero() {
			tradeTime = order.UpdatedAt
		}
		trades = append(trades, map[string]any{
			"id":              formatTradeID(fill.TradeID),
			"orderId":         formatOrderID(order.OrderID),
			"symbol":          order.Symbol,
			"price":           formatFloat(fill.Price),
			"qty":             formatFloat(fill.Qty),
			"commission":      formatFloat(fill.Fee),
			"commissionAsset": firstNonEmpty(fill.FeeAsset, "USDT"),
			"time":            nowMillis(tradeTime),
		})
	}
	writeJSON(w, http.StatusOK, trades)
}

func (s *Server) handleFuturesListenKey(w http.ResponseWriter, r *http.Request) {
	if !s.requireAPIKey(w, r) {
		return
	}
	switch r.Method {
	case http.MethodPost:
		writeJSON(w, http.StatusOK, map[string]string{"listenKey": s.createListenKey()})
	case http.MethodPut:
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		w.WriteHeader(http.StatusOK)
	default:
		unsupportedMethod(w, r.Method)
	}
}

func (s *Server) handleFuturesExchangeInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		unsupportedMethod(w, r.Method)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"symbols": []map[string]any{
			mockExchangeInfoSymbol("ETHUSDT"),
			mockExchangeInfoSymbol("BTCUSDT"),
			mockExchangeInfoSymbol("ZECUSDT"),
		},
	})
}

func (s *Server) cancelOrder(params url.Values) (*mockOrder, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	orderID := params.Get("orderId")
	if orderID == "" {
		orderID = s.ordersByCID[params.Get("origClientOrderId")]
	}
	order, ok := s.orders[orderID]
	if !ok {
		return nil, false
	}
	order.Status = "CANCELED"
	order.UpdatedAt = time.Now().UTC()
	return cloneOrder(order), true
}

func futuresOrderResponse(order *mockOrder) map[string]any {
	status := firstNonEmpty(order.Status, "NEW")
	return map[string]any{
		"orderId":       formatOrderID(order.OrderID),
		"clientOrderId": order.ClientOrderID,
		"symbol":        order.Symbol,
		"side":          order.Side,
		"positionSide":  order.PositionSide,
		"type":          order.OrderType,
		"timeInForce":   order.TimeInForce,
		"origQty":       formatFloat(order.OrigQty),
		"price":         formatFloat(order.Price),
		"avgPrice":      formatFloat(order.AvgPrice),
		"executedQty":   formatFloat(order.ExecutedQty),
		"status":        status,
		"time":          order.CreatedAt.UnixMilli(),
		"updateTime":    order.UpdatedAt.UnixMilli(),
	}
}

func (s *Server) createListenKey() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	listenKey := "listen-" + strconv.FormatInt(s.nextListenSeq, 10)
	s.nextListenSeq++
	if s.listenKeys[listenKey] == nil {
		s.listenKeys[listenKey] = make(map[*wsClient]struct{})
	}
	return listenKey
}
