package mockserver

import (
	"net/http"

	"github.com/hushine-tech/core-service/internal/domain"
)

func (s *Server) registerSpotRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v3/order", s.handleSpotOrder)
	mux.HandleFunc("/api/v3/myTrades", s.handleSpotMyTrades)
	mux.HandleFunc("/api/v3/userDataStream", s.handleSpotUserDataStream)
}

func (s *Server) handleSpotOrder(w http.ResponseWriter, r *http.Request) {
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
			scenario.Market = domain.MarketSpot
		}
		order := s.allocateOrder(domain.MarketSpot, params, scenario)
		writeJSON(w, http.StatusOK, spotOrderResponse(order))
	case http.MethodGet:
		order, ok := s.findOrder(params)
		if !ok {
			writeBinanceError(w, http.StatusBadRequest, -2013, "Order does not exist.")
			return
		}
		writeJSON(w, http.StatusOK, spotOrderResponse(order))
	case http.MethodDelete:
		order, ok := s.cancelOrder(params)
		if !ok {
			writeBinanceError(w, http.StatusBadRequest, -2013, "Order does not exist.")
			return
		}
		writeJSON(w, http.StatusOK, spotOrderResponse(order))
	default:
		unsupportedMethod(w, r.Method)
	}
}

func (s *Server) handleSpotMyTrades(w http.ResponseWriter, r *http.Request) {
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
	writeJSON(w, http.StatusOK, spotTradeResponses(order))
}

func (s *Server) handleSpotUserDataStream(w http.ResponseWriter, r *http.Request) {
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

func spotOrderResponse(order *mockOrder) map[string]any {
	status := firstNonEmpty(order.Status, "NEW")
	return map[string]any{
		"orderId":             formatOrderID(order.OrderID),
		"clientOrderId":       order.ClientOrderID,
		"symbol":              order.Symbol,
		"side":                order.Side,
		"type":                order.OrderType,
		"timeInForce":         order.TimeInForce,
		"price":               formatFloat(order.Price),
		"origQty":             formatFloat(order.OrigQty),
		"executedQty":         formatFloat(order.ExecutedQty),
		"cummulativeQuoteQty": formatFloat(order.ExecutedQty * order.AvgPrice),
		"status":              status,
		"time":                order.CreatedAt.UnixMilli(),
		"updateTime":          order.UpdatedAt.UnixMilli(),
		"fills":               spotTradeResponses(order),
	}
}

func spotTradeResponses(order *mockOrder) []map[string]any {
	trades := make([]map[string]any, 0, len(order.Fills))
	for _, fill := range order.Fills {
		tradeTime := fill.Time
		if tradeTime.IsZero() {
			tradeTime = order.UpdatedAt
		}
		trades = append(trades, map[string]any{
			"id":              formatTradeID(fill.TradeID),
			"orderId":         formatOrderID(order.OrderID),
			"tradeId":         formatTradeID(fill.TradeID),
			"symbol":          order.Symbol,
			"price":           formatFloat(fill.Price),
			"qty":             formatFloat(fill.Qty),
			"commission":      formatFloat(fill.Fee),
			"commissionAsset": firstNonEmpty(fill.FeeAsset, "USDT"),
			"time":            nowMillis(tradeTime),
		})
	}
	return trades
}
