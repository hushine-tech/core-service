package mockserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
)

type Server struct {
	mu            sync.Mutex
	config        Config
	activeScene   SceneMode
	nextOrderID   int64
	nextListenSeq int64
	scenarios     []BinanceScenario
	orders        map[string]*mockOrder
	ordersByCID   map[string]string
	listenKeys    map[string]map[*wsClient]struct{}
	lastRequests  map[string]RequestRecord
}

type mockOrder struct {
	OrderID       string
	ClientOrderID string
	Market        domain.Market
	Symbol        string
	Side          string
	PositionSide  string
	OrderType     string
	TimeInForce   string
	OrigQty       float64
	Price         float64
	Status        string
	ExecutedQty   float64
	AvgPrice      float64
	Fills         []BinanceFill
	ReduceOnly    bool
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type RequestRecord struct {
	Method string
	Path   string
	Params url.Values
}

func New() *Server {
	return NewWithConfig(Config{})
}

func NewWithConfig(cfg Config) *Server {
	return &Server{
		config:        normalizeConfig(cfg),
		activeScene:   SceneNormalFill,
		nextOrderID:   1001,
		nextListenSeq: 1,
		orders:        make(map[string]*mockOrder),
		ordersByCID:   make(map[string]string),
		listenKeys:    make(map[string]map[*wsClient]struct{}),
		lastRequests:  make(map[string]RequestRecord),
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	s.registerFuturesRoutes(mux)
	s.registerSpotRoutes(mux)
	s.registerWSRoutes(mux)
	mux.HandleFunc("/mock/reset", s.handleReset)
	mux.HandleFunc("/mock/scene", s.handleScene)
	mux.HandleFunc("/mock/scenarios", s.handleScenarios)
	mux.HandleFunc("/mock/orders/", s.handleMockOrder)
	return mux
}

func (s *Server) StartHTTP(t testing.TB) *httptest.Server {
	t.Helper()
	return httptest.NewServer(s.Handler())
}

func (s *Server) Reset(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activeScene = SceneNormalFill
	s.nextOrderID = 1001
	s.nextListenSeq = 1
	s.scenarios = nil
	s.orders = make(map[string]*mockOrder)
	s.ordersByCID = make(map[string]string)
	s.listenKeys = make(map[string]map[*wsClient]struct{})
	s.lastRequests = make(map[string]RequestRecord)
	return nil
}

func (s *Server) OrderCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.orders)
}

func (s *Server) SubscriberCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for _, clients := range s.listenKeys {
		total += len(clients)
	}
	return total
}

func (s *Server) EnqueueScenario(scenario BinanceScenario) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scenarios = append(s.scenarios, scenario)
}

func (s *Server) LastRequest(path string) (RequestRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.lastRequests[path]
	return record.clone(), ok
}

func (r RequestRecord) clone() RequestRecord {
	out := RequestRecord{Method: r.Method, Path: r.Path, Params: url.Values{}}
	for key, values := range r.Params {
		out.Params[key] = append([]string(nil), values...)
	}
	return out
}

func (s *Server) nextScenario() BinanceScenario {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.scenarios) == 0 {
		return BinanceScenario{}
	}
	scenario := s.scenarios[0]
	s.scenarios = s.scenarios[1:]
	return scenario
}

func (s *Server) currentScene() SceneMode {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.activeScene
}

func (s *Server) setScene(scene SceneMode) error {
	if scene < SceneNormalFill || scene > SceneTimeout {
		return fmt.Errorf("unsupported scene %d", scene)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activeScene = scene
	return nil
}

func (s *Server) allocateOrder(market domain.Market, params url.Values, scenario BinanceScenario) *mockOrder {
	now := time.Now().UTC()
	clientOrderID := firstNonEmpty(params.Get("newClientOrderId"), params.Get("newClientOrderID"), params.Get("clientOrderId"))
	symbol := strings.ToUpper(firstNonEmpty(params.Get("symbol"), scenario.Symbol, "ETHUSDT"))
	side := strings.ToUpper(firstNonEmpty(params.Get("side"), scenario.Side, "BUY"))
	orderType := strings.ToUpper(firstNonEmpty(params.Get("type"), scenario.OrderType, "MARKET"))
	timeInForce := strings.ToUpper(firstNonEmpty(params.Get("timeInForce"), scenario.TimeInForce))
	positionSide := strings.ToUpper(firstNonEmpty(params.Get("positionSide"), scenario.PositionSide))
	origQty := parseFloatDefault(firstNonEmpty(params.Get("quantity"), strconv.FormatFloat(scenario.OrigQty, 'f', -1, 64)), 1)
	price := parseFloatDefault(firstNonEmpty(params.Get("price"), strconv.FormatFloat(scenario.Price, 'f', -1, 64)), 0)
	status := strings.ToUpper(firstNonEmpty(scenario.Status, "NEW"))
	if status == "" {
		status = "NEW"
	}

	var delayed delayedSceneAction
	s.mu.Lock()
	id := strconv.FormatInt(s.nextOrderID, 10)
	s.nextOrderID++
	order := &mockOrder{
		OrderID:       id,
		ClientOrderID: clientOrderID,
		Market:        market,
		Symbol:        symbol,
		Side:          side,
		PositionSide:  positionSide,
		OrderType:     orderType,
		TimeInForce:   timeInForce,
		OrigQty:       origQty,
		Price:         price,
		Status:        status,
		ReduceOnly:    strings.EqualFold(params.Get("reduceOnly"), "true") || scenario.ReduceOnly,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	if scenario.hasBehavior() {
		for _, step := range scenario.Events {
			s.applyStepLocked(order, step)
		}
	} else {
		delayed = s.applySceneLocked(order, s.activeScene)
	}
	s.orders[id] = order
	if clientOrderID != "" {
		s.ordersByCID[clientOrderID] = id
	}
	out := cloneOrder(order)
	s.mu.Unlock()
	if delayed.orderID != "" {
		go s.completeOrderAfter(delayed.orderID, delayed.delay)
	}
	return out
}

func (s *Server) applyStepLocked(order *mockOrder, step BinanceOrderEventStep) {
	switch step.Kind {
	case EventRESTTradesComplete:
		order.Fills = append(order.Fills, step.Fills...)
		order.ExecutedQty = totalFillQty(order.Fills)
		if order.ExecutedQty > 0 {
			order.AvgPrice = averageFillPrice(order.Fills)
			if order.ExecutedQty >= order.OrigQty {
				order.Status = "FILLED"
			} else {
				order.Status = "PARTIALLY_FILLED"
			}
		}
	case EventOrderCanceled:
		order.Status = "CANCELED"
	case EventOrderExpired:
		order.Status = "EXPIRED"
	case EventAcceptNew, EventRESTTradesIncomplete, EventWSPartialFill, EventWSFinalFill, EventWSDuplicateEvent:
		if order.Status == "" {
			order.Status = "NEW"
		}
	}
	order.UpdatedAt = time.Now().UTC()
}

func (s *Server) findOrder(params url.Values) (*mockOrder, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	orderID := strings.TrimSpace(params.Get("orderId"))
	if orderID == "" {
		orderID = s.ordersByCID[strings.TrimSpace(params.Get("origClientOrderId"))]
	}
	if orderID == "" {
		orderID = s.ordersByCID[strings.TrimSpace(params.Get("newClientOrderId"))]
	}
	order, ok := s.orders[orderID]
	if !ok {
		return nil, false
	}
	return cloneOrder(order), true
}

func (s *Server) recordRequest(r *http.Request, params url.Values) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastRequests[r.URL.Path] = RequestRecord{
		Method: r.Method,
		Path:   r.URL.Path,
		Params: cloneValues(params),
	}
}

func (s *Server) requireAPIKey(w http.ResponseWriter, r *http.Request) bool {
	if strings.TrimSpace(r.Header.Get("X-MBX-APIKEY")) == "" {
		writeBinanceError(w, http.StatusUnauthorized, -2015, "Invalid API-key, IP, or permissions for action.")
		return false
	}
	return true
}

func (s *Server) requireSigned(w http.ResponseWriter, r *http.Request, params url.Values) bool {
	if !s.requireAPIKey(w, r) {
		return false
	}
	if strings.TrimSpace(params.Get("signature")) == "" {
		writeBinanceError(w, http.StatusBadRequest, -1022, "Signature for this request is not valid.")
		return false
	}
	return true
}

func (s *Server) requestParams(r *http.Request) (url.Values, error) {
	if err := r.ParseForm(); err != nil {
		return nil, err
	}
	params := url.Values{}
	for key, values := range r.Form {
		params[key] = append([]string(nil), values...)
	}
	return params, nil
}

func (s *Server) handleReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete && r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	_ = s.Reset(r.Context())
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleScene(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		scene := s.currentScene()
		writeJSON(w, http.StatusOK, map[string]any{
			"scene": int(scene),
			"name":  sceneName(scene),
		})
	case http.MethodPost:
		scene, err := parseSceneRequest(r)
		if err != nil {
			writeBinanceError(w, http.StatusBadRequest, -1100, err.Error())
			return
		}
		if err := s.setScene(scene); err != nil {
			writeBinanceError(w, http.StatusBadRequest, -1100, err.Error())
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{
			"scene": int(scene),
			"name":  sceneName(scene),
		})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleScenarios(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var scenario BinanceScenario
	if err := json.NewDecoder(r.Body).Decode(&scenario); err != nil {
		writeBinanceError(w, http.StatusBadRequest, -1100, "invalid scenario payload")
		return
	}
	s.EnqueueScenario(scenario)
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "queued"})
}

func (s *Server) handleMockOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	orderID := strings.TrimPrefix(r.URL.Path, "/mock/orders/")
	order, ok := s.findOrder(url.Values{"orderId": []string{orderID}})
	if !ok {
		writeBinanceError(w, http.StatusNotFound, -2013, "Order does not exist.")
		return
	}
	writeJSON(w, http.StatusOK, order)
}

func cloneOrder(order *mockOrder) *mockOrder {
	if order == nil {
		return nil
	}
	copyOrder := *order
	copyOrder.Fills = append([]BinanceFill(nil), order.Fills...)
	return &copyOrder
}

func cloneValues(values url.Values) url.Values {
	out := url.Values{}
	for key, list := range values {
		out[key] = append([]string(nil), list...)
	}
	return out
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeBinanceError(w http.ResponseWriter, status, code int, msg string) {
	writeJSON(w, status, map[string]any{"code": code, "msg": msg})
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func parseFloatDefault(raw string, fallback float64) float64 {
	value, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return fallback
	}
	return value
}

func totalFillQty(fills []BinanceFill) float64 {
	total := 0.0
	for _, fill := range fills {
		total += fill.Qty
	}
	return total
}

func averageFillPrice(fills []BinanceFill) float64 {
	totalQty := 0.0
	totalQuote := 0.0
	for _, fill := range fills {
		totalQty += fill.Qty
		totalQuote += fill.Qty * fill.Price
	}
	if totalQty == 0 {
		return 0
	}
	return totalQuote / totalQty
}

func formatFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func formatOrderID(value string) int64 {
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}

func formatTradeID(value string) int64 {
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}

func nowMillis(t time.Time) int64 {
	if t.IsZero() {
		return time.Now().UTC().UnixMilli()
	}
	return t.UTC().UnixMilli()
}

func unsupportedMethod(w http.ResponseWriter, method string) {
	writeBinanceError(w, http.StatusMethodNotAllowed, -1003, fmt.Sprintf("unsupported method %s", method))
}

type delayedSceneAction struct {
	orderID string
	delay   time.Duration
}

func (s *Server) applySceneLocked(order *mockOrder, scene SceneMode) delayedSceneAction {
	switch scene {
	case SceneNormalFill:
		if isPostOnlyOrder(order) {
			order.Status = "NEW"
			break
		}
		addFillLocked(order, order.OrderID+"01", order.OrigQty)
		order.Status = "FILLED"
	case ScenePartialNeverComplete:
		s.applyPartialLiquidityLocked(order, false)
	case ScenePartialThenWSFill:
		if canDelayFinalFill(order) {
			addFillLocked(order, order.OrderID+"01", partialQty(order.OrigQty, s.config.PartialFillRatio))
			order.Status = "PARTIALLY_FILLED"
			return delayedSceneAction{orderID: order.OrderID, delay: s.config.Scene3Delay}
		}
		s.applyPartialLiquidityLocked(order, true)
	case SceneRestingNoFill:
		if canRestOpen(order) {
			order.Status = "NEW"
		} else {
			order.Status = "EXPIRED"
		}
	case ScenePostOnlyWouldTake:
		if isPostOnlyOrder(order) {
			order.Status = "EXPIRED"
		} else {
			addFillLocked(order, order.OrderID+"01", order.OrigQty)
			order.Status = "FILLED"
		}
	case SceneNoLiquidity:
		if canRestOpen(order) {
			order.Status = "NEW"
		} else {
			order.Status = "EXPIRED"
		}
	}
	order.UpdatedAt = time.Now().UTC()
	return delayedSceneAction{}
}

func (s *Server) applyPartialLiquidityLocked(order *mockOrder, fillMarketImmediately bool) {
	switch {
	case isFOKOrder(order):
		order.Status = "EXPIRED"
	case isPostOnlyOrder(order):
		order.Status = "NEW"
	case isIOCOrder(order):
		addFillLocked(order, order.OrderID+"01", partialQty(order.OrigQty, s.config.PartialFillRatio))
		order.Status = "EXPIRED"
	case isMarketOrder(order):
		if fillMarketImmediately {
			addFillLocked(order, order.OrderID+"01", order.OrigQty)
			order.Status = "FILLED"
		} else {
			addFillLocked(order, order.OrderID+"01", partialQty(order.OrigQty, s.config.PartialFillRatio))
			order.Status = "EXPIRED"
		}
	case canRestOpen(order):
		addFillLocked(order, order.OrderID+"01", partialQty(order.OrigQty, s.config.PartialFillRatio))
		order.Status = "PARTIALLY_FILLED"
	default:
		order.Status = "NEW"
	}
}

func (s *Server) completeOrderAfter(orderID string, delay time.Duration) {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	<-timer.C

	var event BinanceOrderEvent
	var market domain.Market
	s.mu.Lock()
	order, ok := s.orders[orderID]
	if !ok || order.Status != "PARTIALLY_FILLED" {
		s.mu.Unlock()
		return
	}
	remaining := order.OrigQty - order.ExecutedQty
	if remaining <= 0 {
		order.Status = "FILLED"
		s.mu.Unlock()
		return
	}
	fill := addFillLocked(order, order.OrderID+"02", remaining)
	order.Status = "FILLED"
	order.UpdatedAt = time.Now().UTC()
	market = order.Market
	event = orderEventFromFill(order, fill, "TRADE", "FILLED")
	s.mu.Unlock()

	if market == domain.MarketSpot {
		s.EmitSpotOrderEvent(event)
		return
	}
	s.EmitFuturesOrderEvent(event)
}

func (scenario BinanceScenario) hasBehavior() bool {
	return scenario.Status != "" || len(scenario.Events) > 0
}

func parseSceneRequest(r *http.Request) (SceneMode, error) {
	raw := strings.TrimSpace(r.URL.Query().Get("scene"))
	if raw == "" && r.Body != nil {
		var payload struct {
			Scene int `json:"scene"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			return 0, fmt.Errorf("invalid scene payload")
		}
		raw = strconv.Itoa(payload.Scene)
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("scene must be an integer")
	}
	return SceneMode(value), nil
}

func (s *Server) handleOrderScenePreflight(w http.ResponseWriter, r *http.Request) bool {
	switch s.currentScene() {
	case SceneExchangeReject:
		writeBinanceError(w, http.StatusBadRequest, -2010, "Mock exchange rejected order.")
		return true
	case SceneRateLimit:
		writeBinanceError(w, http.StatusTooManyRequests, -1003, "Too many requests; mock rate limit.")
		return true
	case SceneTimeout:
		timer := time.NewTimer(s.config.Scene9Delay)
		defer timer.Stop()
		select {
		case <-r.Context().Done():
			return true
		case <-timer.C:
			return false
		}
	default:
		return false
	}
}

func isMarketOrder(order *mockOrder) bool {
	return strings.EqualFold(order.OrderType, "MARKET")
}

func isFOKOrder(order *mockOrder) bool {
	return strings.EqualFold(order.TimeInForce, "FOK")
}

func isIOCOrder(order *mockOrder) bool {
	return strings.EqualFold(order.TimeInForce, "IOC")
}

func isPostOnlyOrder(order *mockOrder) bool {
	return strings.EqualFold(order.TimeInForce, "GTX") || strings.EqualFold(order.OrderType, "LIMIT_MAKER")
}

func canDelayFinalFill(order *mockOrder) bool {
	return !isPostOnlyOrder(order) && !isFOKOrder(order) && !isIOCOrder(order) && !isMarketOrder(order) && canRestOpen(order)
}

func canRestOpen(order *mockOrder) bool {
	if isPostOnlyOrder(order) {
		return true
	}
	if isMarketOrder(order) || isFOKOrder(order) || isIOCOrder(order) {
		return false
	}
	switch strings.ToUpper(strings.TrimSpace(order.TimeInForce)) {
	case "", "GTC", "GTD":
		return strings.EqualFold(order.OrderType, "LIMIT")
	default:
		return false
	}
}

func partialQty(origQty, ratio float64) float64 {
	if origQty <= 0 {
		return 0
	}
	qty := origQty * ratio
	if qty >= origQty {
		return origQty
	}
	return qty
}

func addFillLocked(order *mockOrder, tradeID string, qty float64) BinanceFill {
	remaining := order.OrigQty - order.ExecutedQty
	if qty > remaining {
		qty = remaining
	}
	if qty <= 0 {
		return BinanceFill{}
	}
	price := order.Price
	if price <= 0 {
		price = 2000
	}
	fill := BinanceFill{
		TradeID:  tradeID,
		Qty:      qty,
		Price:    price,
		Fee:      qty * price * 0.0004,
		FeeAsset: "USDT",
		Time:     time.Now().UTC(),
	}
	order.Fills = append(order.Fills, fill)
	order.ExecutedQty = totalFillQty(order.Fills)
	order.AvgPrice = averageFillPrice(order.Fills)
	return fill
}

func orderEventFromFill(order *mockOrder, fill BinanceFill, executionType, status string) BinanceOrderEvent {
	return BinanceOrderEvent{
		Symbol:               order.Symbol,
		ClientOrderID:        order.ClientOrderID,
		ExchangeOrderID:      order.OrderID,
		ExchangeTradeID:      fill.TradeID,
		Side:                 order.Side,
		PositionSide:         order.PositionSide,
		OrderType:            order.OrderType,
		TimeInForce:          order.TimeInForce,
		ExecutionType:        executionType,
		OrderStatus:          status,
		LastFilledQty:        fill.Qty,
		LastFilledPrice:      fill.Price,
		AccumulatedFilledQty: order.ExecutedQty,
		Fee:                  fill.Fee,
		FeeAsset:             fill.FeeAsset,
		ReduceOnly:           order.ReduceOnly,
		EventTime:            fill.Time,
	}
}
