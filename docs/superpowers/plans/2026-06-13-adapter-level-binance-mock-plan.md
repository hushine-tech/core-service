# Adapter-Level Binance Mock Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Binance adapter-owned REST/WebSocket mock service that can be used from Go tests and a local CLI, then use it to test partial fills, recovery, risk-gate combinations, and 14-day force-close behavior through the real adapter/lifecycle paths.

**Architecture:** Keep exchange behavior inside `core-service/internal/exchange/binance`, not in public order code. The common adapter layer only exposes endpoint injection and an optional user-data-stream capability; Binance mock owns Binance REST paths, Binance WebSocket payloads, Binance status names, and Binance scenario helpers. Order lifecycle ingests normalized adapter stream events and persists the same lifecycle events used by REST recovery and strategy-runtime wallet updates.

**Tech Stack:** Go 1.26, `net/http`, `httptest`, `github.com/gorilla/websocket`, existing `adapter`, `binance`, `order/lifecycle`, `order/repository`, and `order/service` packages.

---

## Scope Check

This plan extends the order hardening already implemented in `2026-06-07-binance-order-risk-execution-plan.md`. Do not rebuild public order fields, RiskGate, or existing parser tests. This plan fills the current gap: a connectable, adapter-scoped Binance REST/WS mock and lifecycle ingestion path for WebSocket partial-fill events.

## File Structure

Adapter endpoint injection:

- Create `core-service/internal/exchange/binance/endpoints.go`: default and env-overridden Binance REST/WS endpoints.
- Modify `core-service/internal/exchange/binance/factory.go`: carry endpoint config into executor, state reader, canceller, and stream client.
- Modify `core-service/internal/order/executor/binance.go`: add a base-URL constructor for futures tests and mock routing.
- Modify `core-service/internal/exchange/binance/order_canceller.go`: inject base URL and HTTP client instead of computing fixed endpoints internally.
- Test `core-service/internal/exchange/binance/endpoints_test.go`.

Adapter user-data-stream capability:

- Modify `core-service/internal/exchange/adapter/capabilities.go`: add `UserDataStream`, `UserDataStreamRequest`, and `UserDataOrderEvent`.
- Modify `core-service/internal/exchange/adapter/registry.go`: expose optional `UserDataStream(route)` through type assertion.
- Test `core-service/internal/exchange/adapter/registry_test.go`.
- Create `core-service/internal/exchange/binance/user_data_stream_client.go`: create listenKey, connect `/ws/<listenKey>`, parse Binance events, emit normalized adapter events.
- Test `core-service/internal/exchange/binance/user_data_stream_client_test.go`.

Binance adapter mock:

- Create `core-service/internal/exchange/binance/mockserver/server.go`: shared in-memory mock state and HTTP handler.
- Create `core-service/internal/exchange/binance/mockserver/scenario.go`: Binance-only scenario helpers.
- Create `core-service/internal/exchange/binance/mockserver/rest_spot.go`: Spot REST paths.
- Create `core-service/internal/exchange/binance/mockserver/rest_futures.go`: USD-M Futures REST paths.
- Create `core-service/internal/exchange/binance/mockserver/ws.go`: `/ws/<listenKey>` subscriptions and event broadcast.
- Create `core-service/internal/exchange/binance/mockserver/fixtures.go`: raw Spot/Futures event payload builders.
- Test `core-service/internal/exchange/binance/mockserver/server_test.go`.
- Test `core-service/internal/exchange/binance/mockserver/ws_test.go`.

Lifecycle WebSocket ingestion:

- Create `core-service/internal/order/lifecycle/user_data_ingestor.go`: map adapter user-data events into lifecycle events.
- Modify `core-service/internal/order/repository/repository.go`: add order lookup by exchange/client order reference for WebSocket events.
- Modify `core-service/internal/order/repository/timescale.go`: implement lookup SQL.
- Test `core-service/internal/order/lifecycle/user_data_ingestor_test.go`.
- Test `core-service/internal/order/repository/timescale_lifecycle_test.go`.

CLI and smoke support:

- Create `core-service/cmd/mock-binance/main.go`: long-running local mock service.
- Modify `core-service/Makefile`: add `mock-binance` target.
- Create `core-service/scripts/mock_binance_partial_fill_smoke.sh`: local smoke command that runs targeted mock-backed tests with endpoint env overrides.
- Modify `core-service/docs/superpowers/specs/2026-06-13-adapter-level-exchange-mock-design.md`: add final command names and supported mock control paths.

---

### Task 1: Add Binance Endpoint Injection

**Files:**
- Create: `core-service/internal/exchange/binance/endpoints.go`
- Modify: `core-service/internal/exchange/binance/factory.go`
- Modify: `core-service/internal/order/executor/binance.go`
- Modify: `core-service/internal/exchange/binance/order_canceller.go`
- Test: `core-service/internal/exchange/binance/endpoints_test.go`

- [ ] **Step 1: Write failing endpoint override tests**

Create `core-service/internal/exchange/binance/endpoints_test.go`:

```go
package binance

import (
	"testing"

	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/exchange/adapter"
)

func TestEndpointsFromEnvUsesMarketSpecificOverrides(t *testing.T) {
	t.Setenv("BINANCE_SPOT_REST_BASE_URL", "http://127.0.0.1:19001")
	t.Setenv("BINANCE_FUTURES_REST_BASE_URL", "http://127.0.0.1:19002")
	t.Setenv("BINANCE_SPOT_WS_BASE_URL", "ws://127.0.0.1:19003")
	t.Setenv("BINANCE_FUTURES_WS_BASE_URL", "ws://127.0.0.1:19004")

	spot := EndpointsFromEnv(adapter.Route{Market: domain.MarketSpot, Environment: domain.EnvironmentDemo})
	if spot.RESTBaseURL != "http://127.0.0.1:19001" || spot.WSBaseURL != "ws://127.0.0.1:19003" {
		t.Fatalf("spot endpoints = %+v", spot)
	}

	futures := EndpointsFromEnv(adapter.Route{Market: domain.MarketPerpetualFutures, Environment: domain.EnvironmentDemo})
	if futures.RESTBaseURL != "http://127.0.0.1:19002" || futures.WSBaseURL != "ws://127.0.0.1:19004" {
		t.Fatalf("futures endpoints = %+v", futures)
	}
}

func TestEndpointsFromEnvFallsBackToBinanceDefaults(t *testing.T) {
	spot := EndpointsFromEnv(adapter.Route{Market: domain.MarketSpot, Environment: domain.EnvironmentDemo})
	if spot.RESTBaseURL == "" || spot.WSBaseURL == "" {
		t.Fatalf("spot default endpoints must be populated: %+v", spot)
	}
	futures := EndpointsFromEnv(adapter.Route{Market: domain.MarketPerpetualFutures, Environment: domain.EnvironmentDemo})
	if futures.RESTBaseURL == "" || futures.WSBaseURL == "" {
		t.Fatalf("futures default endpoints must be populated: %+v", futures)
	}
}
```

- [ ] **Step 2: Run the failing tests**

Run:

```bash
cd /Users/xdy/Workplace/hushine/core-service
go test ./internal/exchange/binance -run TestEndpointsFromEnv -count=1
```

Expected: compile failure because `EndpointsFromEnv` and `Endpoints` do not exist.

- [ ] **Step 3: Implement endpoint config**

Create `core-service/internal/exchange/binance/endpoints.go`:

```go
package binance

import (
	"os"
	"strings"

	"github.com/hushine-tech/core-service/internal/domain"
	legacyexchange "github.com/hushine-tech/core-service/internal/exchange"
	"github.com/hushine-tech/core-service/internal/exchange/adapter"
)

const (
	binanceSpotLiveWSBaseURL    = "wss://stream.binance.com:9443"
	binanceSpotTestnetWSBaseURL = "wss://testnet.binance.vision"
	binanceFuturesLiveWSBaseURL = "wss://fstream.binance.com"
	binanceFuturesTestnetWSBaseURL = "wss://stream.binancefuture.com"
)

type Endpoints struct {
	RESTBaseURL string
	WSBaseURL   string
}

func EndpointsFromEnv(route adapter.Route) Endpoints {
	defaults := defaultEndpoints(route)
	switch route.Market {
	case domain.MarketSpot:
		return Endpoints{
			RESTBaseURL: envOrDefault("BINANCE_SPOT_REST_BASE_URL", defaults.RESTBaseURL),
			WSBaseURL:   envOrDefault("BINANCE_SPOT_WS_BASE_URL", defaults.WSBaseURL),
		}
	case domain.MarketPerpetualFutures:
		return Endpoints{
			RESTBaseURL: envOrDefault("BINANCE_FUTURES_REST_BASE_URL", defaults.RESTBaseURL),
			WSBaseURL:   envOrDefault("BINANCE_FUTURES_WS_BASE_URL", defaults.WSBaseURL),
		}
	default:
		return defaults
	}
}

func defaultEndpoints(route adapter.Route) Endpoints {
	switch route.Market {
	case domain.MarketSpot:
		if route.Environment == domain.EnvironmentDemo {
			return Endpoints{RESTBaseURL: legacyexchange.BinanceSpotTestnetURL, WSBaseURL: binanceSpotTestnetWSBaseURL}
		}
		return Endpoints{RESTBaseURL: legacyexchange.BinanceSpotBaseURL, WSBaseURL: binanceSpotLiveWSBaseURL}
	case domain.MarketPerpetualFutures:
		if route.Environment == domain.EnvironmentLive {
			return Endpoints{RESTBaseURL: legacyexchange.BinanceLiveBaseURL, WSBaseURL: binanceFuturesLiveWSBaseURL}
		}
		return Endpoints{RESTBaseURL: legacyexchange.BinanceTestnetBaseURL, WSBaseURL: binanceFuturesTestnetWSBaseURL}
	default:
		return Endpoints{}
	}
}

func envOrDefault(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return strings.TrimRight(value, "/")
}
```

- [ ] **Step 4: Inject endpoints into Binance factory**

Modify `core-service/internal/exchange/binance/factory.go`:

```go
type Factory struct {
	route     adapter.Route
	logger    log.Logger
	endpoints Endpoints
}

func NewFactory(route adapter.Route, logger log.Logger) *Factory {
	return NewFactoryWithEndpoints(route, logger, EndpointsFromEnv(route))
}

func NewFactoryWithEndpoints(route adapter.Route, logger log.Logger, endpoints Endpoints) *Factory {
	if endpoints.RESTBaseURL == "" || endpoints.WSBaseURL == "" {
		defaults := EndpointsFromEnv(route)
		if endpoints.RESTBaseURL == "" {
			endpoints.RESTBaseURL = defaults.RESTBaseURL
		}
		if endpoints.WSBaseURL == "" {
			endpoints.WSBaseURL = defaults.WSBaseURL
		}
	}
	return &Factory{route: route, logger: logger, endpoints: endpoints}
}

func NewBacktestFactory(route adapter.Route) *Factory {
	return &Factory{route: route, endpoints: EndpointsFromEnv(route)}
}
```

Update `SymbolRulesReader`, `OrderStateReader`, `OrderCanceller`, `remoteOrderExecutor`, `futuresBaseURL`, and `spotBaseURL` to use `f.endpoints.RESTBaseURL`.

- [ ] **Step 5: Add futures executor base URL constructor**

Modify `core-service/internal/order/executor/binance.go`:

```go
func NewBinanceExecutorWithBaseURL(baseURL string, logger elog.Logger, clientName string) *BinanceExecutor {
	if strings.TrimSpace(clientName) == "" {
		clientName = "binance_order_custom"
	}
	return &BinanceExecutor{
		baseURL:             strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		httpClient:          httpclient.New(&http.Client{Timeout: 10 * time.Second}, logger, clientName),
		tradeLookupAttempts: defaultTradeLookupAttempts,
		tradeLookupDelay:    defaultTradeLookupDelay,
	}
}
```

Then have demo/live constructors call this helper with their existing base URLs.

- [ ] **Step 6: Inject base URL into order canceller**

Modify `core-service/internal/exchange/binance/order_canceller.go`:

```go
type orderCanceller struct {
	route      adapter.Route
	baseURL    string
	httpClient *http.Client
}
```

In `Factory.OrderCanceller`, return:

```go
return orderCanceller{
	route:      f.route,
	baseURL:    f.endpoints.RESTBaseURL,
	httpClient: &http.Client{Timeout: 10 * time.Second},
}, nil
```

In `cancelRemote`, replace `c.baseURL()+c.orderPath()` with `strings.TrimRight(c.baseURL, "/")+c.orderPath()` and replace the local `httpClient := ...` with:

```go
httpClient := c.httpClient
if httpClient == nil {
	httpClient = &http.Client{Timeout: 10 * time.Second}
}
```

- [ ] **Step 7: Run endpoint tests**

Run:

```bash
cd /Users/xdy/Workplace/hushine/core-service
go test ./internal/exchange/binance -run TestEndpointsFromEnv -count=1
```

Expected: tests pass.

- [ ] **Step 8: Commit**

```bash
cd /Users/xdy/Workplace/hushine
git add core-service/internal/exchange/binance/endpoints.go \
  core-service/internal/exchange/binance/endpoints_test.go \
  core-service/internal/exchange/binance/factory.go \
  core-service/internal/exchange/binance/order_canceller.go \
  core-service/internal/order/executor/binance.go
git commit -m "为 Binance adapter 增加 mock endpoint 注入"
```

### Task 2: Add Optional Adapter User Data Stream Capability

**Files:**
- Modify: `core-service/internal/exchange/adapter/capabilities.go`
- Modify: `core-service/internal/exchange/adapter/registry.go`
- Test: `core-service/internal/exchange/adapter/registry_test.go`

- [ ] **Step 1: Write failing registry tests**

Create or extend `core-service/internal/exchange/adapter/registry_test.go`:

```go
package adapter

import (
	"context"
	"testing"
	"time"
)

type streamFactory struct {
	Factory
	stream UserDataStream
}

func (f streamFactory) UserDataStream() (UserDataStream, error) {
	return f.stream, nil
}

type noopUserDataStream struct{}

func (noopUserDataStream) Listen(context.Context, UserDataStreamRequest, func(context.Context, UserDataOrderEvent) error) error {
	return nil
}

func TestRegistryUserDataStreamReturnsOptionalCapability(t *testing.T) {
	route := Route{}
	reg := NewRegistry()
	want := noopUserDataStream{}
	reg.Register(route, streamFactory{stream: want})

	got, err := reg.UserDataStream(route)
	if err != nil {
		t.Fatalf("UserDataStream returned error: %v", err)
	}
	if got == nil {
		t.Fatal("UserDataStream returned nil")
	}
}

func TestUserDataOrderEventShape(t *testing.T) {
	event := UserDataOrderEvent{
		EventSource:          "websocket",
		EventTime:            time.Unix(1700000000, 0).UTC(),
		Symbol:               "ETHUSDT",
		ClientOrderID:        "cid-1",
		ExchangeOrderID:      "1001",
		ExchangeTradeID:      "9001",
		Side:                 "BUY",
		PositionSide:         "LONG",
		OrderType:            "LIMIT",
		TimeInForce:          "GTC",
		OrderStatus:          "PARTIALLY_FILLED",
		ExecutionType:        "TRADE",
		LastFilledQty:        0.2,
		LastFilledPrice:      2000,
		AccumulatedFilledQty: 0.2,
		Fee:                  0.08,
		FeeAsset:             "USDT",
		ReduceOnly:           false,
	}
	if event.ExchangeTradeID != "9001" || event.LastFilledQty != 0.2 {
		t.Fatalf("unexpected event: %+v", event)
	}
}
```

- [ ] **Step 2: Run the failing tests**

Run:

```bash
cd /Users/xdy/Workplace/hushine/core-service
go test ./internal/exchange/adapter -run 'TestRegistryUserDataStream|TestUserDataOrderEventShape' -count=1
```

Expected: compile failure because user-data stream types and registry method do not exist.

- [ ] **Step 3: Add adapter types**

Append to `core-service/internal/exchange/adapter/capabilities.go`:

```go
type UserDataStreamRequest struct {
	AccountID  int64
	VenueID    int64
	Credential ParsedCredential
}

type UserDataOrderEvent struct {
	EventSource          string
	EventTime            time.Time
	Symbol               string
	ClientOrderID        string
	ExchangeOrderID      string
	ExchangeTradeID      string
	Side                 string
	PositionSide         string
	OrderType            string
	TimeInForce          string
	OrderStatus          string
	ExecutionType        string
	LastFilledQty        float64
	LastFilledPrice      float64
	AccumulatedFilledQty float64
	Fee                  float64
	FeeAsset             string
	ReduceOnly           bool
}

type UserDataStream interface {
	Listen(ctx context.Context, req UserDataStreamRequest, handle func(context.Context, UserDataOrderEvent) error) error
}

type UserDataStreamFactory interface {
	UserDataStream() (UserDataStream, error)
}
```

- [ ] **Step 4: Add optional registry lookup**

Append to `core-service/internal/exchange/adapter/registry.go`:

```go
func (r *Registry) UserDataStream(route Route) (UserDataStream, error) {
	factory, err := r.factory(route)
	if err != nil {
		return nil, err
	}
	streamFactory, ok := factory.(UserDataStreamFactory)
	if !ok {
		return nil, CapabilityUnsupported("user_data_stream")
	}
	return streamFactory.UserDataStream()
}
```

- [ ] **Step 5: Run registry tests**

Run:

```bash
cd /Users/xdy/Workplace/hushine/core-service
go test ./internal/exchange/adapter -run 'TestRegistryUserDataStream|TestUserDataOrderEventShape' -count=1
```

Expected: tests pass.

- [ ] **Step 6: Commit**

```bash
cd /Users/xdy/Workplace/hushine
git add core-service/internal/exchange/adapter/capabilities.go \
  core-service/internal/exchange/adapter/registry.go \
  core-service/internal/exchange/adapter/registry_test.go
git commit -m "增加 adapter user data stream 能力"
```

### Task 3: Implement Binance User Data Stream Client

**Files:**
- Create: `core-service/internal/exchange/binance/user_data_stream_client.go`
- Modify: `core-service/internal/exchange/binance/factory.go`
- Test: `core-service/internal/exchange/binance/user_data_stream_client_test.go`
- Modify: `core-service/go.mod`
- Modify: `core-service/go.sum`

- [ ] **Step 1: Add WebSocket dependency**

Run:

```bash
cd /Users/xdy/Workplace/hushine/core-service
go get github.com/gorilla/websocket@v1.5.3
go mod tidy
```

Expected: `core-service/go.mod` and `core-service/go.sum` include `github.com/gorilla/websocket`.

- [ ] **Step 2: Write failing stream client test**

Create `core-service/internal/exchange/binance/user_data_stream_client_test.go`:

```go
package binance

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/exchange/adapter"
)

func TestBinanceUserDataStreamClientReceivesFuturesPartialFill(t *testing.T) {
	upgrader := websocket.Upgrader{}
	mux := http.NewServeMux()
	mux.HandleFunc("/fapi/v1/listenKey", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("listenKey method = %s", r.Method)
		}
		if r.Header.Get("X-MBX-APIKEY") == "" {
			t.Fatal("missing X-MBX-APIKEY")
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"listenKey": "listen-1"})
	})
	mux.HandleFunc("/ws/listen-1", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		payload := `{"e":"ORDER_TRADE_UPDATE","E":1700000000000,"o":{"s":"ETHUSDT","c":"cid-1","S":"BUY","ps":"LONG","o":"LIMIT","f":"GTC","x":"TRADE","X":"PARTIALLY_FILLED","i":1001,"t":9001,"l":"0.2","L":"2000","z":"0.2","n":"0.08","N":"USDT","R":false}}`
		if err := conn.WriteMessage(websocket.TextMessage, []byte(payload)); err != nil {
			t.Fatalf("write ws: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client := binanceUserDataStreamClient{
		route: adapter.Route{Market: domain.MarketPerpetualFutures},
		endpoints: Endpoints{
			RESTBaseURL: server.URL,
			WSBaseURL:   "ws" + strings.TrimPrefix(server.URL, "http"),
		},
		httpClient: server.Client(),
		dialer:     websocket.DefaultDialer,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var got adapter.UserDataOrderEvent
	err := client.Listen(ctx, adapter.UserDataStreamRequest{
		Credential: adapter.ParsedCredential{Metadata: map[string]string{"api_key": "key", "api_secret": "secret"}},
	}, func(_ context.Context, event adapter.UserDataOrderEvent) error {
		got = event
		cancel()
		return nil
	})
	if err != nil && ctx.Err() == nil {
		t.Fatalf("Listen returned error: %v", err)
	}
	if got.OrderStatus != "PARTIALLY_FILLED" || got.ExchangeTradeID != "9001" || got.LastFilledQty != 0.2 {
		t.Fatalf("unexpected event: %+v", got)
	}
}
```

- [ ] **Step 3: Run the failing stream client test**

Run:

```bash
cd /Users/xdy/Workplace/hushine/core-service
go test ./internal/exchange/binance -run TestBinanceUserDataStreamClientReceivesFuturesPartialFill -count=1
```

Expected: compile failure because `binanceUserDataStreamClient` does not exist.

- [ ] **Step 4: Implement stream client**

Create `core-service/internal/exchange/binance/user_data_stream_client.go`:

```go
package binance

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/exchange/adapter"
)

type binanceUserDataStreamClient struct {
	route      adapter.Route
	endpoints  Endpoints
	httpClient *http.Client
	dialer     *websocket.Dialer
}

type listenKeyResponse struct {
	ListenKey string `json:"listenKey"`
}

func (c binanceUserDataStreamClient) Listen(ctx context.Context, req adapter.UserDataStreamRequest, handle func(context.Context, adapter.UserDataOrderEvent) error) error {
	listenKey, err := c.createListenKey(ctx, req.Credential)
	if err != nil {
		return err
	}
	wsURL := strings.TrimRight(c.endpoints.WSBaseURL, "/") + "/ws/" + listenKey
	dialer := c.dialer
	if dialer == nil {
		dialer = websocket.DefaultDialer
	}
	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("connect binance user data stream: %w", err)
	}
	defer conn.Close()

	parser := NewUserDataStreamParser(c.route.Market)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		_, payload, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("read binance user data stream: %w", err)
		}
		raw, err := parser.ParseOrderEvent(payload)
		if err != nil {
			return err
		}
		if err := handle(ctx, toAdapterUserDataOrderEvent(raw)); err != nil {
			return err
		}
	}
}

func (c binanceUserDataStreamClient) createListenKey(ctx context.Context, credential adapter.ParsedCredential) (string, error) {
	apiKey := strings.TrimSpace(credential.Metadata["api_key"])
	if apiKey == "" {
		return "", fmt.Errorf("%w: missing api_key", ErrInvalidCredential)
	}
	client := c.httpClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	path := "/fapi/v1/listenKey"
	if c.route.Market == domain.MarketSpot {
		path = "/api/v3/userDataStream"
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.endpoints.RESTBaseURL, "/")+path, bytes.NewReader(nil))
	if err != nil {
		return "", err
	}
	httpReq.Header.Set("X-MBX-APIKEY", apiKey)
	resp, err := client.Do(httpReq)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("binance listenKey HTTP %d", resp.StatusCode)
	}
	var decoded listenKeyResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return "", fmt.Errorf("decode binance listenKey: %w", err)
	}
	if strings.TrimSpace(decoded.ListenKey) == "" {
		return "", fmt.Errorf("binance listenKey response missing listenKey")
	}
	return decoded.ListenKey, nil
}

func toAdapterUserDataOrderEvent(event UserDataOrderEvent) adapter.UserDataOrderEvent {
	return adapter.UserDataOrderEvent{
		EventSource:          event.EventSource,
		EventTime:            event.EventTime,
		Symbol:               event.Symbol,
		ClientOrderID:        event.ClientOrderID,
		ExchangeOrderID:      event.ExchangeOrderID,
		ExchangeTradeID:      event.ExchangeTradeID,
		Side:                 event.Side,
		PositionSide:         event.PositionSide,
		OrderType:            event.OrderType,
		TimeInForce:          event.TimeInForce,
		OrderStatus:          event.OrderStatus,
		ExecutionType:        event.ExecutionType,
		LastFilledQty:        event.LastFilledQty,
		LastFilledPrice:      event.LastFilledPrice,
		AccumulatedFilledQty: event.AccumulatedFilledQty,
		Fee:                  event.Fee,
		FeeAsset:             event.FeeAsset,
		ReduceOnly:           event.ReduceOnly,
	}
}
```

- [ ] **Step 5: Wire factory stream capability**

Add to `core-service/internal/exchange/binance/factory.go`:

```go
func (f *Factory) UserDataStream() (adapter.UserDataStream, error) {
	if err := f.requireOrderMarket("user_data_stream"); err != nil {
		return nil, err
	}
	if f.route.Environment == domain.EnvironmentBacktest {
		return nil, adapter.CapabilityUnsupported("user_data_stream")
	}
	return binanceUserDataStreamClient{
		route:      f.route,
		endpoints:  f.endpoints,
		httpClient: &http.Client{Timeout: 10 * time.Second},
		dialer:     websocket.DefaultDialer,
	}, nil
}
```

Add the import for `github.com/gorilla/websocket`.

- [ ] **Step 6: Run stream client tests**

Run:

```bash
cd /Users/xdy/Workplace/hushine/core-service
go test ./internal/exchange/binance -run 'TestBinanceUserDataStreamClient|TestUserDataStreamParser' -count=1
```

Expected: tests pass.

- [ ] **Step 7: Commit**

```bash
cd /Users/xdy/Workplace/hushine
git add core-service/go.mod core-service/go.sum \
  core-service/internal/exchange/binance/factory.go \
  core-service/internal/exchange/binance/user_data_stream_client.go \
  core-service/internal/exchange/binance/user_data_stream_client_test.go
git commit -m "接入 Binance user data stream client"
```

### Task 4: Build Binance Adapter Mock Server Core

**Files:**
- Create: `core-service/internal/exchange/binance/mockserver/server.go`
- Create: `core-service/internal/exchange/binance/mockserver/scenario.go`
- Create: `core-service/internal/exchange/binance/mockserver/rest_futures.go`
- Create: `core-service/internal/exchange/binance/mockserver/rest_spot.go`
- Create: `core-service/internal/exchange/binance/mockserver/ws.go`
- Create: `core-service/internal/exchange/binance/mockserver/fixtures.go`
- Test: `core-service/internal/exchange/binance/mockserver/server_test.go`
- Test: `core-service/internal/exchange/binance/mockserver/ws_test.go`

- [ ] **Step 1: Write failing mock REST/WS tests**

Create `core-service/internal/exchange/binance/mockserver/server_test.go` with two tests:

```go
package mockserver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
)

func TestMockServerFuturesOrderPartialThenTradesComplete(t *testing.T) {
	mock := New()
	server := mock.StartHTTP(t)
	defer server.Close()

	mock.EnqueueScenario(BinanceScenario{
		Market:      domain.MarketPerpetualFutures,
		Symbol:      "ETHUSDT",
		Side:        "BUY",
		PositionSide:"LONG",
		OrderType:   "LIMIT",
		TimeInForce: "GTC",
		OrigQty:     1,
		Price:       2000,
		Events: []BinanceOrderEventStep{
			{Kind: EventAcceptNew},
			{Kind: EventRESTTradesIncomplete},
			{Kind: EventRESTTradesComplete, Fills: []BinanceFill{{TradeID: "9001", Qty: 0.4, Price: 2000, Fee: 0.32, FeeAsset: "USDT"}}},
		},
	})

	form := url.Values{}
	form.Set("symbol", "ETHUSDT")
	form.Set("side", "BUY")
	form.Set("positionSide", "LONG")
	form.Set("type", "LIMIT")
	form.Set("timeInForce", "GTC")
	form.Set("quantity", "1")
	form.Set("price", "2000")
	form.Set("newClientOrderId", "cid-1")
	form.Set("timestamp", "1700000000000")
	form.Set("signature", "sig")
	req, err := http.NewRequest(http.MethodPost, server.URL+"/fapi/v1/order?"+form.Encode(), nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-MBX-APIKEY", "key")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var order map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&order); err != nil {
		t.Fatal(err)
	}
	if order["status"] != "NEW" || order["clientOrderId"] != "cid-1" {
		t.Fatalf("unexpected order response: %+v", order)
	}

	tradeReq, err := http.NewRequest(http.MethodGet, server.URL+"/fapi/v1/userTrades?symbol=ETHUSDT&orderId=1001&timestamp=1700000000001&signature=sig", nil)
	if err != nil {
		t.Fatal(err)
	}
	tradeReq.Header.Set("X-MBX-APIKEY", "key")
	tradeResp, err := server.Client().Do(tradeReq)
	if err != nil {
		t.Fatal(err)
	}
	defer tradeResp.Body.Close()
	var trades []map[string]any
	if err := json.NewDecoder(tradeResp.Body).Decode(&trades); err != nil {
		t.Fatal(err)
	}
	if len(trades) != 1 || trades[0]["id"] != float64(9001) {
		t.Fatalf("unexpected trades: %+v", trades)
	}
}

func TestMockServerResetClearsOrders(t *testing.T) {
	mock := New()
	mock.EnqueueScenario(BinanceScenario{Market: domain.MarketPerpetualFutures, Symbol: "ETHUSDT", Side: "BUY", OrderType: "MARKET", OrigQty: 1})
	if err := mock.Reset(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := mock.OrderCount(); got != 0 {
		t.Fatalf("OrderCount = %d, want 0", got)
	}
}
```

Create `core-service/internal/exchange/binance/mockserver/ws_test.go`:

```go
package mockserver

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hushine-tech/core-service/internal/domain"
)

func TestMockServerBroadcastsFuturesPartialFill(t *testing.T) {
	mock := New()
	server := mock.StartHTTP(t)
	defer server.Close()

	req, err := http.NewRequest(http.MethodPost, server.URL+"/fapi/v1/listenKey", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-MBX-APIKEY", "key")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws/" + body["listenKey"]
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	mock.EmitFuturesOrderEvent(BinanceOrderEvent{
		Symbol: "ETHUSDT", ClientOrderID: "cid-1", ExchangeOrderID: "1001", ExchangeTradeID: "9001",
		Side: "BUY", PositionSide: "LONG", OrderType: "LIMIT", TimeInForce: "GTC",
		ExecutionType: "TRADE", OrderStatus: "PARTIALLY_FILLED",
		LastFilledQty: 0.2, LastFilledPrice: 2000, AccumulatedFilledQty: 0.2,
		Fee: 0.08, FeeAsset: "USDT", EventTime: time.UnixMilli(1700000000000).UTC(),
	})

	_, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(payload), `"e":"ORDER_TRADE_UPDATE"`) || !strings.Contains(string(payload), `"X":"PARTIALLY_FILLED"`) {
		t.Fatalf("unexpected ws payload: %s", payload)
	}
}
```

- [ ] **Step 2: Run failing mockserver tests**

Run:

```bash
cd /Users/xdy/Workplace/hushine/core-service
go test ./internal/exchange/binance/mockserver -count=1
```

Expected: compile failure because the mockserver package does not exist.

- [ ] **Step 3: Implement scenario types**

Create `core-service/internal/exchange/binance/mockserver/scenario.go`:

```go
package mockserver

import (
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
)

type EventKind string

const (
	EventAcceptNew            EventKind = "ACCEPT_NEW"
	EventWSPartialFill        EventKind = "WS_PARTIAL_FILL"
	EventWSFinalFill          EventKind = "WS_FINAL_FILL"
	EventRESTTradesIncomplete EventKind = "REST_TRADES_INCOMPLETE"
	EventRESTTradesComplete   EventKind = "REST_TRADES_COMPLETE"
	EventWSDuplicateEvent     EventKind = "WS_DUPLICATE_EVENT"
	EventOrderCanceled        EventKind = "ORDER_CANCELED"
	EventOrderExpired         EventKind = "ORDER_EXPIRED"
)

type BinanceScenario struct {
	Market       domain.Market
	Symbol       string
	Side         string
	PositionSide string
	OrderType    string
	TimeInForce  string
	PostOnly     bool
	ReduceOnly   bool
	OrigQty      float64
	Price        float64
	Events       []BinanceOrderEventStep
}

type BinanceOrderEventStep struct {
	Kind  EventKind
	Fills []BinanceFill
	Delay time.Duration
}

type BinanceFill struct {
	TradeID  string
	Qty      float64
	Price    float64
	Fee      float64
	FeeAsset string
	Time     time.Time
}

type BinanceOrderEvent struct {
	Symbol               string
	ClientOrderID        string
	ExchangeOrderID      string
	ExchangeTradeID      string
	Side                 string
	PositionSide         string
	OrderType            string
	TimeInForce          string
	ExecutionType        string
	OrderStatus          string
	LastFilledQty        float64
	LastFilledPrice      float64
	AccumulatedFilledQty float64
	Fee                  float64
	FeeAsset             string
	ReduceOnly           bool
	EventTime            time.Time
}
```

- [ ] **Step 4: Implement server state and HTTP handler**

Create `core-service/internal/exchange/binance/mockserver/server.go`:

```go
package mockserver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
)

type Server struct {
	mu            sync.Mutex
	nextOrderID   int64
	nextListenSeq int64
	scenarios     []BinanceScenario
	orders        map[string]*mockOrder
	listenKeys    map[string]map[*wsClient]struct{}
}

type mockOrder struct {
	OrderID       string
	ClientOrderID string
	Scenario      BinanceScenario
	Status        string
	ExecutedQty   float64
	AvgPrice      float64
	Fills         []BinanceFill
}

func New() *Server {
	return &Server{
		nextOrderID:   1001,
		nextListenSeq: 1,
		orders:        make(map[string]*mockOrder),
		listenKeys:    make(map[string]map[*wsClient]struct{}),
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	s.registerFuturesRoutes(mux)
	s.registerSpotRoutes(mux)
	s.registerWSRoutes(mux)
	mux.HandleFunc("/mock/reset", s.handleReset)
	return mux
}

func (s *Server) StartHTTP(t testing.TB) *httptest.Server {
	t.Helper()
	return httptest.NewServer(s.Handler())
}

func (s *Server) Reset(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextOrderID = 1001
	s.nextListenSeq = 1
	s.scenarios = nil
	s.orders = make(map[string]*mockOrder)
	s.listenKeys = make(map[string]map[*wsClient]struct{})
	return nil
}

func (s *Server) OrderCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.orders)
}

func (s *Server) EnqueueScenario(scenario BinanceScenario) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scenarios = append(s.scenarios, scenario)
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

func (s *Server) allocateOrder(clientOrderID string, scenario BinanceScenario) *mockOrder {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := strconv.FormatInt(s.nextOrderID, 10)
	s.nextOrderID++
	order := &mockOrder{OrderID: id, ClientOrderID: clientOrderID, Scenario: scenario, Status: "NEW"}
	s.orders[id] = order
	return order
}

func (s *Server) handleReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete && r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	_ = s.Reset(r.Context())
	w.WriteHeader(http.StatusNoContent)
}
```

- [ ] **Step 5: Implement REST routes**

Create `rest_futures.go` and `rest_spot.go` with the same validation rule: every signed endpoint requires `X-MBX-APIKEY` and `signature`. For futures order placement, respond with Binance-shaped JSON:

```json
{"orderId":1001,"clientOrderId":"cid-1","symbol":"ETHUSDT","side":"BUY","origQty":"1","avgPrice":"0","executedQty":"0","status":"NEW"}
```

For futures trades, return `[]` for `REST_TRADES_INCOMPLETE` and the scenario fills for `REST_TRADES_COMPLETE`:

```json
[{"id":9001,"orderId":1001,"price":"2000","qty":"0.4","commission":"0.32","commissionAsset":"USDT","time":1700000000000}]
```

For Spot, use `/api/v3/order`, `/api/v3/myTrades`, and `/api/v3/userDataStream` paths with Spot field names. Spot `post_only` is represented by order type `LIMIT_MAKER`; Spot `reduce_only` is accepted by platform request but never appears in Binance REST parameters.

- [ ] **Step 6: Implement WS event builders**

Create `fixtures.go` with:

```go
func FuturesOrderTradeUpdate(event BinanceOrderEvent) []byte
func SpotExecutionReport(event BinanceOrderEvent) []byte
```

`FuturesOrderTradeUpdate` must output the raw Binance wrapper:

```json
{"e":"ORDER_TRADE_UPDATE","E":1700000000000,"o":{"s":"ETHUSDT","c":"cid-1","S":"BUY","ps":"LONG","o":"LIMIT","f":"GTC","x":"TRADE","X":"PARTIALLY_FILLED","i":1001,"t":9001,"l":"0.2","L":"2000","z":"0.2","n":"0.08","N":"USDT","R":false}}
```

`SpotExecutionReport` must output raw Spot:

```json
{"e":"executionReport","E":1700000000000,"s":"ETHUSDT","c":"cid-1","S":"SELL","o":"LIMIT_MAKER","f":"GTC","x":"TRADE","X":"PARTIALLY_FILLED","i":1001,"t":9001,"l":"0.2","L":"2000","z":"0.2","n":"0.08","N":"USDT"}
```

- [ ] **Step 7: Implement WS subscriptions**

Create `ws.go`:

```go
type wsClient struct {
	send chan []byte
}

func (s *Server) EmitFuturesOrderEvent(event BinanceOrderEvent) {
	s.broadcast(FuturesOrderTradeUpdate(event))
}

func (s *Server) EmitSpotOrderEvent(event BinanceOrderEvent) {
	s.broadcast(SpotExecutionReport(event))
}
```

`registerWSRoutes` handles `/ws/<listenKey>` with `websocket.Upgrader`, adds the client to `s.listenKeys[listenKey]`, writes every payload from `send`, and removes the client on connection close.

- [ ] **Step 8: Run mockserver tests**

Run:

```bash
cd /Users/xdy/Workplace/hushine/core-service
go test ./internal/exchange/binance/mockserver -count=1
```

Expected: tests pass.

- [ ] **Step 9: Commit**

```bash
cd /Users/xdy/Workplace/hushine
git add core-service/internal/exchange/binance/mockserver
git commit -m "增加 Binance adapter REST 和 WS mock 服务"
```

### Task 5: Test Real Binance Adapter Against Mock Server

**Files:**
- Test: `core-service/internal/exchange/binance/mockserver_integration_test.go`
- Test: `core-service/internal/exchange/binance/order_executor_test.go`

- [ ] **Step 1: Write adapter integration test**

Create `core-service/internal/exchange/binance/mockserver_integration_test.go`:

```go
package binance

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/exchange/adapter"
	"github.com/hushine-tech/core-service/internal/exchange/binance/mockserver"
)

func TestBinanceFactoryPlacesOrderAndReceivesMockWSPartialFill(t *testing.T) {
	mock := mockserver.New()
	server := mock.StartHTTP(t)
	defer server.Close()

	route := adapter.Route{Exchange: domain.ExchangeBinance, Environment: domain.EnvironmentDemo, Market: domain.MarketPerpetualFutures}
	factory := NewFactoryWithEndpoints(route, nil, Endpoints{
		RESTBaseURL: server.URL,
		WSBaseURL:   "ws" + strings.TrimPrefix(server.URL, "http"),
	})
	mock.EnqueueScenario(mockserver.BinanceScenario{
		Market: domain.MarketPerpetualFutures, Symbol: "ETHUSDT", Side: "BUY", PositionSide: "LONG",
		OrderType: "LIMIT", TimeInForce: "GTC", OrigQty: 1, Price: 2000,
	})

	exec, err := factory.OrderExecutor()
	if err != nil {
		t.Fatal(err)
	}
	price := 2000.0
	result, err := exec.PlaceOrder(context.Background(), adapter.OrderRequest{
		AccountID: 1, VenueID: 2, Exchange: domain.ExchangeBinance, Environment: domain.EnvironmentDemo, Market: domain.MarketPerpetualFutures,
		Symbol: "ETHUSDT", Side: "BUY", PositionSide: "LONG", OrderType: "LIMIT", TimeInForce: "GTC", Qty: 1, Price: &price,
		ClientOrderID: "cid-1", Credential: adapter.ParsedCredential{Metadata: map[string]string{"api_key": "key", "api_secret": "secret"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "NEW" || result.ExchangeOrderID != "1001" {
		t.Fatalf("unexpected order result: %+v", result)
	}

	stream, err := factory.UserDataStream()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	events := make(chan adapter.UserDataOrderEvent, 1)
	go func() {
		_ = stream.Listen(ctx, adapter.UserDataStreamRequest{
			AccountID: 1, VenueID: 2, Credential: adapter.ParsedCredential{Metadata: map[string]string{"api_key": "key", "api_secret": "secret"}},
		}, func(_ context.Context, event adapter.UserDataOrderEvent) error {
			events <- event
			cancel()
			return nil
		})
	}()
	time.Sleep(50 * time.Millisecond)
	mock.EmitFuturesOrderEvent(mockserver.BinanceOrderEvent{
		Symbol: "ETHUSDT", ClientOrderID: "cid-1", ExchangeOrderID: "1001", ExchangeTradeID: "9001",
		Side: "BUY", PositionSide: "LONG", OrderType: "LIMIT", TimeInForce: "GTC",
		ExecutionType: "TRADE", OrderStatus: "PARTIALLY_FILLED",
		LastFilledQty: 0.2, LastFilledPrice: 2000, AccumulatedFilledQty: 0.2,
		Fee: 0.08, FeeAsset: "USDT", EventTime: time.UnixMilli(1700000000000).UTC(),
	})
	got := <-events
	if got.OrderStatus != "PARTIALLY_FILLED" || got.ExchangeTradeID != "9001" {
		t.Fatalf("unexpected stream event: %+v", got)
	}
}
```

- [ ] **Step 2: Run adapter integration test**

Run:

```bash
cd /Users/xdy/Workplace/hushine/core-service
go test ./internal/exchange/binance -run TestBinanceFactoryPlacesOrderAndReceivesMockWSPartialFill -count=1
```

Expected: test passes and proves the real Binance adapter can connect to the adapter-owned mock.

- [ ] **Step 3: Add order-combination tests against mock**

Extend `core-service/internal/exchange/binance/order_executor_test.go` with table cases:

```go
cases := []struct {
	name       string
	market     domain.Market
	orderType  string
	tif        string
	postOnly   bool
	reduceOnly bool
	wantType   string
	wantTIF    string
	wantReduce string
}{
	{name: "spot post only uses LIMIT_MAKER", market: domain.MarketSpot, orderType: "LIMIT", tif: "GTC", postOnly: true, wantType: "LIMIT_MAKER"},
	{name: "futures post only uses GTX", market: domain.MarketPerpetualFutures, orderType: "LIMIT", tif: "GTC", postOnly: true, wantType: "LIMIT", wantTIF: "GTX"},
	{name: "futures reduce only is passed", market: domain.MarketPerpetualFutures, orderType: "MARKET", reduceOnly: true, wantType: "MARKET", wantReduce: "true"},
	{name: "spot reduce only sell is platform only", market: domain.MarketSpot, orderType: "MARKET", reduceOnly: true, wantType: "MARKET", wantReduce: ""},
}
```

Each case should place an order through `NewFactoryWithEndpoints` and assert mockserver captured the last raw request parameters.

- [ ] **Step 4: Run Binance adapter tests**

Run:

```bash
cd /Users/xdy/Workplace/hushine/core-service
go test ./internal/exchange/binance ./internal/exchange/binance/mockserver -count=1
```

Expected: tests pass.

- [ ] **Step 5: Commit**

```bash
cd /Users/xdy/Workplace/hushine
git add core-service/internal/exchange/binance/mockserver_integration_test.go \
  core-service/internal/exchange/binance/order_executor_test.go
git commit -m "用 Binance mock 覆盖 adapter 下单和 WS partial fill"
```

### Task 6: Ingest WebSocket Partial Fills Into Lifecycle

**Files:**
- Create: `core-service/internal/order/lifecycle/user_data_ingestor.go`
- Modify: `core-service/internal/order/repository/repository.go`
- Modify: `core-service/internal/order/repository/timescale.go`
- Test: `core-service/internal/order/lifecycle/user_data_ingestor_test.go`
- Test: `core-service/internal/order/repository/timescale_lifecycle_test.go`

- [ ] **Step 1: Write lifecycle ingestor tests**

Create `core-service/internal/order/lifecycle/user_data_ingestor_test.go`:

```go
package lifecycle

import (
	"context"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/internal/exchange/adapter"
)

type userDataStore struct {
	open  OpenOrder
	saved []Event
}

func (s *userDataStore) ResolveOpenOrderByExchangeRef(_ context.Context, venueID int64, exchangeOrderID, clientOrderID string) (OpenOrder, error) {
	if s.open.VenueID == venueID && (s.open.ExchangeOrderID == exchangeOrderID || s.open.ClientOrderID == clientOrderID) {
		return s.open, nil
	}
	return OpenOrder{}, ErrOpenOrderNotFound
}

func (s *userDataStore) SaveLifecycleEvent(_ context.Context, event Event) (Event, error) {
	event.EventID = int64(len(s.saved) + 1)
	s.saved = append(s.saved, event)
	return event, nil
}

func TestUserDataIngestorWritesPartialFillEvent(t *testing.T) {
	store := &userDataStore{open: OpenOrder{
		SessionID: "sess-1", AccountID: 1, VenueID: 2, Environment: 2, Exchange: 1, Market: 2,
		PositionSide: 1, Side: "BUY", IntentID: "intent-1", AttemptID: "attempt-1", OrderID: "order-1",
		ExchangeOrderID: "1001", ClientOrderID: "cid-1", Symbol: "ETHUSDT",
	}}
	ingestor := NewUserDataIngestor(store)
	err := ingestor.Ingest(context.Background(), 2, adapter.UserDataOrderEvent{
		EventSource: "websocket", EventTime: time.UnixMilli(1700000000000).UTC(),
		Symbol: "ETHUSDT", ClientOrderID: "cid-1", ExchangeOrderID: "1001", ExchangeTradeID: "9001",
		Side: "BUY", PositionSide: "LONG", OrderStatus: "PARTIALLY_FILLED", ExecutionType: "TRADE",
		LastFilledQty: 0.2, LastFilledPrice: 2000, AccumulatedFilledQty: 0.2, Fee: 0.08, FeeAsset: "USDT",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(store.saved) != 1 {
		t.Fatalf("saved events = %d, want 1", len(store.saved))
	}
	got := store.saved[0]
	if got.EventType != "fill" || got.EventSource != EventSourceWebsocket || got.FillDelta.ExchangeTradeID != "9001" {
		t.Fatalf("unexpected event: %+v", got)
	}
	if got.OrderState.Status != "PARTIALLY_FILLED" || got.OrderState.RemainingQty != 0 {
		t.Fatalf("unexpected order state: %+v", got.OrderState)
	}
}

func TestUserDataIngestorIgnoresNonTradeNewState(t *testing.T) {
	store := &userDataStore{open: OpenOrder{VenueID: 2, ExchangeOrderID: "1001", ClientOrderID: "cid-1"}}
	ingestor := NewUserDataIngestor(store)
	err := ingestor.Ingest(context.Background(), 2, adapter.UserDataOrderEvent{
		EventSource: "websocket", EventTime: time.Now().UTC(), ExchangeOrderID: "1001", ClientOrderID: "cid-1",
		OrderStatus: "NEW", ExecutionType: "NEW", Symbol: "ETHUSDT", Side: "BUY",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(store.saved) != 0 {
		t.Fatalf("saved events = %d, want 0", len(store.saved))
	}
}
```

- [ ] **Step 2: Run failing ingestor tests**

Run:

```bash
cd /Users/xdy/Workplace/hushine/core-service
go test ./internal/order/lifecycle -run TestUserDataIngestor -count=1
```

Expected: compile failure because `NewUserDataIngestor`, `ErrOpenOrderNotFound`, and resolver interface do not exist.

- [ ] **Step 3: Implement lifecycle user-data ingestor**

Create `core-service/internal/order/lifecycle/user_data_ingestor.go`:

```go
package lifecycle

import (
	"context"
	"errors"
	"strings"

	"github.com/hushine-tech/core-service/internal/exchange/adapter"
)

var ErrOpenOrderNotFound = errors.New("open order not found")

type UserDataOrderResolver interface {
	ResolveOpenOrderByExchangeRef(ctx context.Context, venueID int64, exchangeOrderID, clientOrderID string) (OpenOrder, error)
}

type UserDataIngestStore interface {
	EventStore
	UserDataOrderResolver
}

type UserDataIngestor struct {
	store UserDataIngestStore
}

func NewUserDataIngestor(store UserDataIngestStore) *UserDataIngestor {
	return &UserDataIngestor{store: store}
}

func (i *UserDataIngestor) Ingest(ctx context.Context, venueID int64, event adapter.UserDataOrderEvent) error {
	execType := strings.ToUpper(strings.TrimSpace(event.ExecutionType))
	status := strings.ToUpper(strings.TrimSpace(event.OrderStatus))
	if execType != "TRADE" && !isTerminalUserDataStatus(status) {
		return nil
	}
	order, err := i.store.ResolveOpenOrderByExchangeRef(ctx, venueID, event.ExchangeOrderID, event.ClientOrderID)
	if err != nil {
		return err
	}
	lifecycleEvent := Event{
		SessionID:       order.SessionID,
		AccountID:       order.AccountID,
		VenueID:         order.VenueID,
		Environment:     order.Environment,
		Exchange:        order.Exchange,
		Market:          order.Market,
		PositionSide:    order.PositionSide,
		Side:            firstNonEmpty(event.Side, order.Side),
		IntentID:        order.IntentID,
		AttemptID:       order.AttemptID,
		OrderID:         order.OrderID,
		ExchangeOrderID: firstNonEmpty(event.ExchangeOrderID, order.ExchangeOrderID),
		ExchangeTradeID: event.ExchangeTradeID,
		EventSource:     EventSourceWebsocket,
		OrderStatus:     status,
		OrderState: OrderState{
			ExchangeOrderID: firstNonEmpty(event.ExchangeOrderID, order.ExchangeOrderID),
			ClientOrderID:   firstNonEmpty(event.ClientOrderID, order.ClientOrderID),
			Symbol:          firstNonEmpty(event.Symbol, order.Symbol),
			Status:          status,
			ExecutedQty:     event.AccumulatedFilledQty,
			UpdatedAt:       event.EventTime,
		},
		OccurredAt: event.EventTime,
	}
	if execType == "TRADE" && event.LastFilledQty > 0 {
		lifecycleEvent.EventType = "fill"
		lifecycleEvent.FillDelta = FillDelta{
			ExchangeTradeID: event.ExchangeTradeID,
			ExchangeOrderID: firstNonEmpty(event.ExchangeOrderID, order.ExchangeOrderID),
			Symbol:          firstNonEmpty(event.Symbol, order.Symbol),
			Qty:             event.LastFilledQty,
			FillPrice:       event.LastFilledPrice,
			Fee:             event.Fee,
			FeeAsset:        event.FeeAsset,
			TradeTime:       event.EventTime,
		}
	} else {
		lifecycleEvent.EventType = "terminal"
	}
	if err := ValidateEventRouteFacts(lifecycleEvent); err != nil {
		return err
	}
	_, err = i.store.SaveLifecycleEvent(ctx, lifecycleEvent)
	return err
}

func isTerminalUserDataStatus(status string) bool {
	switch status {
	case "FILLED", "CANCELED", "EXPIRED", "REJECTED":
		return true
	default:
		return false
	}
}
```

- [ ] **Step 4: Add repository resolver interface method**

Modify `core-service/internal/order/repository/repository.go`:

```go
ResolveOpenOrderByExchangeRef(ctx context.Context, venueID int64, exchangeOrderID, clientOrderID string) (lifecycle.OpenOrder, error)
```

- [ ] **Step 5: Implement Timescale resolver**

Modify `core-service/internal/order/repository/timescale.go` with SQL that joins `orders`, `order_attempts`, and `order_intents` and returns the same columns as `ListDueOpenOrders`. Match by:

```sql
o.venue_id = $1
AND (
  ($2 <> '' AND o.exchange_order_id = $2)
  OR ($3 <> '' AND o.client_order_id = $3)
  OR ($3 <> '' AND a.client_order_id = $3)
)
AND COALESCE(o.recovery_status, '') IN ('RECOVERING', 'FILL_PENDING')
```

Map no rows to `lifecycle.ErrOpenOrderNotFound`.

- [ ] **Step 6: Run lifecycle and repository tests**

Run:

```bash
cd /Users/xdy/Workplace/hushine/core-service
go test ./internal/order/lifecycle -run TestUserDataIngestor -count=1
go test ./internal/order/repository -run 'Test.*ResolveOpenOrderByExchangeRef|Test.*Lifecycle' -count=1
```

Expected: tests pass.

- [ ] **Step 7: Commit**

```bash
cd /Users/xdy/Workplace/hushine
git add core-service/internal/order/lifecycle/user_data_ingestor.go \
  core-service/internal/order/lifecycle/user_data_ingestor_test.go \
  core-service/internal/order/repository/repository.go \
  core-service/internal/order/repository/timescale.go \
  core-service/internal/order/repository/timescale_lifecycle_test.go
git commit -m "将 WS partial fill 接入订单 lifecycle"
```

### Task 7: Add CLI Mock Server And Manual Smoke Flow

**Files:**
- Create: `core-service/cmd/mock-binance/main.go`
- Modify: `core-service/Makefile`
- Create: `core-service/scripts/mock_binance_partial_fill_smoke.sh`
- Modify: `core-service/docs/superpowers/specs/2026-06-13-adapter-level-exchange-mock-design.md`

- [ ] **Step 1: Create CLI entrypoint**

Create `core-service/cmd/mock-binance/main.go`:

```go
package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/hushine-tech/core-service/internal/exchange/binance/mockserver"
)

func main() {
	addr := flag.String("addr", ":19000", "HTTP/WebSocket listen address")
	flag.Parse()

	mock := mockserver.New()
	log.Printf("mock Binance REST/WS listening on %s", *addr)
	log.Printf("REST base URL: http://127.0.0.1%s", *addr)
	log.Printf("WS base URL: ws://127.0.0.1%s", *addr)
	if err := http.ListenAndServe(*addr, mock.Handler()); err != nil {
		log.Fatal(err)
	}
}
```

- [ ] **Step 2: Add Makefile target**

Modify `core-service/Makefile`:

```make
.PHONY: mock-binance
mock-binance:
	go run ./cmd/mock-binance -addr :19000
```

- [ ] **Step 3: Add smoke script**

Create `core-service/scripts/mock_binance_partial_fill_smoke.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

export BINANCE_SPOT_REST_BASE_URL="${BINANCE_SPOT_REST_BASE_URL:-http://127.0.0.1:19000}"
export BINANCE_FUTURES_REST_BASE_URL="${BINANCE_FUTURES_REST_BASE_URL:-http://127.0.0.1:19000}"
export BINANCE_SPOT_WS_BASE_URL="${BINANCE_SPOT_WS_BASE_URL:-ws://127.0.0.1:19000}"
export BINANCE_FUTURES_WS_BASE_URL="${BINANCE_FUTURES_WS_BASE_URL:-ws://127.0.0.1:19000}"

go test ./internal/exchange/binance ./internal/exchange/binance/mockserver ./internal/order/lifecycle \
  -run 'Mock|UserData|Partial|Recovery|ForceClose' \
  -count=1
```

Run:

```bash
chmod +x /Users/xdy/Workplace/hushine/core-service/scripts/mock_binance_partial_fill_smoke.sh
```

- [ ] **Step 4: Document manual testing**

Update `core-service/docs/superpowers/specs/2026-06-13-adapter-level-exchange-mock-design.md` with:

```bash
cd /Users/xdy/Workplace/hushine/core-service
make mock-binance
```

In another shell:

```bash
cd /Users/xdy/Workplace/hushine/core-service
BINANCE_SPOT_REST_BASE_URL=http://127.0.0.1:19000 \
BINANCE_FUTURES_REST_BASE_URL=http://127.0.0.1:19000 \
BINANCE_SPOT_WS_BASE_URL=ws://127.0.0.1:19000 \
BINANCE_FUTURES_WS_BASE_URL=ws://127.0.0.1:19000 \
./scripts/mock_binance_partial_fill_smoke.sh
```

- [ ] **Step 5: Run CLI build and smoke script**

Run:

```bash
cd /Users/xdy/Workplace/hushine/core-service
go test ./cmd/mock-binance ./internal/exchange/binance/mockserver -count=1
./scripts/mock_binance_partial_fill_smoke.sh
```

Expected: CLI package builds and smoke tests pass.

- [ ] **Step 6: Commit**

```bash
cd /Users/xdy/Workplace/hushine
git add core-service/cmd/mock-binance/main.go \
  core-service/Makefile \
  core-service/scripts/mock_binance_partial_fill_smoke.sh \
  core-service/docs/superpowers/specs/2026-06-13-adapter-level-exchange-mock-design.md
git commit -m "增加 Binance mock 本地测试入口"
```

### Task 8: Final Verification Matrix

**Files:**
- Modify only files required by failures found in this task.

- [ ] **Step 1: Run adapter and lifecycle focused tests**

Run:

```bash
cd /Users/xdy/Workplace/hushine/core-service
go test ./internal/exchange/adapter ./internal/exchange/binance ./internal/exchange/binance/mockserver ./internal/order/lifecycle -count=1
```

Expected: all tests pass.

- [ ] **Step 2: Run order hardening focused tests**

Run:

```bash
cd /Users/xdy/Workplace/hushine/core-service
go test ./internal/order/risk ./internal/order/service ./internal/order/executor ./internal/order/repository \
  -run 'Risk|Partial|PARTIALLY|FillPending|Recovery|ForceClose|UserData|Mock|OrderType|PostOnly|ReduceOnly|GTD|IOC|FOK' \
  -count=1
```

Expected: all selected tests pass.

- [ ] **Step 3: Run strategy partial-fill focused tests**

Run:

```bash
cd /Users/xdy/Workplace/hushine/strategy-service
PYTHONPATH=.:../strategy-library pytest -q \
  tests/test_strategy_engine.py \
  tests/test_strategy_phase3_runtime.py \
  tests/test_order_client.py \
  -k 'partial or fill_pending or lifecycle or force_close or fee_missing'
```

Expected: all selected tests pass. If no strategy tests are selected for a specific keyword, remove that keyword and run the remaining selected expression.

- [ ] **Step 4: Run full targeted core order suite**

Run:

```bash
cd /Users/xdy/Workplace/hushine/core-service
go test ./internal/exchange/binance ./internal/order/lifecycle ./internal/order/service ./internal/order/risk ./internal/order/executor -count=1
```

Expected: all tests pass.

- [ ] **Step 5: Record progress**

Append to `progress/discuss.md`:

```markdown
## 2026-06-13 Binance Adapter Mock 测试入口落地

- Binance adapter 现在支持 REST/WS endpoint 注入，可指向本地 mock。
- Binance mock 是 adapter 内部实现，不是公共万能交易所 mock。
- 本地测试入口：`cd core-service && make mock-binance`。
- 重点覆盖：partial fill、WS duplicate、REST recovery、FOK/IOC/post-only/GTD/reduce_only、14 天 force-close。
```

Append to `progress/roadmap.md` current priority section:

```markdown
- Binance adapter-level REST/WS mock 已落地，后续接入 OKX 时必须实现 OKX 自己的 mockserver，而不是复用 Binance mock 行为。
```

- [ ] **Step 6: Final commit**

```bash
cd /Users/xdy/Workplace/hushine
git add progress/discuss.md progress/roadmap.md
git commit -m "记录 Binance adapter mock 测试入口"
```

## Manual Testing After Implementation

Use this sequence when you want to exercise the edge cases locally:

1. Start the mock:

```bash
cd /Users/xdy/Workplace/hushine/core-service
make mock-binance
```

2. In another shell, point Binance endpoints at the mock:

```bash
export BINANCE_SPOT_REST_BASE_URL=http://127.0.0.1:19000
export BINANCE_FUTURES_REST_BASE_URL=http://127.0.0.1:19000
export BINANCE_SPOT_WS_BASE_URL=ws://127.0.0.1:19000
export BINANCE_FUTURES_WS_BASE_URL=ws://127.0.0.1:19000
```

3. Run the smoke suite:

```bash
cd /Users/xdy/Workplace/hushine/core-service
./scripts/mock_binance_partial_fill_smoke.sh
```

4. Run specific combinations:

```bash
cd /Users/xdy/Workplace/hushine/core-service
go test ./internal/exchange/binance ./internal/exchange/binance/mockserver \
  -run 'PostOnly|ReduceOnly|GTD|IOC|FOK|Partial|Mock|UserData' \
  -count=1

go test ./internal/order/lifecycle ./internal/order/service \
  -run 'Partial|FillPending|Recovery|ForceClose|UserData' \
  -count=1
```

5. Run strategy-side checks:

```bash
cd /Users/xdy/Workplace/hushine/strategy-service
PYTHONPATH=.:../strategy-library pytest -q \
  tests/test_strategy_engine.py tests/test_strategy_phase3_runtime.py tests/test_order_client.py \
  -k 'partial or fill_pending or lifecycle or force_close or fee_missing'
```

## Acceptance Criteria

- `BINANCE_*_REST_BASE_URL` and `BINANCE_*_WS_BASE_URL` can point the real Binance adapter at a local mock.
- The same `mockserver.Server` is reused by Go tests and `cmd/mock-binance`.
- Binance raw REST and WS payloads enter `internal/exchange/binance`; public order service only sees adapter-normalized results.
- WebSocket `PARTIALLY_FILLED` writes a lifecycle fill event with trade ID, fee, and fill delta.
- Duplicate WebSocket fill events are idempotent through existing lifecycle event identity.
- REST recovery can complete a partial order after WebSocket partial-fill notification.
- FOK partial-fill payloads remain rejected by Binance parser tests.
- Spot post-only, futures post-only, Spot reduce-only platform semantics, futures reduce-only passthrough, IOC, FOK, and GTD are covered by adapter tests.
- 14-day force-close recovery tests still pass with the mock canceller/query paths.
- Documentation states that future adapters must provide their own adapter-scoped mock.
