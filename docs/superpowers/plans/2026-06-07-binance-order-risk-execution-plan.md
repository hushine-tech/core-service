# Binance Order Risk Execution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Binance Spot + USD-M Futures order execution hardening layer with adapter-based order semantics, authoritative pre-order risk checks, user-data-stream-driven fill recovery, a 14-day recovery deadline, and unified hosted/self-hosted RuntimeChannel order requests.

**Architecture:** Public order APIs express platform semantics (`MARKET`, `LIMIT`, `GTC/IOC/FOK/GTD`, `post_only`, `reduce_only`) while exchange-specific mappings live in `core-service/internal/exchange/adapter` implementations. `core-service/internal/order/service` remains responsible for audit flow, calls `order/risk` before exchange execution, and emits normalized lifecycle events consumed by strategy runtimes through control-panel routing.

**Tech Stack:** Go 1.26 services, protobuf/gRPC, TimescaleDB migrations, Python strategy runtime, React/Vite frontend, Binance REST and User Data Stream WebSocket APIs.

---

## Scope Check

This plan spans several repositories, but the work is one execution chain rather than independent product features. Implement in the order below; each task leaves the system testable and commit-worthy.

## File Structure

Core contract and storage:

- Modify `core-service/proto/order_service.proto`: add public order fields.
- Regenerate `core-service/gen/orderv1/*`.
- Modify `strategy-service/strategy_service/gen/order_service_pb2.py` and `strategy-service/strategy_service/gen/order_service_pb2_grpc.py` by running `strategy-service/generate_proto.sh`.
- Create `core-service/internal/order/storage/migrations/0010_order_risk_recovery_contract.sql`: add order semantic fields and recovery columns.
- Modify `core-service/internal/order/repository/repository.go` and `core-service/internal/order/repository/timescale.go`: persist new fields and recovery status.
- Update `db/README.md`: document order table column additions.

Strategy contract:

- Modify `strategy-library/hushine_strategy/types.py`: add `post_only`, `good_till_date`, `reduce_only`.
- Modify `strategy-service/strategy_service/order_client.py`: pass all public order fields.
- Modify `strategy-service/strategy_service/platform_proxy.py`: make proxy client request shape match direct client.
- Modify `strategy-service/tests/test_order_client.py` and `strategy-service/tests/test_platform_proxy.py`: assert field parity.

Adapter and executor:

- Modify `core-service/internal/exchange/adapter/capabilities.go`: add order capability and normalized order types.
- Modify `core-service/internal/exchange/adapter/factory.go` and `registry.go`: expose new capability.
- Modify `core-service/internal/exchange/binance/factory.go`: remove futures-only restriction for order capabilities where Spot is supported.
- Modify `core-service/internal/exchange/binance/order_executor.go`: map public semantics to Binance Spot and Futures REST params.
- Modify `core-service/internal/order/executor/executor.go`: carry `PostOnly`, `GoodTillDate`, and `ReduceOnly`.

Risk gate:

- Create `core-service/internal/order/risk/gate.go`: public risk review interface.
- Create `core-service/internal/order/risk/types.go`: decision, violation, route key, and snapshot types.
- Create `core-service/internal/order/risk/symbol_rules.go`: symbol rule validation.
- Create `core-service/internal/order/risk/balance.go`: spot and futures balance checks.
- Create `core-service/internal/order/risk/pending.go`: pending route blocking.
- Modify `core-service/internal/order/service/grpc.go`: call RiskGate after attempt creation and before executor.

Lifecycle and recovery:

- Modify `core-service/internal/order/lifecycle/events.go`: add event source and terminal recovery event types.
- Modify `core-service/internal/order/lifecycle/ingestor.go`: ingest normalized stream events idempotently.
- Modify `core-service/internal/order/lifecycle/scanner.go`: query only due orders and enforce 14-day deadline.
- Create `core-service/internal/exchange/binance/user_data_stream.go`: manage listenKey, websocket, keepalive, reconnect.
- Create `core-service/internal/exchange/binance/user_data_events.go`: normalize Spot `executionReport` and Futures `ORDER_TRADE_UPDATE`.
- Modify `core-service/cmd/core-service/main.go`: wire user data stream manager and recovery scanner.

Control-plane and runtime:

- Modify `control-panel-service/internal/runtimechannel/platform_proxy.go`: keep unified order method and carry new fields transparently.
- Modify `strategy-service/strategy_service/strategy/base.py`: keep route blocked until terminal lifecycle event, apply incremental fills, handle force-close terminal updates.

Frontend and handler:

- Modify `gateway/quant-handler/internal/app/order_history.go`: expose new order fields and risk/recovery fields.
- Modify `gateway/quant-frontend/src/components/OrderTree.tsx`: display order semantic, risk, and recovery state.
- Modify notification paths in `core-service/internal/order/notification/event.go`: include risk reject, partial fill, fee missing, force close.

---

### Task 1: Extend Public Order Contract

**Files:**
- Modify: `core-service/proto/order_service.proto`
- Regenerate: `core-service/gen/orderv1/order_service.pb.go`
- Regenerate: `core-service/gen/orderv1/order_service_grpc.pb.go`
- Modify: `strategy-library/hushine_strategy/types.py`
- Regenerate: `strategy-service/strategy_service/gen/order_service_pb2.py`
- Regenerate: `strategy-service/strategy_service/gen/order_service_pb2_grpc.py`
- Test: `core-service/internal/order/service/grpc_test.go`
- Test: `strategy-service/tests/test_order_client.py`
- Test: `strategy-service/tests/test_platform_proxy.py`

- [ ] **Step 1: Add failing Go tests for public fields**

Add a test in `core-service/internal/order/service/grpc_test.go` named `TestPlaceOrder_PassesAdvancedOrderContractToExecutor`. It should construct a `PlaceOrderRequest` with:

```go
req.OrderType = "LIMIT"
req.TimeInForce = "GTD"
req.PostOnly = false
req.ReduceOnly = true
req.GoodTillDate = timestamppb.New(time.Unix(1893456000, 0).UTC())
```

Expected assertions:

```go
if !router.lastReq.ReduceOnly {
	t.Fatalf("reduce_only was not forwarded")
}
if router.lastReq.GoodTillDate == nil || router.lastReq.GoodTillDate.Unix() != 1893456000 {
	t.Fatalf("good_till_date = %v, want 1893456000", router.lastReq.GoodTillDate)
}
```

- [ ] **Step 2: Run the failing Go test**

Run:

```bash
cd /Users/xdy/Workplace/hushine/core-service
go test ./internal/order/service -run TestPlaceOrder_PassesAdvancedOrderContractToExecutor -count=1
```

Expected: compile failure because `PlaceOrderRequest` and `executor.OrderRequest` do not yet contain the new fields.

- [ ] **Step 3: Add proto fields**

Modify `core-service/proto/order_service.proto`:

```proto
  bool post_only = 16;       // platform semantic; adapter maps to LIMIT_MAKER or GTX
  google.protobuf.Timestamp good_till_date = 17; // required for GTD
  bool reduce_only = 18;     // platform semantic; spot enforced by RiskGate, futures mapped to exchange
```

Do not renumber existing fields.

- [ ] **Step 4: Regenerate Go and Python stubs**

Run:

```bash
cd /Users/xdy/Workplace/hushine/core-service
make proto-order
cd /Users/xdy/Workplace/hushine/strategy-service
./generate_proto.sh
```

Expected: generated files update with fields `PostOnly`, `GoodTillDate`, and `ReduceOnly`.

- [ ] **Step 5: Extend Python strategy OrderDecision**

Modify `strategy-library/hushine_strategy/types.py`:

```python
@dataclass(frozen=True)
class OrderDecision:
    exchange: str
    market: str
    symbol: str
    side: str
    qty: str
    order_type: str
    price: str | None = None
    position_side: str | None = None
    time_in_force: str | None = None
    post_only: bool = False
    good_till_date: Any | None = None
    reduce_only: bool = False
```

- [ ] **Step 6: Pass new fields in direct OrderClient**

Modify `strategy-service/strategy_service/order_client.py` in `OrderClient.place_order` after time-in-force handling:

```python
kwargs["post_only"] = bool(getattr(decision, "post_only", False))
kwargs["reduce_only"] = bool(getattr(decision, "reduce_only", False))
good_till_date_pb = _market_time_to_proto(getattr(decision, "good_till_date", None))
if good_till_date_pb is not None:
    kwargs["good_till_date"] = good_till_date_pb
```

- [ ] **Step 7: Pass new fields in ProxyOrderClient**

Modify `strategy-service/strategy_service/platform_proxy.py` in `ProxyOrderClient.place_order` to mirror direct client:

```python
order_type = str(getattr(decision, "order_type", None) or "").strip().upper()
if not order_type:
    order_type = "LIMIT" if decision.price is not None else "MARKET"
kwargs["order_type"] = order_type
time_in_force = str(getattr(decision, "time_in_force", None) or "").strip().upper()
if order_type == "LIMIT":
    kwargs["time_in_force"] = time_in_force or "GTC"
kwargs["post_only"] = bool(getattr(decision, "post_only", False))
kwargs["reduce_only"] = bool(getattr(decision, "reduce_only", False))
good_till_date_pb = _market_time_to_proto(getattr(decision, "good_till_date", None))
if good_till_date_pb is not None:
    kwargs["good_till_date"] = good_till_date_pb
```

- [ ] **Step 8: Run contract tests**

Run:

```bash
cd /Users/xdy/Workplace/hushine/core-service
go test ./internal/order/service -run 'TestPlaceOrder_.*OrderContract|TestPlaceOrder_PassesAdvancedOrderContractToExecutor' -count=1
cd /Users/xdy/Workplace/hushine/strategy-service
PYTHONPATH=.:./strategy-library pytest -q tests/test_order_client.py tests/test_platform_proxy.py
```

Expected: all selected tests pass.

- [ ] **Step 9: Commit**

```bash
cd /Users/xdy/Workplace/hushine/core-service
git add proto/order_service.proto gen/orderv1 internal/order/service/grpc_test.go
git commit -m "扩展订单公共执行契约"
cd /Users/xdy/Workplace/hushine/strategy-library
git add hushine_strategy/types.py
git commit -m "扩展策略订单决策字段"
cd /Users/xdy/Workplace/hushine/strategy-service
git add strategy_service/gen strategy_service/order_client.py strategy_service/platform_proxy.py tests/test_order_client.py tests/test_platform_proxy.py
git commit -m "统一 runtime 订单请求字段"
```

### Task 2: Persist Order Semantics And Recovery State

**Files:**
- Create: `core-service/internal/order/storage/migrations/0010_order_risk_recovery_contract.sql`
- Modify: `core-service/internal/order/repository/repository.go`
- Modify: `core-service/internal/order/repository/timescale.go`
- Modify: `core-service/internal/order/repository/timescale_migrations_test.go`
- Modify: `db/README.md`

- [ ] **Step 1: Add migration contract test**

In `core-service/internal/order/repository/timescale_migrations_test.go`, add assertions that the order DB migrations include:

```go
requiredColumns := []string{
	"order_intents.post_only",
	"order_intents.good_till_date",
	"order_intents.reduce_only",
	"order_attempts.risk_status",
	"order_attempts.risk_reasons_json",
	"orders.recovery_status",
	"orders.next_check_at",
	"orders.recovery_deadline_at",
	"orders.force_closed_at",
}
```

- [ ] **Step 2: Run migration test and confirm failure**

```bash
cd /Users/xdy/Workplace/hushine/core-service
go test ./internal/order/repository -run TestOrderMigrations -count=1
```

Expected: failure because migration `0010` does not exist.

- [ ] **Step 3: Add migration**

Create `core-service/internal/order/storage/migrations/0010_order_risk_recovery_contract.sql`:

```sql
ALTER TABLE order_intents
  ADD COLUMN IF NOT EXISTS post_only BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS good_till_date TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS reduce_only BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE order_attempts
  ADD COLUMN IF NOT EXISTS post_only BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS good_till_date TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS reduce_only BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS risk_status TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS risk_reasons_json JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE orders
  ADD COLUMN IF NOT EXISTS post_only BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS good_till_date TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS reduce_only BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS recovery_status TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS recovery_started_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS next_check_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS recovery_deadline_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_recovery_error TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS force_closed_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_orders_recovery_due
  ON orders (next_check_at)
  WHERE recovery_status IN ('OPEN', 'PARTIALLY_FILLED', 'FILL_PENDING', 'FEE_MISSING', 'RECOVERING');
```

- [ ] **Step 4: Extend repository structs**

Add fields to `OrderIntent`, `OrderAttempt`, and `Order` in `core-service/internal/order/repository/repository.go`:

```go
PostOnly     bool
GoodTillDate *time.Time
ReduceOnly   bool
```

Add to `OrderAttempt`:

```go
RiskStatus      string
RiskReasonsJSON string
```

Add to `Order`:

```go
RecoveryStatus     string
RecoveryStartedAt  *time.Time
NextCheckAt        *time.Time
RecoveryDeadlineAt *time.Time
LastRecoveryError  string
ForceClosedAt      *time.Time
```

- [ ] **Step 5: Persist fields in Timescale repository**

Modify `core-service/internal/order/repository/timescale.go` insert/select statements for intents, attempts, and orders so every new struct field is round-tripped. Use helper functions for nullable timestamps:

```go
func nullableTimePtr(value *time.Time) any {
	if value == nil || value.IsZero() {
		return nil
	}
	return *value
}
```

- [ ] **Step 6: Run repository tests**

```bash
cd /Users/xdy/Workplace/hushine/core-service
go test ./internal/order/repository -count=1
```

Expected: pass.

- [ ] **Step 7: Update DB inventory**

Modify `/Users/xdy/Workplace/hushine/db/README.md` in the order DB section to mention the new audit/recovery columns on `order_intents`, `order_attempts`, and `orders`.

- [x] **Step 8: Commit**

```bash
cd /Users/xdy/Workplace/hushine/core-service
git add internal/order/storage/migrations/0010_order_risk_recovery_contract.sql internal/order/repository db/README.md
git commit -m "持久化订单风控与恢复状态"
```

### Task 3: Add Adapter Order Capabilities And Binance Mapping

**Files:**
- Modify: `core-service/internal/exchange/adapter/capabilities.go`
- Modify: `core-service/internal/exchange/adapter/factory.go`
- Modify: `core-service/internal/exchange/adapter/registry.go`
- Modify: `core-service/internal/exchange/binance/factory.go`
- Modify: `core-service/internal/exchange/binance/order_executor.go`
- Modify: `core-service/internal/order/executor/executor.go`
- Test: `core-service/internal/exchange/binance/factory_test.go`
- Test: `core-service/internal/exchange/binance/order_executor_test.go`

- [ ] **Step 1: Add adapter capability tests**

Create tests in `core-service/internal/exchange/binance/order_executor_test.go`:

```go
func TestBinanceSpotPostOnlyMapsToLimitMaker(t *testing.T) {
	req := adapter.OrderRequest{
		Exchange: domain.ExchangeBinance,
		Market: domain.MarketSpot,
		OrderType: "LIMIT",
		TimeInForce: "GTC",
		PostOnly: true,
		Symbol: "ETHUSDT",
		Side: "SELL",
		Qty: 1,
		Price: ptrFloat(3000),
	}
	got, err := normalizeBinanceOrderContract(req)
	if err != nil {
		t.Fatalf("normalize err: %v", err)
	}
	if got.Type != "LIMIT_MAKER" || got.TimeInForce != "" {
		t.Fatalf("spot post-only mapped to %+v, want LIMIT_MAKER without tif", got)
	}
}

func TestBinanceFuturesPostOnlyMapsToGTX(t *testing.T) {
	req := adapter.OrderRequest{
		Exchange: domain.ExchangeBinance,
		Market: domain.MarketPerpetualFutures,
		OrderType: "LIMIT",
		TimeInForce: "GTC",
		PostOnly: true,
		Symbol: "ETHUSDT",
		Side: "SELL",
		Qty: 1,
		Price: ptrFloat(3000),
	}
	got, err := normalizeBinanceOrderContract(req)
	if err != nil {
		t.Fatalf("normalize err: %v", err)
	}
	if got.Type != "LIMIT" || got.TimeInForce != "GTX" {
		t.Fatalf("futures post-only mapped to %+v, want LIMIT/GTX", got)
	}
}
```

- [ ] **Step 2: Run tests and confirm failure**

```bash
cd /Users/xdy/Workplace/hushine/core-service
go test ./internal/exchange/binance -run 'PostOnly|GTD|ReduceOnly' -count=1
```

Expected: compile failure because normalizer and fields are missing.

- [ ] **Step 3: Extend adapter types**

In `core-service/internal/exchange/adapter/capabilities.go`, extend `OrderRequest`:

```go
PostOnly     bool
GoodTillDate *time.Time
ReduceOnly   bool
```

Add:

```go
type OrderCapability struct {
	Market            domain.Market
	OrderTypes        []string
	TimeInForce       []string
	SupportsPostOnly  bool
	SupportsGTD       bool
	SupportsReduceOnly bool
}

type OrderCapabilityProvider interface {
	OrderCapability(ctx context.Context, credential ParsedCredential) (OrderCapability, error)
}
```

Add `OrderCapabilityProvider() (OrderCapabilityProvider, error)` to `Factory` and `Registry`.

- [ ] **Step 4: Add Binance contract normalizer**

In `core-service/internal/exchange/binance/order_executor.go`, add a small internal type:

```go
type binanceOrderContract struct {
	Type         string
	TimeInForce  string
	GoodTillDate *time.Time
	ReduceOnly   bool
}
```

Implement `normalizeBinanceOrderContract(req adapter.OrderRequest) (binanceOrderContract, error)` with the mapping from the design spec.

- [ ] **Step 5: Use normalizer in Binance executor**

Use `normalizeBinanceOrderContract` before building REST params. For Spot, do not send `reduceOnly`. For Futures, send `reduceOnly=true` only when `req.ReduceOnly` is true.

- [ ] **Step 6: Run adapter tests**

```bash
cd /Users/xdy/Workplace/hushine/core-service
go test ./internal/exchange/adapter ./internal/exchange/binance -count=1
```

Expected: pass.

- [ ] **Step 7: Commit**

```bash
cd /Users/xdy/Workplace/hushine/core-service
git add internal/exchange/adapter internal/exchange/binance internal/order/executor
git commit -m "通过 adapter 映射 Binance 订单语义"
```

### Task 4: Introduce Authoritative RiskGate

**Files:**
- Create: `core-service/internal/order/risk/types.go`
- Create: `core-service/internal/order/risk/gate.go`
- Create: `core-service/internal/order/risk/symbol_rules.go`
- Create: `core-service/internal/order/risk/balance.go`
- Create: `core-service/internal/order/risk/pending.go`
- Test: `core-service/internal/order/risk/gate_test.go`
- Modify: `core-service/internal/order/service/grpc.go`
- Test: `core-service/internal/order/service/grpc_test.go`

- [ ] **Step 1: Write RiskGate tests**

Create `core-service/internal/order/risk/gate_test.go` with table tests covering:

```go
cases := []struct {
	name string
	req ReviewRequest
	wantStatus string
	wantCode string
}{
	{name: "spot reduce only buy rejected", wantStatus: "REJECT", wantCode: "SPOT_REDUCE_ONLY_BUY"},
	{name: "spot reduce only sell over unlocked rejected", wantStatus: "REJECT", wantCode: "INSUFFICIENT_UNLOCKED_QTY"},
	{name: "futures missing risk metadata rejected", wantStatus: "REJECT", wantCode: "RISK_METADATA_MISSING"},
	{name: "pending route blocks open order", wantStatus: "REJECT", wantCode: "ROUTE_PENDING_EXECUTION"},
	{name: "supported limit gtc allowed", wantStatus: "ALLOW", wantCode: ""},
}
```

- [ ] **Step 2: Run failing RiskGate tests**

```bash
cd /Users/xdy/Workplace/hushine/core-service
go test ./internal/order/risk -count=1
```

Expected: package does not exist.

- [ ] **Step 3: Add risk types**

Create `core-service/internal/order/risk/types.go`:

```go
package risk

import "time"

type DecisionStatus string

const (
	DecisionAllow  DecisionStatus = "ALLOW"
	DecisionReject DecisionStatus = "REJECT"
)

type Violation struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type Decision struct {
	Status     DecisionStatus `json:"status"`
	ReasonCode string         `json:"reason_code,omitempty"`
	Violations []Violation    `json:"violations,omitempty"`
	Warnings   []Violation    `json:"warnings,omitempty"`
	ReviewedAt time.Time      `json:"reviewed_at"`
}
```

- [ ] **Step 4: Add gate interface and implementation**

Create `core-service/internal/order/risk/gate.go`:

```go
package risk

import "context"

type ReviewRequest struct {
	AccountID int64
	VenueID int64
	Exchange int32
	Market int32
	PositionSide int32
	Symbol string
	Side string
	Qty float64
	Price *float64
	MarkPrice float64
	OrderType string
	TimeInForce string
	PostOnly bool
	GoodTillDate *time.Time
	ReduceOnly bool
}

type Gate interface {
	Review(ctx context.Context, req ReviewRequest) (Decision, error)
}
```

Add `DefaultGate` with injected symbol rule reader, balance reader, capability provider, and pending order reader.

- [ ] **Step 5: Wire RiskGate into PlaceOrder**

Modify `core-service/internal/order/service/grpc.go` so `PlaceOrder` calls `s.riskGate.Review` after `CreateOrderAttempt` and before `s.routerExec.Execute`. On reject:

```go
attempt.Status = "RISK_REJECTED"
attempt.RiskStatus = string(decision.Status)
attempt.RiskReasonsJSON = marshalRiskViolations(decision)
attempt.ErrorMessage = decision.ReasonCode
attempt.Time = time.Now().UTC()
_ = s.repo.FinalizeOrderAttempt(ctx, attempt, nil, nil)
return buildPlaceOrderResponse(attempt, nil, nil), nil
```

- [ ] **Step 6: Run risk and order service tests**

```bash
cd /Users/xdy/Workplace/hushine/core-service
go test ./internal/order/risk ./internal/order/service -count=1
```

Expected: pass.

- [ ] **Step 7: Commit**

```bash
cd /Users/xdy/Workplace/hushine/core-service
git add internal/order/risk internal/order/service internal/order/repository
git commit -m "接入订单下单前风控审查"
```

### Task 5: Add Binance User Data Stream Ingest

**Files:**
- Create: `core-service/internal/exchange/binance/user_data_events.go`
- Create: `core-service/internal/exchange/binance/user_data_stream.go`
- Test: `core-service/internal/exchange/binance/user_data_events_test.go`
- Modify: `core-service/internal/order/lifecycle/events.go`
- Modify: `core-service/internal/order/lifecycle/ingestor.go`
- Test: `core-service/internal/order/lifecycle/events_test.go`

- [ ] **Step 1: Add event normalization tests**

Create `core-service/internal/exchange/binance/user_data_events_test.go` with one Spot fixture and one Futures fixture. The Spot fixture must parse `executionReport` with `x=TRADE`, `X=PARTIALLY_FILLED`, `t=12345`, `l=0.1`, `L=3000`, `n=0.03`, `N=USDT`. The Futures fixture must parse `ORDER_TRADE_UPDATE` with `o.x=TRADE`, `o.X=FILLED`, `o.t=67890`, `o.l=0.2`, `o.L=3100`, `o.n=0.04`, `o.N=USDT`.

- [ ] **Step 2: Run failing tests**

```bash
cd /Users/xdy/Workplace/hushine/core-service
go test ./internal/exchange/binance -run UserData -count=1
```

Expected: fail because user-data event parser does not exist.

- [ ] **Step 3: Implement normalized event type**

Create `user_data_events.go` with:

```go
type UserDataOrderEvent struct {
	EventSource string
	EventTime time.Time
	Symbol string
	ClientOrderID string
	ExchangeOrderID string
	ExchangeTradeID string
	Side string
	PositionSide string
	OrderType string
	TimeInForce string
	OrderStatus string
	ExecutionType string
	LastFilledQty float64
	LastFilledPrice float64
	AccumulatedFilledQty float64
	Fee float64
	FeeAsset string
	ReduceOnly bool
}
```

Add `ParseSpotUserDataOrderEvent` and `ParseFuturesUserDataOrderEvent`.

- [ ] **Step 4: Extend lifecycle event source**

Modify `core-service/internal/order/lifecycle/events.go`:

```go
EventSource string
```

Add the `EventSource` field to the existing `Event` struct directly after `EventType`.

Allowed values for first version: `place_order`, `websocket`, `rest_recovery`, `force_close`.

- [ ] **Step 5: Make ingestor idempotent**

Modify `core-service/internal/order/lifecycle/ingestor.go` so repeated event ingestion with the same `exchange_trade_id` does not create duplicate wallet deltas. The repository `SaveLifecycleEvent` already upserts by event identity; keep that behavior and add tests for duplicate trade ID.

- [ ] **Step 6: Run lifecycle and parser tests**

```bash
cd /Users/xdy/Workplace/hushine/core-service
go test ./internal/exchange/binance ./internal/order/lifecycle -count=1
```

Expected: pass.

- [ ] **Step 7: Commit**

```bash
cd /Users/xdy/Workplace/hushine/core-service
git add internal/exchange/binance/user_data_* internal/order/lifecycle
git commit -m "接入 Binance 用户数据流订单事件"
```

### Task 6: Add REST Recovery Scanner And 14-Day Deadline

**Files:**
- Modify: `core-service/internal/order/lifecycle/scanner.go`
- Modify: `core-service/internal/order/lifecycle/scanner_test.go`
- Modify: `core-service/internal/order/repository/repository.go`
- Modify: `core-service/internal/order/repository/timescale.go`
- Modify: `core-service/internal/order/service/grpc.go`
- Create: `core-service/internal/order/executor/adapter_recovery_client.go`
- Modify: `core-service/cmd/core-service/main.go`

- [x] **Step 1: Add deadline tests**

In `core-service/internal/order/lifecycle/scanner_test.go`, add `TestScannerForceClosesAfterDeadline`. It should create an open order with `RecoveryDeadlineAt` before `now`, fake a successful cancel, fake final trades, and assert a terminal event with:

```go
EventSource: "force_close",
EventType: "terminal",
OrderStatus: "RECOVERY_EXPIRED",
```

- [x] **Step 2: Run failing scanner test**

```bash
cd /Users/xdy/Workplace/hushine/core-service
go test ./internal/order/lifecycle -run TestScannerForceClosesAfterDeadline -count=1
```

Expected: fail because scanner does not know deadline fields.

- [x] **Step 3: Extend scanner open order model**

Modify `OpenOrder` in `scanner.go`:

```go
RecoveryStatus     string
RecoveryStartedAt  time.Time
NextCheckAt        time.Time
RecoveryDeadlineAt time.Time
LastRecoveryError  string
```

- [x] **Step 4: Query only due orders**

Modify repository `ListOpenOrders` SQL so it returns orders where:

```sql
recovery_status IN ('OPEN', 'PARTIALLY_FILLED', 'FILL_PENDING', 'FEE_MISSING', 'RECOVERING')
AND (next_check_at IS NULL OR next_check_at <= NOW())
```

- [x] **Step 5: Implement deadline path**

In `Scanner.ScanOnce`, before normal REST query:

```go
if !order.RecoveryDeadlineAt.IsZero() && !now.Before(order.RecoveryDeadlineAt) {
	writtenForOrder, err := s.forceClose(ctx, order, now)
	written += writtenForOrder
	if err != nil {
		s.backoffVenue(order.VenueID, now)
		continue
	}
	continue
}
```

`forceClose` must cancel, query final order state, query final trades, write final fill events, then write terminal event.

- [x] **Step 6: Run scanner tests**

```bash
cd /Users/xdy/Workplace/hushine/core-service
go test ./internal/order/lifecycle ./internal/order/repository -count=1
```

Expected: pass.

- [x] **Step 7: Wire scanner config**

Modify `core-service/cmd/core-service/main.go` to construct the scanner with deadline-aware repository and adapter reader/canceller. Use config defaults:

```text
BatchSize: 50
InitialBackoff: 5s
MaxBackoff: 60s
RecoveryDeadline: 14d
```

- [ ] **Step 8: Commit**

```bash
cd /Users/xdy/Workplace/hushine/core-service
git add internal/order/lifecycle internal/order/repository cmd/core-service/main.go
git commit -m "增加订单恢复截止与强制终止"
```

### Task 7: Update Runtime Wallet Event Handling

**Files:**
- Modify: `strategy-service/strategy_service/strategy/base.py`
- Modify: `strategy-service/strategy_service/order_client.py`
- Modify: `strategy-library/hushine_strategy/types.py`
- Test: `strategy-service/tests/test_strategy_engine.py`
- Test: `strategy-service/tests/test_order_client.py`

- [ ] **Step 1: Add runtime lifecycle tests**

Add tests in `strategy-service/tests/test_strategy_engine.py`:

```python
def test_force_close_terminal_event_unblocks_route():
    # Build a strategy runtime with one blocked order key.
    # Feed an OrderUpdateEvent(order_status="RECOVERY_EXPIRED", event_type="terminal").
    # Assert the blocked key is removed and wallet qty is unchanged.
```

```python
def test_incremental_fill_event_updates_wallet_once():
    # Feed the same lifecycle fill event twice.
    # Assert wallet position changes once and _settled_lifecycle_event_ids contains the event id.
```

- [ ] **Step 2: Run failing runtime tests**

```bash
cd /Users/xdy/Workplace/hushine/strategy-service
PYTHONPATH=.:./strategy-library pytest -q tests/test_strategy_engine.py -k 'force_close or incremental_fill'
```

Expected: one or both tests fail because terminal force-close handling is not explicit.

- [ ] **Step 3: Extend lifecycle event conversion**

Modify `strategy-service/strategy_service/order_client.py` to preserve event source and terminal recovery status in `OrderUpdateEvent` if generated stubs expose it.

- [ ] **Step 4: Handle terminal recovery events**

Modify `_is_order_update_terminal` in `strategy-service/strategy_service/strategy/base.py`:

```python
if status in {"RECOVERY_EXPIRED", "FORCE_CLOSED"}:
    return True
```

Modify `_consume_order_updates` so terminal events with no fill do not call wallet settlement but do unblock the route and call `on_order_update`.

- [ ] **Step 5: Run strategy tests**

```bash
cd /Users/xdy/Workplace/hushine/strategy-service
PYTHONPATH=.:./strategy-library pytest -q tests/test_strategy_engine.py tests/test_order_client.py
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
cd /Users/xdy/Workplace/hushine/strategy-service
git add strategy_service/strategy/base.py strategy_service/order_client.py tests/test_strategy_engine.py tests/test_order_client.py
git commit -m "处理订单恢复终态与增量钱包更新"
```

### Task 8: Expose Risk And Recovery State In Control Plane And UI

**Files:**
- Modify: `control-panel-service/internal/runtimechannel/platform_proxy.go`
- Modify: `gateway/quant-handler/internal/app/order_history.go`
- Modify: `gateway/quant-handler/internal/app/order_history_test.go`
- Modify: `gateway/quant-frontend/src/components/OrderTree.tsx`
- Modify: `gateway/quant-frontend/src/api.ts`
- Test: `control-panel-service/internal/runtimechannel/platform_proxy_test.go`
- Test: `gateway/quant-handler/internal/app/order_history_test.go`

- [ ] **Step 1: Add platform proxy test**

Add a test in `control-panel-service/internal/runtimechannel/platform_proxy_test.go` that invokes `order.PlaceOrder` with `post_only`, `reduce_only`, and `good_till_date`, then asserts the fake order client received the same values.

- [ ] **Step 2: Run failing control-panel test**

```bash
cd /Users/xdy/Workplace/hushine/control-panel-service
go test ./internal/runtimechannel -run OrderPlace -count=1
```

Expected: failure if new generated order proto is not vendored or fake client does not assert fields.

- [ ] **Step 3: Ensure proxy stays transparent**

Modify `platform_proxy.go` only if required. The desired implementation is still:

```go
req := &orderv1.PlaceOrderRequest{}
if err := unpackRuntimePayload(payload, req); err != nil {
	return nil, err
}
return p.requireOrder().PlaceOrder(ctx, req)
```

Do not manually copy fields in control-panel.

- [ ] **Step 4: Expose fields through handler JSON**

Modify `gateway/quant-handler/internal/app/order_history.go` response structs to include:

```go
PostOnly bool `json:"post_only"`
GoodTillDate string `json:"good_till_date,omitempty"`
ReduceOnly bool `json:"reduce_only"`
RiskStatus string `json:"risk_status,omitempty"`
RiskReasons []riskReasonJSON `json:"risk_reasons,omitempty"`
RecoveryStatus string `json:"recovery_status,omitempty"`
NextCheckAt string `json:"next_check_at,omitempty"`
RecoveryDeadlineAt string `json:"recovery_deadline_at,omitempty"`
```

- [ ] **Step 5: Add handler serialization tests**

Extend `gateway/quant-handler/internal/app/order_history_test.go` to assert the new JSON fields for intents, attempts, and orders.

- [ ] **Step 6: Update frontend types and rendering**

Modify `gateway/quant-frontend/src/api.ts` order entry types with the same fields. Modify `OrderTree.tsx` to render compact badges:

```tsx
{order.post_only && <Badge label="Post-only" />}
{order.reduce_only && <Badge label="Reduce-only" />}
{order.recovery_status && <Badge label={order.recovery_status} />}
```

- [ ] **Step 7: Run UI and handler tests**

```bash
cd /Users/xdy/Workplace/hushine/control-panel-service
go test ./internal/runtimechannel -count=1
cd /Users/xdy/Workplace/hushine/gateway/quant-handler
go test ./internal/app -count=1
cd /Users/xdy/Workplace/hushine/gateway/quant-frontend
npm run build
```

Expected: pass.

- [ ] **Step 8: Commit**

```bash
cd /Users/xdy/Workplace/hushine/control-panel-service
git add internal/runtimechannel
git commit -m "校验 RuntimeChannel 订单请求透传"
cd /Users/xdy/Workplace/hushine/gateway/quant-handler
git add internal/app
git commit -m "透出订单风控与恢复状态"
cd /Users/xdy/Workplace/hushine/gateway/quant-frontend
git add src
git commit -m "展示订单风控与恢复状态"
```

## Final Verification

Run the following before merging or pushing:

```bash
cd /Users/xdy/Workplace/hushine/core-service
go test ./...
cd /Users/xdy/Workplace/hushine/strategy-service
PYTHONPATH=.:./strategy-library pytest tests/ -q
cd /Users/xdy/Workplace/hushine/control-panel-service
go test ./...
cd /Users/xdy/Workplace/hushine/gateway/quant-handler
go test ./...
cd /Users/xdy/Workplace/hushine/gateway/quant-frontend
npm run build
```

Expected:

```text
core-service: all Go packages pass
strategy-service: pytest suite passes
control-panel-service: all Go packages pass
quant-handler: all Go packages pass
quant-frontend: production build succeeds
```

## Self-Review

Spec coverage:

- Public order semantics are covered by Tasks 1 and 3.
- Adapter-only Binance mapping is covered by Task 3.
- RiskGate is covered by Task 4.
- User Data Stream is covered by Task 5.
- REST fallback and 14-day force close are covered by Task 6.
- Runtime wallet updates are covered by Task 7.
- Hosted/self-hosted unified RuntimeChannel request shape is covered by Tasks 1 and 8.
- Frontend and notification visibility are partially covered by Task 8; notification payloads are included in file structure and should be implemented alongside handler/frontend exposure.

Placeholder scan:

- This plan contains no placeholder work items or unspecified follow-up tasks.
- Literal `./...` patterns appear only in Go test commands.

Type consistency:

- Public field names use `post_only`, `good_till_date`, and `reduce_only` in proto/JSON/Python.
- Go struct fields use `PostOnly`, `GoodTillDate`, and `ReduceOnly`.
- Recovery terminal statuses use `RECOVERY_EXPIRED` and `FORCE_CLOSED`.
