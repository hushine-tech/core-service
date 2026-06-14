# Adapter 级交易所 Mock 服务设计

日期：2026-06-13

## 背景

订单链路已经开始支持 partial fill、REST recovery、下单前 RiskGate、14 天恢复截止和 Binance user data stream。`core-service` 启动后会通过后台 `UserDataStreamManager` 为存在 open/recovering order 的 Binance venue 建立 user data WebSocket；REST recovery scanner 继续作为补齐 trades、断线恢复和 14 天强制关闭的兜底。只靠 parser 单元测试和零散 `httptest` 无法覆盖真实运行时最容易出问题的路径：

- 下单成功后只部分成交。
- WebSocket 先到 partial fill，REST trades 稍后才补齐。
- WebSocket 事件重复、乱序或缺字段。
- `FILL_PENDING`、`FEE_MISSING`、`RECOVERING`、`RECOVERY_EXPIRED` 的状态转换。
- strategy-runtime 是否只按 confirmed fill delta 更新钱包，并在恢复完成前保持 route blocked。

因此需要一个可连接 REST 和 WebSocket 的 mock exchange 服务。但这个 mock 不能设计成所有 adapter 共用的“万能交易所”。交易所订单语义、REST 参数、错误码、WS payload 和状态机都不同，公共 mock 会很快偏离真实交易所行为。

## 核心原则

- Mock 属于 exchange adapter 自己，而不是公共 order/service 的共享交易所模型。
- 公共层只提供测试 harness、配置注入和生命周期约定，不定义统一交易所行为。
- 每个 adapter mock 必须模拟该交易所的真实 REST/WS payload、状态码、错误码和边界行为。
- 新 exchange adapter 进入 demo/live 路径前，必须提供对应 adapter mock，或显式 fail-closed 并说明缺口。
- Mock 测试必须覆盖 adapter 输入和平台标准输出两端：交易所原始 payload 进入 adapter，最终落到统一 lifecycle event / fill delta / recovery state。

## 范围

第一版实现 Binance adapter mock：

- Binance Spot REST mock。
- Binance USD-M Futures REST mock。
- Binance Spot user data stream mock，推送 `executionReport`。
- Binance USD-M Futures user data stream mock，推送 `ORDER_TRADE_UPDATE`。
- 既支持测试内启动，也支持 CLI 常驻运行。
- 覆盖 partial fill、FOK/IOC/post-only/GTD/reduce_only、REST recovery、fee missing、重复事件和 14 天 force-close。

不纳入第一版：

- OKX mock 实现。
- 通用交易撮合引擎。
- UI 场景控制台。
- 真实行情撮合。
- 跨交易所原子多腿补偿。

## 目录边界

建议把 mock 放在 adapter 自己目录下：

```text
core-service/internal/exchange/binance/mockserver/
  server.go
  rest_spot.go
  rest_futures.go
  ws.go
  scenario.go
  fixtures.go

core-service/cmd/mock-binance/
  main.go
```

未来 OKX 应该有自己的实现：

```text
core-service/internal/exchange/okx/mockserver/
core-service/cmd/mock-okx/
```

公共层最多提供薄 harness，例如：

```text
core-service/internal/exchange/adapter/mocktest/
  harness.go
```

公共 harness 只处理启动、停止、endpoint 暴露和测试配置注入，不处理 Binance/OKX 行为。

## 公共 Harness 约定

公共接口只表达“如何连接这个 adapter mock”，不表达交易所语义：

```go
type AdapterMockServer interface {
    Start(ctx context.Context) error
    Close() error
    RESTBaseURL(market domain.Market) string
    WSBaseURL(market domain.Market) string
    Reset(ctx context.Context) error
}
```

如果需要测试内控制场景，公共层只调用 adapter 暴露的 typed helper，不定义通用 JSON schema。原因是 Binance 的 `PARTIALLY_FILLED`、OKX 的订单状态和其他交易所的成交模型不一定同构。

## Binance Mock REST

Binance Spot 第一版覆盖：

```text
POST /api/v3/order
GET  /api/v3/order
GET  /api/v3/myTrades
POST /api/v3/userDataStream
PUT  /api/v3/userDataStream
DELETE /api/v3/userDataStream
```

Binance USD-M Futures 第一版覆盖：

```text
POST /fapi/v1/order
GET  /fapi/v1/order
GET  /fapi/v1/userTrades
DELETE /fapi/v1/order
POST /fapi/v1/listenKey
PUT  /fapi/v1/listenKey
DELETE /fapi/v1/listenKey
```

REST mock 必须接受签名参数，但第一版可以只校验 `X-MBX-APIKEY` 存在和 `signature` 存在，不做真实 HMAC 校验。这样能测 adapter 是否按 Binance 方式组包，又不会把测试复杂度放到签名算法上。

## Binance Mock WebSocket

WebSocket 路径：

```text
/ws/<listenKey>
```

Spot 推送 Binance 原始 `executionReport` payload。

Futures 推送 Binance 原始 `ORDER_TRADE_UPDATE` payload。

Mock 必须支持：

- 一个 listenKey 对应多个 WS client。
- 事件按 scenario 主动推送。
- 事件重复推送。
- 事件延迟推送。
- 连接断开后 REST recovery 仍能查到 order/trades。

第一版不要求完全模拟 Binance 24 小时断线和 60 分钟 listenKey 过期，但要提供测试 hook，让后续能注入 listenKey 过期/WS close。

## Binance 场景 Helper

Binance adapter mock 提供 typed helper，而不是公共万能场景：

```go
type BinanceScenario struct {
    Market       domain.Market
    Symbol       string
    Side         string
    PositionSide domain.PositionSide
    OrderType    string
    TimeInForce  string
    PostOnly     bool
    ReduceOnly   bool
    OrigQty      float64
    Price        float64
    Events       []BinanceOrderEventStep
}
```

典型步骤：

```text
ACCEPT_NEW
WS_PARTIAL_FILL
WS_FINAL_FILL
REST_TRADES_DELAYED
REST_TRADES_INCOMPLETE
REST_TRADES_COMPLETE
WS_DUPLICATE_EVENT
ORDER_CANCELED
ORDER_EXPIRED
FORCE_CLOSE_TARGET
```

这些 helper 只在 Binance mock 内部定义。未来 OKX mock 应定义 OKX 自己的 scenario helper。

## Adapter 接入要求

Binance adapter 当前 REST base URL 有 live/testnet 固定值。为了让 mock 可连接，需要将 endpoint 变成 adapter 配置：

```text
BINANCE_SPOT_REST_BASE_URL
BINANCE_FUTURES_REST_BASE_URL
BINANCE_SPOT_WS_BASE_URL
BINANCE_FUTURES_WS_BASE_URL
```

默认值仍然指向 Binance live/testnet。只有测试、CLI smoke 或 mock 环境覆盖这些值。

不能把 mock endpoint 写进公共 order service。order service 仍然只按 route 找 adapter capability。

`core-service` 只依赖 adapter registry 的 `UserDataStream` capability，不直接知道 Binance mock 或真实 Binance endpoint。mock 支持通过同一组 adapter endpoint 环境变量切换：

- 指向真实 Binance：使用默认 endpoint。
- 指向 Binance mock：设置 `BINANCE_*_REST_BASE_URL` 和 `BINANCE_*_WS_BASE_URL` 到 `cmd/mock-binance` 暴露的地址。
- 未来 OKX：必须在 OKX adapter 内提供自己的 `UserDataStream` 和 mockserver。

## 测试分层

### Adapter 单元/集成测试

- Binance mock REST 下单返回 `NEW` / `PARTIALLY_FILLED` / `FILLED`。
- Binance mock WS 推送 `executionReport` / `ORDER_TRADE_UPDATE`。
- `core-service` 后台 `UserDataStreamManager` 能从 open orders 启动 WS stream，并把 partial fill 回调写入 lifecycle。
- FOK + partial fill payload 被 parser 拒绝。
- Spot post-only 映射 `LIMIT_MAKER`。
- Futures post-only 映射 `GTX`。
- Spot GTD fail-closed。
- Futures GTD 带 `goodTillDate`。
- Spot `reduce_only=true` 不透传 Binance 参数。
- Futures `reduce_only=true` 透传 `reduceOnly=true`。

### Order lifecycle 测试

- WS partial fill 生成 lifecycle fill event。
- 重复 WS fill 不重复落库或结算。
- REST trades 缺失时进入 `FILL_PENDING`。
- REST recovery 补齐 trades 后进入终态。
- 14 天 deadline 后调用 cancel、最后 query order/trades，并写 `RECOVERY_EXPIRED` terminal event。

### Strategy runtime 测试

- 只基于 confirmed fill delta 更新钱包。
- partial fill 后同 route 保持 blocked。
- terminal event 后解除 block。
- `FEE_MISSING` 不当作零手续费成交。
- `force_close` terminal event 不凭空结算未知剩余数量。

### CLI smoke

本地可启动 mock：

```bash
cd core-service
make mock-binance
```

测试/服务通过环境变量指向 mock：

```bash
BINANCE_SPOT_REST_BASE_URL=http://127.0.0.1:19000
BINANCE_FUTURES_REST_BASE_URL=http://127.0.0.1:19000
BINANCE_SPOT_WS_BASE_URL=ws://127.0.0.1:19000
BINANCE_FUTURES_WS_BASE_URL=ws://127.0.0.1:19000
```

也可以直接跑 mock-backed smoke：

```bash
cd core-service
./scripts/mock_binance_partial_fill_smoke.sh
```

mock 服务提供最小控制路径：

```text
POST   /mock/scenarios   # enqueue BinanceScenario JSON
POST   /mock/scene       # set simple scene mode for subsequent orders
GET    /mock/scene       # inspect current scene mode
GET    /mock/orders/{id} # inspect mock order
POST   /mock/reset       # reset state
DELETE /mock/reset       # reset state
```

`/mock/scene` 是手动页面测试的推荐入口。订单参数仍然全部来自真实 adapter 请求；scene 只控制 mock 交易所行为：

| Scene | 行为 |
| --- | --- |
| `1` | 正常成交；MARKET / LIMIT / IOC / FOK 直接 `FILLED`，post-only 正常挂单 `NEW` |
| `2` | 部分成交后永不完成；GTC/GTD 保持 `PARTIALLY_FILLED`，IOC 部分成交后 `EXPIRED`，FOK 不成交 `EXPIRED` |
| `3` | GTC/GTD 先 `PARTIALLY_FILLED`，默认 120 秒后通过 WS 推剩余成交并变 `FILLED` |
| `4` | 挂单不成交；GTC/GTD/GTX 返回 `NEW`，MARKET/IOC/FOK 返回 `EXPIRED` |
| `5` | post-only 会吃单；GTX/LIMIT_MAKER 返回 `EXPIRED`，非 post-only 正常成交 |
| `6` | 无流动性；可挂单类型返回 `NEW`，立即成交类型返回 `EXPIRED` |
| `7` | 交易所拒单；返回 Binance 风格 `-2010` |
| `8` | 限流；返回 Binance 风格 `-1003` / HTTP 429 |
| `9` | 超时；mock 延迟响应，用于测试 adapter timeout |

示例：

```bash
curl -X POST 'http://127.0.0.1:19000/mock/scene?scene=3'
curl http://127.0.0.1:19000/mock/scene
```

`scene=3` 和 `scene=9` 的延迟可以在启动 mock 时调整：

```bash
MOCK_BINANCE_SCENE3_DELAY_SECONDS=10 MOCK_BINANCE_SCENE9_DELAY_SECONDS=12 make mock-binance
```

## 验收标准

- 同一个 Binance mock 核心能被 Go 测试和 CLI 复用。
- Binance mock 可被真实 adapter 通过 REST/WS endpoint 连接。
- 至少覆盖一个完整 partial fill 流程：

```text
PlaceOrder -> Binance mock REST accepted
  -> Binance mock WS PARTIALLY_FILLED
  -> lifecycle event
  -> strategy-runtime wallet delta update
  -> route stays blocked
  -> Binance mock REST trades complete
  -> scanner resolves terminal state
  -> route unblocked
```

- 文档和测试明确禁止公共万能交易所 mock。
- 新 adapter checklist 增加 mock adapter 要求。

## 后续扩展

- OKX adapter 接入时，实现 `internal/exchange/okx/mockserver`，不要复用 Binance 行为模型。
- 如果多个 adapter mock 出现重复的 HTTP/WS server 启动代码，再抽到 `adapter/mocktest`；不要提前抽象交易行为。
- 可以后续增加 DSL 或 fixture 文件，但 DSL 必须是 adapter scoped，例如 `binance_scenarios.yaml`，而不是 `exchange_scenarios.yaml`。
