# Binance 订单执行、风控与恢复设计

日期：2026-06-07

## 背景

当前订单链路已经有 `intent -> attempt -> order -> fill` 四层审计结构，并且 Binance USD-M Futures 路径可以通过 `core-service/order.v1` 调用真实交易所。接下来需要把订单能力从 happy path 推进到可用于真实策略运行的阶段：

- 同时支持 Binance Spot 和 USD-M Futures。
- 支持多种订单方式，包括市价、限价、IOC、FOK、post-only、GTD。
- 在下单前引入专门的风控审查模块。
- 对部分成交、成交明细缺失、网络不确定结果做恢复。
- hosted runtime 和 self-hosted runtime 的平台请求统一通过 control-panel 处理。
- 避免无限轮询 Binance；订单恢复超过 14 天后必须强制进入终态。

设计原则：公共订单链路只表达交易意图、审计语义和平台状态；交易所差异必须落在 adapter 能力中，不能写死在公共 `order/service` 或 strategy runtime 里。

## 范围

本设计覆盖第一版实现范围：

- Binance Spot 和 Binance USD-M Futures。
- 公共订单语义：
  - `MARKET`
  - `LIMIT`
  - `time_in_force = GTC / IOC / FOK / GTD`
  - `post_only`
  - `good_till_date`
  - `reduce_only`
- 下单前风控审查。
- Binance user data stream 订单事件接入。
- REST 查询作为断线补偿。
- 14 天恢复截止机制。
- hosted/self-hosted runtime 统一走 control-panel 平台代理。

不纳入第一版：

- OKX 真实下单。
- 跨交易所原子多腿执行。
- 自动追价。
- 自动 market 反向平仓作为恢复兜底。
- Spot margin、portfolio margin、multi-assets futures。
- Binance algo order、OCO、OTO、OTOCO、trailing stop。

## 公共订单语义

策略和公共 order proto 不直接暴露 Binance 特有参数，例如 Futures 的 `GTX` 或 Spot 的 `LIMIT_MAKER`。公共语义如下：

```text
order_type:
  MARKET
  LIMIT

time_in_force:
  GTC
  IOC
  FOK
  GTD

post_only:
  true / false

reduce_only:
  true / false

good_till_date:
  timestamp，可选，仅 GTD 使用
```

约束：

- `MARKET` 不允许带 `price`、`post_only`、`time_in_force`、`good_till_date`。
- `LIMIT` 必须带正数 `price`。
- `post_only=true` 只能用于 `LIMIT`。
- `GTD` 必须带 `good_till_date`。
- `IOC` 和 `FOK` 不允许与 `post_only=true` 组合。
- Spot 支持 `reduce_only`，但它是平台侧语义，不是 Binance Spot 原生参数。
- Spot `reduce_only=true` 只允许减少 base asset 暴露：第一版仅允许 `SELL`，且 `qty <= unlocked base qty`。
- Spot `reduce_only=true` 不允许 `BUY`，因为普通 Spot 没有可被买入减少的空头仓位。
- Futures `reduce_only=true` 映射到交易所原生 reduce-only 语义，并且必须通过本地仓位方向审查。
- 不支持的组合必须在 adapter capability 审查时拒绝，而不是静默降级。

## Adapter 映射

公共代码只调用 adapter 能力，不写 Binance 特例。

| 公共意图 | Binance Spot adapter | Binance USD-M Futures adapter |
| --- | --- | --- |
| `MARKET` | `type=MARKET` | `type=MARKET` |
| `LIMIT + GTC` | `type=LIMIT,timeInForce=GTC` | `type=LIMIT,timeInForce=GTC` |
| `LIMIT + IOC` | `type=LIMIT,timeInForce=IOC` | `type=LIMIT,timeInForce=IOC` |
| `LIMIT + FOK` | `type=LIMIT,timeInForce=FOK` | `type=LIMIT,timeInForce=FOK` |
| `LIMIT + post_only` | `type=LIMIT_MAKER` | `type=LIMIT,timeInForce=GTX` |
| `LIMIT + GTD` | 拒绝，不支持 | `type=LIMIT,timeInForce=GTD,goodTillDate=...` |
| `reduce_only=true` | 不透传 Binance；RiskGate 约束为只允许减少 base asset | 透传 Binance reduce-only，并做本地仓位方向审查 |

adapter 层需要提供：

- `OrderCapabilityProvider`：声明当前 route 支持哪些订单语义和组合。
- `OrderContractNormalizer`：把公共订单语义映射为交易所请求。
- `OrderExecutor`：执行真实下单。
- `OrderStateReader`：查询订单状态。
- `TradeReader`：查询成交明细。
- `OrderCanceller`：取消未终结订单。
- `UserDataStream`：接入交易所订单事件长连接。

## 风控模块

新增 `core-service/internal/order/risk`，作为权威下单前审查层。strategy runtime 可以保留快速 advisory check，但不能作为最终裁决。

调用顺序：

```text
PlaceOrder
  -> validate route/session/account
  -> upsert intent
  -> create attempt(PENDING)
  -> RiskGate.Review
  -> ALLOW: 调 adapter executor
  -> REJECT: attempt = RISK_REJECTED，不调用交易所
```

第一版风控项：

- 账户、venue、session、runtime 归属校验。
- strategy `ORDER_TARGETS` 与请求 route 的一致性。
- order type、time in force、post-only、GTD 组合合法性。
- adapter capability 支持性。
- symbol rules：
  - `minQty`
  - `stepSize`
  - `tickSize`
  - `minNotional`
- Spot 余额：
  - BUY 检查 quote free balance。
  - SELL 检查 base unlocked quantity，即 `qty - locked`。
  - `reduce_only=true` 时只允许 SELL，且 SELL 数量不能超过 base unlocked quantity。
  - `reduce_only=true` 时禁止 BUY。
- Futures 风控：
  - `position_side` 与账户持仓模式一致。
  - 开仓检查 `available_balance`、leverage、initial margin。
  - `reduce_only` 只能减少或关闭现有仓位。
  - 缺少必要 risk metadata 时 fail-closed。
- Pending execution block：
  - 同一 `(account_id, venue_id, exchange, market, symbol, position_side)` 存在未终结订单、`PARTIALLY_FILLED`、`FILL_PENDING`、`FEE_MISSING`、`RECOVERING` 时阻塞新开仓或反向单。
- 同 tick 多订单组合审查：
  - 先聚合资金占用和 route 冲突。
  - 不提供跨交易所原子保证。
  - 不在第一版做自动补偿套利腿。

RiskGate 输出：

```text
RiskDecision:
  status: ALLOW / REJECT
  reason_code
  violations[]
  warnings[]
  review_snapshot
```

拒绝结果必须落入审计，便于前端和通知系统展示。

## User Data Stream 主路径

Binance 提供账户级 user data stream，适合作为订单更新主路径。

Spot：

- 事件：`executionReport`
- 覆盖新订单、成交、部分成交、取消、过期、拒单、手续费、trade id。

USD-M Futures：

- 事件：`ORDER_TRADE_UPDATE`
- 覆盖 `NEW`、`PARTIALLY_FILLED`、`FILLED`、`CANCELED`、`EXPIRED`。
- 带 last fill qty、last fill price、fee、trade id、position side、reduce only、time in force。
- listenKey 有效期 60 分钟，需要 keepalive。
- WebSocket 单连接最长 24 小时，需要主动重连。

平台设计：

```text
Binance user data stream
  -> adapter event normalizer
  -> order lifecycle ingest
  -> order/fill 去重落库
  -> lifecycle event
  -> control-panel RuntimeChannel event
  -> strategy-runtime wallet update
```

去重规则：

- fill 以 `exchange_trade_id` 优先去重。
- 无 trade id 的状态事件以 `(exchange_order_id, status, executed_qty, event_time)` 去重。
- 事件必须可以重复消费。

## REST 兜底恢复

WebSocket 不是持久消息队列，断线、listenKey 过期、进程重启和网络抖动都可能导致事件缺失。因此 REST 查询仍然存在，但只能作为补偿路径。

恢复对象：

- `NEW`
- `PARTIALLY_FILLED`
- `FILL_PENDING`
- `FEE_MISSING`
- `RECOVERING`
- 不确定执行结果 `UNKNOWN`

恢复策略：

- 对每个未终结订单维护：
  - `recovery_started_at`
  - `next_check_at`
  - `recovery_deadline_at`
  - `recovery_status`
  - `last_recovery_error`
- scanner 只查询 `next_check_at <= now` 的订单。
- 查询成功后更新 order state 和新增 fills。
- 查询间隔使用指数退避，并在 WebSocket 正常时降低 REST 查询频率。
- 每个新增 fill 必须生成 lifecycle event。

## 14 天强制终止

订单在 Binance 已接受后，如果超过 14 天仍处于恢复中，平台必须终止恢复，不能无限扫表查询 Binance。

终止流程：

```text
now >= recovery_deadline_at
  -> 如果订单仍 open，调用 adapter cancel
  -> 最后一次 query order + query trades
  -> 落库所有能确认的 fills
  -> 对剩余未成交数量标记 FORCE_CLOSED / RECOVERY_EXPIRED
  -> 生成 lifecycle terminal event
  -> 停止继续轮询
  -> 通知 strategy-runtime 和前端
```

这里的“强制关闭”含义是：取消未成交剩余部分，并终止恢复流程。第一版不自动用 market 反向平仓，因为那是新的主动交易行为，必须由单独的风控策略控制。

## RuntimeChannel 统一入口

正式 runtime 路径统一为：

```text
strategy-runtime
  -> RuntimeChannel
  -> control-panel platform proxy
  -> core-service order.v1
  -> exchange adapter
```

要求：

- hosted runtime 和 self-hosted runtime 使用同一套 platform proxy 方法。
- self-hosted runtime 不直接访问 core-service、order API、Kafka、数据库或交易所凭证。
- hosted runtime 也不依赖一条只有 hosted 能用的订单接口。
- 本地开发可以保留 direct gRPC，但不能作为产品路径依赖。
- `ProxyOrderClient` 必须透传全部公共订单字段，包括 `order_type`、`time_in_force`、`post_only`、`good_till_date`、`reduce_only`。
- control-panel `platform_proxy` 必须统一做 runtime/account/session owner 审计，然后把请求转发给 core-service。

## Strategy Runtime 钱包更新

runtime 对钱包的更新必须只基于已确认 fill delta：

- PlaceOrder 同步响应中带的 confirmed fills 可以立即应用。
- 后续 WebSocket 或 REST recovery 产生的 lifecycle fill event 作为增量应用。
- `FEE_MISSING` 不结算为零手续费成交。
- 重复 fill event 不重复应用。
- 同 route 在恢复完成前保持阻塞。
- runtime 必须把 order update 回调给用户策略，让策略能感知部分成交、取消、过期和强制终止。

## 前端和通知

第一版最小展示要求：

- order detail 显示公共订单语义：`order_type`、`time_in_force`、`post_only`、`good_till_date`、`reduce_only`。
- attempt 显示 risk decision 和 violations。
- order 显示 recovery 状态、next check、deadline。
- lifecycle tree 显示 WebSocket / REST / force-close 来源。
- 强制终止、risk reject、partial fill、fee missing 必须产生用户可见通知。

## 测试策略

core-service：

- adapter capability 单元测试：
  - Spot post-only -> `LIMIT_MAKER`
  - Futures post-only -> `GTX`
  - Spot GTD 拒绝
  - Futures GTD 要求 `good_till_date`
- RiskGate 单元测试：
  - symbol rules
  - 余额不足
  - futures available balance
  - pending execution block
  - unsupported capability
  - same tick 多订单聚合
- order service 测试：
  - risk reject 不调用 executor
  - accepted order 写入恢复字段
  - 14 天 deadline 后 cancel + final reconcile + terminal event
- user data stream ingest 测试：
  - Spot `executionReport`
  - Futures `ORDER_TRADE_UPDATE`
  - 重复事件去重
  - 新 fill 生成 lifecycle event

strategy-service：

- direct OrderClient 和 ProxyOrderClient 字段一致性测试。
- lifecycle event 增量更新钱包测试。
- `FEE_MISSING` 不结算测试。
- forced terminal event 解除 route block 测试。

control-panel-service：

- platform proxy 对 order 请求做 runtime/account/session owner 审计。
- hosted/self-hosted 使用相同 method 和 proto payload。
- 不允许 self-hosted 绕过 RuntimeChannel。

frontend / quant-handler：

- 新字段展示和序列化测试。
- risk reject、partial fill、force-close 状态展示测试。

## 推进顺序

1. 扩展公共 order proto / strategy order type，加入 `post_only`、`good_till_date`、`reduce_only`。
2. 修正 direct OrderClient 与 ProxyOrderClient 的字段一致性。
3. 扩展 adapter capability 和 Binance Spot/Futures 映射。
4. 引入 RiskGate，并接入 PlaceOrder。
5. 引入 Binance user data stream ingest。
6. 引入 REST recovery scanner 与 14 天 deadline。
7. 把 lifecycle event 推送/消费补齐到 control-panel 和 strategy runtime。
8. 补前端可观测性与通知。

## 开放问题

- `GTD` 是否在策略 API 中默认隐藏，只允许高级参数开启。建议：第一版保留字段，但 UI 暂不作为默认入口。
- User data stream 连接归属放在 core-service 还是 control-panel。建议：放在 core-service adapter 层，因为它需要交易所凭证、订单表和 fills 去重；control-panel 只负责把标准 lifecycle event 发给 runtime。
