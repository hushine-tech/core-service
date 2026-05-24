# Account Service Integration

## Breaking change (gRPC)

The public gRPC service **`AccountService` exposes only two RPCs**:

- `GetOnlineAccountInfo`
- `UpdateAccountWalletState`

Removed (**BREAKING**): `GetAccountSnapshot`, `GetAccountStatus`, `OnOrderFilled`, `RefreshAccountFromExchange`, and related proto messages (`AccountSnapshot`, `SnapshotMetadata`, `Environment`, legacy request/response types).

Downstream services (including `strategy-service`) must migrate to the two-RPC model and regenerate stubs from `proto/account_service.proto`.

## Strategy-service contract: only `account_id`

`strategy-service` should use the **same integration code** for backtest, live, and testnet:

- Pass only **`account_id`** (plus wallet payload on update).
- **Do not** send or branch on trading mode in gRPC requests. Whether an account is backtest or exchange-backed is determined **only** by how the account was **registered** in `core-service` (e.g. HTTP `POST /accounts` with `mode` when the account is created).

`core-service` loads `accounts.mode` from storage and routes internally:

| Stored `mode` | `GetOnlineAccountInfo` | `UpdateAccountWalletState` |
|---------------|------------------------|----------------------------|
| `0` (backtest) | Latest snapshot from DB | Request wallet is **authoritative**; persist and return it |
| `1` (live) / `2` (testnet) | Fetch from exchange, then persist | Fetch from exchange; **ignore** request wallet for persistence; return exchange snapshot |

Responses include `wallet.mode` as **informational** (ops, logging). **`strategy-service` may ignore `wallet.mode`** and still behave correctly.

## Proto and stub generation

- Proto path: `proto/account_service.proto`
- Generate stubs: `make proto`
- Generated Go package: `gen/accountv1`

## RPC semantics

### `GetOnlineAccountInfo`

- **Request:** `account_id`
- **Response:** `wallet` (`AccountWalletState`): canonical callers should treat the payload as **summary-only top level + detailed sub-ledgers**:
  - top-level summary fields: `futures`, `spot`, `total_value`, `mode`, `updated_at`
  - display metrics:
  - `spot_estimated_value` — spot leg that matches the exchange adapter for live/testnet (USDT `free`+`locked`, plus `qty*price` only where `price` is set on each asset; `locked` base is already part of `qty`).
  - `futures_position_equity` — futures leg that sums with `spot_estimated_value` to match `total_value` for Binance snapshots (`futures.margin_balance` on canonical paths; for Binance snapshots this is the same quantity as `futures.total_margin_balance`).
  - `metrics_authoritative` — `true` when `spot_estimated_value + futures_position_equity` matches `total_value` within server tolerance (float noise); portals may fall back to local estimates when `false`.
  - `futures.positions[].display_equity` (optional) — isolated-mode row estimate (IM + shell + unrealized); omitted for cross margin.
- **Canonical rule:** top-level `wallet_balance` / `available_balance` / `margin_balance` / `unrealized_pnl` / `total_equity` no longer belong to the canonical response shape; canonical consumers MUST read these balance fields from `wallet.futures.*`.
- **Behavior:** Uses the account’s **registered** mode. Persists the returned state after a successful read.

### `UpdateAccountWalletState`

- **Request:** `account_id`, `futures`, `spot`, aggregates (`total_value`, `wallet_balance`, `available_balance`) — **no mode field** (field `2` is reserved in proto for backward wire compatibility).
- **Response:** `wallet` — canonical state after the call (same summary-only top-level shape as `GetOnlineAccountInfo`).
- **Backtest (`mode` 0):** Request body is authoritative; stored and echoed.
  - For `cross`, bootstrap snapshot SHOULD seed `futures.wallet_balance = futures.initial_balance + deposit_sum - withdrawal_sum`.
  - For `isolated`, bootstrap snapshot SHOULD seed `futures.wallet_balance = Σ position.initial_balance + deposit_sum - withdrawal_sum`.
- **Live / testnet (`mode` 1 / 2):** Exchange is authoritative; request wallet is not used for persistence; response is the fetched snapshot.

## Field mapping for `strategy-service` (`wallet_factory` / `Account`)

- `wallet.futures.margin_mode` → `FutureWallet.margin_mode`
- `wallet.futures.position_mode` → `FutureWallet.position_mode`
- `wallet.futures.initial_balance` / `deposit_sum` / `withdrawal_sum` → cross pool fields
- `wallet.futures.margin_balance` / `wallet_balance` / `available_balance` / `unrealized_pnl` → futures account canonical balances
- `wallet.futures.risk_metadata[]` → metadata-backed parity inputs for `maint_margin` / `liquidation_price`
- `wallet.futures.multi_assets_mode` / `portfolio_margin` → parity-runtime support boundary flags (`mode=2` sees `true` => fail-closed)
- `wallet.futures.positions[].symbol`, `direction`, `initial_balance`, `leverage`, `fee_rate`, `position_qty`, `entry_price`, `mark_price`, `margin_mode` → `Position(...)`
- `wallet.spot.free` / `locked` → `SpotWallet`
- `wallet.spot.assets[].symbol`, `qty`, `locked`, `avg_entry_price`, optional `price` → `SpotAsset`

## Error semantics

- `INVALID_ARGUMENT`: missing `account_id`, or unsupported **registered** account mode
- `NOT_FOUND`: unknown `account_id`
- `UNAVAILABLE`: repository failure, or exchange fetch failure (e.g. live adapter not configured)

## Integration testing (TimescaleDB + curl + mock Binance)

- **Database bootstrap:** `make ensure-db` (or `go run ./cmd/ensure-account-db`) creates database **`account`** if needed and applies `internal/storage/migrations/*.sql`. Env: `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD` (defaults match local dev).
- **Database:** Use `TIMESCALEDB_DSN` pointing at that database for the service and tests.
- **Mock exchange (no real Binance):** start `core-service` with `MOCK_BINANCE=1`. Live/testnet accounts then use `internal/exchange.MockOnlineInfoFetcher` instead of REST calls.
- **Go integration test:** `go test -tags=integration -v ./tests/integration/...` — creates accounts via HTTP handler (same JSON as `curl`), then calls both gRPC RPCs with an in-process gRPC server and mock fetcher. Skips if DSN is unreachable.
- **Shell e2e:** `bash scripts/integration_e2e.sh` or `make e2e` — builds the binary, starts the server with `MOCK_BINANCE=1`, uses **curl** to `POST /accounts` (backtest with `initial_balance` + live `mode=1`), then runs `go run ./cmd/integration-grpc-client` for `backtest` and `live` scenarios.
