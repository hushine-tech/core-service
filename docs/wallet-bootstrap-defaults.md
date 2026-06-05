# Portal wallet bootstrap defaults

When the quant portal creates a **backtest venue** with selected symbols but minimal per-position input, servers apply these defaults so payloads stay compatible with `account_service.proto` and `strategy-service` wallet expectations.

**quant-handler:** wallet bootstrap belongs to `CreateVenue` only. It runs when spot or futures is **meaningful** (non-zero free/locked, non-empty assets, non-empty futures positions, or non-zero cross `initial_balance`). Account creation does not seed or overwrite wallet state.

## Futures (`FuturesPosition`)

| Field | Default | Notes |
|-------|---------|--------|
| `initial_balance` | `1000` | Used as isolated “flat” equity seed when `qty` is 0 (matches Python flat position `get_position_equity` ≈ `initial_balance`). |
| `leverage` | `10` | Must be `> 0` in strategy code. |
| `fee_rate` | `0.0004` | Placeholder taker-style rate; backtest can refine later. |
| `direction` | `0` | One-way book; use `+1` / `-1` per side when `position_mode` is `hedge`. |
| `qty`, `entry_price`, `mark_price`, `unrealized_pnl` | `0` | Flat book until strategy fills. |
| `position_side` | `""` | Populate `LONG` / `SHORT` when hedge mode exposes separate legs. |

## Futures wallet (`FuturesWallet`)

- `margin_mode`: `isolated` or `cross` (lowercase), as in Python `_VALID_MARGIN_MODES`.
- `position_mode`: `one_way` or `hedge` (underscore), as in Python `_VALID_POSITION_MODES`.
- `initial_balance` (wallet-level): for **cross** margin, pool balance; for **isolated**, often `0` with per-position `initial_balance` funding.

## Spot (`SpotAsset`)

| Field | Default |
|-------|---------|
| `qty` | user-supplied or `0` |
| `locked` | `0` |
| `avg_entry_price` | `0` or user mark |
| `price` | optional; required for `qty > 0` when computing estimated value (see `walletagg` / strategy `SpotWallet.get_estimated_value`). |

## Total value

Portal and handler SHOULD compute `total_value` as **futures position equity** + **spot estimated value** using the same rules as `strategy_service/account_client.py::_compute_total_value` (see `quant-handler/internal/walletagg`).
