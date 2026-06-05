# core-service

Core gRPC service for users, accounts, wallet snapshots, orders, sessions, reconciliation, and notification delivery.

**Registry RPCs** (for BFFs such as `quant-handler`): `CreateAccount`, `ListAccounts`, and `GetAccount` expose the same persistence rules as HTTP `/accounts` but **never return** API credentials in responses.

**Symbol catalog RPC:** `ListSymbols` returns cached **USDT spot** and **USDT-M perpetual** symbols from public Binance `exchangeInfo` (refreshed on TTL, default **6h**). Override with env `SYMBOL_CACHE_TTL` (Go duration, e.g. `24h`).

## Development

```bash
make proto
make tidy
make test
make run
```

## Integration (TimescaleDB + curl + gRPC)

- **Create DB + tables (no local `psql` required):** `make ensure-db` runs `go run ./cmd/ensure-account-db` (uses `PGHOST` default `192.168.88.10`, creates database `account`, applies `internal/storage/migrations/*.sql`).
- **DSN default:** `host=192.168.88.10 port=5432 user=postgres password=postgres dbname=account sslmode=disable` (override with `TIMESCALEDB_DSN`).
- **Mock Binance (no real API):** server honors `MOCK_BINANCE=1` for live/testnet fetch paths.
- **Go test (in-process HTTP + gRPC, same DB):** `make test-integration` (or `go test -tags=integration -v ./tests/integration/...`).
- **Shell e2e (curl creates accounts, then `integration-grpc-client`):** `make e2e` or `./scripts/integration_e2e.sh`.

```bash
make ensure-db
export TIMESCALEDB_DSN="host=192.168.88.10 port=5432 user=postgres password=postgres dbname=account sslmode=disable"
make test-integration
# or
make e2e
```
