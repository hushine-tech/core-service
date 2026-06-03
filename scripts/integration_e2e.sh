#!/usr/bin/env bash
# End-to-end: TimescaleDB + core-service (HTTP + gRPC) for backtest account flow.
#
# Prerequisites: Go 1.22+, TimescaleDB reachable, Python 3 (for JSON) or jq.
#
# Usage:
#   ./scripts/integration_e2e.sh
#   TIMESCALEDB_DSN="host=... dbname=account ..." ./scripts/integration_e2e.sh
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export TIMESCALEDB_DSN="${TIMESCALEDB_DSN:-host=192.168.88.10 port=5432 user=postgres password=postgres dbname=account sslmode=disable}"
export HTTP_ADDR="${HTTP_ADDR:-:18080}"
export GRPC_ADDR="${GRPC_ADDR:-:18081}"
export BINLOG_PATH="${BINLOG_PATH:-./logs}"

if [[ "$HTTP_ADDR" == :* ]]; then
  HTTP_BASE="http://127.0.0.1${HTTP_ADDR}"
else
  HTTP_BASE="http://${HTTP_ADDR}"
fi
if [[ "$GRPC_ADDR" == :* ]]; then
  GRPC_HOSTPORT="127.0.0.1${GRPC_ADDR}"
else
  GRPC_HOSTPORT="${GRPC_ADDR}"
fi

mkdir -p "$BINLOG_PATH"

STAMP="$(date +%s)-$$"
BT_NAME="e2e-bt-${STAMP}"
USERNAME="e2e-user-${STAMP}"
PASSWORD="integration-pass-${STAMP}"

BIN="${ROOT}/.build/core-service-e2e"
go build -o "$BIN" ./cmd/core-service

"$BIN" &
PID=$!
cleanup() {
  kill "$PID" 2>/dev/null || true
  wait "$PID" 2>/dev/null || true
}
trap cleanup EXIT

# wait for HTTP
HTTP_OK=0
for _ in $(seq 1 60); do
  if curl -sf "${HTTP_BASE}/accounts?user_id=1" >/dev/null 2>&1; then
    HTTP_OK=1
    sleep 0.3
    break
  fi
  sleep 0.25
done
if [[ "$HTTP_OK" != 1 ]]; then
  echo "ERROR: HTTP server did not become ready at ${HTTP_BASE}" >&2
  exit 1
fi

extract_id() {
  if command -v jq >/dev/null 2>&1; then
    jq -r .account_id
  else
    python3 -c "import sys,json; print(json.load(sys.stdin)['account_id'])"
  fi
}

USER_CREATE_OUTPUT="$(go run ./cmd/integration-grpc-client -addr "$GRPC_HOSTPORT" -scenario create-user -username "$USERNAME" -password "$PASSWORD")"
USER_ID="$(echo "$USER_CREATE_OUTPUT" | awk -F'[ =]' '/user_id=/{print $2}')"
if [[ -z "$USER_ID" ]]; then
  echo "ERROR: failed to create user: ${USER_CREATE_OUTPUT}" >&2
  exit 1
fi
echo "=== gRPC: created user ${USERNAME} (user_id=${USER_ID}) ==="

echo "=== curl: create backtest account context ==="
BT_JSON="$(curl -sS -X POST "${HTTP_BASE}/accounts" \
  -H 'Content-Type: application/json' \
  -d "{\"user_id\":${USER_ID},\"name\":\"${BT_NAME}\",\"environment\":0}")"
echo "$BT_JSON"
BT_ID="$(echo "$BT_JSON" | extract_id)"

echo "=== gRPC client: backtest scenario (account ${BT_ID}) ==="
go run ./cmd/integration-grpc-client -addr "$GRPC_HOSTPORT" -account "$BT_ID" -user "$USER_ID" -scenario backtest

echo "=== OK: e2e finished (data persisted to TimescaleDB) ==="
