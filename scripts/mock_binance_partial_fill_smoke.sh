#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

export BINANCE_SPOT_REST_BASE_URL="${BINANCE_SPOT_REST_BASE_URL:-http://127.0.0.1:19000}"
export BINANCE_FUTURES_REST_BASE_URL="${BINANCE_FUTURES_REST_BASE_URL:-http://127.0.0.1:19000}"
export BINANCE_SPOT_WS_BASE_URL="${BINANCE_SPOT_WS_BASE_URL:-ws://127.0.0.1:19000}"
export BINANCE_FUTURES_WS_BASE_URL="${BINANCE_FUTURES_WS_BASE_URL:-ws://127.0.0.1:19000}"

go test ./internal/exchange/adapter \
  ./internal/exchange/binance \
  ./internal/exchange/binance/mockserver \
  ./internal/order/lifecycle \
  ./internal/order/executor \
  ./internal/order/service \
  ./cmd/core-service \
  -run 'Mock|UserData|Partial|PARTIALLY|FillPending|Recovery|ForceClose|PostOnly|ReduceOnly|GTD|IOC|FOK' \
  -count=1
