# Binance Mock Scene Matrix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Binance adapter mock scenes that control exchange behavior while preserving all order parameters from the real adapter request.

**Architecture:** Keep scene state inside `internal/exchange/binance/mockserver`. Orders continue to be created from real Binance REST parameters; the active scene only decides fill/reject/expire/timeout behavior. `core-service` and strategy-runtime remain unaware of scenes.

**Tech Stack:** Go, `net/http`, existing Binance mockserver, existing adapter/recovery tests.

---

## File Structure

- Modify `core-service/internal/exchange/binance/mockserver/scenario.go`: add scene constants, config, and preset behavior helpers.
- Modify `core-service/internal/exchange/binance/mockserver/server.go`: store active scene, expose `/mock/scene`, apply scenes after order creation, reset scene to normal fill.
- Modify `core-service/internal/exchange/binance/mockserver/rest_futures.go`: return timeout/rate-limit/reject responses before normal order allocation where scene requires it.
- Modify `core-service/internal/exchange/binance/mockserver/rest_spot.go`: mirror futures scene behavior for spot where supported.
- Modify `core-service/cmd/mock-binance/main.go`: read `MOCK_BINANCE_SCENE3_DELAY_SECONDS`.
- Modify tests in `core-service/internal/exchange/binance/mockserver/`: cover scene API and order matrix.
- Modify docs/progress after implementation: document manual scene commands and semantics.

## Tasks

### Task 1: Scene API and Defaults

- [ ] Write failing tests for `POST /mock/scene?scene=2`, `GET /mock/scene`, and reset returning scene 1.
- [ ] Implement `SceneMode`, `Config`, `NewWithConfig`, `handleScene`, and default scene 1.
- [ ] Run `go test ./internal/exchange/binance/mockserver -run 'TestMockServerScene' -count=1`.

### Task 2: Order Behavior Matrix

- [ ] Write failing tests for:
  - scene 1 fills MARKET/LIMIT/FOK/IOC/GTC orders.
  - scene 2 partially fills GTC/GTD but expires FOK and partially fills then expires IOC.
  - scene 3 partially fills GTC/GTD and emits delayed WS final fill.
  - scene 4 leaves GTC/GTD/GTX as NEW and expires MARKET/IOC/FOK.
  - scene 5 expires/rejects post-only orders that would take.
  - scene 6 simulates no liquidity.
  - scene 7 returns Binance rejection.
  - scene 8 returns Binance rate limit.
  - scene 9 causes client-visible timeout/delay.
- [ ] Implement scene application in mockserver using order params from the request.
- [ ] Run targeted mockserver tests.

### Task 3: CLI and Docs

- [ ] Read `MOCK_BINANCE_SCENE3_DELAY_SECONDS` and `MOCK_BINANCE_SCENE9_DELAY_SECONDS` in `cmd/mock-binance`.
- [ ] Update mock smoke and progress docs with scene commands.
- [ ] Run `./scripts/mock_binance_partial_fill_smoke.sh` and a curl-driven local simulation for scenes 1-9.
