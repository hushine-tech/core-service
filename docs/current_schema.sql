-- core-service consolidated schema snapshot
-- ------------------------------------------------------------
-- Mirrors the current end state after applying core-service
-- internal/storage/migrations/*.sql through:
--   0000_create_schema_migrations.sql
--   0012_drop_market_data_control_plane.sql
--
-- Source of truth for runtime remains internal/storage/migrations/*.sql.
-- This snapshot is a review/bootstrap reference generated from the runtime
-- schema on 192.168.88.10 and normalized back to idempotent DDL.

CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS schema_migrations (
    filename   TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS users (
    id            BIGSERIAL   PRIMARY KEY,
    username      TEXT        NOT NULL,
    password_hash TEXT        NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    plan_code     TEXT        NOT NULL DEFAULT 'pro'
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username
    ON users (username);

CREATE INDEX IF NOT EXISTS idx_users_plan_code
    ON users (plan_code);

CREATE TABLE IF NOT EXISTS accounts (
    account_id         BIGSERIAL        PRIMARY KEY,
    user_id            BIGINT           NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name               TEXT             NOT NULL,
    description        TEXT             NOT NULL DEFAULT '',
    mode               INTEGER          NOT NULL DEFAULT 0,
    api_key            TEXT             NOT NULL DEFAULT '',
    api_secret         TEXT             NOT NULL DEFAULT '',
    margin_mode        TEXT             NOT NULL DEFAULT 'cross',
    position_mode      TEXT             NOT NULL DEFAULT 'one_way',
    slippage_bps       DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    default_fee_rate   DOUBLE PRECISION NOT NULL DEFAULT 0.0004,
    created_at         TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    futures_json       JSONB,
    spot_json          JSONB,
    total_value        DOUBLE PRECISION NOT NULL DEFAULT 0,
    wallet_balance     DOUBLE PRECISION NOT NULL DEFAULT 0,
    available_balance  DOUBLE PRECISION NOT NULL DEFAULT 0,
    state_updated_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_accounts_user_id
    ON accounts (user_id);

CREATE UNIQUE INDEX IF NOT EXISTS uidx_accounts_api_key_nonempty
    ON accounts (api_key)
    WHERE api_key <> '';

CREATE TABLE IF NOT EXISTS account_snapshots (
    time              TIMESTAMPTZ      NOT NULL,
    account_id        BIGINT           NOT NULL,
    user_id           BIGINT           NOT NULL,
    mode              INTEGER          NOT NULL DEFAULT 0,
    futures_json      JSONB,
    spot_json         JSONB,
    total_value       DOUBLE PRECISION NOT NULL DEFAULT 0,
    wallet_balance    DOUBLE PRECISION NOT NULL DEFAULT 0,
    available_balance DOUBLE PRECISION NOT NULL DEFAULT 0,
    snapshot_reason   SMALLINT         NOT NULL DEFAULT 0,
    strategy_id       BIGINT           NULL,
    session_id        TEXT             NULL,
    PRIMARY KEY (time, account_id)
);

SELECT create_hypertable(
    'account_snapshots',
    'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_account_snapshots_account_id
    ON account_snapshots (account_id, time DESC);

CREATE INDEX IF NOT EXISTS idx_account_snapshots_user_time
    ON account_snapshots (user_id, account_id, time DESC);

CREATE TABLE IF NOT EXISTS strategies (
    strategy_id  BIGSERIAL     PRIMARY KEY,
    user_id      BIGINT        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name         VARCHAR(200)  NOT NULL,
    version      VARCHAR(20)   NOT NULL,
    description  VARCHAR(2000) NOT NULL DEFAULT '',
    code         TEXT          NOT NULL,
    archived     BOOLEAN       NOT NULL DEFAULT false,
    created_at   TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_strategies_user_id
    ON strategies (user_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_strategies_user_name_version
    ON strategies (user_id, name, version);

CREATE TABLE IF NOT EXISTS account_strategies (
    account_id   BIGINT      NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    strategy_id  BIGINT      NOT NULL REFERENCES strategies(strategy_id),
    active       BOOLEAN     NOT NULL DEFAULT false,
    mounted_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, strategy_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS uidx_account_strategies_active
    ON account_strategies (account_id)
    WHERE active = true;

CREATE TABLE IF NOT EXISTS strategy_sessions (
    session_id      TEXT        PRIMARY KEY,
    account_id      BIGINT      NOT NULL,
    user_id         BIGINT      NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    strategy_id     BIGINT      NOT NULL,
    mode            INTEGER     NOT NULL DEFAULT 0,
    status          TEXT        NOT NULL DEFAULT 'running',
    interval        TEXT        NOT NULL DEFAULT '1m',
    start_time_ms   BIGINT      NULL,
    end_time_ms     BIGINT      NULL,
    bars_processed  INTEGER     NOT NULL DEFAULT 0,
    error           TEXT        NOT NULL DEFAULT '',
    runtime_id      TEXT        NOT NULL DEFAULT '',
    runtime_source  TEXT        NOT NULL DEFAULT '',
    runtime_name    TEXT        NOT NULL DEFAULT '',
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON COLUMN strategy_sessions.status IS
    'running / stopping / recoverable / completed / finished / stopped / failed / stop_failed';

CREATE INDEX IF NOT EXISTS idx_sessions_account
    ON strategy_sessions (account_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_sessions_running
    ON strategy_sessions (status)
    WHERE status = 'running';

CREATE INDEX IF NOT EXISTS idx_strategy_sessions_user_id
    ON strategy_sessions (user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_strategy_sessions_user_runtime_created
    ON strategy_sessions (user_id, runtime_id, created_at DESC)
    WHERE runtime_id <> '';

CREATE INDEX IF NOT EXISTS idx_strategy_sessions_running_runtime
    ON strategy_sessions (runtime_id, status)
    WHERE runtime_id <> '' AND status IN ('running', 'stopping');

CREATE UNIQUE INDEX IF NOT EXISTS uq_strategy_sessions_active_account
    ON strategy_sessions (account_id)
    WHERE status IN ('running', 'stopping');

CREATE TABLE IF NOT EXISTS reconciliation_runs (
    time              TIMESTAMPTZ      NOT NULL,
    run_id            UUID             NOT NULL DEFAULT gen_random_uuid(),
    account_id        BIGINT           NOT NULL,
    user_id           BIGINT           NOT NULL,
    session_id        TEXT             NULL,
    strategy_id       BIGINT           NULL,
    mode              INTEGER          NOT NULL,
    snapshot_reason   SMALLINT         NOT NULL,
    run_type          TEXT             NOT NULL,
    exchange_snapshot JSONB            NOT NULL,
    local_snapshot    JSONB            NOT NULL,
    field_diffs       JSONB            NOT NULL,
    advisory_diffs    JSONB            NOT NULL,
    hard_pass         BOOLEAN          NOT NULL,
    soft_pass         BOOLEAN          NOT NULL,
    PRIMARY KEY (time, run_id)
);

SELECT create_hypertable(
    'reconciliation_runs',
    'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_reconciliation_runs_user_time
    ON reconciliation_runs (user_id, account_id, time DESC);

CREATE INDEX IF NOT EXISTS idx_reconciliation_runs_session_time
    ON reconciliation_runs (session_id, time DESC)
    WHERE session_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_reconciliation_runs_hard_fail
    ON reconciliation_runs (time DESC)
    WHERE NOT hard_pass;

-- Phase D2 moved the market-data control plane to control-panel-service.
-- core-service migration 0012 drops:
--   market_data_streams
--   market_data_requests
--   market_data_leases
--   market_data_history_requests

-- Portfolio/Venue Phase 1 schema note:
-- accounts.environment replaces accounts.mode.
-- venues stores exchange/market credential resources.
-- venue_wallet_states stores current canonical wallet state keyed by venue_id.
-- session_venues snapshots account-bound venues at session start.
-- order_intents stores order route facts.
-- order_attempts, orders, and order_fills keep execution facts and join to order_intents.
