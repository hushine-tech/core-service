CREATE EXTENSION IF NOT EXISTS timescaledb;

DROP TABLE IF EXISTS session_venues CASCADE;
DROP TABLE IF EXISTS venue_events CASCADE;
DROP TABLE IF EXISTS reconciliation_runs CASCADE;
DROP TABLE IF EXISTS account_snapshots CASCADE;
DROP TABLE IF EXISTS strategy_sessions CASCADE;
DROP TABLE IF EXISTS account_strategies CASCADE;
DROP TABLE IF EXISTS venues CASCADE;
DROP TABLE IF EXISTS accounts CASCADE;

CREATE TABLE accounts (
    account_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    environment SMALLINT NOT NULL,
    status SMALLINT NOT NULL DEFAULT 1,
    total_value NUMERIC(38, 18) NOT NULL DEFAULT 0,
    wallet_balance NUMERIC(38, 18) NOT NULL DEFAULT 0,
    available_balance NUMERIC(38, 18) NOT NULL DEFAULT 0,
    snapshot_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    state_updated_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    archived_at TIMESTAMPTZ,
    archived_reason TEXT NOT NULL DEFAULT '',
    CONSTRAINT chk_accounts_environment CHECK (environment IN (0, 1, 2)),
    CONSTRAINT chk_accounts_status CHECK (status IN (1, 2)),
    CONSTRAINT uq_accounts_user_name UNIQUE (user_id, name)
);

CREATE TABLE account_strategies (
    account_id   BIGINT      NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    strategy_id  BIGINT      NOT NULL REFERENCES strategies(strategy_id),
    active       BOOLEAN     NOT NULL DEFAULT false,
    mounted_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, strategy_id)
);

CREATE UNIQUE INDEX uidx_account_strategies_active
    ON account_strategies (account_id)
    WHERE active = true;

CREATE TABLE venues (
    venue_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    account_id BIGINT REFERENCES accounts(account_id) ON DELETE SET NULL,
    exchange SMALLINT NOT NULL,
    market SMALLINT NOT NULL,
    environment SMALLINT NOT NULL,
    status SMALLINT NOT NULL DEFAULT 1,
    display_name TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    api_key TEXT NOT NULL DEFAULT '',
    credential_info TEXT NOT NULL DEFAULT '',
    credential_key_version TEXT NOT NULL DEFAULT 'v1',
    credential_fingerprint TEXT NOT NULL DEFAULT '',
    margin_mode SMALLINT NOT NULL DEFAULT 0,
    position_mode SMALLINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at TIMESTAMPTZ,
    archived_at TIMESTAMPTZ,
    archived_reason TEXT NOT NULL DEFAULT '',
    CONSTRAINT chk_venues_exchange CHECK (exchange IN (1, 2)),
    CONSTRAINT chk_venues_market CHECK (market IN (1, 2, 3)),
    CONSTRAINT chk_venues_environment CHECK (environment IN (0, 1, 2)),
    CONSTRAINT chk_venues_status CHECK (status IN (1, 2, 3, 4)),
    CONSTRAINT chk_venues_margin_mode CHECK (margin_mode IN (0, 1, 2)),
    CONSTRAINT chk_venues_position_mode CHECK (position_mode IN (0, 1, 2)),
    CONSTRAINT chk_venues_spot_modes CHECK (
        market <> 1 OR (margin_mode = 0 AND position_mode = 0)
    ),
    CONSTRAINT chk_venues_perp_modes CHECK (
        market <> 2 OR (margin_mode IN (1, 2) AND position_mode IN (1, 2))
    )
);

CREATE UNIQUE INDEX uidx_venues_api_key_scope
    ON venues(exchange, environment, market, api_key)
    WHERE api_key <> '';

CREATE UNIQUE INDEX uidx_venues_active_account_market
    ON venues(account_id, exchange, market)
    WHERE account_id IS NOT NULL AND status = 1;

CREATE TABLE venue_events (
    event_id BIGSERIAL PRIMARY KEY,
    venue_id BIGINT NOT NULL REFERENCES venues(venue_id) ON DELETE CASCADE,
    account_id BIGINT,
    user_id BIGINT NOT NULL,
    event_type SMALLINT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    detail_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_venue_events_venue_time
    ON venue_events(venue_id, created_at DESC);

CREATE TABLE strategy_sessions (
    session_id TEXT PRIMARY KEY,
    account_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    strategy_id BIGINT,
    environment SMALLINT NOT NULL,
    status SMALLINT NOT NULL,
    interval TEXT NOT NULL DEFAULT '',
    start_time_ms BIGINT,
    end_time_ms BIGINT,
    bars_processed INTEGER NOT NULL DEFAULT 0,
    error TEXT NOT NULL DEFAULT '',
    error_code TEXT NOT NULL DEFAULT '',
    error_message TEXT NOT NULL DEFAULT '',
    error_detail_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    runtime_id TEXT NOT NULL DEFAULT '',
    runtime_source TEXT NOT NULL DEFAULT '',
    runtime_name TEXT NOT NULL DEFAULT '',
    session_type TEXT NOT NULL DEFAULT '',
    runtime_version TEXT NOT NULL DEFAULT '',
    session_name TEXT NOT NULL DEFAULT '',
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_strategy_sessions_environment CHECK (environment IN (0, 1, 2)),
    CONSTRAINT chk_strategy_sessions_status CHECK (status IN (1, 2, 3, 4, 5, 6, 7, 8, 9))
);

CREATE UNIQUE INDEX uq_strategy_sessions_active_account
    ON strategy_sessions(account_id)
    WHERE status IN (3, 4, 5);

CREATE INDEX idx_strategy_sessions_user_created
    ON strategy_sessions(user_id, created_at DESC);

CREATE TABLE session_venues (
    session_id TEXT NOT NULL REFERENCES strategy_sessions(session_id) ON DELETE CASCADE,
    venue_id BIGINT NOT NULL,
    account_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    exchange SMALLINT NOT NULL,
    market SMALLINT NOT NULL,
    environment SMALLINT NOT NULL,
    display_name TEXT NOT NULL DEFAULT '',
    api_key TEXT NOT NULL DEFAULT '',
    credential_fingerprint TEXT NOT NULL DEFAULT '',
    margin_mode SMALLINT NOT NULL,
    position_mode SMALLINT NOT NULL,
    venue_status SMALLINT NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (session_id, venue_id)
);

CREATE TABLE account_snapshots (
    time TIMESTAMPTZ NOT NULL,
    account_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    environment SMALLINT NOT NULL,
    total_value NUMERIC(38, 18) NOT NULL DEFAULT 0,
    wallet_balance NUMERIC(38, 18) NOT NULL DEFAULT 0,
    available_balance NUMERIC(38, 18) NOT NULL DEFAULT 0,
    snapshot_reason SMALLINT NOT NULL,
    strategy_id BIGINT,
    session_id TEXT,
    snapshot_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (time, account_id)
);

SELECT create_hypertable('account_snapshots', 'time',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE);

CREATE INDEX idx_account_snapshots_session_time
    ON account_snapshots(session_id, time DESC);

CREATE TABLE reconciliation_runs (
    time TIMESTAMPTZ NOT NULL,
    run_id TEXT NOT NULL,
    account_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    session_id TEXT NOT NULL DEFAULT '',
    strategy_id BIGINT,
    environment SMALLINT NOT NULL,
    snapshot_reason SMALLINT NOT NULL,
    run_type TEXT NOT NULL,
    hard_pass BOOLEAN NOT NULL,
    soft_pass BOOLEAN NOT NULL,
    account_diff_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    venue_diffs_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (time, run_id)
);

SELECT create_hypertable('reconciliation_runs', 'time',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE);

CREATE INDEX idx_reconciliation_runs_session_time
    ON reconciliation_runs(session_id, time DESC);
