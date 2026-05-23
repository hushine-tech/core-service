CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS account_snapshots (
    time              TIMESTAMPTZ      NOT NULL,
    account_id        BIGINT           NOT NULL,
    mode              INTEGER          NOT NULL DEFAULT 0,
    futures_json      JSONB,
    spot_json         JSONB,
    total_value       DOUBLE PRECISION NOT NULL DEFAULT 0,
    wallet_balance    DOUBLE PRECISION NOT NULL DEFAULT 0,
    available_balance DOUBLE PRECISION NOT NULL DEFAULT 0,
    snapshot_reason   SMALLINT         NOT NULL DEFAULT 0,
    strategy_id       BIGINT           NULL,
    PRIMARY KEY (time, account_id)
);

SELECT create_hypertable('account_snapshots', 'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_account_snapshots_account_id
    ON account_snapshots (account_id, time DESC);
