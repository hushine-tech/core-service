CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS order_fills (
    time          TIMESTAMPTZ      NOT NULL,
    order_id      TEXT             NOT NULL,
    account_id    BIGINT           NOT NULL,
    symbol        TEXT             NOT NULL,
    side          TEXT             NOT NULL,
    qty           DOUBLE PRECISION NOT NULL,
    fill_price    DOUBLE PRECISION NOT NULL,
    fee           DOUBLE PRECISION NOT NULL DEFAULT 0,
    status        TEXT             NOT NULL,
    mode          INTEGER          NOT NULL DEFAULT 0,
    error_message TEXT             NOT NULL DEFAULT '',
    strategy_id   BIGINT           NULL,
    market        VARCHAR(20)      NULL,
    PRIMARY KEY (time, order_id)
);

SELECT create_hypertable('order_fills', 'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_order_fills_account
    ON order_fills (account_id, time DESC);
