CREATE EXTENSION IF NOT EXISTS timescaledb;

DROP TABLE IF EXISTS order_fills CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS order_attempts CASCADE;
DROP TABLE IF EXISTS order_intents CASCADE;

CREATE TABLE order_intents (
    intent_id TEXT PRIMARY KEY,
    time TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    account_id BIGINT NOT NULL,
    venue_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    strategy_id BIGINT NULL,
    session_id TEXT NULL,
    environment SMALLINT NOT NULL,
    exchange SMALLINT NOT NULL,
    market SMALLINT NOT NULL,
    symbol TEXT NOT NULL,
    side SMALLINT NOT NULL,
    position_side SMALLINT NOT NULL DEFAULT 0,
    order_type SMALLINT NOT NULL,
    requested_qty NUMERIC(38, 18) NOT NULL,
    requested_price NUMERIC(38, 18) NULL,
    status SMALLINT NOT NULL,
    reject_code TEXT NOT NULL DEFAULT '',
    reject_message TEXT NOT NULL DEFAULT '',
    CONSTRAINT chk_order_intents_environment CHECK (environment IN (0, 1, 2)),
    CONSTRAINT chk_order_intents_exchange CHECK (exchange IN (1, 2)),
    CONSTRAINT chk_order_intents_market CHECK (market IN (1, 2, 3)),
    CONSTRAINT chk_order_intents_side CHECK (side IN (1, 2)),
    CONSTRAINT chk_order_intents_order_type CHECK (order_type IN (1)),
    CONSTRAINT chk_order_intents_status CHECK (status IN (1, 2))
);

CREATE INDEX idx_order_intents_user_time
    ON order_intents(user_id, account_id, time DESC);

CREATE INDEX idx_order_intents_session_time
    ON order_intents(session_id, time DESC);

CREATE INDEX idx_order_intents_venue_time
    ON order_intents(venue_id, time DESC);

CREATE TABLE order_attempts (
    attempt_id TEXT PRIMARY KEY,
    intent_id TEXT NOT NULL REFERENCES order_intents(intent_id),
    time TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    mark_price NUMERIC(38, 18) NULL,
    client_order_id TEXT NULL,
    status SMALLINT NOT NULL,
    error_code TEXT NOT NULL DEFAULT '',
    error_message TEXT NOT NULL DEFAULT '',
    exchange_order_id TEXT NULL,
    recovery_error TEXT NOT NULL DEFAULT '',
    CONSTRAINT chk_order_attempts_status CHECK (status IN (1, 2, 3, 4, 5))
);

CREATE UNIQUE INDEX uidx_order_attempts_client_order
    ON order_attempts(client_order_id)
    WHERE client_order_id IS NOT NULL AND client_order_id <> '';

CREATE INDEX idx_order_attempts_intent_time
    ON order_attempts(intent_id, time DESC);

CREATE TABLE orders (
    order_id TEXT PRIMARY KEY,
    exchange_order_id TEXT NULL,
    client_order_id TEXT NULL,
    attempt_id TEXT NOT NULL UNIQUE REFERENCES order_attempts(attempt_id),
    intent_id TEXT NOT NULL REFERENCES order_intents(intent_id),
    orig_qty NUMERIC(38, 18) NOT NULL,
    executed_qty NUMERIC(38, 18) NOT NULL DEFAULT 0,
    remaining_qty NUMERIC(38, 18) NOT NULL DEFAULT 0,
    avg_price NUMERIC(38, 18) NOT NULL DEFAULT 0,
    price NUMERIC(38, 18) NULL,
    status SMALLINT NOT NULL,
    error_code TEXT NOT NULL DEFAULT '',
    error_message TEXT NOT NULL DEFAULT '',
    time TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT chk_orders_status CHECK (status IN (1, 2, 3, 4, 5, 6))
);

CREATE INDEX idx_orders_exchange_order
    ON orders(exchange_order_id)
    WHERE exchange_order_id IS NOT NULL AND exchange_order_id <> '';

CREATE INDEX idx_orders_intent_time
    ON orders(intent_id, time DESC);

CREATE TABLE order_fills (
    time TIMESTAMPTZ NOT NULL,
    fill_id TEXT NOT NULL,
    exchange_trade_id TEXT NULL,
    order_id TEXT NOT NULL REFERENCES orders(order_id),
    exchange_order_id TEXT NULL,
    attempt_id TEXT NOT NULL REFERENCES order_attempts(attempt_id),
    intent_id TEXT NOT NULL REFERENCES order_intents(intent_id),
    qty NUMERIC(38, 18) NOT NULL,
    fill_price NUMERIC(38, 18) NOT NULL,
    fee NUMERIC(38, 18) NOT NULL DEFAULT 0,
    fee_asset TEXT NOT NULL DEFAULT '',
    status SMALLINT NOT NULL,
    PRIMARY KEY (time, fill_id),
    CONSTRAINT chk_order_fills_status CHECK (status IN (1, 2))
);

SELECT create_hypertable('order_fills', 'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE);

CREATE INDEX idx_order_fills_intent_time
    ON order_fills(intent_id, time DESC);

CREATE INDEX idx_order_fills_order_time
    ON order_fills(order_id, time DESC);
