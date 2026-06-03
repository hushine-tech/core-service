CREATE EXTENSION IF NOT EXISTS timescaledb;

DO $$
BEGIN
    IF to_regclass('public.legacy_order_fills') IS NULL
       AND to_regclass('public.order_fills') IS NOT NULL
       AND NOT EXISTS (
           SELECT 1
           FROM information_schema.columns
           WHERE table_schema = 'public'
             AND table_name = 'order_fills'
             AND column_name = 'fill_id'
       )
    THEN
        ALTER TABLE order_fills RENAME TO legacy_order_fills;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS order_intents (
    intent_id        TEXT PRIMARY KEY,
    time             TIMESTAMPTZ      NOT NULL,
    updated_at       TIMESTAMPTZ      NOT NULL,
    account_id       BIGINT           NOT NULL,
    user_id          BIGINT           NOT NULL,
    strategy_id      BIGINT           NULL,
    session_id       TEXT             NULL,
    market           VARCHAR(20)      NULL,
    symbol           TEXT             NOT NULL,
    side             TEXT             NOT NULL,
    requested_qty    DOUBLE PRECISION NOT NULL,
    requested_price  DOUBLE PRECISION NULL
);

CREATE INDEX IF NOT EXISTS idx_order_intents_user_time
    ON order_intents (user_id, account_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_order_intents_session_time
    ON order_intents (session_id, time DESC);

CREATE TABLE IF NOT EXISTS order_attempts (
    attempt_id         TEXT PRIMARY KEY,
    intent_id          TEXT             NOT NULL REFERENCES order_intents(intent_id),
    time               TIMESTAMPTZ      NOT NULL,
    updated_at         TIMESTAMPTZ      NOT NULL,
    account_id         BIGINT           NOT NULL,
    user_id            BIGINT           NOT NULL,
    strategy_id        BIGINT           NULL,
    session_id         TEXT             NULL,
    market             VARCHAR(20)      NULL,
    symbol             TEXT             NOT NULL,
    side               TEXT             NOT NULL,
    requested_qty      DOUBLE PRECISION NOT NULL,
    requested_price    DOUBLE PRECISION NULL,
    mark_price         DOUBLE PRECISION NOT NULL DEFAULT 0,
    status             TEXT             NOT NULL,
    error_message      TEXT             NOT NULL DEFAULT '',
    order_id           TEXT             NULL,
    exchange_order_id  TEXT             NULL,
    environment        INTEGER          NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_order_attempts_user_time
    ON order_attempts (user_id, account_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_order_attempts_session_time
    ON order_attempts (session_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_order_attempts_intent
    ON order_attempts (intent_id, time DESC);

CREATE TABLE IF NOT EXISTS orders (
    order_id            TEXT PRIMARY KEY,
    exchange_order_id   TEXT             NULL,
    attempt_id          TEXT             NOT NULL UNIQUE REFERENCES order_attempts(attempt_id),
    intent_id           TEXT             NOT NULL REFERENCES order_intents(intent_id),
    time                TIMESTAMPTZ      NOT NULL,
    updated_at          TIMESTAMPTZ      NOT NULL,
    account_id          BIGINT           NOT NULL,
    user_id             BIGINT           NOT NULL,
    strategy_id         BIGINT           NULL,
    session_id          TEXT             NULL,
    market              VARCHAR(20)      NULL,
    symbol              TEXT             NOT NULL,
    side                TEXT             NOT NULL,
    orig_qty            DOUBLE PRECISION NOT NULL,
    executed_qty        DOUBLE PRECISION NOT NULL DEFAULT 0,
    remaining_qty       DOUBLE PRECISION NOT NULL DEFAULT 0,
    avg_price           DOUBLE PRECISION NOT NULL DEFAULT 0,
    price               DOUBLE PRECISION NULL,
    status              TEXT             NOT NULL,
    error_message       TEXT             NOT NULL DEFAULT '',
    environment         INTEGER          NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_exchange_order
    ON orders (account_id, exchange_order_id)
    WHERE exchange_order_id IS NOT NULL AND exchange_order_id <> '';
CREATE INDEX IF NOT EXISTS idx_orders_user_time
    ON orders (user_id, account_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_orders_session_time
    ON orders (session_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_orders_intent_time
    ON orders (intent_id, time DESC);

CREATE TABLE IF NOT EXISTS order_fills (
    time               TIMESTAMPTZ      NOT NULL,
    fill_id            TEXT             NOT NULL,
    exchange_trade_id  TEXT             NULL,
    order_id           TEXT             NOT NULL REFERENCES orders(order_id),
    exchange_order_id  TEXT             NULL,
    attempt_id         TEXT             NOT NULL REFERENCES order_attempts(attempt_id),
    intent_id          TEXT             NOT NULL REFERENCES order_intents(intent_id),
    account_id         BIGINT           NOT NULL,
    user_id            BIGINT           NOT NULL,
    symbol             TEXT             NOT NULL,
    side               TEXT             NOT NULL,
    qty                DOUBLE PRECISION NOT NULL,
    fill_price         DOUBLE PRECISION NOT NULL,
    fee                DOUBLE PRECISION NOT NULL DEFAULT 0,
    status             TEXT             NOT NULL,
    environment        INTEGER          NOT NULL DEFAULT 0,
    strategy_id        BIGINT           NULL,
    market             VARCHAR(20)      NULL,
    session_id         TEXT             NULL,
    PRIMARY KEY (time, fill_id)
);

SELECT create_hypertable('order_fills', 'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_order_fills_user_time
    ON order_fills (user_id, account_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_order_fills_session_time
    ON order_fills (session_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_order_fills_order_time
    ON order_fills (order_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_order_fills_attempt_time
    ON order_fills (attempt_id, time DESC);
