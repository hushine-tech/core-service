CREATE TABLE IF NOT EXISTS order_lifecycle_events (
    event_id BIGSERIAL PRIMARY KEY,
    session_id TEXT NULL,
    account_id BIGINT NOT NULL,
    venue_id BIGINT NOT NULL,
    intent_id TEXT NULL,
    attempt_id TEXT NULL,
    order_id TEXT NULL,
    exchange_order_id TEXT NULL,
    exchange_trade_id TEXT NULL,
    event_type TEXT NOT NULL,
    order_status TEXT NOT NULL DEFAULT '',
    fill_delta_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    order_state_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uidx_order_lifecycle_exchange_trade
    ON order_lifecycle_events(venue_id, exchange_order_id, exchange_trade_id)
    WHERE exchange_order_id IS NOT NULL AND exchange_trade_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_order_lifecycle_session_cursor
    ON order_lifecycle_events(session_id, event_id)
    WHERE session_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_order_lifecycle_order
    ON order_lifecycle_events(order_id, event_id)
    WHERE order_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_order_lifecycle_venue_time
    ON order_lifecycle_events(venue_id, occurred_at DESC);
