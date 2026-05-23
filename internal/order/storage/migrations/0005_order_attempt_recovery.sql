ALTER TABLE order_attempts
    ADD COLUMN IF NOT EXISTS client_order_id TEXT NULL,
    ADD COLUMN IF NOT EXISTS recovery_error TEXT NOT NULL DEFAULT '';

CREATE UNIQUE INDEX IF NOT EXISTS idx_order_attempts_client_order
    ON order_attempts (account_id, client_order_id)
    WHERE client_order_id IS NOT NULL AND client_order_id <> '';

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS client_order_id TEXT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_client_order
    ON orders (account_id, client_order_id)
    WHERE client_order_id IS NOT NULL AND client_order_id <> '';
