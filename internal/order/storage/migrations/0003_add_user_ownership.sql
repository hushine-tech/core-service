ALTER TABLE order_fills
    ADD COLUMN IF NOT EXISTS user_id BIGINT;
ALTER TABLE order_fills
    ALTER COLUMN user_id SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_order_fills_user_time
    ON order_fills (user_id, account_id, time DESC);
