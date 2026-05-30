ALTER TABLE strategy_sessions
    ADD COLUMN IF NOT EXISTS error_code TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS error_message TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS error_detail_json JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE strategy_sessions
    DROP CONSTRAINT IF EXISTS chk_strategy_sessions_status;

ALTER TABLE strategy_sessions
    ADD CONSTRAINT chk_strategy_sessions_status CHECK (status IN (1, 2, 3, 4, 5, 6, 7, 8, 9));
