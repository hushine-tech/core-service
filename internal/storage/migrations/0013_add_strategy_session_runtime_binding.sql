-- Phase D runtime ownership: bind each strategy session to the runtime that owns it.
-- Empty strings represent legacy sessions created before this migration.
ALTER TABLE strategy_sessions
    ADD COLUMN IF NOT EXISTS runtime_id TEXT NOT NULL DEFAULT '';

ALTER TABLE strategy_sessions
    ADD COLUMN IF NOT EXISTS runtime_source TEXT NOT NULL DEFAULT '';

ALTER TABLE strategy_sessions
    ADD COLUMN IF NOT EXISTS runtime_service_name TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_strategy_sessions_user_runtime_created
    ON strategy_sessions (user_id, runtime_id, created_at DESC)
    WHERE runtime_id <> '';

CREATE INDEX IF NOT EXISTS idx_strategy_sessions_running_runtime
    ON strategy_sessions (runtime_id, status)
    WHERE runtime_id <> '' AND status IN ('running', 'stopping');
