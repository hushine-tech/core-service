-- Runtime/debugger metadata used by the remote debug product flow.
ALTER TABLE strategies
    ADD COLUMN IF NOT EXISTS runtime_version TEXT NOT NULL DEFAULT '1.0.0',
    ADD COLUMN IF NOT EXISTS runtime_profile TEXT NOT NULL DEFAULT 'platform-python-3.13';

ALTER TABLE strategy_sessions
    ADD COLUMN IF NOT EXISTS session_type TEXT NOT NULL DEFAULT 'backtest',
    ADD COLUMN IF NOT EXISTS runtime_version TEXT NOT NULL DEFAULT '1.0.0',
    ADD COLUMN IF NOT EXISTS session_name TEXT NOT NULL DEFAULT '';

UPDATE strategy_sessions
SET session_type = CASE
    WHEN mode = 2 THEN 'testnet'
    ELSE 'backtest'
END
WHERE session_type = '' OR session_type IS NULL;

CREATE INDEX IF NOT EXISTS idx_strategy_sessions_user_type_created
    ON strategy_sessions(user_id, session_type, created_at DESC);
