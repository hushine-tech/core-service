-- 策略运行记录表：持久化 session 生命周期
CREATE TABLE IF NOT EXISTS strategy_sessions (
    session_id      TEXT PRIMARY KEY,
    account_id      BIGINT NOT NULL,
    strategy_id     BIGINT NOT NULL,
    environment     INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'running',
    interval        TEXT NOT NULL DEFAULT '1m',
    start_time_ms   BIGINT NULL,
    end_time_ms     BIGINT NULL,
    bars_processed  INTEGER NOT NULL DEFAULT 0,
    error           TEXT NOT NULL DEFAULT '',
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sessions_account
    ON strategy_sessions (account_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_sessions_running
    ON strategy_sessions (status) WHERE status = 'running';

-- account_snapshots 加 session_id
ALTER TABLE account_snapshots
    ADD COLUMN IF NOT EXISTS session_id TEXT NULL;
