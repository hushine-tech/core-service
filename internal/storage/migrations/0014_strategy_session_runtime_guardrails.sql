-- Runtime/account/session guardrails:
-- 1. 新 session 必须由代码层要求 runtime_id 非空。
-- 2. DB 层保证同一个 account 同时只有一个 active session。
-- 3. 迁移前遗留的 unbound/duplicate active session 会转为 recoverable，避免建索引失败。

UPDATE strategy_sessions
SET status = 'recoverable',
    error = CASE
        WHEN error = '' THEN 'migration: active session without runtime_id was moved to recoverable'
        ELSE error
    END,
    completed_at = COALESCE(completed_at, NOW())
WHERE status IN ('running', 'stopping')
  AND runtime_id = '';

WITH ranked AS (
    SELECT
        session_id,
        ROW_NUMBER() OVER (
            PARTITION BY account_id
            ORDER BY started_at DESC, created_at DESC, session_id DESC
        ) AS rn
    FROM strategy_sessions
    WHERE status IN ('running', 'stopping')
)
UPDATE strategy_sessions s
SET status = 'recoverable',
    error = CASE
        WHEN s.error = '' THEN 'migration: duplicate active account session was moved to recoverable'
        ELSE s.error
    END,
    completed_at = COALESCE(s.completed_at, NOW())
FROM ranked r
WHERE s.session_id = r.session_id
  AND r.rn > 1;

CREATE UNIQUE INDEX IF NOT EXISTS uq_strategy_sessions_active_account
    ON strategy_sessions (account_id)
    WHERE status IN ('running', 'stopping');
