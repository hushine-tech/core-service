-- account_snapshots is an audit/event stream. Multiple sessions can legitimately
-- persist snapshots for the same account and market time, especially repeated
-- backtests with the same start_time_ms.
ALTER TABLE account_snapshots
    DROP CONSTRAINT IF EXISTS account_snapshots_pkey;

CREATE INDEX IF NOT EXISTS idx_account_snapshots_account_id
    ON account_snapshots (account_id, time DESC);

CREATE INDEX IF NOT EXISTS idx_account_snapshots_session_time
    ON account_snapshots (session_id, time DESC);
