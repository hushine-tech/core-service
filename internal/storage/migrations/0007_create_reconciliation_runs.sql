-- Phase C shadow-compare / reconciliation runs table.
-- Written by account-service's reconciliation goroutine; one row per compare
-- run (pass or fail) for audit + distribution analysis.
-- Both snapshots stored in canonical JSON form so future providers (OKX, …)
-- can reuse the same compare table without provider-specific schema.

CREATE EXTENSION IF NOT EXISTS timescaledb;
-- gen_random_uuid() lives in pgcrypto. The extension exposes a server-side
-- UUID generator so the repository INSERT doesn't need to pre-allocate IDs,
-- and so two back-to-back reconciliation runs on the same (time, account_id)
-- can't collide on the primary key.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS reconciliation_runs (
    time              TIMESTAMPTZ      NOT NULL,
    run_id            UUID             NOT NULL DEFAULT gen_random_uuid(),
    account_id        BIGINT           NOT NULL,
    user_id           BIGINT           NOT NULL,
    session_id        TEXT             NULL,
    strategy_id       BIGINT           NULL,
    mode              INTEGER          NOT NULL,   -- 1 live / 2 testnet
    snapshot_reason   SMALLINT         NOT NULL,   -- matches domain.SnapshotReason
    run_type          TEXT             NOT NULL,   -- checkpoint / event / sampled
    exchange_snapshot JSONB            NOT NULL,   -- canonical AccountWalletState
    local_snapshot    JSONB            NOT NULL,   -- canonical AccountWalletState
    field_diffs       JSONB            NOT NULL,   -- Hard + Soft tier diffs
    advisory_diffs    JSONB            NOT NULL,   -- Advisory tier diffs (not gated)
    hard_pass         BOOLEAN          NOT NULL,
    soft_pass         BOOLEAN          NOT NULL,
    -- (time, run_id) is the hypertable-compatible PK: time is the partition
    -- column (required by Timescale) and run_id tie-breaks concurrent writes.
    PRIMARY KEY (time, run_id)
);

SELECT create_hypertable('reconciliation_runs', 'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_reconciliation_runs_user_time
    ON reconciliation_runs (user_id, account_id, time DESC);

CREATE INDEX IF NOT EXISTS idx_reconciliation_runs_session_time
    ON reconciliation_runs (session_id, time DESC)
    WHERE session_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_reconciliation_runs_hard_fail
    ON reconciliation_runs (time DESC)
    WHERE NOT hard_pass;
