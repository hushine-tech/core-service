DROP INDEX IF EXISTS uq_strategy_sessions_active_account;

CREATE UNIQUE INDEX uq_strategy_sessions_active_account
    ON strategy_sessions (account_id)
    WHERE status IN (3, 4);
