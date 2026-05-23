-- user ownership model: users -> accounts -> strategies/sessions/snapshots

CREATE TABLE IF NOT EXISTS users (
    id            BIGSERIAL   PRIMARY KEY,
    username      TEXT        NOT NULL,
    password_hash TEXT        NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username
    ON users (username);

ALTER TABLE accounts
    ADD COLUMN IF NOT EXISTS user_id BIGINT;
ALTER TABLE accounts
    ALTER COLUMN user_id SET NOT NULL;
ALTER TABLE accounts
    DROP CONSTRAINT IF EXISTS fk_accounts_user_id;
ALTER TABLE accounts
    ADD CONSTRAINT fk_accounts_user_id
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE;
CREATE INDEX IF NOT EXISTS idx_accounts_user_id
    ON accounts (user_id);

ALTER TABLE strategies
    ADD COLUMN IF NOT EXISTS user_id BIGINT;
ALTER TABLE strategies
    ALTER COLUMN user_id SET NOT NULL;
ALTER TABLE strategies
    DROP CONSTRAINT IF EXISTS fk_strategies_user_id;
ALTER TABLE strategies
    ADD CONSTRAINT fk_strategies_user_id
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE;
CREATE INDEX IF NOT EXISTS idx_strategies_user_id
    ON strategies (user_id);
ALTER TABLE strategies
    DROP CONSTRAINT IF EXISTS strategies_name_version_key;
CREATE UNIQUE INDEX IF NOT EXISTS idx_strategies_user_name_version
    ON strategies (user_id, name, version);

ALTER TABLE strategy_sessions
    ADD COLUMN IF NOT EXISTS user_id BIGINT;
ALTER TABLE strategy_sessions
    ALTER COLUMN user_id SET NOT NULL;
ALTER TABLE strategy_sessions
    DROP CONSTRAINT IF EXISTS fk_strategy_sessions_user_id;
ALTER TABLE strategy_sessions
    ADD CONSTRAINT fk_strategy_sessions_user_id
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE;
CREATE INDEX IF NOT EXISTS idx_strategy_sessions_user_id
    ON strategy_sessions (user_id, created_at DESC);

ALTER TABLE account_snapshots
    ADD COLUMN IF NOT EXISTS user_id BIGINT;
ALTER TABLE account_snapshots
    ALTER COLUMN user_id SET NOT NULL;
CREATE INDEX IF NOT EXISTS idx_account_snapshots_user_time
    ON account_snapshots (user_id, account_id, time DESC);
