-- 策略表：不可修改，只能归档
CREATE TABLE IF NOT EXISTS strategies (
    strategy_id  BIGSERIAL    PRIMARY KEY,
    name         VARCHAR(200) NOT NULL,
    version      VARCHAR(20)  NOT NULL,
    description  VARCHAR(2000) NOT NULL DEFAULT '',
    code         TEXT         NOT NULL,
    archived     BOOLEAN      NOT NULL DEFAULT false,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (name, version)
);

-- 帐号-策略挂载表：一个帐号可以挂载多个策略，但只有一个 active
CREATE TABLE IF NOT EXISTS account_strategies (
    account_id   BIGINT      NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    strategy_id  BIGINT      NOT NULL REFERENCES strategies(strategy_id),
    active       BOOLEAN     NOT NULL DEFAULT false,
    mounted_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, strategy_id)
);

-- 保证同一帐号最多一个 active 策略
CREATE UNIQUE INDEX IF NOT EXISTS uidx_account_strategies_active
    ON account_strategies (account_id)
    WHERE active = true;
