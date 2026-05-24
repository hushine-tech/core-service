CREATE TABLE IF NOT EXISTS accounts (
    account_id       BIGSERIAL       PRIMARY KEY,
    name             TEXT            NOT NULL,
    description      TEXT            NOT NULL DEFAULT '',
    mode             INTEGER         NOT NULL DEFAULT 0,
    api_key          TEXT            NOT NULL DEFAULT '',
    api_secret       TEXT            NOT NULL DEFAULT '',
    margin_mode      TEXT            NOT NULL DEFAULT 'cross',
    position_mode    TEXT            NOT NULL DEFAULT 'one_way',
    slippage_bps     DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    default_fee_rate DOUBLE PRECISION NOT NULL DEFAULT 0.0004,
    created_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    futures_json     JSONB,
    spot_json        JSONB,
    total_value      DOUBLE PRECISION NOT NULL DEFAULT 0,
    wallet_balance   DOUBLE PRECISION NOT NULL DEFAULT 0,
    available_balance DOUBLE PRECISION NOT NULL DEFAULT 0,
    state_updated_at TIMESTAMPTZ
);
