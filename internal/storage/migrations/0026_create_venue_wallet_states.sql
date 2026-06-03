CREATE TABLE IF NOT EXISTS venue_wallet_states (
    venue_id BIGINT PRIMARY KEY REFERENCES venues(venue_id) ON DELETE CASCADE,
    account_id BIGINT REFERENCES accounts(account_id) ON DELETE SET NULL,
    user_id BIGINT NOT NULL,
    exchange SMALLINT NOT NULL,
    environment SMALLINT NOT NULL,
    market SMALLINT NOT NULL,
    total_value NUMERIC(38, 18) NOT NULL DEFAULT 0,
    wallet_balance NUMERIC(38, 18) NOT NULL DEFAULT 0,
    available_balance NUMERIC(38, 18) NOT NULL DEFAULT 0,
    snapshot_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_venue_wallet_states_exchange CHECK (exchange IN (1, 2)),
    CONSTRAINT chk_venue_wallet_states_environment CHECK (environment IN (0, 1, 2)),
    CONSTRAINT chk_venue_wallet_states_market CHECK (market IN (1, 2, 3))
);

CREATE INDEX IF NOT EXISTS idx_venue_wallet_states_account
    ON venue_wallet_states(account_id, venue_id);
