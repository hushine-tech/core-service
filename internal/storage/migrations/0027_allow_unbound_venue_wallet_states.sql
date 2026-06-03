ALTER TABLE venue_wallet_states
    ALTER COLUMN account_id DROP NOT NULL;

ALTER TABLE venue_wallet_states
    DROP CONSTRAINT IF EXISTS venue_wallet_states_account_id_fkey;

ALTER TABLE venue_wallet_states
    ADD CONSTRAINT venue_wallet_states_account_id_fkey
    FOREIGN KEY (account_id) REFERENCES accounts(account_id) ON DELETE SET NULL;
