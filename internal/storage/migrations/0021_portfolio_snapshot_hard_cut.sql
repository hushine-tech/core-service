CREATE OR REPLACE VIEW current_portfolio_snapshots AS
SELECT
    account_id,
    user_id,
    environment,
    total_value,
    wallet_balance,
    available_balance,
    snapshot_json,
    state_updated_at,
    updated_at
FROM accounts;
