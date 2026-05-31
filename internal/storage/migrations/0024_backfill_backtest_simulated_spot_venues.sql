-- Superseded by 0025_remove_unsupported_backtest_simulated_spot_venues.sql.
-- Backtest spot routes are removed until spot symbol rules and execution are
-- implemented in the exchange capability registry.

INSERT INTO venues (
    user_id,
    account_id,
    exchange,
    market,
    environment,
    status,
    display_name,
    description,
    api_key,
    credential_info,
    credential_key_version,
    credential_fingerprint,
    margin_mode,
    position_mode,
    created_at,
    updated_at
)
SELECT
    a.user_id,
    a.account_id,
    1,
    1,
    0,
    1,
    'Simulated Binance Spot',
    'default simulated venue',
    '',
    '',
    'v1',
    '',
    0,
    0,
    now(),
    now()
FROM accounts a
WHERE a.environment = 0
  AND a.status = 1
  AND NOT EXISTS (
      SELECT 1
      FROM venues v
      WHERE v.account_id = a.account_id
        AND v.exchange = 1
        AND v.market = 1
        AND v.status = 1
  )
ON CONFLICT DO NOTHING;
