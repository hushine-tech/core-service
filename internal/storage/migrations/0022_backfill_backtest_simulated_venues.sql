-- Existing backtest accounts created before venue routing may not have the
-- default simulated Binance futures venue. Order routing is venue-first, so
-- those accounts must be backfilled before they can run strategies.

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
    2,
    0,
    1,
    'Simulated Binance Perpetual Futures',
    'default simulated venue',
    '',
    '',
    'v1',
    '',
    1,
    1,
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
        AND v.market = 2
        AND v.status = 1
  )
ON CONFLICT DO NOTHING;
