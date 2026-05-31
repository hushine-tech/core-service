-- Backtest spot execution is not implemented in the Binance capability
-- registry yet. Remove the previously backfilled default spot venue so
-- preflight fails closed instead of advertising an unusable route.

DELETE FROM venues
WHERE exchange = 1
  AND market = 1
  AND environment = 0
  AND status = 1
  AND api_key = ''
  AND credential_info = ''
  AND description = 'default simulated venue';
