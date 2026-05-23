-- Phase A: 一个 account 唯一绑定一个 Binance api_key。
-- 约束只作用于非空 api_key（回测帐号 api_key 为空字符串，不受约束）。
-- 避免两个 account 共用同一把 key 导致策略互踩、对账无法进行。

CREATE UNIQUE INDEX IF NOT EXISTS uidx_accounts_api_key_nonempty
    ON accounts (api_key)
    WHERE api_key <> '';
