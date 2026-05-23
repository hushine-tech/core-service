-- Add users.plan_code for runtime control plane (Phase D1).
--
-- plan_code maps a user to a tier in control-panel-service's runtime_plans
-- config (see control-panel-service/config.yaml -> runtime_plans). Effective
-- per-user limits are min(user_plan_limit, platform_limit) — see Phase D1
-- design doc.
--
-- Default 'pro' is for current dev convenience (matches
-- runtime_platform.default_plan_code in control-panel-service config). Before
-- production rollout, consider flipping the default and existing-row
-- backfill to 'free' as the conservative tier.
ALTER TABLE users
    ADD COLUMN IF NOT EXISTS plan_code TEXT NOT NULL DEFAULT 'pro';

CREATE INDEX IF NOT EXISTS idx_users_plan_code
    ON users (plan_code);
