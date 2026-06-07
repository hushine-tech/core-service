CREATE TABLE IF NOT EXISTS notification_settings (
    user_id BIGINT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    system_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    strategy_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    custom_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    last_delivery_status TEXT,
    last_delivery_error TEXT,
    last_delivery_at TIMESTAMPTZ,
    last_test_message_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS notification_channels (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    channel TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'unbound',
    target_id TEXT,
    target_type TEXT,
    target_label TEXT,
    bind_code_hash TEXT,
    bind_code_expires_at TIMESTAMPTZ,
    bound_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    last_delivery_status TEXT,
    last_delivery_error TEXT,
    last_delivery_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT notification_channels_user_channel_unique UNIQUE (user_id, channel),
    CONSTRAINT notification_channels_channel_check CHECK (channel IN ('telegram')),
    CONSTRAINT notification_channels_status_check CHECK (status IN ('unbound', 'pending', 'bound', 'revoked'))
);

CREATE INDEX IF NOT EXISTS idx_notification_channels_bind_code_hash
    ON notification_channels (bind_code_hash)
    WHERE bind_code_hash IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_notification_channels_bind_code_expires_at
    ON notification_channels (bind_code_expires_at)
    WHERE bind_code_hash IS NOT NULL;

CREATE TABLE IF NOT EXISTS notification_plans (
    plan_code TEXT PRIMARY KEY,
    notification_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    allow_system BOOLEAN NOT NULL DEFAULT FALSE,
    allow_strategy BOOLEAN NOT NULL DEFAULT FALSE,
    allow_custom BOOLEAN NOT NULL DEFAULT FALSE,
    custom_rate_limit_per_minute INT NOT NULL DEFAULT 0,
    custom_rate_limit_burst INT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT notification_plans_rate_limit_nonnegative CHECK (
        custom_rate_limit_per_minute >= 0 AND custom_rate_limit_burst >= 0
    )
);

INSERT INTO notification_plans (
    plan_code,
    notification_enabled,
    allow_system,
    allow_strategy,
    allow_custom,
    custom_rate_limit_per_minute,
    custom_rate_limit_burst
) VALUES
    ('free', FALSE, FALSE, FALSE, FALSE, 0, 0),
    ('developer', TRUE, TRUE, TRUE, FALSE, 0, 0),
    ('pro', TRUE, TRUE, TRUE, TRUE, 30, 10)
ON CONFLICT (plan_code) DO UPDATE SET
    notification_enabled = EXCLUDED.notification_enabled,
    allow_system = EXCLUDED.allow_system,
    allow_strategy = EXCLUDED.allow_strategy,
    allow_custom = EXCLUDED.allow_custom,
    custom_rate_limit_per_minute = EXCLUDED.custom_rate_limit_per_minute,
    custom_rate_limit_burst = EXCLUDED.custom_rate_limit_burst,
    updated_at = NOW();
