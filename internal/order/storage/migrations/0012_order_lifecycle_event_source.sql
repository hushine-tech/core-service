ALTER TABLE order_lifecycle_events
    ADD COLUMN IF NOT EXISTS event_source TEXT NOT NULL DEFAULT '';
