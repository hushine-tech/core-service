ALTER TABLE order_lifecycle_events
    ADD COLUMN IF NOT EXISTS event_identity TEXT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uidx_order_lifecycle_event_identity
    ON order_lifecycle_events(venue_id, event_identity)
    WHERE event_identity IS NOT NULL;
