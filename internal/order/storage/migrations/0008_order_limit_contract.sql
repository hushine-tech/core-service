ALTER TABLE order_intents
    DROP CONSTRAINT IF EXISTS chk_order_intents_order_type;

ALTER TABLE order_intents
    ADD CONSTRAINT chk_order_intents_order_type CHECK (order_type IN (1, 2));
