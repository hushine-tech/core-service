ALTER TABLE order_attempts
    DROP CONSTRAINT IF EXISTS chk_order_attempts_status;

ALTER TABLE order_attempts
    ADD CONSTRAINT chk_order_attempts_status
    CHECK (status IN (1, 2, 3, 4, 5, 6, 7, 8));
