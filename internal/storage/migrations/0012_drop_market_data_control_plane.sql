-- Phase D2: market-data control plane moved to control-panel-service.
--
-- The four market_data_* tables were re-created in `control-panel-service/
-- internal/storage/migrations/0003-0006_*.sql`, and the operator-driven
-- migration tool (`control-panel-service/scripts/migrate_market_data`)
-- copied existing rows from this database into `control_panel.market_data_*`
-- with the destination's BIGSERIAL sequences resynced to MAX(<pk>).
--
-- This migration drops the source tables. CASCADE is used because:
--   * market_data_streams is referenced by market_data_requests.stream_id
--     and market_data_leases.stream_id (both ON DELETE CASCADE in the
--     original schema, but explicit CASCADE here keeps the order
--     declaration-independent).
--   * market_data_history_requests does not reference any other table but
--     is grouped here for one atomic cleanup.
--
-- Rollback: re-apply migrations 0009 + 0010 from this directory; restore
-- rows from the operator's pre-D2 `pg_dump`. The cutover is one-way under
-- normal operation — there is no in-product rollback path.
DROP TABLE IF EXISTS market_data_history_requests CASCADE;
DROP TABLE IF EXISTS market_data_leases             CASCADE;
DROP TABLE IF EXISTS market_data_requests           CASCADE;
DROP TABLE IF EXISTS market_data_streams            CASCADE;
