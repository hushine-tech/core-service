-- Phase C fix: promote reconciliation_runs PK from (time, account_id) to
-- (time, run_id) so two compare goroutines that happen to stamp the same
-- microsecond for the same account can't collide on the primary key.
--
-- Idempotent: runs safely on fresh databases (0007 already carries the new
-- schema) and on existing databases (ALTER only fires when the old PK is
-- still present).

CREATE EXTENSION IF NOT EXISTS pgcrypto;

ALTER TABLE reconciliation_runs
    ADD COLUMN IF NOT EXISTS run_id UUID NOT NULL DEFAULT gen_random_uuid();

DO $$
DECLARE
    pk_name TEXT;
    pk_cols TEXT;
BEGIN
    SELECT tc.constraint_name,
           string_agg(kc.column_name, ',' ORDER BY kc.ordinal_position)
      INTO pk_name, pk_cols
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kc
        ON tc.constraint_name = kc.constraint_name
       AND tc.table_schema    = kc.table_schema
     WHERE tc.table_name      = 'reconciliation_runs'
       AND tc.constraint_type = 'PRIMARY KEY'
     GROUP BY tc.constraint_name;

    IF pk_cols = 'time,account_id' THEN
        EXECUTE format('ALTER TABLE reconciliation_runs DROP CONSTRAINT %I', pk_name);
        ALTER TABLE reconciliation_runs ADD PRIMARY KEY (time, run_id);
    END IF;
END;
$$;
