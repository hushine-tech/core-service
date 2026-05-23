DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'strategy_sessions'
          AND column_name = 'runtime_service_name'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'strategy_sessions'
          AND column_name = 'runtime_name'
    ) THEN
        ALTER TABLE strategy_sessions RENAME COLUMN runtime_service_name TO runtime_name;
    END IF;
END $$;
