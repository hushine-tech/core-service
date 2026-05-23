-- order_fills 加 session_id 关联到策略运行记录
ALTER TABLE order_fills ADD COLUMN IF NOT EXISTS session_id TEXT NULL;
