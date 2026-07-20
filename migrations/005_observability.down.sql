ALTER TABLE task_attempts
  DROP COLUMN IF EXISTS executing_at,
  DROP COLUMN IF EXISTS scheduled_at;
