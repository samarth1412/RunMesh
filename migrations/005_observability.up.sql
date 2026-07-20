ALTER TABLE task_attempts
  ADD COLUMN scheduled_at TIMESTAMPTZ DEFAULT now(),
  ADD COLUMN executing_at TIMESTAMPTZ;

UPDATE task_attempts
SET scheduled_at = started_at,
    executing_at = started_at;

ALTER TABLE task_attempts
  ALTER COLUMN scheduled_at SET NOT NULL;
