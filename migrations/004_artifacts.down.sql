ALTER TABLE task_attempts DROP COLUMN IF EXISTS log_artifact_uri;
DROP TABLE IF EXISTS artifacts;
DROP TYPE IF EXISTS artifact_status;
DROP TYPE IF EXISTS artifact_kind;
