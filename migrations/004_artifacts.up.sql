CREATE TYPE artifact_kind AS ENUM ('input', 'output', 'log');
CREATE TYPE artifact_status AS ENUM ('PENDING', 'READY');

CREATE TABLE artifacts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  kind artifact_kind NOT NULL,
  status artifact_status NOT NULL DEFAULT 'PENDING',
  object_key TEXT NOT NULL UNIQUE,
  object_uri TEXT NOT NULL UNIQUE,
  content_type TEXT NOT NULL,
  declared_size BIGINT NOT NULL CHECK (declared_size >= 0),
  size_bytes BIGINT,
  checksum_sha256 TEXT,
  workflow_run_id UUID REFERENCES workflow_runs(id) ON DELETE SET NULL,
  task_run_id UUID REFERENCES task_runs(id) ON DELETE SET NULL,
  created_by UUID REFERENCES users(id),
  ready_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX artifacts_tenant_created_idx ON artifacts (tenant_id, created_at DESC);
CREATE INDEX artifacts_workflow_idx ON artifacts (workflow_run_id) WHERE workflow_run_id IS NOT NULL;
ALTER TABLE task_attempts ADD COLUMN log_artifact_uri TEXT;
