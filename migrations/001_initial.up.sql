CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TYPE user_role AS ENUM ('admin', 'developer', 'operator', 'viewer');
CREATE TYPE workflow_status AS ENUM ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED');
CREATE TYPE task_status AS ENUM ('BLOCKED', 'READY', 'DISPATCHING', 'LEASED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'RETRY_WAIT', 'DEAD', 'CANCELLED');

CREATE TABLE tenants (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  plan TEXT NOT NULL DEFAULT 'development',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE users (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  oidc_subject TEXT NOT NULL,
  email TEXT NOT NULL,
  role user_role NOT NULL DEFAULT 'viewer',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, oidc_subject)
);

CREATE TABLE api_keys (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  name TEXT NOT NULL,
  key_hash BYTEA NOT NULL UNIQUE,
  scopes TEXT[] NOT NULL DEFAULT '{}',
  expires_at TIMESTAMPTZ,
  revoked_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE workflow_definitions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  name TEXT NOT NULL,
  version INTEGER NOT NULL CHECK (version > 0),
  dag_spec JSONB NOT NULL,
  created_by UUID REFERENCES users(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, name, version)
);
CREATE INDEX workflow_definitions_tenant_name_idx ON workflow_definitions (tenant_id, name, version DESC);

CREATE TABLE workflow_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  workflow_definition_id UUID NOT NULL REFERENCES workflow_definitions(id),
  workflow_version INTEGER NOT NULL,
  status workflow_status NOT NULL DEFAULT 'PENDING',
  input JSONB NOT NULL DEFAULT '{}',
  input_artifact_uri TEXT,
  idempotency_key TEXT NOT NULL,
  trace_parent TEXT,
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, idempotency_key)
);
CREATE INDEX workflow_runs_tenant_created_idx ON workflow_runs (tenant_id, created_at DESC);

CREATE TABLE task_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workflow_run_id UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
  task_key TEXT NOT NULL,
  handler TEXT NOT NULL,
  status task_status NOT NULL,
  priority INTEGER NOT NULL DEFAULT 0,
  available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  attempt_count INTEGER NOT NULL DEFAULT 0,
  maximum_attempts INTEGER NOT NULL DEFAULT 3 CHECK (maximum_attempts > 0),
  timeout_seconds INTEGER NOT NULL DEFAULT 300 CHECK (timeout_seconds > 0),
  lease_owner TEXT,
  lease_expires_at TIMESTAMPTZ,
  input JSONB NOT NULL DEFAULT '{}',
  output JSONB,
  output_artifact_uri TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (workflow_run_id, task_key)
);
CREATE INDEX task_claim_idx ON task_runs (status, available_at, priority DESC);
CREATE INDEX task_lease_expiry_idx ON task_runs (lease_expires_at) WHERE status IN ('LEASED', 'RUNNING');

CREATE TABLE task_dependencies (
  workflow_run_id UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
  task_run_id UUID NOT NULL REFERENCES task_runs(id) ON DELETE CASCADE,
  depends_on_task_run_id UUID NOT NULL REFERENCES task_runs(id) ON DELETE CASCADE,
  PRIMARY KEY (task_run_id, depends_on_task_run_id),
  CHECK (task_run_id <> depends_on_task_run_id)
);
CREATE INDEX task_dependencies_upstream_idx ON task_dependencies (depends_on_task_run_id);

CREATE TABLE task_attempts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  task_run_id UUID NOT NULL REFERENCES task_runs(id) ON DELETE CASCADE,
  attempt_number INTEGER NOT NULL,
  worker_id TEXT NOT NULL,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ended_at TIMESTAMPTZ,
  exit_status TEXT,
  error_type TEXT,
  error_message TEXT,
  trace_id TEXT,
  artifact_uri TEXT,
  UNIQUE (task_run_id, attempt_number)
);

CREATE TABLE outbox_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  aggregate_type TEXT NOT NULL,
  aggregate_id UUID NOT NULL,
  event_type TEXT NOT NULL,
  payload JSONB NOT NULL,
  trace_parent TEXT,
  published_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX outbox_unpublished_idx ON outbox_events (created_at) WHERE published_at IS NULL;

CREATE TABLE schedules (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  workflow_definition_id UUID NOT NULL REFERENCES workflow_definitions(id),
  cron_expression TEXT NOT NULL,
  timezone TEXT NOT NULL DEFAULT 'UTC',
  next_execution_at TIMESTAMPTZ NOT NULL,
  misfire_policy TEXT NOT NULL DEFAULT 'skip' CHECK (misfire_policy IN ('skip', 'catch_up_once')),
  enabled BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX schedules_due_idx ON schedules (next_execution_at) WHERE enabled;

CREATE TABLE audit_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  actor_id UUID,
  action TEXT NOT NULL,
  resource_type TEXT NOT NULL,
  resource_id UUID,
  metadata JSONB NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX audit_events_tenant_created_idx ON audit_events (tenant_id, created_at DESC);

CREATE TABLE worker_heartbeats (
  worker_id TEXT PRIMARY KEY,
  handlers TEXT[] NOT NULL DEFAULT '{}',
  active_tasks INTEGER NOT NULL DEFAULT 0,
  metadata JSONB NOT NULL DEFAULT '{}',
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
