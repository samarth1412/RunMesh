DROP TABLE IF EXISTS worker_heartbeats, audit_events, schedules, outbox_events, task_attempts, task_dependencies, task_runs, workflow_runs, workflow_definitions, api_keys, users, tenants CASCADE;
DROP TYPE IF EXISTS task_status, workflow_status, user_role;
