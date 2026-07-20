DELETE FROM api_keys WHERE id = '00000000-0000-0000-0000-000000000002';
UPDATE users SET oidc_subject = 'local-admin' WHERE id = '00000000-0000-0000-0000-000000000001';

ALTER TABLE worker_heartbeats DROP CONSTRAINT worker_heartbeats_pkey;
ALTER TABLE worker_heartbeats DROP COLUMN tenant_id;
ALTER TABLE worker_heartbeats ADD PRIMARY KEY (worker_id);

ALTER TABLE api_keys
  DROP COLUMN last_used_at,
  DROP COLUMN created_by;
