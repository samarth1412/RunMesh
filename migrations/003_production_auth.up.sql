ALTER TABLE api_keys
  ADD COLUMN created_by UUID REFERENCES users(id),
  ADD COLUMN last_used_at TIMESTAMPTZ;

DELETE FROM worker_heartbeats;
ALTER TABLE worker_heartbeats DROP CONSTRAINT worker_heartbeats_pkey;
ALTER TABLE worker_heartbeats
  ADD COLUMN tenant_id UUID NOT NULL REFERENCES tenants(id);
ALTER TABLE worker_heartbeats
  ADD PRIMARY KEY (tenant_id, worker_id);

UPDATE users
SET oidc_subject = '11111111-1111-1111-1111-111111111111'
WHERE id = '00000000-0000-0000-0000-000000000001';

INSERT INTO api_keys(id,tenant_id,name,key_hash,scopes,created_by)
VALUES (
  '00000000-0000-0000-0000-000000000002',
  '00000000-0000-0000-0000-000000000001',
  'Local worker',
  digest(convert_to('local-development-pepper','UTF8') || decode('00','hex') || convert_to('rm_00000000-0000-0000-0000-000000000002_local-development-worker-key','UTF8'),'sha256'),
  ARRAY['workers:execute'],
  '00000000-0000-0000-0000-000000000001'
) ON CONFLICT (id) DO NOTHING;
