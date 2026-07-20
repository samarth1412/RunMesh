INSERT INTO tenants (id, name, plan)
VALUES ('00000000-0000-0000-0000-000000000001', 'Local Development', 'development')
ON CONFLICT DO NOTHING;

INSERT INTO users (id, tenant_id, oidc_subject, email, role)
VALUES (
  '00000000-0000-0000-0000-000000000001',
  '00000000-0000-0000-0000-000000000001',
  'local-admin',
  'admin@runmesh.local',
  'admin'
)
ON CONFLICT DO NOTHING;
