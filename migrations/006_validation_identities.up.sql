-- Stable local identities used by browser and cross-tenant security validation.
-- Production installations may remove these identities after bootstrapping their
-- own pre-provisioned OIDC subjects.
INSERT INTO tenants (id, name, plan)
VALUES ('00000000-0000-0000-0000-000000000010', 'Validation Tenant', 'development')
ON CONFLICT DO NOTHING;

INSERT INTO users (id, tenant_id, oidc_subject, email, role)
VALUES
  ('00000000-0000-0000-0000-000000000010', '00000000-0000-0000-0000-000000000010', '22222222-2222-2222-2222-222222222222', 'tenant-admin@runmesh.local', 'admin'),
  ('00000000-0000-0000-0000-000000000011', '00000000-0000-0000-0000-000000000001', '33333333-3333-3333-3333-333333333333', 'viewer@runmesh.local', 'viewer')
ON CONFLICT DO NOTHING;
