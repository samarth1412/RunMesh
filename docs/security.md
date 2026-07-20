# Security model

Production mode verifies OIDC signatures, issuer, audience, and expiry against the configured JWKS. A signed tenant claim and subject must match a pre-provisioned PostgreSQL user; RunMesh reads the role from that row rather than trusting a role supplied by the token. Roles are ordered `viewer < operator < developer < admin`.

Automation uses tenant-scoped `rm_<id>_<secret>` API keys. Secrets are displayed only at creation or rotation and are stored as SHA-256 hashes bound to an application pepper. API-key scopes are checked per endpoint, and expired or revoked keys fail authentication. Production workers require the `workers:execute` scope and every worker operation verifies that the task belongs to the key's tenant.

Compose imports a local Keycloak realm and uses the same JWT verification path as production. The legacy shared worker token is accepted only when development auth is explicitly enabled. Database queries remain tenant-scoped after authentication. The API caps request size, DAG node/edge counts, and handler/name lengths. Audit events are append-only, including API-key creation, rotation, and revocation.

Containers run as non-root. Helm templates include restricted pod security contexts and default-deny network policy. Secrets enter through environment-backed Kubernetes Secrets or a cloud secret manager; they are never committed.
