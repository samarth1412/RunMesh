# Security model

Production mode requires an authenticated OIDC identity and derives tenant, user, role, and scopes from verified claims. Roles are ordered `viewer < operator < developer < admin`; endpoints declare a minimum role. API keys are displayed once and stored only as SHA-256 hashes with an application pepper.

Local development auth is intentionally explicit and must be disabled outside Compose. Database queries remain tenant-scoped even after authentication. The API caps request size, DAG node/edge counts, and handler/name lengths. Audit events are append-only.

Containers run as non-root. Helm templates include restricted pod security contexts and default-deny network policy. Secrets enter through environment-backed Kubernetes Secrets or a cloud secret manager; they are never committed.
