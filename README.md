# RunMesh

RunMesh is a multi-tenant, failure-tolerant workflow orchestrator. It stores versioned DAGs, turns workflow runs into dependency-aware task runs, and delivers those tasks with **at-least-once** semantics. PostgreSQL is authoritative; Kafka/Redpanda is the durable delivery channel, and a transactional outbox bridges the two.

## Run locally

```bash
docker compose up --build
```

Then open:

- Dashboard: http://localhost:3000
- API and OpenAPI UI: http://localhost:8080/docs
- Worker gRPC endpoint: localhost:7001
- Grafana: http://localhost:3001 (`admin` / `runmesh`)
- Redpanda Console: http://localhost:8081

Requests are scoped to the authenticated tenant. Production configuration rejects development auth and requires a verified OIDC token or scoped API key.

Large inputs, outputs, and structured task logs use tenant-owned S3/MinIO artifacts with short-lived presigned transfers; see [artifact handling](docs/artifacts.md).

The dashboard signs in through the bundled Keycloak realm. Use `admin` / `runmesh`. The control plane verifies the resulting JWT and resolves tenant membership and role from PostgreSQL.

Dashboard state refreshes immediately from the authenticated, tenant-scoped `/v1/stream` WebSocket. Notifications are intentionally non-durable: clients re-read PostgreSQL-backed API state after each event and retain a 15-second polling fallback while Redis or the connection is unavailable.

Prometheus scrapes the control plane, scheduler, OpenTelemetry collector, and Redpanda. Grafana provisions platform overview, scheduler, worker, Kafka lag, workflow failure, and tenant usage dashboards plus actionable alerts from `deploy/docker/prometheus-rules.yaml`. Application logs are JSON and include service, tenant, workflow/task, trace, and span identifiers when available.

## Quick start

```bash
TOKEN="$(curl -sS http://localhost:8180/realms/runmesh/protocol/openid-connect/token \
  -d grant_type=password -d client_id=runmesh-web -d username=admin -d password=runmesh \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')"

curl -sS http://localhost:8080/v1/workflows \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"name":"hello","tasks":{"greet":{"handler":"examples.greet"}}}'

curl -sS http://localhost:8080/v1/workflows/<workflow-id>/runs \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: first-run' \
  -d '{"input":{"name":"Ada"}}'
```

The bundled example Python worker handles `examples.greet`, `examples.upper`, and `examples.notify`.

## Guarantees

- At-least-once task delivery; handlers must be idempotent.
- Conditional state transitions and expiring leases fence stale workers.
- Per-run event ordering through Kafka partition keys.
- Idempotent workflow submission within a tenant.
- PostgreSQL remains the source of truth through broker failures.
- Redis enforces an atomic per-tenant API token bucket and fails closed in production without entering the execution path.
- Every attempt is retained, including errors and trace IDs.

See [docs/delivery-semantics.md](docs/delivery-semantics.md) and [docs/architecture.md](docs/architecture.md).

Production Helm, AWS validation, release images, and rolling-upgrade instructions are in [docs/deployment.md](docs/deployment.md).

## Repository map

- `cmd/control-plane`: public REST API and worker callbacks
- `cmd/scheduler`: ready-task claims, lease recovery, and outbox publishing
- `cmd/worker-go`: protocol interoperability example
- `internal`: domain, PostgreSQL, scheduling, messaging, auth, telemetry
- `sdk/python`: public Python worker SDK
- `proto`: language-neutral worker contract
- `web`: React operations dashboard
- `deploy`, `infra`: Compose, Helm/Kubernetes, and AWS Terraform
