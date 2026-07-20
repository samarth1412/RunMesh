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

The local stack enables a development identity. Requests are scoped to the seeded tenant; production configuration rejects development auth and expects a trusted OIDC proxy/JWT verifier.

## Quick start

```bash
curl -sS http://localhost:8080/v1/workflows \
  -H 'Content-Type: application/json' \
  -d '{"name":"hello","tasks":{"greet":{"handler":"examples.greet"}}}'

curl -sS http://localhost:8080/v1/workflows/<workflow-id>/runs \
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
- Every attempt is retained, including errors and trace IDs.

See [docs/delivery-semantics.md](docs/delivery-semantics.md) and [docs/architecture.md](docs/architecture.md).

## Repository map

- `cmd/control-plane`: public REST API and worker callbacks
- `cmd/scheduler`: ready-task claims, lease recovery, and outbox publishing
- `cmd/worker-go`: protocol interoperability example
- `internal`: domain, PostgreSQL, scheduling, messaging, auth, telemetry
- `sdk/python`: public Python worker SDK
- `proto`: language-neutral worker contract
- `web`: React operations dashboard
- `deploy`, `infra`: Compose, Helm/Kubernetes, and AWS Terraform
