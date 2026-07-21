# Contributing to RunMesh

Thank you for helping improve RunMesh. Changes should preserve tenant isolation,
at-least-once delivery, per-workflow ordering, and backward-compatible protobuf
evolution.

## Local setup

The shortest path to a complete development environment is Docker Engine or
Docker Desktop with Docker Compose v2:

```bash
git clone https://github.com/samarth1412/RunMesh.git
cd RunMesh
docker compose up --build -d
docker compose ps
bash tests/e2e/smoke.sh
```

For development outside containers, install Go 1.25, Python 3.11 or newer, and
Node.js 22. Then install the Python and web dependencies:

```bash
python3 -m pip install -e 'sdk/python[dev]'
npm --prefix web ci
```

## Testing

Run focused tests while developing and the relevant full suite before opening a
pull request:

```bash
gofmt -w cmd internal sdk/go gen tests/integration
go vet ./...
go test -race ./cmd/... ./internal/... ./sdk/go/...
go test -coverprofile=workflow-coverage.out ./internal/workflow
scripts/check-go-coverage.sh workflow-coverage.out 70

ruff check sdk/python examples
(cd sdk/python && mypy runmesh && pytest) # includes a 40% worker-SDK floor

npm --prefix web run lint
npm --prefix web run build
npm --prefix web run test:coverage

buf lint
go test -tags=integration -count=1 -timeout=20m ./tests/integration
```

Integration tests use Docker through Testcontainers. The browser suite expects
the Compose stack:

```bash
docker compose up -d --build
npm --prefix web run test:e2e
```

CI additionally checks protobuf compatibility, container vulnerabilities,
Terraform, Helm, rendered Kubernetes resources, the Compose smoke path, and a
kind rolling upgrade.

## Protobuf changes

Only make additive, backward-compatible changes to `proto/`. Regenerate both Go
and Python clients and commit them in the same pull request:

```bash
buf generate
buf lint
buf breaking --against '.git#branch=origin/main'
```

If `buf` is not installed locally, use the pinned container:

```bash
docker run --rm -v "$PWD:/workspace" -w /workspace \
  bufbuild/buf:1.47.2 generate
```

## Database migrations

Schema changes require paired, reversible migrations. Create the next numbered
`.up.sql` and `.down.sql` files under `migrations/`, use idempotent DDL where it
is safe, and test both directions against disposable data. Apply pending local
migrations with:

```bash
make migrate
```

Never edit a migration that may already have been applied; add a new migration.

## Benchmarks and failure tests

Performance claims require raw, machine-readable evidence tied to a source
commit. Start from a disposable Compose environment and run:

```bash
tests/benchmarks/run-local.sh tests/benchmarks/results/YYYY-MM-DDTHHMMSSZ
```

The benchmark driver starts the local stack, stops schedulers, scales and terminates workers, and writes
into a new result directory. It refuses to overwrite existing evidence. Do not point it at shared or production
infrastructure. Do not hand-edit raw output. Record failed runs honestly, and
distinguish API throughput, scheduling throughput, and end-to-end completion
throughput. See [`docs/benchmarks.md`](docs/benchmarks.md).

## Pull requests

- Keep changes focused and explain the behavior or failure mode being changed.
- Add tests for new state transitions, authorization boundaries, and recovery
  behavior.
- Update OpenAPI, protobuf clients, deployment configuration, and documentation
  with the behavior they describe.
- Include exact validation commands and results. Do not describe a partial test
  run as a full validation.
- Do not include secrets, local environment files, generated credentials, or
  unreviewed benchmark output.
- Preserve existing commit attribution and do not rewrite shared history.
