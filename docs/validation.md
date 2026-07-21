# Local validation report

This report records the checks run while preparing the `v0.1.0` candidate on
2026-07-20/21. They ran on the Apple M4 / 16 GiB Docker Desktop environment
captured in the latest benchmark directory. GitHub-hosted Actions, CodeQL, a
published release, and real AWS infrastructure were **not** run or created.

## Test and static-analysis ledger

| Area | Command | Result |
| --- | --- | --- |
| Go formatting | `test -z "$(gofmt -l cmd internal sdk/go gen tests/integration)"` | Passed |
| Go analysis | `go vet ./...` (Go 1.25 container) | Passed |
| Go race suite | `go test -race -covermode=atomic -coverprofile=coverage.out ./cmd/... ./internal/... ./sdk/go/...` | Passed; 11.8% repository coverage |
| Workflow coverage | `go test -coverprofile=workflow-coverage.out ./internal/workflow` | Passed; 73.6% (70% gate) |
| Authentication coverage | `go test -coverprofile=auth-coverage.out ./internal/auth` | Passed; 40.6% (40% gate) |
| Live-event coverage | `go test -coverprofile=live-coverage.out ./internal/live` | Passed; 73.5% (70% gate) |
| Go integration/chaos | `go test -tags=integration -count=1 -timeout=15m ./tests/integration` with Docker socket and Testcontainers host override | Passed in 93.547 seconds |
| Python lint | `ruff check sdk/python examples` | Passed |
| Python types/tests | `cd sdk/python && mypy runmesh && pytest` | Passed; 6 tests, 40.56% coverage |
| Dashboard lint/build | `npm --prefix web run lint && npm --prefix web run build` | Passed |
| Dashboard tests | `npm --prefix web run test:coverage` | Passed; 5 tests; 67.94% statements, 52.14% branches, 58.24% functions, 81.37% lines |
| Dashboard dependencies | `npm --prefix web audit --audit-level=high` | Passed; 0 vulnerabilities |
| Browser E2E | `npm --prefix web run test:e2e` against the real Compose services | Passed; 4 Playwright tests in 11.4 seconds |
| Compose smoke | `bash tests/e2e/smoke.sh` | Passed |
| Protobuf | `buf lint` | Passed |
| GitHub workflow syntax | `actionlint` | Passed |
| Helm | `helm lint deploy/helm/runmesh ...` | Passed |
| Rendered manifests | `helm template ... \| kubeconform -strict -summary -kubernetes-version 1.31.0` | Passed; 24 resources valid |
| Terraform | `terraform -chdir=infra/terraform fmt -check -recursive`, `init -backend=false`, `validate`, and `test` using Terraform 1.10.5 | Passed; 1 mock-provider test, 0 failed; no apply |
| Rolling upgrade | `tests/deployment/kind-rolling-upgrade.sh` | Passed; workflow `93bf8f7f-af7e-4135-9f0c-85090b1ad949` completed through the local kind rollout |
| Secret scan | `gitleaks git --staged --redact --no-banner` | Passed before the implementation commit; repeated before the evidence commit |
| Container builds | Compose builds for control plane, scheduler, Go worker, Python worker, and web | Passed |
| Container vulnerabilities | `trivy image --scanners vuln --severity CRITICAL --ignore-unfixed --exit-code 1` for all five images | Passed after patching the dashboard runtime; 0 applicable critical findings |
| Local benchmark | `tests/benchmarks/run-local.sh <dated-result-directory>` | Blocking verifier passed; see [benchmark evidence](benchmarks.md) |

The initial dashboard vulnerability scan failed on two fixed OpenSSL findings in
the Nginx/Alpine runtime. The runtime was upgraded and its OpenSSL packages were
patched during the image build; the rebuilt image then passed. An initial
benchmark attempt also stopped during dependency readiness, and an earlier
complete run omitted p99 from its k6 summaries. Both issues were corrected
before the headline benchmark was recorded. Superseded raw runs are retained
and labelled instead of being represented as final evidence.

## What the automated suites prove

- Scheduler replicas use PostgreSQL `FOR UPDATE SKIP LOCKED` claims without
  double-claiming work.
- Kafka publication occurs outside the database claim transaction, recovers
  expired publisher claims, and tolerates duplicates around uncertain broker
  acknowledgement without losing outbox events.
- Per-run Kafka keys preserve ordering, and the local six-partition topology is
  exercised by multiple scheduler and worker replicas.
- Active lease, worker identity, tenant ownership, retry, cancellation, and
  stale-worker fencing conditions guard worker mutations.
- OIDC/API-key authentication, scopes, revocation, expiry, rate-limit isolation,
  artifact ownership/limits, and cross-tenant denial paths are covered.
- Redis, PostgreSQL, Kafka, duplicate-delivery, slow-artifact, and multi-worker
  termination recovery paths have Docker-backed coverage.

## Deliberately unverified here

- GitHub-hosted Actions and CodeQL must run after the branch is pushed. Local
  equivalents passed, but this report does not call remote CI green.
- AWS Terraform was validated and mock-tested only. It was not applied.
- Multi-architecture images, SBOMs, provenance, GHCR publication, and the
  `v0.1.0` release remain tag-triggered release work and were not published.
- Performance evidence is local Docker Desktop evidence, not production
  capacity or a service-level objective.
