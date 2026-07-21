# v0.1.0 release notes (draft)

This draft prepares the first RunMesh release. It is not evidence that a tag, GitHub release, container image, or cloud deployment exists.

## What it demonstrates

- Durable, dependency-aware DAG execution backed by PostgreSQL.
- At-least-once Kafka delivery with a transactional outbox and lease-fenced workers.
- Tenant-scoped OIDC and API-key authentication, rate limiting, and artifacts.
- Go and Python worker interoperability and a live React operations dashboard.
- Reproducible integration, chaos, deployment, and local benchmark checks.

## Before publishing

1. Complete a clean-clone Compose run and the validation checklist in `CONTRIBUTING.md`.
2. Run benchmarks against the release candidate and commit their untouched raw output.
3. Confirm CI is green for that commit.
4. Review image names and permissions, then create the `v0.1.0` tag manually.
5. Verify multi-architecture manifests, SBOMs, provenance attestations, and Trivy results.

## Known limitations

See `CHANGELOG.md` and the README limitations section. In particular, AWS has not been applied and benchmark results are local evidence only.

## Suggested GitHub metadata

- Description: `Multi-tenant workflow orchestration with durable DAG execution, Kafka delivery, and lease-based failure recovery.`
- Topics: `distributed-systems`, `workflow-engine`, `golang`, `kafka`,
  `postgresql`, `kubernetes`, `observability`, `react`, `python`
- Social preview: use the real operations-overview screenshot in `docs/demo`
  with a short RunMesh title treatment; do not imply hosted production usage.
