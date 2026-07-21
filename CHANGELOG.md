# Changelog

All notable changes are documented here. This project follows Semantic Versioning.

## [0.1.0] - Unreleased

### Added

- Multi-tenant workflow and DAG lifecycle APIs with OIDC and scoped API-key authentication.
- Go and Python workers using Kafka delivery, expiring leases, fenced mutations, retries, cancellation, and dead-letter replay.
- S3-compatible input, output, and log artifacts with presigned transfer URLs.
- Per-tenant Redis rate limiting and live WebSocket notifications with a polling fallback.
- Prometheus metrics, OpenTelemetry traces, structured logs, and six Grafana dashboards.
- Docker Compose, Helm, validated AWS Terraform, browser tests, chaos tests, and reproducible local benchmark tooling.

### Reliability

- Transactional outbox publication now claims work in short PostgreSQL transactions and publishes outside row locks.
- Workflow-run Kafka keys preserve ordering across multiple partitions and concurrent publishers.
- Worker operations verify tenant ownership, worker identity, active state, and unexpired leases.
- Fixed Kafka/Redpanda topic initialization on a completely fresh stack, where `rpk topic describe` could report success before the topic actually existed.
- Fixed heterogeneous Go/Python worker capability routing on a shared consumer group.

### Known limitations

- Benchmark evidence is local and does not establish production capacity.
- The Terraform configuration is validated with mock providers but has not been applied to AWS.
- Kafka is externally supplied; the Terraform configuration does not create MSK.
- At-least-once execution cannot make arbitrary handler side effects exactly-once.
- `v0.1.0` images and release artifacts have not been published.
