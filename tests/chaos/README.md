# Chaos validation

Run these only against disposable environments.

The Docker-backed integration suite automates the correctness and outage matrix:

| Failure | Automated proof |
| --- | --- |
| Worker termination and lease expiry | `TestWorkerCrashLeaseRecovery` |
| PostgreSQL restart and pool recovery | `TestPostgresRestartPreservesWorkflowAndPoolRecovery` |
| Kafka latency and durable outbox drain | `TestTransactionalOutboxRecoversAfterKafkaLatency` |
| Duplicate Kafka delivery | `TestDuplicateDispatchIsLeasedOnce` |
| Redis outage and automatic recovery | `TestTenantRateLimitAndRedisOutageRecovery` |
| Slow MinIO upload and attempt preservation | `TestArtifactOwnershipPresigningAndSlowUploadRecovery` |

Run the matrix with:

```sh
go test -tags=integration -count=1 -timeout=10m ./tests/integration
```

The local 10,000-task worker-termination exercise and the benchmark evidence
capture are automated by `tests/benchmarks/run-local.sh`. Each run records the
commit SHA, host details, container image digests, raw results, and zero-loss
counts. Performance targets are report-only; task loss or isolation failures are
blocking.

Each exercise should capture the run ID, task attempt rows, outbox rows, Prometheus snapshot, and exact container image SHAs.
