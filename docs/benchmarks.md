# Benchmarks

RunMesh keeps dated, reproducible benchmark evidence in the repository. Results are
reported with their source commit, hardware, image digests, commands, and raw
machine-readable output; they are evidence from that environment rather than a
universal capacity claim.

## Latest recorded run

The [2026-07-21T002100Z local run](../tests/benchmarks/results/2026-07-21T002100Z/README.md)
used an Apple M4 MacBook Air with 16 GiB RAM and Docker Desktop 4.82.0.

| Scenario | Result |
| --- | ---: |
| Authenticated API reads | 100.023 requests/s; 1.416/3.228/5.745 ms p50/p95/p99; 0 failures |
| Workflow submissions | 50.026 requests/s; 1.560/3.236/13.553 ms p50/p95/p99; 0 failures |
| Simultaneously active workflows | 1,000 created in 20.200 seconds; 0 failures |
| Queued end-to-end completion | 1,000 tasks at 30.747 tasks/s |
| Scheduler dispatch | 412.482 tasks/s across 10,000 tasks |
| Worker termination and lease recovery | 20 workers terminated; 10,000/10,000 succeeded; 6 recovered; 0 lost |
| Duplicate submission and delivery | Passed |

The run used two scheduler replicas, a six-partition local Redpanda topic, and
12 workers for the queued completion phase. The termination scenario killed
all 20 Go-worker containers while six tasks were actively running. Its complete
drain took 202.867 seconds. The six lease-expiration-to-next-attempt samples
were 125.145/167.824/168.519 seconds p50/p95/p99; the backlog makes this a
correctness result and exposes a recovery-latency optimization opportunity.

All 15 blocking verification checks passed. New records appeared on every
Kafka partition, duplicate submission returned the same run while preserving
the original input, and the duplicate-delivery integration test permitted only
one successful lease.

## Reproduce locally

With the Compose stack running, execute:

```bash
tests/benchmarks/run-local.sh
```

The driver exercises authenticated API load, 1,000 active workflows, scheduler
dispatch, 10,000-task worker termination and lease recovery, duplicate submission,
and duplicate Kafka delivery. Its verifier treats task loss and isolation failures
as blocking while reporting performance values honestly.

For focused k6 load generation, use the scripts under [`tests/load`](../tests/load):

```bash
k6 run tests/load/api-read.js
k6 run tests/load/submissions.js
```
