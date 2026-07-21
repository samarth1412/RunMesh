# Benchmarks

RunMesh keeps dated, reproducible benchmark evidence in the repository. Results are
reported with their source commit, hardware, image digests, commands, and raw
machine-readable output; they are evidence from that environment rather than a
universal capacity claim.

## Latest recorded run (canonical)

The [2026-07-21T003600Z local run](../tests/benchmarks/results/2026-07-21T003600Z/README.md)
used an Apple M4 MacBook Air with 16 GiB RAM and Docker Desktop 4.82.0, against
source commit `c4b248638f2ae36c4a7bbaf48df37ee59ac1bf74`. It is the canonical
run: it postdates a fresh-stack Kafka/Redpanda topic-initialization fix in
`compose.yaml` that earlier runs (including 2026-07-21T002100Z) predate.

| Scenario | Result |
| --- | ---: |
| Authenticated API reads | 100.0 requests/s; 1.701/3.307/4.657 ms p50/p95/p99; 0 failures |
| Workflow submissions | 50.028 requests/s; 1.587/7.012/220.024 ms p50/p95/p99; 0 failures |
| Simultaneously active workflows | 1,000 created in 18.224 seconds; 0 failures |
| Queued end-to-end completion | 1,000 tasks at 57.608 tasks/s |
| Scheduler dispatch | 407.403 tasks/s across 10,000 tasks |
| Worker termination and lease recovery | 20 workers terminated; 10,000/10,000 succeeded; 7 recovered; 0 lost |
| Duplicate submission and delivery | Passed |

The run used two scheduler replicas, a six-partition local Redpanda topic, and
12 workers for the queued completion phase. The termination scenario killed
all 20 Go-worker containers while seven tasks were actively running (see the
[run's README](../tests/benchmarks/results/2026-07-21T003600Z/README.md) for a
note on a benign 6-vs-7 recording-timing discrepancy between two evidence
files). Its complete drain took 185.971 seconds. The seven
lease-expiration-to-next-attempt samples were
132.797/140.148/142.414 seconds p50/p95/p99; the backlog makes this a
correctness result and exposes a recovery-latency optimization opportunity.

All 15 blocking verification checks passed. New records appeared on every
Kafka partition, duplicate submission returned the same run while preserving
the original input, and the duplicate-delivery integration test permitted only
one successful lease.

Superseded raw runs remain in [`tests/benchmarks/results`](../tests/benchmarks/results/)
for history, each labelled with why it was superseded.

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
