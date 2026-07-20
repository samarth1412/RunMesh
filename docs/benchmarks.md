# Benchmarks

RunMesh keeps dated, reproducible benchmark evidence in the repository. Results are
reported with their source commit, hardware, image digests, commands, and raw
machine-readable output; they are evidence from that environment rather than a
universal capacity claim.

## Latest recorded run

The [2026-07-20 local run](../tests/benchmarks/results/2026-07-20/README.md)
used an Apple M4 MacBook Air with 16 GiB RAM and Docker Desktop 4.82.0.

| Scenario | Result |
| --- | ---: |
| Authenticated API load | 100.017 requests/second, 4.316 ms p95, 0 failures |
| Simultaneously active workflows | 1,000 |
| Scheduler dispatch | 196.078 tasks/second across 10,000 tasks |
| Worker termination and lease recovery | 10,000/10,000 succeeded, 0 lost |
| Duplicate submission and delivery | Passed |

The local Redpanda topology used one partition, so worker consumption was serial.
The dispatch result met the target; the worker-termination run is reported as
correctness and recovery evidence, not as a throughput claim.

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
