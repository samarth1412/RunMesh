# Local benchmark evidence — 2026-07-21T003600Z (canonical)

Source commit: `c4b248638f2ae36c4a7bbaf48df37ee59ac1bf74`

This is the canonical benchmark run for the current `HEAD`. It supersedes the
[2026-07-20T235240Z](../2026-07-20T235240Z/README.md) and
[2026-07-21T000619Z](../2026-07-21T000619Z/README.md) runs, which predate a
fresh-stack Kafka/Redpanda topic-initialization fix in `compose.yaml`
(`rpk topic describe` could report success before the topic actually existed
on a brand-new stack). It also supersedes
[2026-07-21T002100Z](../2026-07-21T002100Z/README.md), whose source commit
(`0c48987`) came before that fix. The fix itself only changes Compose
initialization ordering on a first-ever `docker compose up`; it does not
change scheduler, worker, or outbox runtime behavior, but this run was
recorded fresh against corrected `HEAD` rather than reused.

This run was produced on an Apple M4 MacBook Air with 16 GiB RAM using
Docker Desktop 4.82.0. Performance values are report-only local evidence;
correctness, tenant isolation, and zero-loss checks are blocking.

| Scenario | Recorded result |
| --- | --- |
| Authenticated reads at 100 requests/s | 3,000 requests, 0 failures; 1.701/3.307/4.657 ms p50/p95/p99 |
| Workflow submissions at 50 requests/s | 1,501 requests, 0 failures; 1.587/7.012/220.024 ms p50/p95/p99 |
| Simultaneously active workflows | 1,000 created in 18.224 seconds, 0 failures; 5.340/7.700/8.798 ms creation p50/p95/p99 |
| Queued end-to-end completion | 1,000 tasks in 17.359 seconds; 57.608 tasks/s |
| Scheduler dispatch | 10,000 tasks in 24.546 seconds; 407.403 dispatches/s |
| Worker termination and recovery | 20 workers terminated; 10,000/10,000 succeeded; 7 recovered tasks/duplicate attempts; 0 permanent loss; 185.971-second full drain |
| Recovered-attempt delay | 7 samples; 132.797/140.148/142.414 seconds p50/p95/p99 from expired attempt end to next attempt start |
| Duplicate submission | Same run returned with `201` then `200`; original input preserved |
| Duplicate Kafka delivery | `TestDuplicateDispatchIsLeasedOnce` passed |

The topology used two scheduler replicas, six Redpanda partitions, 12 workers
for the 1,000-task completion phase, and 20 Go workers for termination/recovery.
The verifier observed new records on all six partitions and passed all 15
blocking checks. Recovery latency includes queueing behind the 10,000-task
backlog and is reported as a limitation, not a latency target.

`worker-termination.json` records `in_flight_tasks: 6`, a shell-variable
snapshot captured immediately before the four-tasks-running threshold broke
the readiness loop. The subsequent database query in
`in-flight-before-termination.json` (`running_count: 7`) is more precise and
matches the 7 recovered/duplicate attempts in `worker-crash-result.json`.
Both values satisfy the verifier's `>= 4` threshold; this is a benign
recording-timing artifact in the driver script, not a correctness issue, and
is called out here rather than silently reconciled.

Raw evidence in this directory is not manually edited:

- `api-read.json` and `workflow-submissions.json` are k6 summaries.
- `active-workflows*.json`, `end-to-end-completion*.json`, and
  `scheduler-dispatch.json` record workload creation and database outcomes.
- `in-flight-before-termination.json`, `worker-termination.json`,
  `worker-crash-result.json`, and `lease-recovery-distribution.json` record the
  worker-loss experiment.
- `duplicate-submission.json` and `duplicate-delivery-go-test.json` capture the
  two idempotency checks.
- `kafka-topology-*.json`, `compose-topology.json`, `hardware.txt`,
  `image-digests.json`, and `commit-sha.txt` identify the environment.
- `verification.json` is the machine-readable acceptance result, reproducible
  with `python3 tests/benchmarks/verify.py tests/benchmarks/results/2026-07-21T003600Z`.
- `commands.sh` is the exact benchmark driver copied at run start.
