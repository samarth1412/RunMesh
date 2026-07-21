# Local benchmark evidence — 2026-07-21T002100Z

Source commit: `0c48987d5259bcdc62cadaecd163d996ad0af0a4`

This run was produced on an Apple M4 MacBook Air with 16 GiB RAM using
Docker Desktop 4.82.0. Performance values are report-only local evidence;
correctness, tenant isolation, and zero-loss checks are blocking.

| Scenario | Recorded result |
| --- | --- |
| Authenticated reads at 100 requests/s | 3,001 requests, 0 failures; 1.416/3.228/5.745 ms p50/p95/p99 |
| Workflow submissions at 50 requests/s | 1,501 requests, 0 failures; 1.560/3.236/13.553 ms p50/p95/p99 |
| Simultaneously active workflows | 1,000 created in 20.200 seconds, 0 failures; 6.076/8.640/9.736 ms creation p50/p95/p99 |
| Queued end-to-end completion | 1,000 tasks in 32.524 seconds; 30.747 tasks/s |
| Scheduler dispatch | 10,000 tasks in 24.243 seconds; 412.482 dispatches/s |
| Worker termination and recovery | 20 workers terminated with 6 tasks running; 10,000/10,000 succeeded; 6 recovered tasks/duplicate attempts; 0 permanent loss; 202.867-second full drain |
| Recovered-attempt delay | 6 samples; 125.145/167.824/168.519 seconds p50/p95/p99 from expired attempt end to next attempt start |
| Duplicate submission | Same run returned with `201` then `200`; original input preserved |
| Duplicate Kafka delivery | `TestDuplicateDispatchIsLeasedOnce` passed |

The topology used two scheduler replicas, six Redpanda partitions, 12 workers
for the 1,000-task completion phase, and 20 Go workers for termination/recovery.
The verifier observed new records on all six partitions and passed all 15
blocking checks. Recovery latency includes queueing behind the 10,000-task
backlog and is reported as a limitation, not a latency target.

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
- `verification.json` is the machine-readable acceptance result.
- `commands.sh` is the exact benchmark driver copied at run start.
