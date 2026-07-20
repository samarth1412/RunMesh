# Local validation results — 2026-07-20

Source commit: `ec94ea25d4c34874486a2a92484965c9bddc85df`

These results were produced on an Apple M4 MacBook Air with 16 GiB RAM using
Docker Desktop 4.82.0. Performance values are report-only; correctness and
zero-loss checks are blocking.

| Scenario | Result |
| --- | --- |
| Authenticated API at 100 requests/second | 3,001 requests, 0 failures, 4.316 ms p95 |
| Simultaneously active workflows | 1,000 workflows and 1,000 tasks active |
| Scheduler dispatch | 10,000 tasks in 51 seconds, 196.078 dispatches/second |
| Worker termination and lease recovery | 20 workers terminated; 10,000/10,000 tasks succeeded; 0 lost; 10,001 attempts; 1 recovered task |
| Duplicate submission | Same run returned with `201` then `200`; original input preserved |
| Duplicate Kafka delivery | `TestDuplicateDispatchIsLeasedOnce` passed |

Raw inputs and outputs are in this directory:

- `api-100-rps.json` is the k6 machine-readable summary.
- `active-workflows*.json` records API creation and the PostgreSQL count.
- `scheduler-dispatch.json` records measured dispatch throughput.
- `worker-termination.json`, `worker-crash-setup.json`, and
  `worker-crash-result.json` record the 10,000-task recovery proof.
- `duplicate-submission.json` and `duplicate-delivery-go-test.json` record both
  idempotency paths.
- `hardware.txt`, `image-digests.json`, and `commit-sha.txt` identify the test
  environment.
- `verification.json` records every blocking evidence gate.
- `commands.sh` is the exact benchmark driver used for this run.

The local Redpanda configuration has one partition, so worker consumption is
serial even with 20 replicas. The dispatch target is nevertheless met, while
the worker-recovery result is reported as correctness evidence rather than a
worker-throughput claim.
