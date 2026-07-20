# Chaos exercises

Run these only against disposable environments.

- Kill a worker during a 60-second handler and assert the task returns to `READY` within two lease intervals.
- Put Toxiproxy between scheduler and Redpanda, add 5 seconds of latency, and assert outbox depth rises then drains.
- Duplicate a `task.dispatch` message and assert only one lease transition succeeds.
- Stop Redis and confirm workflow state continues while rate limiting fails closed in production mode.
- Delay MinIO uploads past the handler timeout and verify retry exhaustion preserves every attempt.

Each exercise should capture the run ID, task attempt rows, outbox rows, Prometheus snapshot, and exact container image SHAs.
