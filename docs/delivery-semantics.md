# Delivery semantics

RunMesh promises **at-least-once**, not exactly-once, task execution.

1. A state transaction moves a task from `READY` to `DISPATCHING` and appends an outbox event.
2. A publisher atomically claims the oldest unpublished event for that workflow, commits the short claim, and sends it to Kafka outside a database transaction using the workflow-run ID as the partition key.
3. It conditionally records `published_at`. A crash after publish but before that acknowledgement can publish a duplicate.
4. A worker leases with a conditional transition. A duplicate cannot acquire an already owned, running, or terminal task.
5. If a worker runs user code and dies before completion is recorded, its lease expires and the task is delivered again.

Handlers should use `context.idempotency_key`, normally the task-run ID, for side effects. The database retains each attempt. A stale worker cannot complete a task after its lease has been reassigned because completion is fenced by `lease_owner` and state.

Events are ordered within a workflow run because all of its messages share a Kafka partition key. There is no global order across runs.

Claims expire so another publisher can recover abandoned work. The publish timeout must remain shorter than the claim TTL; otherwise two publishers could concurrently send the same ordering key while the first call is still running.

Workers in one Kafka consumer group must advertise the same handler set because Kafka assigns partitions, not individual handlers. Use a separate group ID for a heterogeneous capability pool; each group sees the dispatch, unsupported pools leave it untouched, and the database lease selects the single executor. The bundled Go and Python workers intentionally implement the same example handlers and share `runmesh-workers`.
