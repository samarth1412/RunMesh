# Delivery semantics

RunMesh promises **at-least-once**, not exactly-once, task execution.

1. A state transaction moves a task from `READY` to `DISPATCHING` and appends an outbox event.
2. The publisher sends the event to Kafka using the workflow-run ID as the partition key.
3. A crash after publish but before recording `published_at` can publish a duplicate.
4. A worker leases with a conditional transition. A duplicate cannot acquire an already owned, running, or terminal task.
5. If a worker runs user code and dies before completion is recorded, its lease expires and the task is delivered again.

Handlers should use `context.idempotency_key`, normally the task-run ID, for side effects. The database retains each attempt. A stale worker cannot complete a task after its lease has been reassigned because completion is fenced by `lease_owner` and state.

Events are ordered within a workflow run because all of its messages share a Kafka partition key. There is no global order across runs.
