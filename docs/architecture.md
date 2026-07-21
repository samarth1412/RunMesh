# Architecture

RunMesh deliberately starts as four deployables: control plane, scheduler, workers, and web.

```text
React web -> REST control plane -> PostgreSQL <- scheduler
                                    |              |
                              transactional     lease recovery
                                  outbox            |
                                    v              v
                                Redpanda ------> workers
                                                  |
                                          fenced callbacks
                                                  v
                                              PostgreSQL
```

The control plane validates and versions DAGs, creates runs transactionally, handles tenant-scoped reads and mutations, and exposes worker lease/completion callbacks. The scheduler uses `FOR UPDATE SKIP LOCKED` so replicas can claim distinct tasks. Outbox publishers claim a workflow's head event in a short database operation, release database locks before Kafka I/O, and acknowledge the claim afterward. A crash in the acknowledgement gap can republish an event; task IDs and compare-and-swap transitions make consumers safe under duplicate delivery.

Redis enforces the public API's per-tenant token bucket and carries tenant-scoped live UI hints after the transactional outbox commit. Those hints only trigger durable API reads; they never replace PostgreSQL or Kafka. Losing Redis fails public requests closed in production, while the dashboard polls and scheduling/execution continue independently. MinIO stores oversized input/output/log artifacts. PostgreSQL contains references to those artifacts.

## State model

Tasks follow this constrained lifecycle:

```text
BLOCKED -> READY -> DISPATCHING -> LEASED -> RUNNING -> SUCCEEDED
   |         |                         |         |
   +---------+-------------------------+---------+-> CANCELLED
                                                 +-> RETRY_WAIT -> READY
                                                 +-> DEAD
```

No API performs an unqualified state change. Worker mutations include the task ID, expected state, worker ID, and unexpired lease when appropriate.

## Tenant boundary

The authenticated principal supplies tenant and role. Resource IDs from paths never establish tenancy. Every public SQL query also filters by the principal tenant ID.

## Decision records

- [PostgreSQL as source of truth](adr/0001-postgresql-source-of-truth.md)
- [At-least-once delivery](adr/0002-at-least-once-delivery.md)
- [Kafka for durable delivery](adr/0003-kafka-durable-delivery.md)
- [Redis outside the durable path](adr/0004-redis-outside-durable-path.md)
- [Transactional outbox recovery](adr/0005-transactional-outbox-recovery.md)
- [Worker leases and fencing](adr/0006-worker-leases-and-fencing.md)
- [Per-workflow ordering](adr/0007-per-workflow-ordering.md)
