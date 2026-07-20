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

The control plane validates and versions DAGs, creates runs transactionally, handles tenant-scoped reads and mutations, and exposes worker lease/completion callbacks. The scheduler uses `FOR UPDATE SKIP LOCKED` so replicas can claim distinct tasks. The outbox publisher may republish after a crash; task IDs and compare-and-swap transitions make consumers safe under duplicate delivery.

Redis is reserved for rate-limit counters and live UI hints; losing it must not corrupt execution. MinIO stores oversized input/output/log artifacts. PostgreSQL contains references to those artifacts.

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
