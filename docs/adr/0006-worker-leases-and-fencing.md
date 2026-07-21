# ADR 0006: Worker leases and fencing

Status: Accepted

## Context

Workers can pause, partition, or restart while holding tasks, and an old process may resume after recovery reassigned its work.

## Decision

Lease through a conditional state transition and require the tenant, task, worker ID, active state, and unexpired lease for worker mutations. Record each attempt separately. Recover expired leases to retry wait or dead letter based on the attempt budget.

## Consequences

Only the current lease holder can mutate a task. Long tasks must heartbeat, and recovery begins only after lease expiry; therefore the configured lease bounds failover latency.
