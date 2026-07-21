# ADR 0001: PostgreSQL is the execution source of truth

Status: Accepted

## Context

Workflow state must remain queryable and recoverable when brokers, workers, or optional services are unavailable.

## Decision

Persist definitions, runs, tasks, attempts, leases, dependencies, and outbox events transactionally in PostgreSQL. Kafka transports work but never decides the durable task state. Redis carries rate-limit state and live hints only.

## Consequences

Correctness can be reasoned about through database constraints and conditional updates. PostgreSQL availability and write throughput bound the control plane, and schema migrations require operational care.
