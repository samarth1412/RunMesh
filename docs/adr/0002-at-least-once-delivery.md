# ADR 0002: At-least-once delivery

Status: Accepted

## Context

A process can fail after a broker publish or handler side effect but before recording the corresponding acknowledgement.

## Decision

Allow duplicate messages and attempts, and prevent duplicate successful leases with compare-and-swap state transitions. Expose the stable task-run ID as the handler idempotency key.

## Consequences

RunMesh does not lose durable work merely to suppress duplicates. Handlers with external side effects must deduplicate those effects; the orchestrator cannot promise exactly-once behavior for arbitrary code.
