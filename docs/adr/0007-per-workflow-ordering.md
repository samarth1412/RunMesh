# ADR 0007: Preserve per-workflow ordering

Status: Accepted

## Context

Global ordering limits throughput, while events within one workflow must not overtake their predecessors.

## Decision

Derive each outbox ordering key from the workflow-run ID, claim only that key's head event, and use the same ID as the Kafka message key. Different workflow runs may publish and execute concurrently.

## Consequences

Kafka partition count limits useful consumer parallelism. Ordering is guaranteed within a run, not across tenants or workflow runs, and changing partition counts can remap future keys without reordering records already in a partition.
