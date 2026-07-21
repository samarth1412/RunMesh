# ADR 0005: Claim-based transactional outbox recovery

Status: Accepted

## Context

Publishing to Kafka inside a PostgreSQL transaction holds row locks across network I/O. Publishing first without a durable claim allows competing publishers to reorder a workflow.

## Decision

Claim only the oldest unpublished event for each workflow ordering key in a short `SKIP LOCKED` statement. Publish outside the transaction with a timeout, then conditionally acknowledge the claim. Claims expire and are explicitly released on known errors.

## Consequences

Database lock time no longer includes broker latency and publishers can process different workflows concurrently. A crash after publish but before database acknowledgement causes a duplicate, never a lost event. A claim timeout must remain longer than the publish timeout.
