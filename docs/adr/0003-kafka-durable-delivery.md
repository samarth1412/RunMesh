# ADR 0003: Kafka for durable work delivery

Status: Accepted

## Context

Workers need resumable, partitioned delivery with consumer-group coordination and explicit offset commits.

## Decision

Publish dispatch events to Kafka or Redpanda through the transactional outbox and commit consumer offsets only after a lease conflict or completed processing path.

## Consequences

Workers scale through partitions and consumer groups. Kafka remains an operational dependency for new dispatch, but broker outages do not erase work because unpublished events remain in PostgreSQL.
