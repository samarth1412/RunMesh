# ADR 0004: Redis stays outside durable execution

Status: Accepted

## Context

Rate limiting and low-latency UI notifications benefit from Redis, but workflow correctness must not depend on ephemeral state.

## Decision

Use Redis for atomic tenant token buckets and Pub/Sub hints. Public production requests fail closed if limits cannot be enforced; schedulers, Kafka consumers, and durable state transitions do not call Redis.

## Consequences

A Redis outage can reject public API traffic and degrade the UI to polling without losing or stopping already durable work.
