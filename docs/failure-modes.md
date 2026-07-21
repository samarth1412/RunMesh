# Failure modes

| Failure | Behavior |
|---|---|
| Broker unavailable | State and outbox commit; publisher retries later |
| Duplicate broker message | Lease compare-and-swap rejects the duplicate |
| Worker crash | Expired lease becomes retryable or dead after attempt exhaustion |
| Completion response lost | The durable completion remains committed; a redelivered dispatch cannot lease the terminal task, so user code is not rerun for that message |
| Scheduler crash | Database locks are released; another replica claims work |
| Redis unavailable | Rate limiting fails closed in production and live WebSocket hints reconnect with polling fallback; durable scheduling and execution remain unaffected |
| Artifact store unavailable | Worker reports a retryable failure |
| Cancellation during execution | Heartbeat returns cancellation; worker token is set |
