# Failure modes

| Failure | Behavior |
|---|---|
| Broker unavailable | State and outbox commit; publisher retries later |
| Duplicate broker message | Lease compare-and-swap rejects the duplicate |
| Worker crash | Expired lease becomes retryable or dead after attempt exhaustion |
| Completion response lost | Repeated completion sees the terminal task and is idempotent |
| Scheduler crash | Database locks are released; another replica claims work |
| Redis unavailable | Rate limiting degrades closed in production; execution state is unaffected |
| Artifact store unavailable | Worker reports a retryable failure |
| Cancellation during execution | Heartbeat returns cancellation; worker token is set |
