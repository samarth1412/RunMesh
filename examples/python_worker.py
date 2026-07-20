import asyncio
import os

from runmesh import TaskContext, Worker

worker = Worker(
    endpoint=os.getenv("RUNMESH_ENDPOINT", "control-plane:7001"),
    http_endpoint=os.getenv("RUNMESH_HTTP_ENDPOINT", "control-plane:8080"),
    api_key=os.getenv("RUNMESH_INTERNAL_TOKEN", "local-development-token"),
    brokers=os.getenv("RUNMESH_KAFKA_BROKERS", "redpanda:9092"),
    group_id=os.getenv("RUNMESH_KAFKA_GROUP_ID", "runmesh-workers"),
)


@worker.task("examples.greet")
async def greet(payload: dict, context: TaskContext) -> dict:
    await asyncio.sleep(0.1)
    return {"message": f"Hello, {payload.get('name', 'world')}!"}


@worker.task("examples.upper")
def upper(payload: dict, context: TaskContext) -> dict:
    return {"text": str(payload.get("text", "")).upper()}


@worker.task("examples.notify")
async def notify(payload: dict, context: TaskContext) -> dict:
    # Real side effects should deduplicate with context.idempotency_key.
    return {"delivered": True, "idempotency_key": context.idempotency_key}


@worker.task("examples.slow")
async def slow(payload: dict, context: TaskContext) -> dict:
    delay = max(0, min(int(payload.get("delay_ms", 200)), 30_000)) / 1000
    await asyncio.sleep(delay)
    context.raise_if_cancelled()
    return {"slept_ms": int(delay * 1000), "worker": "python"}


@worker.task("validation.dead")
def validation_dead(payload: dict, context: TaskContext) -> dict:
    raise RuntimeError("intentional permanent failure for validation")


if __name__ == "__main__":
    worker.run()
