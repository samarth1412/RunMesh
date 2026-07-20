import asyncio
import os

from runmesh import TaskContext, Worker

worker = Worker(
    endpoint=os.getenv("RUNMESH_ENDPOINT", "control-plane:7001"),
    http_endpoint=os.getenv("RUNMESH_HTTP_ENDPOINT", "control-plane:8080"),
    api_key=os.getenv("RUNMESH_INTERNAL_TOKEN", "local-development-token"),
    brokers=os.getenv("RUNMESH_KAFKA_BROKERS", "redpanda:9092"),
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


if __name__ == "__main__":
    worker.run()
