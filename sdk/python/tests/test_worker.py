import asyncio
from typing import Any, Self, cast

import aiohttp
import pytest

from google.protobuf.struct_pb2 import Struct

from runmesh import TaskContext, Worker
from runmesh.v1 import worker_pb2
from runmesh.worker import _task_message


class _NoContentResponse:
    status = 204

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    def raise_for_status(self) -> None:
        return None


class _RecordingSession:
    payload: dict[str, Any] | None = None

    def post(self, _: str, *, json: dict[str, Any], headers: object) -> _NoContentResponse:
        self.payload = json
        return _NoContentResponse()


def test_registers_handler() -> None:
    worker = Worker("localhost:8080", "token")

    @worker.task("example.hello")
    def hello(payload: dict, context: TaskContext) -> dict:
        return payload

    assert worker._handlers["example.hello"] is hello
    with pytest.raises(ValueError):
        worker.task("example.hello")(hello)


def test_context_idempotency_and_cancellation() -> None:
    context = TaskContext("task-1", "run-1", 1, 30)
    assert context.idempotency_key == "task-1"
    context.heartbeat()
    assert context._heartbeat_requested.is_set()
    context._cancelled.set()
    with pytest.raises(asyncio.CancelledError):
        context.raise_if_cancelled()


def test_protobuf_task_contract() -> None:
    task_input = Struct()
    task_input.update({"pages": 14})
    task = worker_pb2.TaskSnapshot(
        id="task", workflow_run_id="run", handler="documents.extract",
        input=task_input, attempt=2, timeout_seconds=30, status="LEASED",
    )
    decoded = _task_message(task)
    assert decoded["attempt_count"] == 2
    assert decoded["input"]["pages"] == 14


async def test_worker_heartbeat_payload_excludes_task_worker_id() -> None:
    worker = Worker("localhost:7001", "token", worker_id="worker-1")
    session = _RecordingSession()
    payload = {"handlers": ["example.hello"], "active_tasks": 0, "metadata": {}}

    await worker._post(
        cast(aiohttp.ClientSession, session),
        "/internal/v1/workers/worker-1/heartbeat",
        payload,
    )

    assert session.payload == payload
