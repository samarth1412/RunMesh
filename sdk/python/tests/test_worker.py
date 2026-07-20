import asyncio

import pytest

from google.protobuf.struct_pb2 import Struct

from runmesh import TaskContext, Worker
from runmesh.v1 import worker_pb2
from runmesh.worker import _task_message


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
