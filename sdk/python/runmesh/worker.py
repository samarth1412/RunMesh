from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import os
import platform
import signal
import socket
import uuid
from collections.abc import Awaitable, Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, TypeVar, cast

import aiohttp
import grpc
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord
from google.protobuf.struct_pb2 import Struct

from .v1 import worker_pb2, worker_pb2_grpc

JSON = dict[str, Any]
logger = logging.getLogger("runmesh.worker")


class _JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: JSON = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "service": "runmesh-worker-python",
            "message": record.getMessage(),
        }
        for key in ("tenant_id", "workflow_run_id", "task_run_id", "trace_id", "span_id"):
            value = getattr(record, key, None)
            if value:
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, separators=(",", ":"))


if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(_JSONFormatter())
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
Handler = Callable[[JSON, "TaskContext"], Any | Awaitable[Any]]
T = TypeVar("T")


class RetryableError(Exception):
    """A handler failure that should consume an attempt and retry."""


class PermanentError(Exception):
    """A handler failure that should be moved to the dead-letter queue."""


@dataclass(slots=True)
class TaskContext:
    task_run_id: str
    workflow_run_id: str
    attempt: int
    timeout_seconds: int
    trace_parent: str | None = None
    _cancelled: asyncio.Event = field(default_factory=asyncio.Event)
    _heartbeat_requested: asyncio.Event = field(default_factory=asyncio.Event)
    _artifact_uploader: Callable[[str, str, bytes], Awaitable[str]] | None = None

    @property
    def idempotency_key(self) -> str:
        return self.task_run_id

    @property
    def trace_id(self) -> str | None:
        if not self.trace_parent:
            return None
        parts = self.trace_parent.split("-")
        return parts[1] if len(parts) == 4 else None

    @property
    def cancelled(self) -> bool:
        return self._cancelled.is_set()

    def heartbeat(self) -> None:
        """Request an immediate heartbeat without blocking handler code."""
        self._heartbeat_requested.set()

    def raise_if_cancelled(self) -> None:
        if self.cancelled:
            raise asyncio.CancelledError("workflow run was cancelled")

    async def upload_artifact(
        self,
        data: bytes | str | Mapping[str, Any],
        *,
        kind: str = "output",
        content_type: str = "application/octet-stream",
    ) -> str:
        """Upload a task-owned artifact and return its opaque artifact URI."""
        if self._artifact_uploader is None:
            raise RuntimeError("artifact uploads are only available while a task is running")
        if isinstance(data, str):
            encoded = data.encode()
        elif isinstance(data, bytes):
            encoded = data
        else:
            encoded = json.dumps(data, separators=(",", ":")).encode()
            content_type = "application/json"
        return await self._artifact_uploader(kind, content_type, encoded)


class Worker:
    """Consumes durable dispatch events and executes registered handlers.

    The task-run ID is the handler's stable idempotency key. A handler can run more
    than once when a process dies after its side effect but before completion is saved.
    """

    def __init__(
        self,
        endpoint: str,
        api_key: str,
        *,
        http_endpoint: str | None = None,
        brokers: str | None = None,
        topic: str = "runmesh.tasks",
        group_id: str = "runmesh-workers",
        worker_id: str | None = None,
        concurrency: int = 8,
        heartbeat_interval: float = 10.0,
    ) -> None:
        self.endpoint = endpoint.removeprefix("grpc://").removeprefix("http://").rstrip("/")
        default_http = (
            self.endpoint[:-4] + "8080" if self.endpoint.endswith("7001") else self.endpoint
        )
        self.base_url = (
            f"http://{(http_endpoint or default_http).removeprefix('http://').rstrip('/')}"
        )
        self.api_key = api_key
        self.brokers: str = brokers or os.environ.get("RUNMESH_KAFKA_BROKERS") or "localhost:19092"
        self.topic = topic
        self.group_id = group_id
        self.worker_id = worker_id or f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
        self.concurrency = concurrency
        self.heartbeat_interval = heartbeat_interval
        self._handlers: dict[str, Handler] = {}
        self._stopping = asyncio.Event()
        self._active: set[asyncio.Task[None]] = set()
        self._grpc_stub: Any = None

    def task(self, name: str) -> Callable[[Handler], Handler]:
        def register(handler: Handler) -> Handler:
            if name in self._handlers:
                raise ValueError(f"handler {name!r} is already registered")
            self._handlers[name] = handler
            return handler

        return register

    def run(self) -> None:
        asyncio.run(self.serve())

    async def serve(self) -> None:
        if not self._handlers:
            raise RuntimeError("register at least one task before starting a worker")
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            with suppress(NotImplementedError):
                loop.add_signal_handler(signum, self._stopping.set)
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            channel = grpc.aio.insecure_channel(self.endpoint)
            self._grpc_stub = worker_pb2_grpc.WorkerServiceStub(channel)
            reporter = asyncio.create_task(self._report_worker(session))
            consumers = [
                asyncio.create_task(self._consume_loop(session, index))
                for index in range(self.concurrency)
            ]
            try:
                await self._stopping.wait()
            finally:
                self._stopping.set()
                reporter.cancel()
                with suppress(asyncio.CancelledError):
                    await reporter
                for consumer_task in consumers:
                    consumer_task.cancel()
                await asyncio.gather(*consumers, return_exceptions=True)
                await channel.close()

    async def _consume_loop(self, session: aiohttp.ClientSession, index: int) -> None:
        # One sequential consumer per concurrency slot preserves at-least-once commits:
        # an offset is never committed while earlier work in that consumer is unfinished.
        consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.brokers.split(","),
            group_id=self.group_id,
            client_id=f"{self.worker_id}-{index}",
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        await consumer.start()
        try:
            async for message in consumer:
                if self._stopping.is_set():
                    return
                if _header(message, "event_type") == "task.dispatch":
                    work = asyncio.current_task()
                    if work is not None:
                        self._active.add(work)
                    try:
                        await self._process(session, consumer, message)
                    finally:
                        if work is not None:
                            self._active.discard(work)
                else:
                    await consumer.commit()
        finally:
            await consumer.stop()

    async def _process(
        self,
        session: aiohttp.ClientSession,
        consumer: AIOKafkaConsumer,
        message: ConsumerRecord[Any, Any],
    ) -> None:
        dispatch = json.loads(message.value)
        task_id = dispatch["task_run_id"]
        trace_parent = _header(message, "traceparent")
        handler = self._handlers.get(dispatch["handler"])
        if handler is None:
            # Heterogeneous capability pools use distinct consumer groups. This
            # group acknowledges handlers it does not own without claiming the
            # database task; a capable group can acquire the durable lease.
            await consumer.commit()
            return
        try:
            task = await self._post(
                session, f"/internal/v1/tasks/{task_id}/lease", {}, trace_parent=trace_parent
            )
        except Conflict:
            await consumer.commit()
            return
        await self._post(
            session, f"/internal/v1/tasks/{task_id}/start", {}, trace_parent=trace_parent
        )
        context = TaskContext(
            task_run_id=task_id,
            workflow_run_id=task["workflow_run_id"],
            attempt=task["attempt_count"],
            timeout_seconds=task["timeout_seconds"],
            trace_parent=trace_parent,
            _artifact_uploader=lambda kind, content_type, data: self._upload_artifact(
                session, task_id, kind, content_type, data, trace_parent
            ),
        )
        heartbeat = asyncio.create_task(self._heartbeat(session, context))
        try:
            task_input = task["input"]
            if task.get("input_artifact_uri"):
                task_input = json.loads(
                    await self._download_artifact(
                        session, task_id, str(task["input_artifact_uri"]), trace_parent
                    )
                )
            if isinstance(task_input, str):
                task_input = json.loads(task_input)
            if inspect.iscoroutinefunction(handler):
                result = await asyncio.wait_for(
                    handler(task_input, context), timeout=context.timeout_seconds
                )
            else:
                result = await asyncio.wait_for(
                    asyncio.to_thread(handler, task_input, context), context.timeout_seconds
                )
            context.raise_if_cancelled()
            encoded_result = json.dumps(
                result if result is not None else {}, separators=(",", ":")
            ).encode()
            output_artifact_uri = ""
            output_value = result if result is not None else {}
            if len(encoded_result) > 256 * 1024:
                output_artifact_uri = await self._upload_artifact(
                    session, task_id, "output", "application/json", encoded_result, trace_parent
                )
                output_value = {}
            log_data = json.dumps({"task_run_id": task_id, "status": "SUCCEEDED"}).encode()
            log_artifact_uri = await self._upload_artifact(
                session, task_id, "log", "application/json", log_data, trace_parent
            )
            await self._post(
                session,
                f"/internal/v1/tasks/{task_id}/complete",
                {
                    "output": output_value,
                    "output_artifact_uri": output_artifact_uri,
                    "log_artifact_uri": log_artifact_uri,
                },
                trace_parent=trace_parent,
            )
        except PermanentError as exc:
            await self._report_failure(
                session, task_id, exc, retryable=False, trace_parent=trace_parent
            )
        except (RetryableError, TimeoutError, asyncio.TimeoutError) as exc:
            await self._report_failure(
                session, task_id, exc, retryable=True, trace_parent=trace_parent
            )
        except asyncio.CancelledError:
            await self._report_failure(
                session,
                task_id,
                RetryableError("execution cancelled"),
                retryable=True,
                trace_parent=trace_parent,
            )
            raise
        except Exception as exc:
            logger.exception(
                "handler failed",
                extra={
                    "workflow_run_id": context.workflow_run_id,
                    "task_run_id": task_id,
                    "trace_id": context.trace_id,
                },
            )
            await self._report_failure(
                session, task_id, exc, retryable=True, trace_parent=trace_parent
            )
        finally:
            heartbeat.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat
        await consumer.commit()

    async def _heartbeat(self, session: aiohttp.ClientSession, context: TaskContext) -> None:
        while True:
            try:
                await asyncio.wait_for(
                    context._heartbeat_requested.wait(), timeout=self.heartbeat_interval
                )
            except TimeoutError:
                pass
            context._heartbeat_requested.clear()
            response = await self._post(
                session,
                f"/internal/v1/tasks/{context.task_run_id}/heartbeat",
                {},
                trace_parent=context.trace_parent,
            )
            if response.get("cancelled"):
                context._cancelled.set()

    async def _report_failure(
        self,
        session: aiohttp.ClientSession,
        task_id: str,
        exc: Exception,
        *,
        retryable: bool,
        trace_parent: str | None,
    ) -> None:
        with suppress(Conflict):
            await self._post(
                session,
                f"/internal/v1/tasks/{task_id}/fail",
                {
                    "retryable": retryable,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc)[:4000],
                },
                trace_parent=trace_parent,
            )

    async def _post(
        self,
        session: aiohttp.ClientSession,
        path: str,
        payload: Mapping[str, Any],
        *,
        trace_parent: str | None = None,
    ) -> JSON:
        if "/tasks/" in path:
            body = {"worker_id": self.worker_id, **payload}
            return await self._task_rpc(path, body, trace_parent)
        headers = {"Authorization": f"Bearer {self.api_key}"}
        if trace_parent:
            headers["traceparent"] = trace_parent
        async with session.post(self.base_url + path, json=payload, headers=headers) as response:
            if response.status == 409:
                raise Conflict(await response.text())
            response.raise_for_status()
            if response.status == 204:
                return {}
            return cast(JSON, await response.json())

    async def _task_rpc(
        self, path: str, payload: Mapping[str, Any], trace_parent: str | None
    ) -> JSON:
        task_id, action = path.split("/tasks/", 1)[1].split("/", 1)
        metadata = [("authorization", f"Bearer {self.api_key}")]
        if trace_parent:
            metadata.append(("traceparent", trace_parent))
        try:
            if action == "lease":
                response = await self._grpc_stub.Lease(
                    worker_pb2.LeaseRequest(task_run_id=task_id, worker_id=self.worker_id),
                    metadata=metadata,
                )
                return _task_message(response.task)
            if action == "start":
                response = await self._grpc_stub.Start(
                    worker_pb2.StartRequest(task_run_id=task_id, worker_id=self.worker_id),
                    metadata=metadata,
                )
                return _task_message(response.task)
            if action == "heartbeat":
                response = await self._grpc_stub.Heartbeat(
                    worker_pb2.HeartbeatRequest(task_run_id=task_id, worker_id=self.worker_id),
                    metadata=metadata,
                )
                return {"cancelled": response.cancelled}
            if action == "complete":
                output = Struct()
                output.update(cast(Mapping[str, Any], payload.get("output") or {}))
                response = await self._grpc_stub.Complete(
                    worker_pb2.CompleteRequest(
                        task_run_id=task_id,
                        worker_id=self.worker_id,
                        output=output,
                        output_artifact_uri=str(payload.get("output_artifact_uri") or ""),
                        log_artifact_uri=str(payload.get("log_artifact_uri") or ""),
                    ),
                    metadata=metadata,
                )
                return _task_message(response.task)
            if action == "fail":
                response = await self._grpc_stub.Fail(
                    worker_pb2.FailRequest(
                        task_run_id=task_id,
                        worker_id=self.worker_id,
                        retryable=bool(payload.get("retryable")),
                        error_type=str(payload.get("error_type") or ""),
                        error_message=str(payload.get("error_message") or ""),
                        trace_id=str(payload.get("trace_id") or ""),
                    ),
                    metadata=metadata,
                )
                return _task_message(response.task)
            raise ValueError(f"unknown worker action {action!r}")
        except grpc.aio.AioRpcError as exc:
            if exc.code() in {grpc.StatusCode.FAILED_PRECONDITION, grpc.StatusCode.ALREADY_EXISTS}:
                raise Conflict(exc.details()) from exc
            raise

    async def _upload_artifact(
        self,
        session: aiohttp.ClientSession,
        task_id: str,
        kind: str,
        content_type: str,
        data: bytes,
        trace_parent: str | None,
    ) -> str:
        metadata = [("authorization", f"Bearer {self.api_key}")]
        if trace_parent:
            metadata.append(("traceparent", trace_parent))
        created = await self._grpc_stub.CreateArtifactUpload(
            worker_pb2.CreateArtifactUploadRequest(
                task_run_id=task_id,
                worker_id=self.worker_id,
                kind=kind,
                content_type=content_type,
                size_bytes=len(data),
                checksum_sha256=hashlib.sha256(data).hexdigest(),
            ),
            metadata=metadata,
        )
        upload_headers = {
            key: value for key, value in created.headers.items() if key.lower() != "host"
        }
        upload_headers.setdefault("Content-Type", content_type)
        async with session.put(
            created.upload_url, data=data, headers=upload_headers
        ) as response:
            if response.status < 200 or response.status >= 300:
                detail = (await response.text())[:1000]
                raise RetryableError(
                    f"artifact upload returned {response.status}: {detail}"
                )
        completed = await self._grpc_stub.CompleteArtifactUpload(
            worker_pb2.CompleteArtifactUploadRequest(
                artifact_id=created.artifact_id,
                worker_id=self.worker_id,
                task_run_id=task_id,
            ),
            metadata=metadata,
        )
        return str(completed.artifact_uri)

    async def _download_artifact(
        self,
        session: aiohttp.ClientSession,
        task_id: str,
        artifact_uri: str,
        trace_parent: str | None,
    ) -> bytes:
        metadata = [("authorization", f"Bearer {self.api_key}")]
        if trace_parent:
            metadata.append(("traceparent", trace_parent))
        signed = await self._grpc_stub.GetArtifactDownload(
            worker_pb2.GetArtifactDownloadRequest(
                artifact_uri=artifact_uri,
                worker_id=self.worker_id,
                task_run_id=task_id,
            ),
            metadata=metadata,
        )
        async with session.get(signed.download_url) as response:
            response.raise_for_status()
            return await response.read()

    async def _report_worker(self, session: aiohttp.ClientSession) -> None:
        while True:
            try:
                await self._post(
                    session,
                    f"/internal/v1/workers/{self.worker_id}/heartbeat",
                    {
                        "handlers": sorted(self._handlers),
                        "active_tasks": len(self._active),
                        "metadata": {"runtime": platform.python_version(), "sdk": "python"},
                    },
                )
            except Exception:
                logger.exception("worker heartbeat failed")
            await asyncio.sleep(self.heartbeat_interval)


class Conflict(Exception):
    pass


def _header(message: ConsumerRecord[Any, Any], key: str) -> str | None:
    for header_key, value in message.headers:
        if header_key == key:
            return cast(str, value.decode())
    return None


def _task_message(task: Any) -> JSON:
    return {
        "id": task.id,
        "workflow_run_id": task.workflow_run_id,
        "task_key": task.task_key,
        "handler": task.handler,
        "status": task.status,
        "attempt_count": task.attempt,
        "timeout_seconds": task.timeout_seconds,
        "input": dict(task.input),
        "input_artifact_uri": task.input_artifact_uri,
    }
