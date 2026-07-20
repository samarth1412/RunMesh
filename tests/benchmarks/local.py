#!/usr/bin/env python3
"""Dependency-free local benchmark setup and correctness probes."""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


def request(method: str, url: str, token: str = "", body: object | None = None, headers: dict[str, str] | None = None) -> tuple[int, dict]:
    encoded = json.dumps(body).encode() if body is not None else None
    request_headers = dict(headers or {})
    if token:
        request_headers["Authorization"] = f"Bearer {token}"
    if body is not None:
        request_headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=encoded, method=method, headers=request_headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            raw = response.read()
            return response.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as error:
        raw = error.read()
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = {"body": raw.decode(errors="replace")}
        return error.code, payload


def oidc_token(identity_url: str) -> str:
    form = urllib.parse.urlencode({"grant_type": "password", "client_id": "runmesh-web", "username": "admin", "password": "runmesh"}).encode()
    req = urllib.request.Request(identity_url, data=form, method="POST", headers={"Content-Type": "application/x-www-form-urlencoded"})
    with urllib.request.urlopen(req, timeout=30) as response:
        return json.load(response)["access_token"]


def post_with_retry(api: str, token: str, path: str, body: object, key: str | None = None) -> tuple[int, dict, float]:
    headers = {"Idempotency-Key": key} if key else None
    for attempt in range(10):
        started = time.perf_counter()
        status, payload = request("POST", api + path, token, body, headers)
        elapsed = time.perf_counter() - started
        if status not in (429, 503):
            return status, payload, elapsed
        time.sleep(min(0.05 * (attempt + 1), 0.5))
    return status, payload, elapsed


def create_workflow(api: str, token: str, name: str, task_count: int, handler: str) -> str:
    tasks = {f"task-{index:04d}": {"handler": handler, "maximum_attempts": 3, "timeout_seconds": 60} for index in range(task_count)}
    status, payload, _ = post_with_retry(api, token, "/v1/workflows", {"name": name, "tasks": tasks})
    if status != 201:
        raise RuntimeError(f"create workflow failed: {status} {payload}")
    return payload["id"]


def latency_summary(values: list[float]) -> dict[str, float]:
    ordered = sorted(values)

    def percentile(value: float) -> float:
        index = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * value)))
        return round(ordered[index] * 1000, 3)

    return {
        "p50_ms": percentile(0.50),
        "p95_ms": percentile(0.95),
        "p99_ms": percentile(0.99),
    }


def active_workflows(api: str, token: str, count: int, prefix: str) -> dict:
    workflow_id = create_workflow(api, token, f"{prefix}-definition", 1, "examples.slow")
    latencies: list[float] = []
    run_ids: list[str] = []
    started = time.perf_counter()
    for index in range(count):
        status, payload, elapsed = post_with_retry(api, token, f"/v1/workflows/{workflow_id}/runs", {"input": {"delay_ms": 20}}, f"{prefix}-{index:04d}")
        if status != 201:
            raise RuntimeError(f"create active run {index} failed: {status} {payload}")
        run_ids.append(payload["id"])
        latencies.append(elapsed)
        # Keep below the configured steady-state tenant rate.
        time.sleep(0.011)
    elapsed = time.perf_counter() - started
    return {
        "scenario": "simultaneously_active_workflows",
        "requested": count,
        "created": len(run_ids),
        "workflow_id": workflow_id,
        "idempotency_prefix": prefix,
        "elapsed_seconds": round(elapsed, 3),
        "create_latency": latency_summary(latencies),
        "failure_count": 0,
        "failure_rate": 0,
        "sample_run_ids": run_ids[:5],
    }


def prepare_tasks(api: str, token: str, tasks: int, prefix: str) -> dict:
    per_run = 500
    if tasks % per_run:
        raise ValueError("task total must be divisible by 500")
    workflow_id = create_workflow(api, token, f"{prefix}-definition", per_run, "examples.slow")
    runs = []
    started = time.perf_counter()
    for index in range(tasks // per_run):
        status, payload, _ = post_with_retry(api, token, f"/v1/workflows/{workflow_id}/runs", {"input": {"delay_ms": 20}}, f"{prefix}-{index:04d}")
        if status != 201:
            raise RuntimeError(f"create load run {index} failed: {status} {payload}")
        runs.append(payload["id"])
    return {
        "scenario": "worker_termination_and_lease_recovery",
        "task_count": tasks,
        "run_count": len(runs),
        "tasks_per_run": per_run,
        "workflow_id": workflow_id,
        "idempotency_prefix": prefix,
        "preparation_seconds": round(time.perf_counter() - started, 3),
        "run_ids": runs,
    }


def duplicate_submission(api: str, token: str, prefix: str) -> dict:
    workflow_id = create_workflow(api, token, f"{prefix}-definition", 1, "examples.greet")
    key = f"{prefix}-same-key"
    first_status, first, _ = post_with_retry(api, token, f"/v1/workflows/{workflow_id}/runs", {"input": {"name": "first"}}, key)
    second_status, second, _ = post_with_retry(api, token, f"/v1/workflows/{workflow_id}/runs", {"input": {"name": "different"}}, key)
    passed = first_status == 201 and second_status == 200 and first.get("id") == second.get("id") and first.get("input") == second.get("input")
    return {
        "scenario": "duplicate_submission",
        "first_status": first_status,
        "second_status": second_status,
        "first_run_id": first.get("id"),
        "second_run_id": second.get("id"),
        "same_run": first.get("id") == second.get("id"),
        "original_input_preserved": first.get("input") == second.get("input"),
        "passed": passed,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("token", "definition", "active", "prepare", "duplicate"))
    parser.add_argument("--api", default="http://localhost:8080")
    parser.add_argument("--identity", default="http://localhost:8180/realms/runmesh/protocol/openid-connect/token")
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--tasks", type=int, default=10_000)
    parser.add_argument("--prefix", default=f"benchmark-{int(time.time())}")
    args = parser.parse_args()
    token = oidc_token(args.identity)
    if args.command == "token":
        print(token)
    elif args.command == "definition":
        print(create_workflow(args.api, token, f"{args.prefix}-definition", 1, "examples.greet"))
    elif args.command == "active":
        print(json.dumps(active_workflows(args.api, token, args.count, args.prefix), indent=2, sort_keys=True))
    elif args.command == "prepare":
        print(json.dumps(prepare_tasks(args.api, token, args.tasks, args.prefix), indent=2, sort_keys=True))
    else:
        result = duplicate_submission(args.api, token, args.prefix)
        print(json.dumps(result, indent=2, sort_keys=True))
        if not result["passed"]:
            sys.exit(1)


if __name__ == "__main__":
    main()
