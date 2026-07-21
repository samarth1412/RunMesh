#!/usr/bin/env python3
"""Fail unless committed benchmark evidence satisfies correctness gates."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def load(path: Path) -> dict:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("result_dir", type=Path)
    args = parser.parse_args()
    root = args.result_dir

    api = load(root / "api-read.json")
    submissions = load(root / "workflow-submissions.json")
    active = load(root / "active-workflows.json")
    active_db = load(root / "active-workflows-database.json")
    completion = load(root / "end-to-end-completion-database.json")
    crash = load(root / "worker-crash-result.json")
    termination = load(root / "worker-termination.json")
    recovery = load(root / "lease-recovery-distribution.json")
    duplicate = load(root / "duplicate-submission.json")
    kafka_before = load(root / "kafka-topology-before.json")[0]
    kafka_after = load(root / "kafka-topology-after.json")[0]
    delivery_events = [json.loads(line) for line in (root / "duplicate-delivery-go-test.json").read_text(encoding="utf-8").splitlines()]

    before_offsets = {item["partition"]: item["high_watermark"] for item in kafka_before["partitions"]}
    used_partitions = sum(item["high_watermark"] > before_offsets.get(item["partition"], 0) for item in kafka_after["partitions"])

    checks = {
        "api_requests": api["metrics"]["http_reqs"]["count"] >= 3000,
        "api_zero_failures": api["metrics"]["http_req_failed"]["value"] == 0,
        "submission_requests": submissions["metrics"]["http_reqs"]["count"] >= 1400,
        "submission_zero_failures": submissions["metrics"]["http_req_failed"]["value"] == 0,
        "active_workflows": active["created"] == active["requested"] == active_db["active_runs"] == 1000,
        "end_to_end_zero_task_loss": completion["tasks"] == completion["succeeded"] == 1000 and completion["other"] == 0,
        "six_kafka_partitions": kafka_after["summary"]["partitions"] == 6,
        "multi_partition_delivery": used_partitions >= 4,
        "worker_pool_terminated": termination["terminated_workers"] == 20,
        "multiple_tasks_in_flight": termination["in_flight_tasks"] >= 4,
        "ten_thousand_tasks": crash["tasks"] == crash["succeeded"] == 10_000,
        "zero_task_loss": crash["other"] == 0,
        "lease_recovered": crash["attempts"] > crash["tasks"] and crash["recovered_tasks"] >= 4 and recovery["samples"] >= 4,
        "duplicate_submission": duplicate["passed"] is True,
        "duplicate_delivery": any(event.get("Action") == "pass" and event.get("Test") == "TestDuplicateDispatchIsLeasedOnce" for event in delivery_events),
    }
    output = {"passed": all(checks.values()), "checks": checks, "observed_partitions_with_new_records": used_partitions}
    print(json.dumps(output, indent=2, sort_keys=True))
    if not output["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
