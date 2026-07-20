#!/usr/bin/env bash
set -euo pipefail
api="http://localhost:8080"
for _ in $(seq 1 60); do
  if curl -fsS "$api/health/ready" >/dev/null; then break; fi
  sleep 2
done
curl -fsS "$api/health/ready" >/dev/null
workflow_id="$(curl -fsS -X POST "$api/v1/workflows" -H 'Content-Type: application/json' -d '{"name":"ci-smoke","tasks":{"greet":{"handler":"examples.greet"}}}' | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"
run_id="$(curl -fsS -X POST "$api/v1/workflows/$workflow_id/runs" -H 'Content-Type: application/json' -H 'Idempotency-Key: ci-smoke' -d '{"input":{"name":"CI"}}' | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"
for _ in $(seq 1 60); do
  status="$(curl -fsS "$api/v1/runs/$run_id" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')"
  case "$status" in SUCCEEDED) exit 0;; FAILED|CANCELLED) exit 1;; esac
  sleep 2
done
echo "run did not finish: $run_id" >&2
exit 1
