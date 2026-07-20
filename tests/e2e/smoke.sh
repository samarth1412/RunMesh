#!/usr/bin/env bash
set -euo pipefail
api="http://localhost:8080"
identity="http://localhost:8180/realms/runmesh/protocol/openid-connect/token"
for _ in $(seq 1 60); do
  if curl -fsS "$identity" -d 'grant_type=password' -d 'client_id=runmesh-web' -d 'username=admin' -d 'password=runmesh' >/dev/null; then break; fi
  sleep 2
done
token="$(curl -fsS "$identity" -d 'grant_type=password' -d 'client_id=runmesh-web' -d 'username=admin' -d 'password=runmesh' | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')"
authorization="Authorization: Bearer $token"
stamp="$(date +%s)"
for _ in $(seq 1 60); do
  if curl -fsS "$api/health/ready" >/dev/null; then break; fi
  sleep 2
done
curl -fsS "$api/health/ready" >/dev/null
workflow_id="$(curl -fsS -X POST "$api/v1/workflows" -H "$authorization" -H 'Content-Type: application/json' -d "{\"name\":\"ci-smoke-$stamp\",\"tasks\":{\"greet\":{\"handler\":\"examples.greet\"}}}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"
run_id="$(curl -fsS -X POST "$api/v1/workflows/$workflow_id/runs" -H "$authorization" -H 'Content-Type: application/json' -H "Idempotency-Key: ci-smoke-$stamp" -d '{"input":{"name":"CI"}}' | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"
for _ in $(seq 1 60); do
  status="$(curl -fsS "$api/v1/runs/$run_id" -H "$authorization" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')"
  case "$status" in SUCCEEDED) exit 0;; FAILED|CANCELLED) exit 1;; esac
  sleep 2
done
echo "run did not finish: $run_id" >&2
exit 1
