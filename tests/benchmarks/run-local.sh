#!/usr/bin/env bash
set -euo pipefail

# Run against the disposable local Compose stack. This script never provisions
# cloud resources and intentionally leaves the stack available for inspection.
result_dir=${1:-tests/benchmarks/results/$(date +%F)}
mkdir -p "$result_dir"
run_suffix=$(date -u +%Y%m%dT%H%M%SZ)
active_prefix="validation-active-$run_suffix"
crash_prefix="validation-crash-$run_suffix"
duplicate_prefix="validation-duplicate-$run_suffix"

benchmark_token=$(python3 tests/benchmarks/local.py token)
cp "$0" "$result_dir/commands.sh"

docker run --rm --network host \
  -e RUNMESH_TOKEN="$benchmark_token" -e RUNMESH_RATE=100 -e RUNMESH_DURATION=30s \
  -v "$PWD/tests/load":/scripts:ro -v "$PWD/$result_dir":/results \
  grafana/k6:0.57.0 run --summary-export=/results/api-100-rps.json /scripts/api-read.js

docker compose stop scheduler worker-python worker-go
python3 tests/benchmarks/local.py active --count 1000 --prefix "$active_prefix" > "$result_dir/active-workflows.json"
docker compose exec -T postgres psql -U runmesh -d runmesh -Atc \
  "SELECT json_build_object('active_runs',count(*),'active_tasks',sum(task_count)) FROM (SELECT wr.id,count(tr.id) task_count FROM workflow_runs wr JOIN task_runs tr ON tr.workflow_run_id=wr.id WHERE wr.idempotency_key LIKE '$active_prefix-%' AND wr.status='RUNNING' GROUP BY wr.id) rows" \
  > "$result_dir/active-workflows-database.json"

# Remove the completed active-count fixture before measuring dispatch throughput.
docker compose exec -T postgres psql -U runmesh -d runmesh -c \
  "UPDATE task_runs SET status='CANCELLED' WHERE workflow_run_id IN (SELECT id FROM workflow_runs WHERE idempotency_key LIKE '$active_prefix-%'); UPDATE workflow_runs SET status='CANCELLED',completed_at=now() WHERE idempotency_key LIKE '$active_prefix-%';" >/dev/null

python3 tests/benchmarks/local.py prepare --tasks 10000 --prefix "$crash_prefix" > "$result_dir/worker-crash-setup.json"
dispatch_start=$(date +%s)
docker compose up -d scheduler
while [[ $(docker compose exec -T postgres psql -U runmesh -d runmesh -Atc "SELECT count(*) FROM task_runs tr JOIN workflow_runs wr ON wr.id=tr.workflow_run_id WHERE wr.idempotency_key LIKE '$crash_prefix-%' AND tr.status IN ('READY','BLOCKED')") != 0 ]]; do sleep 0.1; done
dispatch_end=$(date +%s)
python3 - "$dispatch_start" "$dispatch_end" > "$result_dir/scheduler-dispatch.json" <<'PY'
import json, sys
elapsed = max(1, int(sys.argv[2]) - int(sys.argv[1]))
print(json.dumps({"scenario":"scheduler_dispatch","tasks":10000,"elapsed_seconds":round(elapsed,3),"dispatches_per_second":round(10000/elapsed,3)}, indent=2))
PY

docker compose up -d --build --scale worker-go=20 worker-go
while (( $(docker compose exec -T postgres psql -U runmesh -d runmesh -Atc "SELECT count(*) FROM task_runs tr JOIN workflow_runs wr ON wr.id=tr.workflow_run_id WHERE wr.idempotency_key LIKE '$crash_prefix-%' AND tr.status='RUNNING'") < 10 )); do sleep 0.1; done
worker_ids=( $(docker compose ps -q worker-go) )
docker kill "${worker_ids[@]}"
docker compose up -d --scale worker-go=20 worker-go
recovery_start=$(date +%s)
while [[ $(docker compose exec -T postgres psql -U runmesh -d runmesh -Atc "SELECT count(*) FROM workflow_runs WHERE idempotency_key LIKE '$crash_prefix-%' AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED')") != 0 ]]; do sleep 1; done
recovery_end=$(date +%s)
docker compose exec -T postgres psql -U runmesh -d runmesh -Atc \
  "SELECT json_build_object('scenario','worker_termination_and_lease_recovery','tasks',count(*),'succeeded',count(*) FILTER (WHERE tr.status='SUCCEEDED'),'other',count(*) FILTER (WHERE tr.status!='SUCCEEDED'),'attempts',sum(tr.attempt_count),'recovered_tasks',count(*) FILTER (WHERE tr.attempt_count > 1),'recovery_seconds',$((recovery_end-recovery_start))) FROM task_runs tr JOIN workflow_runs wr ON wr.id=tr.workflow_run_id WHERE wr.idempotency_key LIKE '$crash_prefix-%'" \
  > "$result_dir/worker-crash-result.json"

python3 tests/benchmarks/local.py duplicate --prefix "$duplicate_prefix" > "$result_dir/duplicate-submission.json"
docker run --rm \
  -v "$PWD":/src -w /src -v /var/run/docker.sock:/var/run/docker.sock \
  -e TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal golang:1.25 \
  sh -c 'go test -tags=integration -count=1 -run TestDuplicateDispatchIsLeasedOnce -json ./tests/integration' \
  > "$result_dir/duplicate-delivery-go-test.json"

docker compose images --format json > "$result_dir/image-digests.json"
{
  uname -a
  sysctl -n machdep.cpu.brand_string 2>/dev/null || true
  sysctl -n hw.memsize 2>/dev/null || true
  docker version --format '{{json .}}'
} > "$result_dir/hardware.txt"
git rev-parse HEAD > "$result_dir/commit-sha.txt"

echo "Benchmark evidence written to $result_dir"
