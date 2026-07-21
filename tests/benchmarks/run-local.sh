#!/usr/bin/env bash
set -euo pipefail

# Runs only against the local Compose project. It never provisions cloud
# resources. Raw result directories are immutable: choose a new path to rerun.
result_dir=${1:-tests/benchmarks/results/$(date -u +%Y-%m-%dT%H%M%SZ)}
if [[ -e "$result_dir" ]]; then
  echo "result directory already exists: $result_dir" >&2
  exit 1
fi
mkdir -p "$result_dir"
run_suffix=$(date -u +%Y%m%dT%H%M%SZ)
active_prefix="validation-active-$run_suffix"
crash_prefix="validation-crash-$run_suffix"
duplicate_prefix="validation-duplicate-$run_suffix"

cp "$0" "$result_dir/commands.sh"
docker compose up -d --build
for endpoint in \
  http://localhost:8180/realms/runmesh/.well-known/openid-configuration \
  http://localhost:8080/health/ready; do
  ready=0
  for _ in $(seq 1 90); do
    if curl -fsS "$endpoint" >/dev/null; then ready=1; break; fi
    sleep 2
  done
  [[ "$ready" == 1 ]] || { echo "benchmark dependency did not become ready: $endpoint" >&2; exit 1; }
done
benchmark_token=$(python3 tests/benchmarks/local.py token)

docker compose exec -T redpanda rpk topic describe runmesh.tasks --format json > "$result_dir/kafka-topology-before.json"
read_workflow=$(python3 tests/benchmarks/local.py definition --prefix "validation-read-$run_suffix")
docker run --rm --add-host host.docker.internal:host-gateway \
  -e RUNMESH_API=http://host.docker.internal:8080 \
  -e RUNMESH_READ_PATH="/v1/workflows/$read_workflow" \
  -e RUNMESH_TOKEN="$benchmark_token" -e RUNMESH_RATE=100 -e RUNMESH_DURATION=30s \
  -v "$PWD/tests/load":/scripts:ro -v "$PWD/$result_dir":/results \
  grafana/k6:0.57.0 run --summary-export=/results/api-read.json /scripts/api-read.js

sleep 3
submission_workflow=$(python3 tests/benchmarks/local.py definition --prefix "validation-submission-$run_suffix")
docker run --rm --add-host host.docker.internal:host-gateway \
  -e RUNMESH_API=http://host.docker.internal:8080 \
  -e RUNMESH_TOKEN="$benchmark_token" -e RUNMESH_WORKFLOW_ID="$submission_workflow" \
  -e RUNMESH_RATE=50 -e RUNMESH_DURATION=30s \
  -v "$PWD/tests/load":/scripts:ro -v "$PWD/$result_dir":/results \
  grafana/k6:0.57.0 run --summary-export=/results/workflow-submissions.json /scripts/submissions.js

docker compose stop scheduler worker-python worker-go
python3 tests/benchmarks/local.py active --count 1000 --prefix "$active_prefix" > "$result_dir/active-workflows.json"
docker compose exec -T postgres psql -U runmesh -d runmesh -Atc \
  "SELECT json_build_object('active_runs',count(*),'active_tasks',sum(task_count)) FROM (SELECT wr.id,count(tr.id) task_count FROM workflow_runs wr JOIN task_runs tr ON tr.workflow_run_id=wr.id WHERE wr.idempotency_key LIKE '$active_prefix-%' AND wr.status='RUNNING' GROUP BY wr.id) rows" \
  > "$result_dir/active-workflows-database.json"

# Start two schedulers and twelve workers against six partitions, then measure
# drain throughput for the 1,000 distinct workflow keys created above.
completion_start=$(python3 -c 'import time; print(time.time_ns())')
docker compose up -d --scale scheduler=2 scheduler
docker compose up -d --build --scale worker-go=12 worker-go
while [[ $(docker compose exec -T postgres psql -U runmesh -d runmesh -Atc "SELECT count(*) FROM workflow_runs WHERE idempotency_key LIKE '$active_prefix-%' AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED')") != 0 ]]; do sleep 0.2; done
completion_end=$(python3 -c 'import time; print(time.time_ns())')
python3 - "$completion_start" "$completion_end" > "$result_dir/end-to-end-completion.json" <<'PY'
import json, sys
elapsed = (int(sys.argv[2]) - int(sys.argv[1])) / 1_000_000_000
print(json.dumps({"scenario":"queued_end_to_end_completion","tasks":1000,"elapsed_seconds":round(elapsed,3),"tasks_per_second":round(1000/elapsed,3)}, indent=2))
PY
docker compose exec -T postgres psql -U runmesh -d runmesh -Atc \
  "SELECT json_build_object('tasks',count(*),'succeeded',count(*) FILTER (WHERE tr.status='SUCCEEDED'),'other',count(*) FILTER (WHERE tr.status!='SUCCEEDED')) FROM task_runs tr JOIN workflow_runs wr ON wr.id=tr.workflow_run_id WHERE wr.idempotency_key LIKE '$active_prefix-%'" \
  > "$result_dir/end-to-end-completion-database.json"

# Begin at the current topic end so this scenario observes only its own work.
docker compose stop worker-go
docker compose exec -T redpanda rpk group seek runmesh-workers --to end --topics runmesh.tasks >/dev/null
python3 tests/benchmarks/local.py prepare --tasks 10000 --prefix "$crash_prefix" > "$result_dir/worker-crash-setup.json"
dispatch_start=$(python3 -c 'import time; print(time.time_ns())')
while [[ $(docker compose exec -T postgres psql -U runmesh -d runmesh -Atc "SELECT count(*) FROM task_runs tr JOIN workflow_runs wr ON wr.id=tr.workflow_run_id WHERE wr.idempotency_key LIKE '$crash_prefix-%' AND tr.status IN ('READY','BLOCKED')") != 0 ]]; do sleep 0.1; done
dispatch_end=$(python3 -c 'import time; print(time.time_ns())')
python3 - "$dispatch_start" "$dispatch_end" > "$result_dir/scheduler-dispatch.json" <<'PY'
import json, sys
elapsed = (int(sys.argv[2]) - int(sys.argv[1])) / 1_000_000_000
print(json.dumps({"scenario":"scheduler_dispatch","tasks":10000,"elapsed_seconds":round(elapsed,3),"dispatches_per_second":round(10000/elapsed,3)}, indent=2))
PY

# Keep enough tasks running to occupy every available Kafka partition before
# terminating the entire 20-container worker pool.
docker compose exec -T postgres psql -U runmesh -d runmesh -c \
  "WITH selected AS (SELECT tr.id FROM task_runs tr JOIN workflow_runs wr ON wr.id=tr.workflow_run_id WHERE wr.idempotency_key LIKE '$crash_prefix-%' ORDER BY tr.id LIMIT 100) UPDATE task_runs SET input='{\"delay_ms\":5000}'::jsonb WHERE id IN (SELECT id FROM selected);" >/dev/null
docker compose up -d --build --scale worker-go=20 worker-go
deadline=$((SECONDS+60))
while true; do
  running=$(docker compose exec -T postgres psql -U runmesh -d runmesh -Atc "SELECT count(*) FROM task_runs tr JOIN workflow_runs wr ON wr.id=tr.workflow_run_id WHERE wr.idempotency_key LIKE '$crash_prefix-%' AND tr.status='RUNNING'")
  [[ "$running" -ge 4 ]] && break
  [[ "$SECONDS" -ge "$deadline" ]] && { echo "fewer than four tasks became concurrently RUNNING" >&2; exit 1; }
  sleep 0.1
done
docker compose exec -T postgres psql -U runmesh -d runmesh -Atc \
  "SELECT json_build_object('running_count',count(*),'task_ids',json_agg(id ORDER BY id)) FROM (SELECT tr.id FROM task_runs tr JOIN workflow_runs wr ON wr.id=tr.workflow_run_id WHERE wr.idempotency_key LIKE '$crash_prefix-%' AND tr.status='RUNNING') active" \
  > "$result_dir/in-flight-before-termination.json"
worker_ids=( $(docker compose ps -q worker-go) )
docker kill "${worker_ids[@]}" >/dev/null
python3 - "${#worker_ids[@]}" "$crash_prefix" "$running" > "$result_dir/worker-termination.json" <<'PY'
import datetime, json, sys
print(json.dumps({"terminated_workers":int(sys.argv[1]),"idempotency_prefix":sys.argv[2],"in_flight_tasks":int(sys.argv[3]),"terminated_at":datetime.datetime.now(datetime.timezone.utc).isoformat()}, indent=2))
PY
recovery_start=$(python3 -c 'import time; print(time.time_ns())')
docker compose up -d --scale worker-go=20 worker-go
while [[ $(docker compose exec -T postgres psql -U runmesh -d runmesh -Atc "SELECT count(*) FROM workflow_runs WHERE idempotency_key LIKE '$crash_prefix-%' AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED')") != 0 ]]; do sleep 1; done
recovery_end=$(python3 -c 'import time; print(time.time_ns())')
recovery_seconds=$(python3 -c "print(round(($recovery_end-$recovery_start)/1_000_000_000,3))")
docker compose exec -T postgres psql -U runmesh -d runmesh -Atc \
  "SELECT json_build_object('scenario','worker_termination_and_lease_recovery','tasks',count(*),'succeeded',count(*) FILTER (WHERE tr.status='SUCCEEDED'),'other',count(*) FILTER (WHERE tr.status!='SUCCEEDED'),'attempts',sum(tr.attempt_count),'recovered_tasks',count(*) FILTER (WHERE tr.attempt_count > 1),'duplicate_attempts',sum(tr.attempt_count)-count(*),'recovery_seconds',$recovery_seconds) FROM task_runs tr JOIN workflow_runs wr ON wr.id=tr.workflow_run_id WHERE wr.idempotency_key LIKE '$crash_prefix-%'" \
  > "$result_dir/worker-crash-result.json"
docker compose exec -T postgres psql -U runmesh -d runmesh -Atc \
  "WITH gaps AS (SELECT EXTRACT(EPOCH FROM (next_attempt.started_at-expired.ended_at))*1000 milliseconds FROM task_attempts expired JOIN task_attempts next_attempt ON next_attempt.task_run_id=expired.task_run_id AND next_attempt.attempt_number=expired.attempt_number+1 JOIN task_runs tr ON tr.id=expired.task_run_id JOIN workflow_runs wr ON wr.id=tr.workflow_run_id WHERE wr.idempotency_key LIKE '$crash_prefix-%' AND expired.error_type='LeaseExpired') SELECT json_build_object('samples',count(*),'p50_ms',percentile_cont(0.50) WITHIN GROUP (ORDER BY milliseconds),'p95_ms',percentile_cont(0.95) WITHIN GROUP (ORDER BY milliseconds),'p99_ms',percentile_cont(0.99) WITHIN GROUP (ORDER BY milliseconds)) FROM gaps" \
  > "$result_dir/lease-recovery-distribution.json"

python3 tests/benchmarks/local.py duplicate --prefix "$duplicate_prefix" > "$result_dir/duplicate-submission.json"
docker run --rm -v "$PWD":/src -w /src -v /var/run/docker.sock:/var/run/docker.sock \
  -v runmesh-go-mod:/go/pkg/mod -v runmesh-go-build:/root/.cache/go-build \
  -e TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal golang:1.25 \
  /usr/local/go/bin/go test -tags=integration -count=1 -run TestDuplicateDispatchIsLeasedOnce -json ./tests/integration \
  > "$result_dir/duplicate-delivery-go-test.json"

docker compose images --format json > "$result_dir/image-digests.json"
docker compose ps --format json > "$result_dir/compose-topology.json"
docker compose exec -T redpanda rpk topic describe runmesh.tasks --format json > "$result_dir/kafka-topology-after.json"
{
  uname -a
  sysctl -n machdep.cpu.brand_string 2>/dev/null || true
  sysctl -n hw.memsize 2>/dev/null || true
  docker version --format '{{json .}}'
} > "$result_dir/hardware.txt"
git rev-parse HEAD > "$result_dir/commit-sha.txt"
python3 tests/benchmarks/verify.py "$result_dir" > "$result_dir/verification.json"

echo "Benchmark evidence written to $result_dir"
