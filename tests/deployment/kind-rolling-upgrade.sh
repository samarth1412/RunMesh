#!/usr/bin/env bash
set -euo pipefail

cluster="${RUNMESH_KIND_CLUSTER:-runmesh-upgrade}"
namespace="${RUNMESH_KIND_NAMESPACE:-runmesh}"
api="http://127.0.0.1:18080"
port_forward_pid=""

cleanup() {
  if [[ -n "$port_forward_pid" ]]; then
    kill "$port_forward_pid" 2>/dev/null || true
    wait "$port_forward_pid" 2>/dev/null || true
  fi
  kind delete cluster --name "$cluster" >/dev/null 2>&1 || true
}
trap cleanup EXIT

diagnostics() {
  echo "kind upgrade diagnostics" >&2
  kubectl -n "$namespace" get pods -o wide >&2 || true
  kubectl -n "$namespace" get events --sort-by=.lastTimestamp >&2 || true
  kubectl -n "$namespace" logs -l app.kubernetes.io/name=runmesh --all-containers --tail=100 --prefix >&2 || true
}
trap diagnostics ERR

start_port_forward() {
  if [[ -n "$port_forward_pid" ]]; then
    kill "$port_forward_pid" 2>/dev/null || true
    wait "$port_forward_pid" 2>/dev/null || true
  fi
  kubectl -n "$namespace" port-forward service/runmesh-control-plane 18080:8080 >/tmp/runmesh-kind-port-forward.log 2>&1 &
  port_forward_pid=$!
  for _ in $(seq 1 60); do curl -fsS "$api/health/ready" >/dev/null && return; sleep 2; done
  echo "control-plane port-forward did not become ready" >&2
  return 1
}

for command in docker kind kubectl helm curl python3; do
  command -v "$command" >/dev/null || { echo "$command is required" >&2; exit 1; }
done

kind create cluster --name "$cluster" --wait 120s
if [[ "${RUNMESH_SKIP_BUILD:-0}" != "1" ]]; then
  docker build -t runmesh/control-plane:rolling --build-arg SERVICE=control-plane .
  docker build -t runmesh/scheduler:rolling --build-arg SERVICE=scheduler .
  docker build -t runmesh/worker-go:rolling --build-arg SERVICE=worker-go .
  docker build -t runmesh/web:rolling -f web/Dockerfile --build-arg VITE_DEV_AUTH=true .
fi
kind load docker-image --name "$cluster" runmesh/control-plane:rolling runmesh/scheduler:rolling runmesh/worker-go:rolling runmesh/web:rolling

kubectl create namespace "$namespace"
kubectl -n "$namespace" apply -f tests/deployment/kind-dependencies.yaml
kubectl -n "$namespace" rollout status deployment/postgres --timeout=180s
kubectl -n "$namespace" rollout status deployment/redis --timeout=180s
kubectl -n "$namespace" rollout status deployment/redpanda --timeout=240s

kubectl -n "$namespace" create configmap runmesh-migrations --from-file=migrations
kubectl -n "$namespace" apply -f - <<'YAML'
apiVersion: batch/v1
kind: Job
metadata: {name: runmesh-migrate}
spec:
  backoffLimit: 3
  template:
    spec:
      restartPolicy: OnFailure
      containers:
        - name: migrate
          image: migrate/migrate:v4.18.2
          args: [-path, /migrations, -database, "postgres://runmesh:runmesh@postgres:5432/runmesh?sslmode=disable", up]
          volumeMounts: [{name: migrations, mountPath: /migrations, readOnly: true}]
      volumes: [{name: migrations, configMap: {name: runmesh-migrations}}]
YAML
kubectl -n "$namespace" wait --for=condition=complete job/runmesh-migrate --timeout=180s

helm upgrade --install runmesh deploy/helm/runmesh --namespace "$namespace" -f tests/deployment/kind-values.yaml \
  --set secret.databaseURL="postgres://runmesh:runmesh@postgres:5432/runmesh?sslmode=disable" \
  --set secret.redisURL=redis://redis:6379/0 --set secret.internalToken=local-development-token --set secret.apiKeyPepper=kind-test-pepper

for deployment in runmesh-control-plane runmesh-scheduler runmesh-web runmesh-worker-default-go; do
  kubectl -n "$namespace" rollout status "deployment/$deployment" --timeout=240s
done

start_port_forward
curl -fsS "$api/health/ready" >/dev/null

workflow_payload="$(python3 - <<'PY'
import json
tasks = {}
for index in range(12):
    key = f"step-{index:02d}"
    task = {"handler": "examples.slow", "timeout_seconds": 30}
    if index:
        task["depends_on"] = [f"step-{index - 1:02d}"]
    tasks[key] = task
print(json.dumps({"name": "rolling-upgrade", "tasks": tasks}))
PY
)"
workflow_id="$(curl -fsS -X POST "$api/v1/workflows" -H 'Content-Type: application/json' -d "$workflow_payload" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"
run_id="$(curl -fsS -X POST "$api/v1/workflows/$workflow_id/runs" -H 'Content-Type: application/json' -H 'Idempotency-Key: rolling-upgrade' -d '{"input":{"delay_ms":1000}}' | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"

for _ in $(seq 1 60); do
  status="$(curl -fsS "$api/v1/runs/$run_id" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')"
  [[ "$status" == RUNNING ]] && break
  [[ "$status" == FAILED || "$status" == CANCELLED ]] && { echo "run became $status before upgrade" >&2; exit 1; }
  sleep 1
done
[[ "$status" == RUNNING ]] || { echo "run never entered RUNNING" >&2; exit 1; }

kubectl -n "$namespace" rollout restart deployment/runmesh-control-plane deployment/runmesh-scheduler deployment/runmesh-web deployment/runmesh-worker-default-go
for deployment in runmesh-control-plane runmesh-scheduler runmesh-web runmesh-worker-default-go; do
  kubectl -n "$namespace" rollout status "deployment/$deployment" --timeout=240s
done
start_port_forward

for _ in $(seq 1 180); do
  response="$(curl -fsS --retry 3 --retry-connrefused --retry-all-errors "$api/v1/runs/$run_id")" || { sleep 1; continue; }
  status="$(printf '%s' "$response" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')" || { sleep 1; continue; }
  [[ "$status" == SUCCEEDED ]] && { echo "rolling upgrade preserved workflow $run_id"; exit 0; }
  [[ "$status" == FAILED || "$status" == CANCELLED ]] && { echo "run became $status during upgrade" >&2; exit 1; }
  sleep 1
done
echo "run did not complete after rolling upgrade: $run_id" >&2
exit 1
