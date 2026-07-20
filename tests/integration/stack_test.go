//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/runmesh/runmesh/internal/scheduler"
	"github.com/runmesh/runmesh/internal/storage"
	"github.com/runmesh/runmesh/internal/workflow"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestPostgresIdempotencyAndConcurrentClaims(t *testing.T) {
	ctx := context.Background()
	container := start(t, ctx, testcontainers.ContainerRequest{
		Image: "postgres:17-alpine", ExposedPorts: []string{"5432/tcp"},
		Env:        map[string]string{"POSTGRES_DB": "runmesh", "POSTGRES_USER": "runmesh", "POSTGRES_PASSWORD": "runmesh"},
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(time.Minute),
	})
	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "5432/tcp")
	databaseURL := fmt.Sprintf("postgres://runmesh:runmesh@%s:%s/runmesh?sslmode=disable", host, port.Port())
	applyMigrations(t, ctx, databaseURL)
	store, err := storage.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	dag := workflow.DAG{Tasks: map[string]workflow.TaskSpec{}}
	for i := 0; i < 40; i++ {
		key := fmt.Sprintf("task-%02d", i)
		dag.Tasks[key] = workflow.TaskSpec{Handler: "test.handle"}
	}
	definition, err := store.CreateWorkflow(ctx, "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000001", "claims", dag)
	if err != nil {
		t.Fatal(err)
	}
	first, created, err := store.CreateRun(ctx, "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000001", definition.ID, "same-key", json.RawMessage(`{"value":1}`))
	if err != nil || !created {
		t.Fatalf("first submission: created=%v err=%v", created, err)
	}
	second, created, err := store.CreateRun(ctx, "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000001", definition.ID, "same-key", json.RawMessage(`{"value":2}`))
	if err != nil || created || second.ID != first.ID {
		t.Fatalf("duplicate submission: first=%s second=%s created=%v err=%v", first.ID, second.ID, created, err)
	}

	claimed := map[string]bool{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				tx, e := store.Pool.BeginTx(ctx, pgx.TxOptions{})
				if e != nil {
					t.Error(e)
					return
				}
				var id string
				e = tx.QueryRow(ctx, `SELECT id FROM task_runs WHERE workflow_run_id=$1 AND status='READY' FOR UPDATE SKIP LOCKED LIMIT 1`, first.ID).Scan(&id)
				if e == pgx.ErrNoRows {
					_ = tx.Rollback(ctx)
					return
				}
				if e != nil {
					_ = tx.Rollback(ctx)
					t.Error(e)
					return
				}
				if _, e = tx.Exec(ctx, `UPDATE task_runs SET status='DISPATCHING' WHERE id=$1`, id); e != nil {
					_ = tx.Rollback(ctx)
					t.Error(e)
					return
				}
				if e = tx.Commit(ctx); e != nil {
					t.Error(e)
					return
				}
				mu.Lock()
				if claimed[id] {
					t.Errorf("task %s claimed twice", id)
				}
				claimed[id] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(claimed) != 40 {
		t.Fatalf("claimed %d tasks, want 40", len(claimed))
	}
}

func TestRedisRedpandaAndMinIOContainers(t *testing.T) {
	ctx := context.Background()
	requests := []testcontainers.ContainerRequest{
		{Image: "redis:7.4-alpine", ExposedPorts: []string{"6379/tcp"}, WaitingFor: wait.ForLog("Ready to accept connections").WithStartupTimeout(time.Minute)},
		{Image: "redpandadata/redpanda:v24.3.15", ExposedPorts: []string{"9092/tcp"}, Cmd: []string{"redpanda", "start", "--smp", "1", "--memory", "512M", "--overprovisioned", "--node-id", "0", "--kafka-addr", "0.0.0.0:9092"}, WaitingFor: wait.ForListeningPort("9092/tcp").WithStartupTimeout(2 * time.Minute)},
		{Image: "minio/minio:RELEASE.2025-02-07T23-21-09Z", ExposedPorts: []string{"9000/tcp"}, Env: map[string]string{"MINIO_ROOT_USER": "runmesh", "MINIO_ROOT_PASSWORD": "runmesh-development"}, Cmd: []string{"server", "/data"}, WaitingFor: wait.ForListeningPort("9000/tcp").WithStartupTimeout(time.Minute)},
	}
	for _, request := range requests {
		start(t, ctx, request)
	}
}

func TestWorkerCrashLeaseRecovery(t *testing.T) {
	ctx := context.Background()
	container := start(t, ctx, testcontainers.ContainerRequest{
		Image: "postgres:17-alpine", ExposedPorts: []string{"5432/tcp"},
		Env:        map[string]string{"POSTGRES_DB": "runmesh", "POSTGRES_USER": "runmesh", "POSTGRES_PASSWORD": "runmesh"},
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(time.Minute),
	})
	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "5432/tcp")
	databaseURL := fmt.Sprintf("postgres://runmesh:runmesh@%s:%s/runmesh?sslmode=disable", host, port.Port())
	applyMigrations(t, ctx, databaseURL)
	store, err := storage.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	dag := workflow.DAG{Tasks: map[string]workflow.TaskSpec{
		"recover": {Handler: "test.recover", MaximumAttempts: 3},
	}}
	definition, err := store.CreateWorkflow(ctx, "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000001", "worker-crash", dag)
	if err != nil {
		t.Fatal(err)
	}
	run, created, err := store.CreateRun(ctx, "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000001", definition.ID, "worker-crash", json.RawMessage(`{"value":1}`))
	if err != nil || !created {
		t.Fatalf("create run: created=%v err=%v", created, err)
	}

	service := scheduler.Service{Store: store}
	if err = service.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	run, err = store.GetRun(ctx, "00000000-0000-0000-0000-000000000001", run.ID)
	if err != nil || len(run.Tasks) != 1 || run.Tasks[0].Status != "DISPATCHING" {
		t.Fatalf("task was not dispatched: run=%+v err=%v", run, err)
	}
	taskID := run.Tasks[0].ID
	first, err := store.LeaseTask(ctx, taskID, "worker-crashed", 150*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.StartTask(ctx, taskID, "worker-crashed"); err != nil {
		t.Fatal(err)
	}
	waitForLeaseExpiry(t, ctx, store, taskID)

	if err = service.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	recovered, err := store.GetRun(ctx, "00000000-0000-0000-0000-000000000001", run.ID)
	if err != nil {
		t.Fatal(err)
	}
	task := recovered.Tasks[0]
	if task.Status != "RETRY_WAIT" || task.AttemptCount != 1 || task.LeaseOwner != nil || task.LeaseExpiresAt != nil {
		t.Fatalf("expired lease was not recovered: %+v", task)
	}
	if task.AvailableAt.Before(time.Now().Add(time.Second)) {
		t.Fatalf("retry backoff was not applied: available_at=%s", task.AvailableAt)
	}
	if _, _, err = store.HeartbeatTask(ctx, taskID, "worker-crashed", time.Minute); !errors.Is(err, storage.ErrLeaseLost) {
		t.Fatalf("stale worker heartbeat error=%v, want ErrLeaseLost", err)
	}
	assertAttempt(t, ctx, store, taskID, 1, "worker-crashed", "RETRY_WAIT", "LeaseExpired")
	assertOutboxEvent(t, ctx, store, taskID, "task.retry_wait")

	time.Sleep(time.Until(task.AvailableAt) + 25*time.Millisecond)
	if err = service.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := store.LeaseTask(ctx, taskID, "worker-replacement", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if second.AttemptCount != first.AttemptCount+1 {
		t.Fatalf("replacement attempt=%d, want %d", second.AttemptCount, first.AttemptCount+1)
	}
	if _, err = store.StartTask(ctx, taskID, "worker-replacement"); err != nil {
		t.Fatal(err)
	}
	if _, err = store.CompleteTask(ctx, taskID, "worker-replacement", json.RawMessage(`{"recovered":true}`), ""); err != nil {
		t.Fatal(err)
	}
	completed, err := store.GetRun(ctx, "00000000-0000-0000-0000-000000000001", run.ID)
	if err != nil || completed.Status != "SUCCEEDED" || completed.Tasks[0].Status != "SUCCEEDED" {
		t.Fatalf("replacement worker did not complete run: run=%+v err=%v", completed, err)
	}
	assertAttempt(t, ctx, store, taskID, 2, "worker-replacement", "SUCCEEDED", "")
}

func waitForLeaseExpiry(t *testing.T, ctx context.Context, store *storage.Store, taskID string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var expired bool
		if err := store.Pool.QueryRow(ctx, `SELECT lease_expires_at <= now() FROM task_runs WHERE id=$1`, taskID).Scan(&expired); err != nil {
			t.Fatal(err)
		}
		if expired {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("lease did not expire")
}

func assertAttempt(t *testing.T, ctx context.Context, store *storage.Store, taskID string, attempt int, workerID, exitStatus, errorType string) {
	t.Helper()
	var gotWorker, gotStatus, gotError string
	var endedAt time.Time
	err := store.Pool.QueryRow(ctx, `SELECT worker_id,ended_at,exit_status,COALESCE(error_type,'') FROM task_attempts WHERE task_run_id=$1 AND attempt_number=$2`, taskID, attempt).Scan(&gotWorker, &endedAt, &gotStatus, &gotError)
	if err != nil {
		t.Fatal(err)
	}
	if gotWorker != workerID || gotStatus != exitStatus || gotError != errorType || endedAt.IsZero() {
		t.Fatalf("attempt %d mismatch: worker=%q status=%q error=%q ended_at=%s", attempt, gotWorker, gotStatus, gotError, endedAt)
	}
}

func assertOutboxEvent(t *testing.T, ctx context.Context, store *storage.Store, taskID, eventType string) {
	t.Helper()
	var count int
	if err := store.Pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE aggregate_id=$1 AND event_type=$2`, taskID, eventType).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("outbox event %q count=%d, want 1", eventType, count)
	}
}

func start(t *testing.T, ctx context.Context, request testcontainers.ContainerRequest) testcontainers.Container {
	t.Helper()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: request, Started: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	return container
}
func applyMigrations(t *testing.T, ctx context.Context, databaseURL string) {
	t.Helper()
	connection, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(ctx)
	for _, name := range []string{"001_initial.up.sql", "002_seed_development.up.sql"} {
		path := filepath.Join("..", "..", "migrations", name)
		sql, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if _, err = connection.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("migration %s: %v", name, err)
		}
	}
}
