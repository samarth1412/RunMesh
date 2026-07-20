//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
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
