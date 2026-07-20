//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	toxiproxyclient "github.com/Shopify/toxiproxy/v2/client"
	"github.com/jackc/pgx/v5"
	"github.com/runmesh/runmesh/internal/messaging"
	"github.com/runmesh/runmesh/internal/scheduler"
	"github.com/runmesh/runmesh/internal/storage"
	"github.com/runmesh/runmesh/internal/workflow"
	"github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	tctoxiproxy "github.com/testcontainers/testcontainers-go/modules/toxiproxy"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestPostgresIdempotencyAndConcurrentClaims(t *testing.T) {
	ctx := context.Background()
	store := postgresStore(t, ctx)
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

func TestPostgresRestartPreservesWorkflowAndPoolRecovery(t *testing.T) {
	ctx := context.Background()
	container, store := restartablePostgresStore(t, ctx)
	tenantID := "00000000-0000-0000-0000-000000000001"
	userID := "00000000-0000-0000-0000-000000000001"
	dag := workflow.DAG{Tasks: map[string]workflow.TaskSpec{
		"active":     {Handler: "test.active"},
		"downstream": {Handler: "test.downstream", DependsOn: []string{"active"}},
	}}
	definition, err := store.CreateWorkflow(ctx, tenantID, userID, "postgres-restart", dag)
	if err != nil {
		t.Fatal(err)
	}
	run, created, err := store.CreateRun(ctx, tenantID, userID, definition.ID, "postgres-restart", json.RawMessage(`{"value":1}`))
	if err != nil || !created {
		t.Fatalf("create run: created=%v err=%v", created, err)
	}
	service := scheduler.Service{Store: store}
	if err = service.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	run, err = store.GetRun(ctx, tenantID, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	activeID := taskByKey(t, run, "active").ID
	downstreamID := taskByKey(t, run, "downstream").ID
	if _, err = store.LeaseTask(ctx, activeID, "worker-before-restart", 2*time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err = store.StartTask(ctx, activeID, "worker-before-restart"); err != nil {
		t.Fatal(err)
	}

	exitCode, _, err := container.Exec(ctx, []string{"sh", "-c", `touch /tmp/runmesh-hold-postgres && kill -INT "$(head -n 1 "${PGDATA:-/var/lib/postgresql/data}/postmaster.pid")"`})
	if err != nil || exitCode != 0 {
		t.Fatalf("stop PostgreSQL process: exit=%d err=%v", exitCode, err)
	}
	outageDeadline := time.Now().Add(10 * time.Second)
	for {
		downContext, cancelDown := context.WithTimeout(ctx, 250*time.Millisecond)
		pingErr := store.Ping(downContext)
		cancelDown()
		if pingErr != nil {
			break
		}
		if time.Now().After(outageDeadline) {
			t.Fatal("PostgreSQL process did not stop before the outage deadline")
		}
		time.Sleep(50 * time.Millisecond)
	}
	downSchedulerContext, cancelDownScheduler := context.WithTimeout(ctx, 2*time.Second)
	if schedulerErr := service.RunOnce(downSchedulerContext); schedulerErr == nil {
		cancelDownScheduler()
		t.Fatal("scheduler cycle succeeded while PostgreSQL was stopped")
	}
	cancelDownScheduler()

	exitCode, _, err = container.Exec(ctx, []string{"sh", "-c", "rm -f /tmp/runmesh-hold-postgres"})
	if err != nil || exitCode != 0 {
		t.Fatalf("restart PostgreSQL process: exit=%d err=%v", exitCode, err)
	}
	recoveryContext, cancelRecovery := context.WithTimeout(ctx, 30*time.Second)
	defer cancelRecovery()
	for {
		err = store.Ping(recoveryContext)
		if err == nil {
			break
		}
		select {
		case <-recoveryContext.Done():
			t.Fatalf("existing PostgreSQL pool did not recover: last error: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
	}

	recovered, err := store.GetRun(ctx, tenantID, run.ID)
	if err != nil || recovered.Status != "RUNNING" {
		t.Fatalf("recover run after restart: run=%+v err=%v", recovered, err)
	}
	active := taskByKey(t, recovered, "active")
	downstream := taskByKey(t, recovered, "downstream")
	if active.Status != "RUNNING" || active.LeaseOwner == nil || *active.LeaseOwner != "worker-before-restart" || downstream.Status != "BLOCKED" {
		t.Fatalf("durable task state changed across restart: active=%+v downstream=%+v", active, downstream)
	}
	duplicate, created, err := store.CreateRun(ctx, tenantID, userID, definition.ID, "postgres-restart", json.RawMessage(`{"value":2}`))
	if err != nil || created || duplicate.ID != run.ID {
		t.Fatalf("idempotency after restart: original=%s duplicate=%s created=%v err=%v", run.ID, duplicate.ID, created, err)
	}
	if _, cancelled, heartbeatErr := store.HeartbeatTask(ctx, activeID, "worker-before-restart", time.Minute); heartbeatErr != nil || cancelled {
		t.Fatalf("heartbeat after restart: cancelled=%v err=%v", cancelled, heartbeatErr)
	}
	if _, err = store.CompleteTask(ctx, activeID, "worker-before-restart", json.RawMessage(`{"recovered":true}`), ""); err != nil {
		t.Fatal(err)
	}
	if err = service.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = store.LeaseTask(ctx, downstreamID, "worker-after-restart", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err = store.StartTask(ctx, downstreamID, "worker-after-restart"); err != nil {
		t.Fatal(err)
	}
	if _, err = store.CompleteTask(ctx, downstreamID, "worker-after-restart", json.RawMessage(`{"finished":true}`), ""); err != nil {
		t.Fatal(err)
	}
	completed, err := store.GetRun(ctx, tenantID, run.ID)
	if err != nil || completed.Status != "SUCCEEDED" {
		t.Fatalf("workflow did not finish after PostgreSQL recovery: run=%+v err=%v", completed, err)
	}
	for _, task := range completed.Tasks {
		if task.Status != "SUCCEEDED" {
			t.Fatalf("task %q did not finish after PostgreSQL recovery: %+v", task.TaskKey, task)
		}
	}
	assertAttempt(t, ctx, store, activeID, 1, "worker-before-restart", "SUCCEEDED", "")
	assertAttempt(t, ctx, store, downstreamID, 1, "worker-after-restart", "SUCCEEDED", "")
	assertOutboxEvent(t, ctx, store, activeID, "task.succeeded")
	assertOutboxEvent(t, ctx, store, downstreamID, "task.succeeded")
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
	store := postgresStore(t, ctx)

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

func TestDuplicateDispatchIsLeasedOnce(t *testing.T) {
	ctx := context.Background()
	store := postgresStore(t, ctx)
	broker := redpandaBroker(t, ctx)
	tenantID := "00000000-0000-0000-0000-000000000001"
	userID := "00000000-0000-0000-0000-000000000001"
	dag := workflow.DAG{Tasks: map[string]workflow.TaskSpec{
		"deduplicated": {Handler: "test.deduplicated"},
	}}
	definition, err := store.CreateWorkflow(ctx, tenantID, userID, "duplicate-dispatch", dag)
	if err != nil {
		t.Fatal(err)
	}
	run, created, err := store.CreateRun(ctx, tenantID, userID, definition.ID, "duplicate-dispatch", json.RawMessage(`{"value":1}`))
	if err != nil || !created {
		t.Fatalf("create run: created=%v err=%v", created, err)
	}
	service := scheduler.Service{Store: store}
	if err = service.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	run, err = store.GetRun(ctx, tenantID, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	taskID := taskByKey(t, run, "deduplicated").ID
	var dispatchPayload []byte
	if err = store.Pool.QueryRow(ctx, `SELECT payload FROM outbox_events WHERE aggregate_id=$1 AND event_type='task.dispatch'`, taskID).Scan(&dispatchPayload); err != nil {
		t.Fatal(err)
	}

	topic := "duplicate-dispatch"
	createTopic(t, ctx, broker, topic)
	publisher := messaging.NewPublisher([]string{broker}, topic)
	t.Cleanup(func() { _ = publisher.Close() })
	for range 2 {
		if err = publisher.Publish(ctx, []byte(run.ID), dispatchPayload, map[string]string{"event_type": "task.dispatch"}); err != nil {
			t.Fatal(err)
		}
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker}, Topic: topic, GroupID: "duplicate-dispatch-workers",
		MinBytes: 1, MaxBytes: 1e6, StartOffset: kafka.FirstOffset,
	})
	t.Cleanup(func() { _ = reader.Close() })
	fetchContext, cancelFetch := context.WithTimeout(ctx, 10*time.Second)
	defer cancelFetch()
	messages := make([]kafka.Message, 2)
	for i := range messages {
		messages[i], err = reader.FetchMessage(fetchContext)
		if err != nil {
			t.Fatal(err)
		}
		var envelope struct {
			TaskRunID string `json:"task_run_id"`
		}
		if err = json.Unmarshal(messages[i].Value, &envelope); err != nil || envelope.TaskRunID != taskID {
			t.Fatalf("duplicate dispatch %d: task=%q err=%v", i, envelope.TaskRunID, err)
		}
	}

	type leaseResult struct {
		workerID string
		task     workflow.TaskRun
		err      error
	}
	results := make(chan leaseResult, 2)
	start := make(chan struct{})
	for i := range messages {
		workerID := fmt.Sprintf("duplicate-worker-%d", i+1)
		go func() {
			<-start
			task, leaseErr := store.LeaseTask(ctx, taskID, workerID, time.Minute)
			results <- leaseResult{workerID: workerID, task: task, err: leaseErr}
		}()
	}
	close(start)
	var winner, loser leaseResult
	for range messages {
		result := <-results
		switch {
		case result.err == nil:
			if winner.workerID != "" {
				t.Fatalf("multiple workers leased duplicate dispatch: first=%+v second=%+v", winner, result)
			}
			winner = result
		case errors.Is(result.err, storage.ErrLeaseLost):
			loser = result
		default:
			t.Fatalf("unexpected lease result: %+v", result)
		}
	}
	if winner.workerID == "" || loser.workerID == "" || winner.task.AttemptCount != 1 {
		t.Fatalf("duplicate dispatch results: winner=%+v loser=%+v", winner, loser)
	}
	var attemptCount, attemptRows int
	if err = store.Pool.QueryRow(ctx, `SELECT attempt_count,(SELECT count(*) FROM task_attempts WHERE task_run_id=$1) FROM task_runs WHERE id=$1`, taskID).Scan(&attemptCount, &attemptRows); err != nil {
		t.Fatal(err)
	}
	if attemptCount != 1 || attemptRows != 1 {
		t.Fatalf("duplicate delivery created attempts: task_count=%d rows=%d", attemptCount, attemptRows)
	}
	if _, err = store.StartTask(ctx, taskID, winner.workerID); err != nil {
		t.Fatal(err)
	}
	if _, err = store.StartTask(ctx, taskID, loser.workerID); !errors.Is(err, storage.ErrLeaseLost) {
		t.Fatalf("losing worker start error=%v, want ErrLeaseLost", err)
	}
	if _, err = store.CompleteTask(ctx, taskID, loser.workerID, json.RawMessage(`{"duplicate":true}`), ""); !errors.Is(err, storage.ErrLeaseLost) {
		t.Fatalf("losing worker completion error=%v, want ErrLeaseLost", err)
	}
	if _, err = store.CompleteTask(ctx, taskID, winner.workerID, json.RawMessage(`{"duplicate":false}`), ""); err != nil {
		t.Fatal(err)
	}
	if _, err = store.CompleteTask(ctx, taskID, winner.workerID, json.RawMessage(`{"duplicate":true}`), ""); !errors.Is(err, storage.ErrLeaseLost) {
		t.Fatalf("duplicate completion error=%v, want ErrLeaseLost", err)
	}
	if err = reader.CommitMessages(ctx, messages...); err != nil {
		t.Fatal(err)
	}
	completed, err := store.GetRun(ctx, tenantID, run.ID)
	if err != nil || completed.Status != "SUCCEEDED" || completed.Tasks[0].Status != "SUCCEEDED" {
		t.Fatalf("deduplicated workflow did not succeed: run=%+v err=%v", completed, err)
	}
	assertAttempt(t, ctx, store, taskID, 1, winner.workerID, "SUCCEEDED", "")
	assertOutboxEvent(t, ctx, store, taskID, "task.leased")
	assertOutboxEvent(t, ctx, store, taskID, "task.succeeded")
}

func TestTransactionalOutboxRecoversAfterBrokerOutage(t *testing.T) {
	ctx := context.Background()
	store := postgresStore(t, ctx)
	container, broker := redpandaContainer(t, ctx)
	topic := "outbox-recovery"
	createTopic(t, ctx, broker, topic)
	publisher := messaging.NewPublisher([]string{broker}, topic)
	t.Cleanup(func() { _ = publisher.Close() })
	service := scheduler.Service{Store: store, Publisher: publisher}
	tenantID := "00000000-0000-0000-0000-000000000001"
	userID := "00000000-0000-0000-0000-000000000001"
	dag := workflow.DAG{Tasks: map[string]workflow.TaskSpec{
		"durable": {Handler: "test.durable"},
	}}
	definition, err := store.CreateWorkflow(ctx, tenantID, userID, "outbox-recovery", dag)
	if err != nil {
		t.Fatal(err)
	}
	run, created, err := store.CreateRun(ctx, tenantID, userID, definition.ID, "outbox-recovery", json.RawMessage(`{"value":1}`))
	if err != nil || !created {
		t.Fatalf("create run: created=%v err=%v", created, err)
	}
	if err = service.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	run, err = store.GetRun(ctx, tenantID, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	taskID := taskByKey(t, run, "durable").ID
	if taskByKey(t, run, "durable").Status != "DISPATCHING" {
		t.Fatalf("task state was not committed before publication: %+v", run.Tasks)
	}

	type outboxEvent struct {
		id, eventType string
	}
	rows, err := store.Pool.Query(ctx, `SELECT id,event_type FROM outbox_events WHERE aggregate_id IN ($1,$2) AND published_at IS NULL ORDER BY created_at,id`, run.ID, taskID)
	if err != nil {
		t.Fatal(err)
	}
	var pending []outboxEvent
	for rows.Next() {
		var event outboxEvent
		if err = rows.Scan(&event.id, &event.eventType); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		pending = append(pending, event)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 {
		t.Fatalf("pending outbox events=%+v, want workflow creation and task dispatch", pending)
	}

	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	if err = provider.Client().ContainerPause(ctx, container.GetContainerID()); err != nil {
		t.Fatal(err)
	}
	brokerSuspended := true
	t.Cleanup(func() {
		if brokerSuspended {
			_ = provider.Client().ContainerUnpause(context.Background(), container.GetContainerID())
		}
	})
	downContext, cancelDown := context.WithTimeout(ctx, 2*time.Second)
	err = service.PublishOnce(downContext)
	cancelDown()
	if err == nil {
		t.Fatal("outbox publication succeeded while Redpanda was unavailable")
	}
	var unpublished int
	if err = store.Pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE id IN ($1,$2) AND published_at IS NULL`, pending[0].id, pending[1].id).Scan(&unpublished); err != nil {
		t.Fatal(err)
	}
	if unpublished != len(pending) {
		t.Fatalf("broker failure marked durable events published: unpublished=%d want=%d", unpublished, len(pending))
	}
	committed, err := store.GetRun(ctx, tenantID, run.ID)
	if err != nil || taskByKey(t, committed, "durable").Status != "DISPATCHING" {
		t.Fatalf("broker failure changed authoritative workflow state: run=%+v err=%v", committed, err)
	}

	if err = provider.Client().ContainerUnpause(ctx, container.GetContainerID()); err != nil {
		t.Fatal(err)
	}
	brokerSuspended = false
	recoveryContext, cancelRecovery := context.WithTimeout(ctx, 30*time.Second)
	defer cancelRecovery()
	for {
		err = service.PublishOnce(recoveryContext)
		if err == nil {
			break
		}
		select {
		case <-recoveryContext.Done():
			t.Fatalf("outbox did not recover before deadline: last error: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
	}
	if err = store.Pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE id IN ($1,$2) AND published_at IS NULL`, pending[0].id, pending[1].id).Scan(&unpublished); err != nil {
		t.Fatal(err)
	}
	if unpublished != 0 {
		t.Fatalf("outbox backlog did not drain after broker recovery: unpublished=%d", unpublished)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{broker}, Topic: topic, Partition: 0, MinBytes: 1, MaxBytes: 1e6, StartOffset: kafka.FirstOffset})
	t.Cleanup(func() { _ = reader.Close() })
	readContext, cancelRead := context.WithTimeout(ctx, 10*time.Second)
	defer cancelRead()
	want := make(map[string]string, len(pending))
	for _, event := range pending {
		want[event.id] = event.eventType
	}
	for range pending {
		message, readErr := reader.FetchMessage(readContext)
		if readErr != nil {
			t.Fatal(readErr)
		}
		eventID := headerValue(message.Headers, "event_id")
		eventType := headerValue(message.Headers, "event_type")
		if string(message.Key) != run.ID || want[eventID] != eventType {
			t.Fatalf("recovered message: key=%q event_id=%q event_type=%q", message.Key, eventID, eventType)
		}
		delete(want, eventID)
	}
	if len(want) != 0 {
		t.Fatalf("outbox messages were not recovered: %+v", want)
	}
	if err = service.PublishOnce(ctx); err != nil {
		t.Fatal(err)
	}
	noDuplicateContext, cancelNoDuplicate := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancelNoDuplicate()
	if message, readErr := reader.FetchMessage(noDuplicateContext); !errors.Is(readErr, context.DeadlineExceeded) {
		t.Fatalf("already-published events were sent again: message=%+v err=%v", message, readErr)
	}
}

func TestTransactionalOutboxRecoversAfterKafkaLatency(t *testing.T) {
	ctx := context.Background()
	store := postgresStore(t, ctx)
	container, broker := redpandaContainer(t, ctx)
	topic := "outbox-proxy-recovery"
	createTopic(t, ctx, broker, topic)
	proxyAddress, proxy := kafkaProxy(t, ctx, container)

	dialer := &net.Dialer{Timeout: 2 * time.Second}
	transport := &kafka.Transport{
		Dial: func(dialContext context.Context, network, _ string) (net.Conn, error) {
			return dialer.DialContext(dialContext, network, proxyAddress)
		},
		DialTimeout: 2 * time.Second,
	}
	publisher := messaging.NewPublisher([]string{broker}, topic, messaging.WithTransport(transport))
	t.Cleanup(func() { _ = publisher.Close() })
	service := scheduler.Service{Store: store, Publisher: publisher}
	tenantID := "00000000-0000-0000-0000-000000000001"
	userID := "00000000-0000-0000-0000-000000000001"
	dag := workflow.DAG{Tasks: map[string]workflow.TaskSpec{
		"durable": {Handler: "test.durable"},
	}}
	definition, err := store.CreateWorkflow(ctx, tenantID, userID, "outbox-proxy-recovery", dag)
	if err != nil {
		t.Fatal(err)
	}
	run, created, err := store.CreateRun(ctx, tenantID, userID, definition.ID, "outbox-proxy-recovery", json.RawMessage(`{"value":1}`))
	if err != nil || !created {
		t.Fatalf("create run: created=%v err=%v", created, err)
	}
	if err = service.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	run, err = store.GetRun(ctx, tenantID, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	taskID := taskByKey(t, run, "durable").ID
	if taskByKey(t, run, "durable").Status != "DISPATCHING" {
		t.Fatalf("task state was not committed before publication: %+v", run.Tasks)
	}

	type outboxEvent struct {
		id, eventType string
	}
	rows, err := store.Pool.Query(ctx, `SELECT id,event_type FROM outbox_events WHERE aggregate_id IN ($1,$2) AND published_at IS NULL ORDER BY created_at,id`, run.ID, taskID)
	if err != nil {
		t.Fatal(err)
	}
	var pending []outboxEvent
	for rows.Next() {
		var event outboxEvent
		if err = rows.Scan(&event.id, &event.eventType); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		pending = append(pending, event)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 {
		t.Fatalf("pending outbox events=%+v, want workflow creation and task dispatch", pending)
	}

	if _, err = proxy.AddToxic("kafka-latency", "latency", "downstream", 1, toxiproxyclient.Attributes{"latency": 5000, "jitter": 0}); err != nil {
		t.Fatal(err)
	}
	latencyContext, cancelLatency := context.WithTimeout(ctx, 750*time.Millisecond)
	err = service.PublishOnce(latencyContext)
	cancelLatency()
	if err == nil {
		t.Fatal("outbox publication succeeded through five seconds of Kafka latency")
	}
	var unpublished int
	if err = store.Pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE id IN ($1,$2) AND published_at IS NULL`, pending[0].id, pending[1].id).Scan(&unpublished); err != nil {
		t.Fatal(err)
	}
	if unpublished != len(pending) {
		t.Fatalf("Kafka latency marked durable events published: unpublished=%d want=%d", unpublished, len(pending))
	}
	committed, err := store.GetRun(ctx, tenantID, run.ID)
	if err != nil || taskByKey(t, committed, "durable").Status != "DISPATCHING" {
		t.Fatalf("Kafka latency changed authoritative workflow state: run=%+v err=%v", committed, err)
	}

	if err = proxy.RemoveToxic("kafka-latency"); err != nil {
		t.Fatal(err)
	}
	transport.CloseIdleConnections()
	recoveryContext, cancelRecovery := context.WithTimeout(ctx, 30*time.Second)
	defer cancelRecovery()
	for {
		err = service.PublishOnce(recoveryContext)
		if err == nil {
			break
		}
		select {
		case <-recoveryContext.Done():
			t.Fatalf("outbox did not recover before deadline: last error: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
	}
	if err = store.Pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE id IN ($1,$2) AND published_at IS NULL`, pending[0].id, pending[1].id).Scan(&unpublished); err != nil {
		t.Fatal(err)
	}
	if unpublished != 0 {
		t.Fatalf("outbox backlog did not drain after Kafka latency cleared: unpublished=%d", unpublished)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{broker}, Topic: topic, Partition: 0, MinBytes: 1, MaxBytes: 1e6, StartOffset: kafka.FirstOffset})
	t.Cleanup(func() { _ = reader.Close() })
	readContext, cancelRead := context.WithTimeout(ctx, 10*time.Second)
	defer cancelRead()
	want := make(map[string]string, len(pending))
	for _, event := range pending {
		want[event.id] = event.eventType
	}
	for range pending {
		message, readErr := reader.FetchMessage(readContext)
		if readErr != nil {
			t.Fatal(readErr)
		}
		eventID := headerValue(message.Headers, "event_id")
		eventType := headerValue(message.Headers, "event_type")
		if string(message.Key) != run.ID || want[eventID] != eventType {
			t.Fatalf("recovered message: key=%q event_id=%q event_type=%q", message.Key, eventID, eventType)
		}
		delete(want, eventID)
	}
	if len(want) != 0 {
		t.Fatalf("outbox messages were not recovered: %+v", want)
	}
	if err = service.PublishOnce(ctx); err != nil {
		t.Fatal(err)
	}
	noDuplicateContext, cancelNoDuplicate := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancelNoDuplicate()
	if message, readErr := reader.FetchMessage(noDuplicateContext); !errors.Is(readErr, context.DeadlineExceeded) {
		t.Fatalf("already-published events were sent again: message=%+v err=%v", message, readErr)
	}
}

func TestRunCancellationFencesWorkers(t *testing.T) {
	ctx := context.Background()
	store := postgresStore(t, ctx)
	tenantID := "00000000-0000-0000-0000-000000000001"
	userID := "00000000-0000-0000-0000-000000000001"
	service := scheduler.Service{Store: store}

	t.Run("leased and running workers observe cancellation", func(t *testing.T) {
		dag := workflow.DAG{Tasks: map[string]workflow.TaskSpec{
			"running":    {Handler: "test.running"},
			"leased":     {Handler: "test.leased"},
			"downstream": {Handler: "test.downstream", DependsOn: []string{"running"}},
		}}
		definition, err := store.CreateWorkflow(ctx, tenantID, userID, "cancellation", dag)
		if err != nil {
			t.Fatal(err)
		}
		run, created, err := store.CreateRun(ctx, tenantID, userID, definition.ID, "cancellation", json.RawMessage(`{"value":1}`))
		if err != nil || !created {
			t.Fatalf("create run: created=%v err=%v", created, err)
		}
		if err = service.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
		run, err = store.GetRun(ctx, tenantID, run.ID)
		if err != nil {
			t.Fatal(err)
		}
		runningID := taskByKey(t, run, "running").ID
		leasedID := taskByKey(t, run, "leased").ID
		downstreamID := taskByKey(t, run, "downstream").ID
		if _, err = store.LeaseTask(ctx, runningID, "worker-running", time.Minute); err != nil {
			t.Fatal(err)
		}
		if _, err = store.StartTask(ctx, runningID, "worker-running"); err != nil {
			t.Fatal(err)
		}
		if _, err = store.LeaseTask(ctx, leasedID, "worker-leased", time.Minute); err != nil {
			t.Fatal(err)
		}

		if err = store.CancelRun(ctx, tenantID, userID, run.ID); err != nil {
			t.Fatal(err)
		}
		if _, cancelled, heartbeatErr := store.HeartbeatTask(ctx, runningID, "worker-running", time.Minute); heartbeatErr != nil || !cancelled {
			t.Fatalf("running heartbeat: cancelled=%v err=%v", cancelled, heartbeatErr)
		}
		if _, cancelled, heartbeatErr := store.HeartbeatTask(ctx, leasedID, "worker-leased", time.Minute); heartbeatErr != nil || !cancelled {
			t.Fatalf("leased heartbeat: cancelled=%v err=%v", cancelled, heartbeatErr)
		}
		if _, err = store.StartTask(ctx, leasedID, "worker-leased"); !errors.Is(err, storage.ErrLeaseLost) {
			t.Fatalf("start after cancellation error=%v, want ErrLeaseLost", err)
		}
		if _, err = store.CompleteTask(ctx, runningID, "worker-running", json.RawMessage(`{"late":true}`), ""); !errors.Is(err, storage.ErrLeaseLost) {
			t.Fatalf("completion after cancellation error=%v, want ErrLeaseLost", err)
		}
		cancelFailure := storage.Failure{Retryable: true, ErrorType: "RunCancelled", ErrorMessage: "workflow run was cancelled"}
		if task, failErr := store.FailTask(ctx, runningID, "worker-running", cancelFailure); failErr != nil || task.Status != "CANCELLED" {
			t.Fatalf("cancel running task: task=%+v err=%v", task, failErr)
		}
		if task, failErr := store.FailTask(ctx, leasedID, "worker-leased", cancelFailure); failErr != nil || task.Status != "CANCELLED" {
			t.Fatalf("cancel leased task: task=%+v err=%v", task, failErr)
		}

		cancelledRun, err := store.GetRun(ctx, tenantID, run.ID)
		if err != nil || cancelledRun.Status != "CANCELLED" {
			t.Fatalf("cancelled run: run=%+v err=%v", cancelledRun, err)
		}
		for _, task := range cancelledRun.Tasks {
			if task.Status != "CANCELLED" {
				t.Fatalf("task %q survived cancellation: %+v", task.TaskKey, task)
			}
		}
		assertAttempt(t, ctx, store, runningID, 1, "worker-running", "CANCELLED", "RunCancelled")
		assertAttempt(t, ctx, store, leasedID, 1, "worker-leased", "CANCELLED", "RunCancelled")
		assertOutboxEvent(t, ctx, store, runningID, "task.cancelled")
		assertOutboxEvent(t, ctx, store, leasedID, "task.cancelled")
		var auditCount, successCount, downstreamDispatches int
		if err = store.Pool.QueryRow(ctx, `SELECT count(*) FROM audit_events WHERE resource_id=$1 AND action='run.cancel'`, run.ID).Scan(&auditCount); err != nil {
			t.Fatal(err)
		}
		if err = store.Pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE aggregate_id IN ($1,$2) AND event_type='task.succeeded'`, runningID, leasedID).Scan(&successCount); err != nil {
			t.Fatal(err)
		}
		if err = store.Pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE aggregate_id=$1 AND event_type='task.dispatch'`, downstreamID).Scan(&downstreamDispatches); err != nil {
			t.Fatal(err)
		}
		if auditCount != 1 || successCount != 0 || downstreamDispatches != 0 {
			t.Fatalf("cancellation side effects: audits=%d successes=%d downstream_dispatches=%d", auditCount, successCount, downstreamDispatches)
		}
	})

	t.Run("concurrent cancellation wins before completion", func(t *testing.T) {
		dag := workflow.DAG{Tasks: map[string]workflow.TaskSpec{
			"racing": {Handler: "test.racing"},
		}}
		definition, err := store.CreateWorkflow(ctx, tenantID, userID, "cancellation-race", dag)
		if err != nil {
			t.Fatal(err)
		}
		run, created, err := store.CreateRun(ctx, tenantID, userID, definition.ID, "cancellation-race", json.RawMessage(`{}`))
		if err != nil || !created {
			t.Fatalf("create run: created=%v err=%v", created, err)
		}
		if err = service.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
		run, err = store.GetRun(ctx, tenantID, run.ID)
		if err != nil {
			t.Fatal(err)
		}
		taskID := taskByKey(t, run, "racing").ID
		if _, err = store.LeaseTask(ctx, taskID, "worker-racing", time.Minute); err != nil {
			t.Fatal(err)
		}
		if _, err = store.StartTask(ctx, taskID, "worker-racing"); err != nil {
			t.Fatal(err)
		}

		cancelTx, err := store.Pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer cancelTx.Rollback(ctx)
		if _, err = cancelTx.Exec(ctx, `UPDATE workflow_runs SET status='CANCELLED',completed_at=now() WHERE id=$1`, run.ID); err != nil {
			t.Fatal(err)
		}
		completion := make(chan error, 1)
		go func() {
			_, completionErr := store.CompleteTask(ctx, taskID, "worker-racing", json.RawMessage(`{"late":true}`), "")
			completion <- completionErr
		}()
		select {
		case completionErr := <-completion:
			t.Fatalf("completion bypassed the in-flight cancellation lock: %v", completionErr)
		case <-time.After(200 * time.Millisecond):
		}
		if err = cancelTx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if completionErr := <-completion; !errors.Is(completionErr, storage.ErrLeaseLost) {
			t.Fatalf("concurrent completion error=%v, want ErrLeaseLost", completionErr)
		}
		if task, failErr := store.FailTask(ctx, taskID, "worker-racing", storage.Failure{Retryable: true, ErrorType: "RunCancelled", ErrorMessage: "workflow run was cancelled"}); failErr != nil || task.Status != "CANCELLED" {
			t.Fatalf("finish concurrent cancellation: task=%+v err=%v", task, failErr)
		}
		assertAttempt(t, ctx, store, taskID, 1, "worker-racing", "CANCELLED", "RunCancelled")
	})
}

func TestRetryExhaustionAndDeadLetterReplay(t *testing.T) {
	ctx := context.Background()
	store := postgresStore(t, ctx)
	tenantID := "00000000-0000-0000-0000-000000000001"
	userID := "00000000-0000-0000-0000-000000000001"
	dag := workflow.DAG{Tasks: map[string]workflow.TaskSpec{
		"unstable":    {Handler: "test.unstable", MaximumAttempts: 2},
		"independent": {Handler: "test.independent"},
		"downstream":  {Handler: "test.downstream", DependsOn: []string{"unstable"}},
	}}
	definition, err := store.CreateWorkflow(ctx, tenantID, userID, "dead-letter-replay", dag)
	if err != nil {
		t.Fatal(err)
	}
	run, created, err := store.CreateRun(ctx, tenantID, userID, definition.ID, "dead-letter-replay", json.RawMessage(`{"value":1}`))
	if err != nil || !created {
		t.Fatalf("create run: created=%v err=%v", created, err)
	}
	service := scheduler.Service{Store: store}
	if err = service.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	run, err = store.GetRun(ctx, tenantID, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	taskID := taskByKey(t, run, "unstable").ID

	first, err := store.LeaseTask(ctx, taskID, "worker-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.StartTask(ctx, taskID, "worker-1"); err != nil {
		t.Fatal(err)
	}
	failed, err := store.FailTask(ctx, taskID, "worker-1", storage.Failure{Retryable: true, ErrorType: "Transient", ErrorMessage: "first failure"})
	if err != nil || failed.Status != "RETRY_WAIT" {
		t.Fatalf("first failure: task=%+v err=%v", failed, err)
	}
	assertAttempt(t, ctx, store, taskID, 1, "worker-1", "RETRY_WAIT", "Transient")
	time.Sleep(time.Until(failed.AvailableAt) + 25*time.Millisecond)
	if err = service.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}

	second, err := store.LeaseTask(ctx, taskID, "worker-2", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.StartTask(ctx, taskID, "worker-2"); err != nil {
		t.Fatal(err)
	}
	dead, err := store.FailTask(ctx, taskID, "worker-2", storage.Failure{Retryable: true, ErrorType: "Transient", ErrorMessage: "second failure"})
	if err != nil || dead.Status != "DEAD" || second.AttemptCount != 2 {
		t.Fatalf("retry exhaustion: task=%+v lease=%+v err=%v", dead, second, err)
	}
	assertAttempt(t, ctx, store, taskID, 2, "worker-2", "DEAD", "Transient")
	assertOutboxEvent(t, ctx, store, taskID, "task.dead")
	failedRun, err := store.GetRun(ctx, tenantID, run.ID)
	if err != nil || failedRun.Status != "FAILED" || failedRun.CompletedAt == nil {
		t.Fatalf("workflow did not fail after exhaustion: run=%+v err=%v", failedRun, err)
	}
	if taskByKey(t, failedRun, "independent").Status != "CANCELLED" || taskByKey(t, failedRun, "downstream").Status != "CANCELLED" {
		t.Fatalf("unfinished tasks were not cancelled: %+v", failedRun.Tasks)
	}
	deadTasks, err := store.DeadLetter(ctx, tenantID)
	if err != nil || len(deadTasks) != 1 || deadTasks[0].ID != taskID {
		t.Fatalf("dead-letter listing: tasks=%+v err=%v", deadTasks, err)
	}

	if err = store.ReplayDead(ctx, tenantID, userID, taskID); err != nil {
		t.Fatal(err)
	}
	replayed, err := store.GetRun(ctx, tenantID, run.ID)
	if err != nil || replayed.Status != "RUNNING" || replayed.CompletedAt != nil {
		t.Fatalf("replay did not reopen workflow: run=%+v err=%v", replayed, err)
	}
	replayedTask := taskByKey(t, replayed, "unstable")
	if replayedTask.Status != "READY" || replayedTask.MaximumAttempts != 3 {
		t.Fatalf("replayed task is not claimable: %+v", replayedTask)
	}
	if taskByKey(t, replayed, "independent").Status != "READY" || taskByKey(t, replayed, "downstream").Status != "BLOCKED" {
		t.Fatalf("cancelled task states were not rebuilt: %+v", replayed.Tasks)
	}
	deadTasks, err = store.DeadLetter(ctx, tenantID)
	if err != nil || len(deadTasks) != 0 {
		t.Fatalf("replayed task remained in dead letter: tasks=%+v err=%v", deadTasks, err)
	}
	assertOutboxEvent(t, ctx, store, taskID, "task.replayed")
	var auditCount int
	if err = store.Pool.QueryRow(ctx, `SELECT count(*) FROM audit_events WHERE tenant_id=$1 AND actor_id=$2 AND action='task.replay' AND resource_id=$3`, tenantID, userID, taskID).Scan(&auditCount); err != nil || auditCount != 1 {
		t.Fatalf("replay audit count=%d err=%v", auditCount, err)
	}

	if err = service.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	independentID := taskByKey(t, replayed, "independent").ID
	if _, err = store.LeaseTask(ctx, independentID, "worker-independent", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err = store.StartTask(ctx, independentID, "worker-independent"); err != nil {
		t.Fatal(err)
	}
	if _, err = store.CompleteTask(ctx, independentID, "worker-independent", json.RawMessage(`{"independent":true}`), ""); err != nil {
		t.Fatal(err)
	}
	third, err := store.LeaseTask(ctx, taskID, "worker-3", time.Minute)
	if err != nil || third.AttemptCount != first.AttemptCount+2 {
		t.Fatalf("third lease: task=%+v err=%v", third, err)
	}
	if _, err = store.StartTask(ctx, taskID, "worker-3"); err != nil {
		t.Fatal(err)
	}
	if _, err = store.CompleteTask(ctx, taskID, "worker-3", json.RawMessage(`{"replayed":true}`), ""); err != nil {
		t.Fatal(err)
	}
	if err = service.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	beforeDownstream, err := store.GetRun(ctx, tenantID, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	downstreamID := taskByKey(t, beforeDownstream, "downstream").ID
	if _, err = store.LeaseTask(ctx, downstreamID, "worker-downstream", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err = store.StartTask(ctx, downstreamID, "worker-downstream"); err != nil {
		t.Fatal(err)
	}
	if _, err = store.CompleteTask(ctx, downstreamID, "worker-downstream", json.RawMessage(`{"downstream":true}`), ""); err != nil {
		t.Fatal(err)
	}
	completed, err := store.GetRun(ctx, tenantID, run.ID)
	if err != nil || completed.Status != "SUCCEEDED" {
		t.Fatalf("replayed workflow did not succeed: run=%+v err=%v", completed, err)
	}
	for _, completedTask := range completed.Tasks {
		if completedTask.Status != "SUCCEEDED" {
			t.Fatalf("task %q did not succeed after replay: %+v", completedTask.TaskKey, completedTask)
		}
	}
	assertAttempt(t, ctx, store, taskID, 3, "worker-3", "SUCCEEDED", "")
}

func taskByKey(t *testing.T, run workflow.Run, key string) workflow.TaskRun {
	t.Helper()
	for _, task := range run.Tasks {
		if task.TaskKey == key {
			return task
		}
	}
	t.Fatalf("task %q not found in run %+v", key, run)
	return workflow.TaskRun{}
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

func postgresStore(t *testing.T, ctx context.Context) *storage.Store {
	t.Helper()
	_, store := postgresStoreContainer(t, ctx)
	return store
}

func postgresStoreContainer(t *testing.T, ctx context.Context) (testcontainers.Container, *storage.Store) {
	t.Helper()
	return postgresStoreFromRequest(t, ctx, testcontainers.ContainerRequest{
		Image: "postgres:17-alpine", ExposedPorts: []string{"5432/tcp"},
		Env:        map[string]string{"POSTGRES_DB": "runmesh", "POSTGRES_USER": "runmesh", "POSTGRES_PASSWORD": "runmesh"},
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(time.Minute),
	})
}

func restartablePostgresStore(t *testing.T, ctx context.Context) (testcontainers.Container, *storage.Store) {
	t.Helper()
	return postgresStoreFromRequest(t, ctx, testcontainers.ContainerRequest{
		Image: "postgres:17-alpine", ExposedPorts: []string{"5432/tcp"},
		Env:        map[string]string{"POSTGRES_DB": "runmesh", "POSTGRES_USER": "runmesh", "POSTGRES_PASSWORD": "runmesh"},
		Entrypoint: []string{"sh", "-c"},
		Cmd: []string{`trap 'kill -TERM "$child" 2>/dev/null; wait "$child"; exit 0' TERM INT
while true; do
  while [ -f /tmp/runmesh-hold-postgres ]; do sleep 0.1; done
  /usr/local/bin/docker-entrypoint.sh postgres &
  child=$!
  wait "$child"
done`},
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(time.Minute),
	})
}

func postgresStoreFromRequest(t *testing.T, ctx context.Context, request testcontainers.ContainerRequest) (testcontainers.Container, *storage.Store) {
	t.Helper()
	container := start(t, ctx, request)
	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "5432/tcp")
	databaseURL := fmt.Sprintf("postgres://runmesh:runmesh@%s:%s/runmesh?sslmode=disable", host, port.Port())
	applyMigrations(t, ctx, databaseURL)
	store, err := storage.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	return container, store
}

func redpandaBroker(t *testing.T, ctx context.Context) string {
	t.Helper()
	_, broker := redpandaContainer(t, ctx)
	return broker
}

func redpandaContainer(t *testing.T, ctx context.Context) (*redpanda.Container, string) {
	t.Helper()
	container, err := redpanda.Run(ctx, "redpandadata/redpanda:v24.3.15", redpanda.WithAutoCreateTopics())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	broker, err := container.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return container, broker
}

func kafkaProxy(t *testing.T, ctx context.Context, broker *redpanda.Container) (string, *toxiproxyclient.Proxy) {
	t.Helper()
	brokerIP, err := broker.ContainerIP(ctx)
	if err != nil {
		t.Fatal(err)
	}
	container, err := tctoxiproxy.Run(ctx, "ghcr.io/shopify/toxiproxy:2.12.0", tctoxiproxy.WithProxy("kafka", net.JoinHostPort(brokerIP, "9092")))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	host, port, err := container.ProxiedEndpoint(8666)
	if err != nil {
		t.Fatal(err)
	}
	uri, err := container.URI(ctx)
	if err != nil {
		t.Fatal(err)
	}
	proxy, err := toxiproxyclient.NewClient(uri).Proxy("kafka")
	if err != nil {
		t.Fatal(err)
	}
	return net.JoinHostPort(host, port), proxy
}

func createTopic(t *testing.T, ctx context.Context, broker, topic string) {
	t.Helper()
	connection, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if err = connection.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
}

func headerValue(headers []kafka.Header, key string) string {
	for _, header := range headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
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
