package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/runmesh/runmesh/internal/tracecontext"
	"github.com/runmesh/runmesh/internal/workflow"
)

type Failure struct {
	Retryable    bool   `json:"retryable"`
	ErrorType    string `json:"error_type"`
	ErrorMessage string `json:"error_message"`
	TraceID      string `json:"trace_id"`
}

func (s *Store) LeaseTask(ctx context.Context, taskID, workerID string, lease time.Duration) (workflow.TaskRun, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return workflow.TaskRun{}, err
	}
	defer tx.Rollback(ctx)
	var t workflow.TaskRun
	err = tx.QueryRow(ctx, `UPDATE task_runs SET status='LEASED',lease_owner=$2,lease_expires_at=$3,attempt_count=attempt_count+1,updated_at=now()
		WHERE id=$1 AND status='DISPATCHING'
		RETURNING id,workflow_run_id,task_key,handler,status,priority,available_at,attempt_count,maximum_attempts,timeout_seconds,lease_owner,lease_expires_at,input,output,output_artifact_uri`, taskID, workerID, nowPlus(lease)).Scan(&t.ID, &t.WorkflowRunID, &t.TaskKey, &t.Handler, &t.Status, &t.Priority, &t.AvailableAt, &t.AttemptCount, &t.MaximumAttempts, &t.TimeoutSeconds, &t.LeaseOwner, &t.LeaseExpiresAt, &t.Input, &t.Output, &t.OutputArtifactURI)
	if errors.Is(err, pgx.ErrNoRows) {
		return t, ErrLeaseLost
	}
	if err != nil {
		return t, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO task_attempts(task_run_id,attempt_number,worker_id) VALUES($1,$2,$3)`, t.ID, t.AttemptCount, workerID)
	if err != nil {
		return t, err
	}
	payload, _ := json.Marshal(map[string]any{"workflow_run_id": t.WorkflowRunID, "task_run_id": t.ID, "task_key": t.TaskKey, "worker_id": workerID, "attempt": t.AttemptCount})
	_, err = tx.Exec(ctx, `INSERT INTO outbox_events(aggregate_type,aggregate_id,event_type,payload,trace_parent) VALUES('task_run',$1,'task.leased',$2,$3)`, t.ID, payload, tracecontext.FromContext(ctx))
	if err != nil {
		return t, err
	}
	return t, tx.Commit(ctx)
}

func (s *Store) StartTask(ctx context.Context, taskID, workerID string) (workflow.TaskRun, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return workflow.TaskRun{}, err
	}
	defer tx.Rollback(ctx)
	var runStatus string
	err = tx.QueryRow(ctx, `SELECT r.status::text FROM task_runs t JOIN workflow_runs r ON r.id=t.workflow_run_id WHERE t.id=$1 AND t.status='LEASED' AND t.lease_owner=$2 AND t.lease_expires_at>now() FOR UPDATE OF r,t`, taskID, workerID).Scan(&runStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		return workflow.TaskRun{}, ErrLeaseLost
	}
	if err != nil {
		return workflow.TaskRun{}, err
	}
	if runStatus != "RUNNING" {
		return workflow.TaskRun{}, ErrLeaseLost
	}
	var t workflow.TaskRun
	err = tx.QueryRow(ctx, `UPDATE task_runs SET status='RUNNING',updated_at=now() WHERE id=$1 AND status='LEASED' AND lease_owner=$2 AND lease_expires_at>now() RETURNING id,workflow_run_id,task_key,handler,status,priority,available_at,attempt_count,maximum_attempts,timeout_seconds,lease_owner,lease_expires_at,input,output,output_artifact_uri`, taskID, workerID).Scan(&t.ID, &t.WorkflowRunID, &t.TaskKey, &t.Handler, &t.Status, &t.Priority, &t.AvailableAt, &t.AttemptCount, &t.MaximumAttempts, &t.TimeoutSeconds, &t.LeaseOwner, &t.LeaseExpiresAt, &t.Input, &t.Output, &t.OutputArtifactURI)
	if errors.Is(err, pgx.ErrNoRows) {
		return t, ErrLeaseLost
	}
	if err != nil {
		return t, err
	}
	return t, tx.Commit(ctx)
}

func (s *Store) HeartbeatTask(ctx context.Context, taskID, workerID string, lease time.Duration) (time.Time, bool, error) {
	expires := nowPlus(lease)
	var cancelled bool
	err := s.Pool.QueryRow(ctx, `WITH renewed AS (UPDATE task_runs SET lease_expires_at=$3,updated_at=now() WHERE id=$1 AND status IN ('LEASED','RUNNING') AND lease_owner=$2 AND lease_expires_at>now() RETURNING workflow_run_id) SELECT wr.status='CANCELLED' FROM renewed r JOIN workflow_runs wr ON wr.id=r.workflow_run_id`, taskID, workerID, expires).Scan(&cancelled)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, false, ErrLeaseLost
	}
	return expires, cancelled, err
}

func (s *Store) CompleteTask(ctx context.Context, taskID, workerID string, output json.RawMessage, artifactURI string) (workflow.TaskRun, error) {
	if len(output) == 0 {
		output = []byte(`{}`)
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return workflow.TaskRun{}, err
	}
	defer tx.Rollback(ctx)
	var runStatus string
	err = tx.QueryRow(ctx, `SELECT r.status::text FROM task_runs t JOIN workflow_runs r ON r.id=t.workflow_run_id WHERE t.id=$1 AND t.status='RUNNING' AND t.lease_owner=$2 AND t.lease_expires_at>now() FOR UPDATE OF r,t`, taskID, workerID).Scan(&runStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		return workflow.TaskRun{}, ErrLeaseLost
	}
	if err != nil {
		return workflow.TaskRun{}, err
	}
	if runStatus != "RUNNING" {
		return workflow.TaskRun{}, ErrLeaseLost
	}
	var t workflow.TaskRun
	err = tx.QueryRow(ctx, `UPDATE task_runs SET status='SUCCEEDED',output=$3,output_artifact_uri=NULLIF($4,''),lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND status='RUNNING' AND lease_owner=$2 AND lease_expires_at>now() RETURNING id,workflow_run_id,task_key,handler,status,priority,available_at,attempt_count,maximum_attempts,timeout_seconds,lease_owner,lease_expires_at,input,output,output_artifact_uri`, taskID, workerID, output, artifactURI).Scan(&t.ID, &t.WorkflowRunID, &t.TaskKey, &t.Handler, &t.Status, &t.Priority, &t.AvailableAt, &t.AttemptCount, &t.MaximumAttempts, &t.TimeoutSeconds, &t.LeaseOwner, &t.LeaseExpiresAt, &t.Input, &t.Output, &t.OutputArtifactURI)
	if errors.Is(err, pgx.ErrNoRows) {
		return t, ErrLeaseLost
	}
	if err != nil {
		return t, err
	}
	_, err = tx.Exec(ctx, `UPDATE task_attempts SET ended_at=now(),exit_status='SUCCEEDED',artifact_uri=NULLIF($3,'') WHERE task_run_id=$1 AND attempt_number=$2`, t.ID, t.AttemptCount, artifactURI)
	if err != nil {
		return t, err
	}
	// A blocked task is ready only when every upstream task has succeeded.
	rows, err := tx.Query(ctx, `UPDATE task_runs child SET status='READY',available_at=now(),updated_at=now() WHERE child.workflow_run_id=$1 AND child.status='BLOCKED' AND EXISTS(SELECT 1 FROM task_dependencies d WHERE d.task_run_id=child.id AND d.depends_on_task_run_id=$2) AND NOT EXISTS(SELECT 1 FROM task_dependencies d JOIN task_runs upstream ON upstream.id=d.depends_on_task_run_id WHERE d.task_run_id=child.id AND upstream.status<>'SUCCEEDED') RETURNING child.id,child.task_key`, t.WorkflowRunID, t.ID)
	if err != nil {
		return t, err
	}
	type child struct{ id, key string }
	var children []child
	for rows.Next() {
		var c child
		if err = rows.Scan(&c.id, &c.key); err != nil {
			rows.Close()
			return t, err
		}
		children = append(children, c)
	}
	rows.Close()
	for _, c := range children {
		payload, _ := json.Marshal(map[string]any{"workflow_run_id": t.WorkflowRunID, "task_run_id": c.id, "task_key": c.key})
		if _, err = tx.Exec(ctx, `INSERT INTO outbox_events(aggregate_type,aggregate_id,event_type,payload,trace_parent) VALUES('task_run',$1,'task.ready',$2,$3)`, c.id, payload, tracecontext.FromContext(ctx)); err != nil {
			return t, err
		}
	}
	payload, _ := json.Marshal(map[string]any{"workflow_run_id": t.WorkflowRunID, "task_run_id": t.ID, "task_key": t.TaskKey, "attempt": t.AttemptCount})
	if _, err = tx.Exec(ctx, `INSERT INTO outbox_events(aggregate_type,aggregate_id,event_type,payload,trace_parent) VALUES('task_run',$1,'task.succeeded',$2,$3)`, t.ID, payload, tracecontext.FromContext(ctx)); err != nil {
		return t, err
	}
	var remaining int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM task_runs WHERE workflow_run_id=$1 AND status<>'SUCCEEDED'`, t.WorkflowRunID).Scan(&remaining); err != nil {
		return t, err
	}
	if remaining == 0 {
		_, err = tx.Exec(ctx, `UPDATE workflow_runs SET status='SUCCEEDED',completed_at=now() WHERE id=$1 AND status='RUNNING'`, t.WorkflowRunID)
		if err != nil {
			return t, err
		}
	}
	return t, tx.Commit(ctx)
}

func (s *Store) FailTask(ctx context.Context, taskID, workerID string, f Failure) (workflow.TaskRun, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return workflow.TaskRun{}, err
	}
	defer tx.Rollback(ctx)
	var attempt, max int
	var runID, taskKey, runStatus string
	err = tx.QueryRow(ctx, `SELECT t.workflow_run_id,t.task_key,t.attempt_count,t.maximum_attempts,r.status::text FROM task_runs t JOIN workflow_runs r ON r.id=t.workflow_run_id WHERE t.id=$1 AND t.status IN ('LEASED','RUNNING') AND t.lease_owner=$2 AND t.lease_expires_at>now() FOR UPDATE OF t`, taskID, workerID).Scan(&runID, &taskKey, &attempt, &max, &runStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		return workflow.TaskRun{}, ErrLeaseLost
	}
	if err != nil {
		return workflow.TaskRun{}, err
	}
	status := "DEAD"
	available := time.Now().UTC()
	if runStatus == "CANCELLED" {
		status = "CANCELLED"
	} else if f.Retryable && attempt < max {
		status = "RETRY_WAIT"
		available = nowPlus(workflow.RetryDelay(attempt))
	}
	var t workflow.TaskRun
	err = tx.QueryRow(ctx, `UPDATE task_runs SET status=$3,available_at=$4,lease_owner=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND lease_owner=$2 RETURNING id,workflow_run_id,task_key,handler,status,priority,available_at,attempt_count,maximum_attempts,timeout_seconds,lease_owner,lease_expires_at,input,output,output_artifact_uri`, taskID, workerID, status, available).Scan(&t.ID, &t.WorkflowRunID, &t.TaskKey, &t.Handler, &t.Status, &t.Priority, &t.AvailableAt, &t.AttemptCount, &t.MaximumAttempts, &t.TimeoutSeconds, &t.LeaseOwner, &t.LeaseExpiresAt, &t.Input, &t.Output, &t.OutputArtifactURI)
	if err != nil {
		return t, err
	}
	_, err = tx.Exec(ctx, `UPDATE task_attempts SET ended_at=now(),exit_status=$3,error_type=$4,error_message=$5,trace_id=NULLIF($6,'') WHERE task_run_id=$1 AND attempt_number=$2`, taskID, attempt, status, f.ErrorType, f.ErrorMessage, f.TraceID)
	if err != nil {
		return t, err
	}
	event := "task.retry_wait"
	if status == "CANCELLED" {
		event = "task.cancelled"
	}
	if status == "DEAD" {
		event = "task.dead"
		_, err = tx.Exec(ctx, `UPDATE workflow_runs SET status='FAILED',completed_at=now() WHERE id=$1 AND status='RUNNING'`, runID)
		if err != nil {
			return t, err
		}
		_, err = tx.Exec(ctx, `UPDATE task_runs SET status='CANCELLED',updated_at=now() WHERE workflow_run_id=$1 AND status IN ('BLOCKED','READY','DISPATCHING','RETRY_WAIT')`, runID)
		if err != nil {
			return t, err
		}
	}
	payload, _ := json.Marshal(map[string]any{"workflow_run_id": runID, "task_run_id": taskID, "task_key": taskKey, "attempt": attempt, "error_type": f.ErrorType, "error_message": f.ErrorMessage, "available_at": available})
	_, err = tx.Exec(ctx, `INSERT INTO outbox_events(aggregate_type,aggregate_id,event_type,payload,trace_parent) VALUES('task_run',$1,$2,$3,$4)`, taskID, event, payload, tracecontext.FromContext(ctx))
	if err != nil {
		return t, err
	}
	return t, tx.Commit(ctx)
}

func (s *Store) WorkerHeartbeat(ctx context.Context, workerID string, handlers []string, active int, metadata json.RawMessage) error {
	if len(metadata) == 0 {
		metadata = []byte(`{}`)
	}
	_, err := s.Pool.Exec(ctx, `INSERT INTO worker_heartbeats(worker_id,handlers,active_tasks,metadata,last_seen_at) VALUES($1,$2,$3,$4,now()) ON CONFLICT(worker_id) DO UPDATE SET handlers=excluded.handlers,active_tasks=excluded.active_tasks,metadata=excluded.metadata,last_seen_at=now()`, workerID, handlers, active, metadata)
	return err
}

type WorkerInfo struct {
	WorkerID    string          `json:"worker_id"`
	Handlers    []string        `json:"handlers"`
	ActiveTasks int             `json:"active_tasks"`
	Metadata    json.RawMessage `json:"metadata"`
	LastSeenAt  time.Time       `json:"last_seen_at"`
}

func (s *Store) ListWorkers(ctx context.Context) ([]WorkerInfo, error) {
	rows, err := s.Pool.Query(ctx, `SELECT worker_id,handlers,active_tasks,metadata,last_seen_at FROM worker_heartbeats ORDER BY last_seen_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []WorkerInfo
	for rows.Next() {
		var w WorkerInfo
		if err = rows.Scan(&w.WorkerID, &w.Handlers, &w.ActiveTasks, &w.Metadata, &w.LastSeenAt); err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

func (s *Store) DeadLetter(ctx context.Context, tenantID string) ([]workflow.TaskRun, error) {
	rows, err := s.Pool.Query(ctx, `SELECT t.id,t.workflow_run_id,t.task_key,t.handler,t.status,t.priority,t.available_at,t.attempt_count,t.maximum_attempts,t.timeout_seconds,t.lease_owner,t.lease_expires_at,t.input,t.output,t.output_artifact_uri FROM task_runs t JOIN workflow_runs r ON r.id=t.workflow_run_id WHERE r.tenant_id=$1 AND t.status='DEAD' ORDER BY t.updated_at DESC`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []workflow.TaskRun
	for rows.Next() {
		var t workflow.TaskRun
		if err = rows.Scan(&t.ID, &t.WorkflowRunID, &t.TaskKey, &t.Handler, &t.Status, &t.Priority, &t.AvailableAt, &t.AttemptCount, &t.MaximumAttempts, &t.TimeoutSeconds, &t.LeaseOwner, &t.LeaseExpiresAt, &t.Input, &t.Output, &t.OutputArtifactURI); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}
func (s *Store) ReplayDead(ctx context.Context, tenantID, userID, taskID string) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var runID, taskKey string
	err = tx.QueryRow(ctx, `SELECT t.workflow_run_id,t.task_key FROM task_runs t JOIN workflow_runs r ON r.id=t.workflow_run_id WHERE t.id=$1 AND r.tenant_id=$2 AND t.status='DEAD' FOR UPDATE OF t,r`, taskID, tenantID).Scan(&runID, &taskKey)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE workflow_runs SET status='RUNNING',completed_at=NULL WHERE id=$1 AND status='FAILED'`, runID); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE task_runs SET status='READY',available_at=now(),maximum_attempts=GREATEST(maximum_attempts,attempt_count+1),updated_at=now() WHERE id=$1`, taskID); err != nil {
		return err
	}
	// Retry exhaustion cancels work that has not started. Rebuild those states
	// from dependency completion and any retry delay that was already assigned.
	if _, err = tx.Exec(ctx, `UPDATE task_runs t SET status=CASE WHEN EXISTS(SELECT 1 FROM task_dependencies d JOIN task_runs upstream ON upstream.id=d.depends_on_task_run_id WHERE d.task_run_id=t.id AND upstream.status<>'SUCCEEDED') THEN 'BLOCKED'::task_status WHEN t.available_at>now() THEN 'RETRY_WAIT'::task_status ELSE 'READY'::task_status END,updated_at=now() WHERE t.workflow_run_id=$1 AND t.status='CANCELLED'`, runID); err != nil {
		return err
	}
	payload := mustJSON(map[string]any{"workflow_run_id": runID, "task_run_id": taskID, "task_key": taskKey})
	if _, err = tx.Exec(ctx, `INSERT INTO outbox_events(aggregate_type,aggregate_id,event_type,payload,trace_parent) SELECT 'task_run',$1,'task.replayed',$2,trace_parent FROM workflow_runs WHERE id=$3`, taskID, payload, runID); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO audit_events(tenant_id,actor_id,action,resource_type,resource_id) VALUES($1,$2,'task.replay','task_run',$3)`, tenantID, nullableUUID(userID), taskID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("marshal internal event: %v", err))
	}
	return b
}
