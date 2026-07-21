package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/samarth1412/RunMesh/internal/tracecontext"
	"github.com/samarth1412/RunMesh/internal/workflow"
)

var ErrNotFound = errors.New("not found")
var ErrConflict = errors.New("conflict")
var ErrLeaseLost = errors.New("lease lost")

type Store struct{ Pool *pgxpool.Pool }

func New(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{Pool: pool}, nil
}
func (s *Store) Close()                         { s.Pool.Close() }
func (s *Store) Ping(ctx context.Context) error { return s.Pool.Ping(ctx) }

func (s *Store) CreateWorkflow(ctx context.Context, tenantID, userID, name string, dag workflow.DAG) (workflow.Definition, error) {
	if err := workflow.ValidateDAG(dag); err != nil {
		return workflow.Definition{}, err
	}
	if name == "" {
		name = dag.Name
	}
	if name == "" {
		return workflow.Definition{}, fmt.Errorf("name is required")
	}
	raw, _ := json.Marshal(dag)
	var d workflow.Definition
	var dagRaw []byte
	err := s.Pool.QueryRow(ctx, `INSERT INTO workflow_definitions (tenant_id,name,version,dag_spec,created_by)
		VALUES ($1,$2,1,$3,$4) RETURNING id,tenant_id,name,version,dag_spec,created_at`, tenantID, name, raw, nullableUUID(userID)).Scan(&d.ID, &d.TenantID, &d.Name, &d.Version, &dagRaw, &d.CreatedAt)
	if err != nil {
		if isUnique(err) {
			return d, ErrConflict
		}
		return d, err
	}
	_ = json.Unmarshal(dagRaw, &d.DAG)
	_, _ = s.Pool.Exec(ctx, `INSERT INTO audit_events(tenant_id,actor_id,action,resource_type,resource_id) VALUES($1,$2,'workflow.create','workflow',$3)`, tenantID, nullableUUID(userID), d.ID)
	return d, nil
}

func (s *Store) CreateVersion(ctx context.Context, tenantID, userID, definitionID string, dag workflow.DAG) (workflow.Definition, error) {
	if err := workflow.ValidateDAG(dag); err != nil {
		return workflow.Definition{}, err
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return workflow.Definition{}, err
	}
	defer tx.Rollback(ctx)
	var name string
	if err := tx.QueryRow(ctx, `SELECT name FROM workflow_definitions WHERE id=$1 AND tenant_id=$2`, definitionID, tenantID).Scan(&name); errors.Is(err, pgx.ErrNoRows) {
		return workflow.Definition{}, ErrNotFound
	} else if err != nil {
		return workflow.Definition{}, err
	}
	var version int
	if err := tx.QueryRow(ctx, `SELECT version FROM workflow_definitions WHERE tenant_id=$1 AND name=$2 ORDER BY version DESC LIMIT 1 FOR UPDATE`, tenantID, name).Scan(&version); err != nil {
		return workflow.Definition{}, err
	}
	raw, _ := json.Marshal(dag)
	var d workflow.Definition
	var dagRaw []byte
	err = tx.QueryRow(ctx, `INSERT INTO workflow_definitions(tenant_id,name,version,dag_spec,created_by) VALUES($1,$2,$3,$4,$5) RETURNING id,tenant_id,name,version,dag_spec,created_at`, tenantID, name, version+1, raw, nullableUUID(userID)).Scan(&d.ID, &d.TenantID, &d.Name, &d.Version, &dagRaw, &d.CreatedAt)
	if err != nil {
		return workflow.Definition{}, err
	}
	_ = json.Unmarshal(dagRaw, &d.DAG)
	if _, err = tx.Exec(ctx, `INSERT INTO audit_events(tenant_id,actor_id,action,resource_type,resource_id,metadata) VALUES($1,$2,'workflow.version','workflow',$3,jsonb_build_object('version',$4))`, tenantID, nullableUUID(userID), d.ID, d.Version); err != nil {
		return d, err
	}
	return d, tx.Commit(ctx)
}

func (s *Store) ListWorkflows(ctx context.Context, tenantID string) ([]workflow.Definition, error) {
	rows, err := s.Pool.Query(ctx, `SELECT DISTINCT ON (name) id,tenant_id,name,version,dag_spec,created_at FROM workflow_definitions WHERE tenant_id=$1 ORDER BY name,version DESC`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []workflow.Definition
	for rows.Next() {
		var d workflow.Definition
		var raw []byte
		if err := rows.Scan(&d.ID, &d.TenantID, &d.Name, &d.Version, &raw, &d.CreatedAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(raw, &d.DAG)
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Store) GetWorkflow(ctx context.Context, tenantID, id string) (workflow.Definition, error) {
	var d workflow.Definition
	var raw []byte
	err := s.Pool.QueryRow(ctx, `SELECT id,tenant_id,name,version,dag_spec,created_at FROM workflow_definitions WHERE id=$1 AND tenant_id=$2`, id, tenantID).Scan(&d.ID, &d.TenantID, &d.Name, &d.Version, &raw, &d.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return d, ErrNotFound
	}
	if err == nil {
		_ = json.Unmarshal(raw, &d.DAG)
	}
	return d, err
}

func (s *Store) CreateRun(ctx context.Context, tenantID, userID, definitionID, idempotencyKey string, input json.RawMessage, inputArtifactIDs ...string) (workflow.Run, bool, error) {
	if idempotencyKey == "" {
		return workflow.Run{}, false, fmt.Errorf("idempotency key is required")
	}
	if len(input) == 0 {
		input = []byte(`{}`)
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return workflow.Run{}, false, err
	}
	defer tx.Rollback(ctx)
	var inputArtifactID, inputArtifactURI string
	if len(inputArtifactIDs) > 0 {
		inputArtifactID = inputArtifactIDs[0]
	}
	if inputArtifactID != "" {
		err = tx.QueryRow(ctx, `SELECT object_uri FROM artifacts WHERE id=$1 AND tenant_id=$2 AND kind='input' AND status='READY' FOR UPDATE`, inputArtifactID, tenantID).Scan(&inputArtifactURI)
		if errors.Is(err, pgx.ErrNoRows) {
			return workflow.Run{}, false, ErrNotFound
		}
		if err != nil {
			return workflow.Run{}, false, err
		}
	}
	var version int
	var dagRaw []byte
	err = tx.QueryRow(ctx, `SELECT version,dag_spec FROM workflow_definitions WHERE id=$1 AND tenant_id=$2`, definitionID, tenantID).Scan(&version, &dagRaw)
	if errors.Is(err, pgx.ErrNoRows) {
		return workflow.Run{}, false, ErrNotFound
	}
	if err != nil {
		return workflow.Run{}, false, err
	}
	var dag workflow.DAG
	if err = json.Unmarshal(dagRaw, &dag); err != nil {
		return workflow.Run{}, false, err
	}
	var run workflow.Run
	traceParent := tracecontext.FromContext(ctx)
	err = tx.QueryRow(ctx, `INSERT INTO workflow_runs(tenant_id,workflow_definition_id,workflow_version,status,input,input_artifact_uri,idempotency_key,started_at,trace_parent) VALUES($1,$2,$3,'RUNNING',$4,NULLIF($5,''),$6,now(),$7) ON CONFLICT(tenant_id,idempotency_key) DO NOTHING RETURNING id,workflow_definition_id,workflow_version,status,input,input_artifact_uri,idempotency_key,started_at,completed_at,created_at`, tenantID, definitionID, version, input, inputArtifactURI, idempotencyKey, traceParent).Scan(&run.ID, &run.WorkflowDefinitionID, &run.WorkflowVersion, &run.Status, &run.Input, &run.InputArtifactURI, &run.IdempotencyKey, &run.StartedAt, &run.CompletedAt, &run.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		existing, e := s.getRunTx(ctx, tx, tenantID, idempotencyKey)
		return existing, false, e
	}
	if err != nil {
		return run, false, err
	}
	if inputArtifactID != "" {
		command, updateErr := tx.Exec(ctx, `UPDATE artifacts SET workflow_run_id=$2 WHERE id=$1 AND workflow_run_id IS NULL`, inputArtifactID, run.ID)
		if updateErr != nil {
			return run, false, updateErr
		}
		if command.RowsAffected() != 1 {
			return run, false, ErrConflict
		}
	}
	ids := make(map[string]string, len(dag.Tasks))
	for key := range dag.Tasks {
		ids[key] = uuid.NewString()
	}
	for key, spec := range dag.Tasks {
		spec = workflow.WithDefaults(spec)
		status := "BLOCKED"
		if len(spec.DependsOn) == 0 {
			status = "READY"
		}
		_, err = tx.Exec(ctx, `INSERT INTO task_runs(id,workflow_run_id,task_key,handler,status,priority,maximum_attempts,timeout_seconds,input) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, ids[key], run.ID, key, spec.Handler, status, spec.Priority, spec.MaximumAttempts, spec.TimeoutSeconds, input)
		if err != nil {
			return run, false, err
		}
	}
	for key, spec := range dag.Tasks {
		for _, dep := range spec.DependsOn {
			_, err = tx.Exec(ctx, `INSERT INTO task_dependencies(workflow_run_id,task_run_id,depends_on_task_run_id) VALUES($1,$2,$3)`, run.ID, ids[key], ids[dep])
			if err != nil {
				return run, false, err
			}
		}
	}
	payload, _ := json.Marshal(map[string]any{"workflow_run_id": run.ID, "definition_id": definitionID, "status": "RUNNING"})
	_, err = tx.Exec(ctx, `INSERT INTO outbox_events(aggregate_type,aggregate_id,event_type,payload,trace_parent) VALUES('workflow_run',$1,'workflow.run.created',$2,$3)`, run.ID, payload, traceParent)
	if err != nil {
		return run, false, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO audit_events(tenant_id,actor_id,action,resource_type,resource_id) VALUES($1,$2,'run.create','workflow_run',$3)`, tenantID, nullableUUID(userID), run.ID)
	if err != nil {
		return run, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return run, false, err
	}
	return run, true, nil
}

func (s *Store) getRunTx(ctx context.Context, tx pgx.Tx, tenantID, idempotencyKey string) (workflow.Run, error) {
	var r workflow.Run
	err := tx.QueryRow(ctx, `SELECT id,workflow_definition_id,workflow_version,status,input,input_artifact_uri,idempotency_key,started_at,completed_at,created_at FROM workflow_runs WHERE tenant_id=$1 AND idempotency_key=$2`, tenantID, idempotencyKey).Scan(&r.ID, &r.WorkflowDefinitionID, &r.WorkflowVersion, &r.Status, &r.Input, &r.InputArtifactURI, &r.IdempotencyKey, &r.StartedAt, &r.CompletedAt, &r.CreatedAt)
	return r, err
}

func (s *Store) GetRun(ctx context.Context, tenantID, id string) (workflow.Run, error) {
	var r workflow.Run
	err := s.Pool.QueryRow(ctx, `SELECT id,workflow_definition_id,workflow_version,status,input,input_artifact_uri,idempotency_key,started_at,completed_at,created_at FROM workflow_runs WHERE id=$1 AND tenant_id=$2`, id, tenantID).Scan(&r.ID, &r.WorkflowDefinitionID, &r.WorkflowVersion, &r.Status, &r.Input, &r.InputArtifactURI, &r.IdempotencyKey, &r.StartedAt, &r.CompletedAt, &r.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, ErrNotFound
	}
	if err != nil {
		return r, err
	}
	rows, err := s.Pool.Query(ctx, `SELECT id,workflow_run_id,task_key,handler,status,priority,available_at,attempt_count,maximum_attempts,timeout_seconds,lease_owner,lease_expires_at,input,output,output_artifact_uri,
		(SELECT log_artifact_uri FROM task_attempts WHERE task_run_id=task_runs.id AND log_artifact_uri IS NOT NULL ORDER BY attempt_number DESC LIMIT 1),
		ARRAY(SELECT upstream.task_key FROM task_dependencies d JOIN task_runs upstream ON upstream.id=d.depends_on_task_run_id WHERE d.task_run_id=task_runs.id ORDER BY upstream.task_key)
		FROM task_runs WHERE workflow_run_id=$1 ORDER BY created_at,task_key`, id)
	if err != nil {
		return r, err
	}
	taskIndexes := make(map[string]int)
	for rows.Next() {
		var t workflow.TaskRun
		if err = rows.Scan(&t.ID, &t.WorkflowRunID, &t.TaskKey, &t.Handler, &t.Status, &t.Priority, &t.AvailableAt, &t.AttemptCount, &t.MaximumAttempts, &t.TimeoutSeconds, &t.LeaseOwner, &t.LeaseExpiresAt, &t.Input, &t.Output, &t.OutputArtifactURI, &t.LogArtifactURI, &t.DependsOn); err != nil {
			rows.Close()
			return r, err
		}
		taskIndexes[t.ID] = len(r.Tasks)
		r.Tasks = append(r.Tasks, t)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return r, err
	}
	rows.Close()
	attemptRows, err := s.Pool.Query(ctx, `SELECT a.task_run_id,a.attempt_number,a.worker_id,a.scheduled_at,a.executing_at,a.started_at,a.ended_at,
		COALESCE(a.exit_status,''),COALESCE(a.error_type,''),COALESCE(a.error_message,''),COALESCE(a.trace_id,''),COALESCE(a.artifact_uri,''),COALESCE(a.log_artifact_uri,'')
		FROM task_attempts a JOIN task_runs t ON t.id=a.task_run_id WHERE t.workflow_run_id=$1 ORDER BY t.created_at,t.task_key,a.attempt_number`, id)
	if err != nil {
		return r, err
	}
	defer attemptRows.Close()
	for attemptRows.Next() {
		var taskID string
		var attempt workflow.TaskAttempt
		if err = attemptRows.Scan(&taskID, &attempt.AttemptNumber, &attempt.WorkerID, &attempt.ScheduledAt, &attempt.ExecutingAt, &attempt.StartedAt, &attempt.EndedAt, &attempt.ExitStatus, &attempt.ErrorType, &attempt.ErrorMessage, &attempt.TraceID, &attempt.ArtifactURI, &attempt.LogArtifactURI); err != nil {
			return r, err
		}
		if index, ok := taskIndexes[taskID]; ok {
			r.Tasks[index].Attempts = append(r.Tasks[index].Attempts, attempt)
		}
	}
	return r, attemptRows.Err()
}

func (s *Store) ListRuns(ctx context.Context, tenantID string, limit int) ([]workflow.Run, error) {
	if limit < 1 || limit > 200 {
		limit = 50
	}
	rows, err := s.Pool.Query(ctx, `SELECT id,workflow_definition_id,workflow_version,status,input,input_artifact_uri,idempotency_key,started_at,completed_at,created_at FROM workflow_runs WHERE tenant_id=$1 ORDER BY created_at DESC LIMIT $2`, tenantID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []workflow.Run
	for rows.Next() {
		var r workflow.Run
		if err = rows.Scan(&r.ID, &r.WorkflowDefinitionID, &r.WorkflowVersion, &r.Status, &r.Input, &r.InputArtifactURI, &r.IdempotencyKey, &r.StartedAt, &r.CompletedAt, &r.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) CancelRun(ctx context.Context, tenantID, userID, id string) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	ct, err := tx.Exec(ctx, `UPDATE workflow_runs SET status='CANCELLED',completed_at=now() WHERE id=$1 AND tenant_id=$2 AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED')`, id, tenantID)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		var exists bool
		if e := tx.QueryRow(ctx, `SELECT true FROM workflow_runs WHERE id=$1 AND tenant_id=$2`, id, tenantID).Scan(&exists); errors.Is(e, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return ErrConflict
	}
	// Active attempts keep their lease long enough to observe cancellation on a heartbeat.
	// Their eventual failure callback is converted to CANCELLED rather than a retry.
	_, err = tx.Exec(ctx, `UPDATE task_runs SET status='CANCELLED',updated_at=now() WHERE workflow_run_id=$1 AND status IN ('BLOCKED','READY','DISPATCHING','RETRY_WAIT')`, id)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO audit_events(tenant_id,actor_id,action,resource_type,resource_id) VALUES($1,$2,'run.cancel','workflow_run',$3)`, tenantID, nullableUUID(userID), id)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) RetryRun(ctx context.Context, tenantID, userID, id string) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var ok bool
	if err = tx.QueryRow(ctx, `SELECT true FROM workflow_runs WHERE id=$1 AND tenant_id=$2 FOR UPDATE`, id, tenantID).Scan(&ok); errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	ct, err := tx.Exec(ctx, `UPDATE task_runs SET status='READY',available_at=now(),maximum_attempts=GREATEST(maximum_attempts,attempt_count+1),lease_owner=NULL,lease_expires_at=NULL,updated_at=now() WHERE workflow_run_id=$1 AND status='DEAD'`, id)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return ErrConflict
	}
	_, err = tx.Exec(ctx, `UPDATE workflow_runs SET status='RUNNING',completed_at=NULL WHERE id=$1`, id)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO audit_events(tenant_id,actor_id,action,resource_type,resource_id) VALUES($1,$2,'run.retry','workflow_run',$3)`, tenantID, nullableUUID(userID), id)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) Events(ctx context.Context, tenantID, runID string) ([]workflow.Event, error) {
	var ok bool
	if err := s.Pool.QueryRow(ctx, `SELECT true FROM workflow_runs WHERE id=$1 AND tenant_id=$2`, runID, tenantID).Scan(&ok); errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	}
	rows, err := s.Pool.Query(ctx, `SELECT id,event_type,payload,created_at FROM outbox_events WHERE aggregate_id=$1::uuid OR payload->>'workflow_run_id'=$1::text ORDER BY created_at,id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []workflow.Event
	for rows.Next() {
		var e workflow.Event
		if err = rows.Scan(&e.ID, &e.Type, &e.Payload, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func nullableUUID(v string) any {
	if v == "" {
		return nil
	}
	return v
}
func isUnique(err error) bool {
	return err != nil && (errors.Is(err, ErrConflict) || contains(err.Error(), "SQLSTATE 23505"))
}
func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
func nowPlus(d time.Duration) time.Time { return time.Now().UTC().Add(d) }
