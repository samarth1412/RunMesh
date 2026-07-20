package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/robfig/cron/v3"
	"github.com/runmesh/runmesh/internal/messaging"
	"github.com/runmesh/runmesh/internal/storage"
	"github.com/runmesh/runmesh/internal/tracecontext"
	"github.com/runmesh/runmesh/internal/workflow"
)

type Service struct {
	Store                            *storage.Store
	Publisher                        *messaging.Publisher
	ScheduleInterval, OutboxInterval time.Duration
}

func (s *Service) Run(ctx context.Context) error {
	errc := make(chan error, 2)
	go func() { errc <- s.runScheduler(ctx) }()
	go func() { errc <- s.runOutbox(ctx) }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errc:
		return err
	}
}

// RunOnce executes one scheduling cycle without starting background loops.
// It is useful for deterministic recovery checks and administrative tooling.
func (s *Service) RunOnce(ctx context.Context) error {
	return s.tick(ctx)
}

// PublishOnce executes one transactional outbox publication cycle without
// starting the background loop. It is useful for deterministic recovery checks
// and administrative tooling.
func (s *Service) PublishOnce(ctx context.Context) error {
	return s.publishBatch(ctx)
}

func (s *Service) runScheduler(ctx context.Context) error {
	ticker := time.NewTicker(s.ScheduleInterval)
	defer ticker.Stop()
	for {
		if err := s.tick(ctx); err != nil {
			slog.Error("scheduler tick", "error", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
func (s *Service) tick(ctx context.Context) error {
	if err := s.fireSchedules(ctx); err != nil {
		return err
	}
	if _, err := s.Store.Pool.Exec(ctx, `UPDATE task_runs SET status='READY',updated_at=now() WHERE status='RETRY_WAIT' AND available_at<=now()`); err != nil {
		return err
	}
	if err := s.recoverExpired(ctx); err != nil {
		return err
	}
	tx, err := s.Store.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT t.id,t.workflow_run_id,t.task_key,t.handler,COALESCE(r.trace_parent,'') FROM task_runs t JOIN workflow_runs r ON r.id=t.workflow_run_id WHERE t.status='READY' AND t.available_at<=now() ORDER BY t.priority DESC,t.available_at FOR UPDATE OF t SKIP LOCKED LIMIT 100`)
	if err != nil {
		return err
	}
	type task struct{ id, runID, key, handler, traceParent string }
	var tasks []task
	for rows.Next() {
		var t task
		if err = rows.Scan(&t.id, &t.runID, &t.key, &t.handler, &t.traceParent); err != nil {
			rows.Close()
			return err
		}
		tasks = append(tasks, t)
	}
	rows.Close()
	for _, t := range tasks {
		ct, err := tx.Exec(ctx, `UPDATE task_runs SET status='DISPATCHING',updated_at=now() WHERE id=$1 AND status='READY'`, t.id)
		if err != nil {
			return err
		}
		if ct.RowsAffected() != 1 {
			continue
		}
		payload, _ := json.Marshal(map[string]any{"task_run_id": t.id, "workflow_run_id": t.runID, "task_key": t.key, "handler": t.handler})
		if _, err = tx.Exec(ctx, `INSERT INTO outbox_events(aggregate_type,aggregate_id,event_type,payload,trace_parent) VALUES('task_run',$1,'task.dispatch',$2,$3)`, t.id, payload, t.traceParent); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *Service) fireSchedules(ctx context.Context) error {
	tx, err := s.Store.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT id,tenant_id,workflow_definition_id,cron_expression,timezone,next_execution_at,misfire_policy FROM schedules WHERE enabled AND next_execution_at<=now() ORDER BY next_execution_at FOR UPDATE SKIP LOCKED LIMIT 50`)
	if err != nil {
		return err
	}
	type dueSchedule struct {
		id, tenantID, definitionID, expression, timezone string
		due                                              time.Time
	}
	var due []dueSchedule
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	for rows.Next() {
		var item dueSchedule
		var misfire string
		if err = rows.Scan(&item.id, &item.tenantID, &item.definitionID, &item.expression, &item.timezone, &item.due, &misfire); err != nil {
			rows.Close()
			return err
		}
		location, locationErr := time.LoadLocation(item.timezone)
		if locationErr != nil {
			rows.Close()
			return locationErr
		}
		schedule, parseErr := parser.Parse(item.expression)
		if parseErr != nil {
			rows.Close()
			return parseErr
		}
		base := item.due.In(location)
		if misfire == "skip" && time.Since(item.due) > time.Minute {
			base = time.Now().In(location)
		}
		next := schedule.Next(base)
		if _, err = tx.Exec(ctx, `UPDATE schedules SET next_execution_at=$2 WHERE id=$1`, item.id, next.UTC()); err != nil {
			rows.Close()
			return err
		}
		due = append(due, item)
	}
	rows.Close()
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	for _, item := range due {
		key := "schedule:" + item.id + ":" + item.due.UTC().Format(time.RFC3339Nano)
		if _, _, err = s.Store.CreateRun(ctx, item.tenantID, "", item.definitionID, key, json.RawMessage(`{}`)); err != nil && !errors.Is(err, storage.ErrConflict) {
			return err
		}
	}
	return nil
}
func (s *Service) recoverExpired(ctx context.Context) error {
	tx, err := s.Store.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT t.id,t.workflow_run_id,t.task_key,t.attempt_count,t.maximum_attempts,r.status::text FROM task_runs t JOIN workflow_runs r ON r.id=t.workflow_run_id WHERE t.status IN ('LEASED','RUNNING') AND t.lease_expires_at<=now() FOR UPDATE OF t SKIP LOCKED LIMIT 100`)
	if err != nil {
		return err
	}
	type expired struct {
		id, runID, key, runStatus string
		attempt, max              int
	}
	var all []expired
	for rows.Next() {
		var e expired
		if err = rows.Scan(&e.id, &e.runID, &e.key, &e.attempt, &e.max, &e.runStatus); err != nil {
			rows.Close()
			return err
		}
		all = append(all, e)
	}
	rows.Close()
	for _, e := range all {
		status := "DEAD"
		available := time.Now().UTC()
		event := "task.dead"
		if e.runStatus == "CANCELLED" {
			status = "CANCELLED"
			event = "task.cancelled"
		} else if e.attempt < e.max {
			status = "RETRY_WAIT"
			available = time.Now().UTC().Add(workflow.RetryDelay(e.attempt))
			event = "task.retry_wait"
		}
		_, err = tx.Exec(ctx, `UPDATE task_runs SET status=$2,available_at=$3,lease_owner=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1`, e.id, status, available)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE task_attempts SET ended_at=now(),exit_status=$3,error_type='LeaseExpired',error_message='worker heartbeat lease expired' WHERE task_run_id=$1 AND attempt_number=$2`, e.id, e.attempt, status)
		if err != nil {
			return err
		}
		payload := mustJSON(map[string]any{"workflow_run_id": e.runID, "task_run_id": e.id, "task_key": e.key, "attempt": e.attempt, "available_at": available, "error_type": "LeaseExpired"})
		_, err = tx.Exec(ctx, `INSERT INTO outbox_events(aggregate_type,aggregate_id,event_type,payload,trace_parent) SELECT 'task_run',$1,$2,$3,trace_parent FROM workflow_runs WHERE id=$4`, e.id, event, payload, e.runID)
		if err != nil {
			return err
		}
		if status == "DEAD" {
			_, err = tx.Exec(ctx, `UPDATE workflow_runs SET status='FAILED',completed_at=now() WHERE id=$1 AND status='RUNNING'`, e.runID)
			if err != nil {
				return err
			}
		}
	}
	return tx.Commit(ctx)
}

func (s *Service) runOutbox(ctx context.Context) error {
	ticker := time.NewTicker(s.OutboxInterval)
	defer ticker.Stop()
	for {
		if err := s.publishBatch(ctx); err != nil {
			slog.Error("outbox publish", "error", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
func (s *Service) publishBatch(ctx context.Context) error {
	tx, err := s.Store.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT id,aggregate_id,event_type,payload,COALESCE(trace_parent,'') FROM outbox_events WHERE published_at IS NULL ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 100`)
	if err != nil {
		return err
	}
	type event struct {
		id, aggregateID, eventType, traceParent string
		payload                                 []byte
	}
	var events []event
	for rows.Next() {
		var e event
		if err = rows.Scan(&e.id, &e.aggregateID, &e.eventType, &e.payload, &e.traceParent); err != nil {
			rows.Close()
			return err
		}
		events = append(events, e)
	}
	rows.Close()
	for _, e := range events {
		var envelope struct {
			WorkflowRunID string `json:"workflow_run_id"`
		}
		_ = json.Unmarshal(e.payload, &envelope)
		key := e.aggregateID
		if envelope.WorkflowRunID != "" {
			key = envelope.WorkflowRunID
		}
		publishContext := tracecontext.IntoContext(ctx, e.traceParent)
		if err = s.Publisher.Publish(publishContext, []byte(key), e.payload, map[string]string{"event_type": e.eventType, "event_id": e.id}); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `UPDATE outbox_events SET published_at=now() WHERE id=$1 AND published_at IS NULL`, e.id); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

var _ = errors.Is
