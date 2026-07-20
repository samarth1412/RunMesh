package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/robfig/cron/v3"
	"github.com/samarth1412/RunMesh/internal/live"
	"github.com/samarth1412/RunMesh/internal/storage"
	"github.com/samarth1412/RunMesh/internal/tracecontext"
	"github.com/samarth1412/RunMesh/internal/workflow"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type Service struct {
	Store                            *storage.Store
	Publisher                        EventPublisher
	Live                             *live.Broker
	Published                        *prometheus.CounterVec
	ScheduleInterval, OutboxInterval time.Duration
	OutboxBatchSize                  int
	OutboxClaimTTL                   time.Duration
	OutboxPublishTimeout             time.Duration
}

type EventPublisher interface {
	Publish(context.Context, []byte, []byte, map[string]string) error
}

const (
	defaultOutboxBatchSize      = 100
	defaultOutboxClaimTTL       = 30 * time.Second
	defaultOutboxPublishTimeout = 10 * time.Second
)

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
	for {
		published, err := s.publishClaimedBatch(ctx)
		if err != nil {
			return err
		}
		if published == 0 {
			return nil
		}
	}
}

func (s *Service) publishClaimedBatch(ctx context.Context) (int, error) {
	batchSize := s.OutboxBatchSize
	if batchSize <= 0 {
		batchSize = defaultOutboxBatchSize
	}
	claimTTL := s.OutboxClaimTTL
	if claimTTL <= 0 {
		claimTTL = defaultOutboxClaimTTL
	}
	publishTimeout := s.OutboxPublishTimeout
	if publishTimeout <= 0 {
		publishTimeout = defaultOutboxPublishTimeout
	}
	if publishTimeout >= claimTTL {
		return 0, fmt.Errorf("outbox publish timeout %s must be shorter than claim TTL %s", publishTimeout, claimTTL)
	}

	claimToken := uuid.NewString()
	rows, err := s.Store.Pool.Query(ctx, `WITH candidates AS (
		SELECT o.id FROM outbox_events o
		WHERE o.published_at IS NULL
		  AND (o.claim_token IS NULL OR o.claim_expires_at<=now())
		  AND NOT EXISTS (
			SELECT 1 FROM outbox_events earlier
			WHERE earlier.ordering_key=o.ordering_key
			  AND earlier.published_at IS NULL
			  AND (earlier.created_at,earlier.id)<(o.created_at,o.id)
		  )
		ORDER BY o.created_at,o.id
		FOR UPDATE OF o SKIP LOCKED
		LIMIT $1
	)
	UPDATE outbox_events o
	SET claim_token=$2,claim_expires_at=now()+make_interval(secs => $3),publish_attempts=publish_attempts+1
	FROM candidates c
	WHERE o.id=c.id
	RETURNING o.id,o.aggregate_id,o.ordering_key,o.event_type,o.payload,COALESCE(o.trace_parent,''),o.created_at,
		COALESCE(
			(SELECT tenant_id::text FROM workflow_runs WHERE id=o.aggregate_id AND o.aggregate_type='workflow_run'),
			(SELECT r.tenant_id::text FROM task_runs t JOIN workflow_runs r ON r.id=t.workflow_run_id WHERE t.id=o.aggregate_id AND o.aggregate_type='task_run'),
			(SELECT tenant_id::text FROM workflow_definitions WHERE id=o.aggregate_id AND o.aggregate_type='workflow_definition'),
			''
		)`, batchSize, claimToken, claimTTL.Seconds())
	if err != nil {
		return 0, err
	}
	type event struct {
		id, aggregateID, orderingKey, eventType, traceParent, tenantID string
		payload                                                        []byte
		createdAt                                                      time.Time
	}
	var events []event
	for rows.Next() {
		var e event
		if err = rows.Scan(&e.id, &e.aggregateID, &e.orderingKey, &e.eventType, &e.payload, &e.traceParent, &e.createdAt, &e.tenantID); err != nil {
			rows.Close()
			return 0, err
		}
		events = append(events, e)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()

	releaseClaims := func() {
		releaseContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer cancel()
		if _, releaseErr := s.Store.Pool.Exec(releaseContext, `UPDATE outbox_events SET claim_token=NULL,claim_expires_at=NULL WHERE claim_token=$1 AND published_at IS NULL`, claimToken); releaseErr != nil {
			slog.Warn("outbox claim release failed", "claim_token", claimToken, "error", releaseErr)
		}
	}
	for _, e := range events {
		publishContext := tracecontext.IntoContext(ctx, e.traceParent)
		publishContext, cancel := context.WithTimeout(publishContext, publishTimeout)
		publishContext, span := otel.Tracer("runmesh/scheduler").Start(publishContext, "outbox.publish", trace.WithAttributes(attribute.String("tenant_id", e.tenantID), attribute.String("event.id", e.id), attribute.String("event.type", e.eventType)))
		if err = s.Publisher.Publish(publishContext, []byte(e.orderingKey), e.payload, map[string]string{"event_type": e.eventType, "event_id": e.id}); err != nil {
			span.RecordError(err)
			span.End()
			cancel()
			releaseClaims()
			return 0, err
		}
		span.End()
		cancel()
		command, ackErr := s.Store.Pool.Exec(ctx, `UPDATE outbox_events SET published_at=now(),claim_token=NULL,claim_expires_at=NULL WHERE id=$1 AND published_at IS NULL AND claim_token=$2`, e.id, claimToken)
		if ackErr != nil {
			releaseClaims()
			return 0, ackErr
		}
		if command.RowsAffected() != 1 {
			releaseClaims()
			return 0, fmt.Errorf("outbox claim lost before acknowledgement: event %s", e.id)
		}
		if s.Published != nil {
			s.Published.WithLabelValues(e.tenantID, e.eventType).Inc()
		}
		if s.Live != nil {
			if liveErr := s.Live.Publish(ctx, e.tenantID, live.Event{ID: e.id, Type: e.eventType, AggregateID: e.aggregateID, Payload: e.payload, OccurredAt: e.createdAt.UTC().Format(time.RFC3339Nano)}); liveErr != nil {
				slog.Warn("live notification unavailable", "tenant_id", e.tenantID, "event_id", e.id, "event_type", e.eventType, "error", liveErr)
			}
		}
	}
	return len(events), nil
}
func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

var _ = errors.Is
