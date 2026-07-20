package telemetry

import (
	"context"
	"math"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/runmesh/runmesh/internal/storage"
)

var durationBuckets = []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900, 3600}

type DatabaseCollector struct {
	store                                                                  *storage.Store
	queue, tenantQueue, outbox, runs, retries, dead, workers, heartbeatAge *prometheus.Desc
	workflowDuration, schedulingDelay, executionDuration, artifactLatency  *prometheus.Desc
	tenantRuns, tenantAttempts, tenantArtifactBytes                        *prometheus.Desc
}

func NewDatabaseCollector(store *storage.Store) *DatabaseCollector {
	return &DatabaseCollector{
		store:               store,
		queue:               prometheus.NewDesc("task_queue_depth", "Tasks by current state.", []string{"status"}, nil),
		tenantQueue:         prometheus.NewDesc("tenant_task_queue_depth", "Tasks by tenant and current state.", []string{"tenant_id", "status"}, nil),
		outbox:              prometheus.NewDesc("outbox_unpublished_events", "Transactional outbox events awaiting publication.", nil, nil),
		runs:                prometheus.NewDesc("workflow_runs_total", "Durable workflow runs by state.", []string{"status"}, nil),
		retries:             prometheus.NewDesc("task_retries_total", "Task attempts beyond the first.", nil, nil),
		dead:                prometheus.NewDesc("task_dead_letter_total", "Tasks moved to dead letter over platform lifetime.", nil, nil),
		workers:             prometheus.NewDesc("worker_active_tasks", "Active tasks reported by each tenant worker.", []string{"tenant_id", "worker_id"}, nil),
		heartbeatAge:        prometheus.NewDesc("worker_heartbeat_age_seconds", "Seconds since the most recent worker heartbeat.", []string{"tenant_id", "worker_id"}, nil),
		workflowDuration:    prometheus.NewDesc("workflow_duration_seconds", "End-to-end duration of completed workflows.", []string{"tenant_id"}, nil),
		schedulingDelay:     prometheus.NewDesc("task_scheduling_delay_seconds", "Time from attempt availability until lease acquisition.", []string{"tenant_id"}, nil),
		executionDuration:   prometheus.NewDesc("task_execution_duration_seconds", "Time spent executing user task code.", []string{"tenant_id"}, nil),
		artifactLatency:     prometheus.NewDesc("artifact_latency_seconds", "Time from artifact metadata creation until upload completion.", []string{"tenant_id", "kind"}, nil),
		tenantRuns:          prometheus.NewDesc("tenant_workflow_runs_total", "Workflow usage by tenant and status.", []string{"tenant_id", "status"}, nil),
		tenantAttempts:      prometheus.NewDesc("tenant_task_attempts_total", "Task attempt usage by tenant.", []string{"tenant_id"}, nil),
		tenantArtifactBytes: prometheus.NewDesc("tenant_artifact_bytes_total", "Ready artifact bytes by tenant and kind.", []string{"tenant_id", "kind"}, nil),
	}
}

func (c *DatabaseCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, descriptor := range []*prometheus.Desc{c.queue, c.tenantQueue, c.outbox, c.runs, c.retries, c.dead, c.workers, c.heartbeatAge, c.workflowDuration, c.schedulingDelay, c.executionDuration, c.artifactLatency, c.tenantRuns, c.tenantAttempts, c.tenantArtifactBytes} {
		ch <- descriptor
	}
}

func (c *DatabaseCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	c.grouped(ctx, ch, c.queue, prometheus.GaugeValue, `SELECT status::text,count(*) FROM task_runs GROUP BY status`, 1)
	c.grouped(ctx, ch, c.tenantQueue, prometheus.GaugeValue, `SELECT r.tenant_id::text,t.status::text,count(*) FROM task_runs t JOIN workflow_runs r ON r.id=t.workflow_run_id GROUP BY r.tenant_id,t.status`, 2)
	c.single(ctx, ch, c.outbox, prometheus.GaugeValue, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`)
	c.single(ctx, ch, c.retries, prometheus.CounterValue, `SELECT count(*) FROM task_attempts WHERE attempt_number>1`)
	c.single(ctx, ch, c.dead, prometheus.CounterValue, `SELECT count(*) FROM outbox_events WHERE event_type='task.dead'`)
	c.grouped(ctx, ch, c.runs, prometheus.CounterValue, `SELECT status::text,count(*) FROM workflow_runs GROUP BY status`, 1)
	c.grouped(ctx, ch, c.tenantRuns, prometheus.CounterValue, `SELECT tenant_id::text,status::text,count(*) FROM workflow_runs GROUP BY tenant_id,status`, 2)
	c.grouped(ctx, ch, c.tenantAttempts, prometheus.CounterValue, `SELECT r.tenant_id::text,count(*) FROM task_attempts a JOIN task_runs t ON t.id=a.task_run_id JOIN workflow_runs r ON r.id=t.workflow_run_id GROUP BY r.tenant_id`, 1)
	c.grouped(ctx, ch, c.tenantArtifactBytes, prometheus.CounterValue, `SELECT tenant_id::text,kind::text,COALESCE(sum(size_bytes),0) FROM artifacts WHERE status='READY' GROUP BY tenant_id,kind`, 2)

	rows, err := c.store.Pool.Query(ctx, `SELECT tenant_id::text,worker_id,active_tasks,GREATEST(0,extract(epoch FROM now()-last_seen_at)) FROM worker_heartbeats`)
	if err == nil {
		for rows.Next() {
			var tenantID, workerID string
			var active int
			var age float64
			if rows.Scan(&tenantID, &workerID, &active, &age) == nil {
				ch <- prometheus.MustNewConstMetric(c.workers, prometheus.GaugeValue, float64(active), tenantID, workerID)
				ch <- prometheus.MustNewConstMetric(c.heartbeatAge, prometheus.GaugeValue, age, tenantID, workerID)
			}
		}
		rows.Close()
	}
	c.histograms(ctx, ch, c.workflowDuration, `SELECT tenant_id::text,extract(epoch FROM completed_at-started_at) FROM workflow_runs WHERE completed_at IS NOT NULL AND started_at IS NOT NULL`, 1)
	c.histograms(ctx, ch, c.schedulingDelay, `SELECT r.tenant_id::text,extract(epoch FROM a.started_at-a.scheduled_at) FROM task_attempts a JOIN task_runs t ON t.id=a.task_run_id JOIN workflow_runs r ON r.id=t.workflow_run_id WHERE a.scheduled_at IS NOT NULL`, 1)
	c.histograms(ctx, ch, c.executionDuration, `SELECT r.tenant_id::text,extract(epoch FROM a.ended_at-a.executing_at) FROM task_attempts a JOIN task_runs t ON t.id=a.task_run_id JOIN workflow_runs r ON r.id=t.workflow_run_id WHERE a.ended_at IS NOT NULL AND a.executing_at IS NOT NULL`, 1)
	c.histograms(ctx, ch, c.artifactLatency, `SELECT tenant_id::text,kind::text,extract(epoch FROM ready_at-created_at) FROM artifacts WHERE ready_at IS NOT NULL`, 2)
}

func (c *DatabaseCollector) grouped(ctx context.Context, ch chan<- prometheus.Metric, descriptor *prometheus.Desc, valueType prometheus.ValueType, query string, labelCount int) {
	rows, err := c.store.Pool.Query(ctx, query)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		labels := make([]string, labelCount)
		pointers := make([]any, 0, labelCount+1)
		for index := range labels {
			pointers = append(pointers, &labels[index])
		}
		var value float64
		pointers = append(pointers, &value)
		if rows.Scan(pointers...) == nil {
			ch <- prometheus.MustNewConstMetric(descriptor, valueType, value, labels...)
		}
	}
}

func (c *DatabaseCollector) single(ctx context.Context, ch chan<- prometheus.Metric, descriptor *prometheus.Desc, valueType prometheus.ValueType, query string) {
	var value float64
	if c.store.Pool.QueryRow(ctx, query).Scan(&value) == nil {
		ch <- prometheus.MustNewConstMetric(descriptor, valueType, value)
	}
}

type histogramAggregate struct {
	count   uint64
	sum     float64
	buckets map[float64]uint64
}

func (c *DatabaseCollector) histograms(ctx context.Context, ch chan<- prometheus.Metric, descriptor *prometheus.Desc, query string, labelCount int) {
	rows, err := c.store.Pool.Query(ctx, query)
	if err != nil {
		return
	}
	defer rows.Close()
	aggregates := map[string]*histogramAggregate{}
	labelsByKey := map[string][]string{}
	for rows.Next() {
		labels := make([]string, labelCount)
		pointers := make([]any, 0, labelCount+1)
		for index := range labels {
			pointers = append(pointers, &labels[index])
		}
		var value float64
		pointers = append(pointers, &value)
		if rows.Scan(pointers...) != nil || math.IsNaN(value) || value < 0 {
			continue
		}
		key := ""
		for _, label := range labels {
			key += "\x00" + label
		}
		aggregate := aggregates[key]
		if aggregate == nil {
			aggregate = &histogramAggregate{buckets: map[float64]uint64{}}
			aggregates[key] = aggregate
			labelsByKey[key] = labels
		}
		aggregate.count++
		aggregate.sum += value
		for _, boundary := range durationBuckets {
			if value <= boundary {
				aggregate.buckets[boundary]++
			}
		}
	}
	for key, aggregate := range aggregates {
		ch <- prometheus.MustNewConstHistogram(descriptor, aggregate.count, aggregate.sum, aggregate.buckets, labelsByKey[key]...)
	}
}
