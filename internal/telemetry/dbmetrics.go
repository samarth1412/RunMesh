package telemetry

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/runmesh/runmesh/internal/storage"
)

type DatabaseCollector struct {
	store                                       *storage.Store
	queue, outbox, runs, retries, dead, workers *prometheus.Desc
}

func NewDatabaseCollector(store *storage.Store) *DatabaseCollector {
	return &DatabaseCollector{store: store, queue: prometheus.NewDesc("task_queue_depth", "Tasks by current state", []string{"status"}, nil), outbox: prometheus.NewDesc("outbox_unpublished_events", "Transactional outbox events awaiting publication", nil, nil), runs: prometheus.NewDesc("workflow_runs_total", "Durable workflow runs by state", []string{"status"}, nil), retries: prometheus.NewDesc("task_retries_total", "Task attempts beyond the first", nil, nil), dead: prometheus.NewDesc("task_dead_letter_total", "Tasks moved to dead letter over platform lifetime", nil, nil), workers: prometheus.NewDesc("worker_active_tasks", "Active tasks reported by each worker", []string{"worker_id"}, nil)}
}
func (c *DatabaseCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{c.queue, c.outbox, c.runs, c.retries, c.dead, c.workers} {
		ch <- d
	}
}
func (c *DatabaseCollector) Collect(ch chan<- prometheus.Metric) {
	ctx := context.Background()
	rows, err := c.store.Pool.Query(ctx, `SELECT status::text,count(*) FROM task_runs GROUP BY status`)
	if err == nil {
		for rows.Next() {
			var status string
			var count int64
			if rows.Scan(&status, &count) == nil {
				ch <- prometheus.MustNewConstMetric(c.queue, prometheus.GaugeValue, float64(count), status)
			}
		}
		rows.Close()
	}
	c.single(ctx, ch, c.outbox, prometheus.GaugeValue, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`)
	c.single(ctx, ch, c.retries, prometheus.CounterValue, `SELECT count(*) FROM task_attempts WHERE attempt_number>1`)
	c.single(ctx, ch, c.dead, prometheus.CounterValue, `SELECT count(*) FROM outbox_events WHERE event_type='task.dead'`)
	rows, err = c.store.Pool.Query(ctx, `SELECT status::text,count(*) FROM workflow_runs GROUP BY status`)
	if err == nil {
		for rows.Next() {
			var status string
			var count int64
			if rows.Scan(&status, &count) == nil {
				ch <- prometheus.MustNewConstMetric(c.runs, prometheus.CounterValue, float64(count), status)
			}
		}
		rows.Close()
	}
	rows, err = c.store.Pool.Query(ctx, `SELECT worker_id,active_tasks FROM worker_heartbeats`)
	if err == nil {
		for rows.Next() {
			var id string
			var active int
			if rows.Scan(&id, &active) == nil {
				ch <- prometheus.MustNewConstMetric(c.workers, prometheus.GaugeValue, float64(active), id)
			}
		}
		rows.Close()
	}
}
func (c *DatabaseCollector) single(ctx context.Context, ch chan<- prometheus.Metric, desc *prometheus.Desc, valueType prometheus.ValueType, query string) {
	var value int64
	if c.store.Pool.QueryRow(ctx, query).Scan(&value) == nil {
		ch <- prometheus.MustNewConstMetric(desc, valueType, float64(value))
	}
}
