package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/runmesh/runmesh/internal/config"
	"github.com/runmesh/runmesh/internal/live"
	"github.com/runmesh/runmesh/internal/messaging"
	"github.com/runmesh/runmesh/internal/scheduler"
	"github.com/runmesh/runmesh/internal/storage"
	"github.com/runmesh/runmesh/internal/telemetry"
)

func main() {
	telemetry.ConfigureLogging("runmesh-scheduler")
	cfg, err := config.Load()
	if err != nil {
		slog.Error("configuration", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	shutdownTelemetry, err := telemetry.Setup(ctx, "runmesh-scheduler")
	if err != nil {
		slog.Error("telemetry", "error", err)
		os.Exit(1)
	}
	defer shutdownTelemetry(context.Background())
	store, err := storage.New(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("database", "error", err)
		os.Exit(1)
	}
	defer store.Close()
	publisher := messaging.NewPublisher(cfg.KafkaBrokers, cfg.KafkaTopic)
	defer publisher.Close()
	liveBroker, err := live.New(cfg.RedisURL, prometheus.DefaultRegisterer)
	if err != nil {
		slog.Error("live notification broker", "error", err)
		os.Exit(1)
	}
	defer liveBroker.Close()
	published := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "scheduler_outbox_published_total", Help: "Outbox events published to Kafka by tenant and event type."}, []string{"tenant_id", "event_type"})
	prometheus.MustRegister(published)
	service := scheduler.Service{Store: store, Publisher: publisher, Live: liveBroker, Published: published, ScheduleInterval: cfg.SchedulerInterval, OutboxInterval: cfg.OutboxInterval}
	metricsServer := &http.Server{Addr: cfg.SchedulerMetricsAddr, Handler: promhttp.Handler(), ReadHeaderTimeout: 3 * time.Second}
	go func() {
		if serveErr := metricsServer.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			slog.Error("scheduler metrics server", "error", serveErr)
			stop()
		}
	}()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = metricsServer.Shutdown(shutdown)
	}()
	slog.Info("scheduler started")
	if err = service.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("scheduler stopped", "error", err)
		os.Exit(1)
	}
}
