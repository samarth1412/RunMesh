package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/runmesh/runmesh/internal/config"
	"github.com/runmesh/runmesh/internal/messaging"
	"github.com/runmesh/runmesh/internal/scheduler"
	"github.com/runmesh/runmesh/internal/storage"
	"github.com/runmesh/runmesh/internal/telemetry"
)

func main() {
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
	service := scheduler.Service{Store: store, Publisher: publisher, ScheduleInterval: cfg.SchedulerInterval, OutboxInterval: cfg.OutboxInterval}
	slog.Info("scheduler started")
	if err = service.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("scheduler stopped", "error", err)
		os.Exit(1)
	}
}
