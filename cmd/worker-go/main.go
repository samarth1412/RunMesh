package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/runmesh/runmesh/internal/telemetry"
	"github.com/runmesh/runmesh/internal/tracecontext"
	runmesh "github.com/runmesh/runmesh/sdk/go"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type dispatch struct {
	TaskRunID string `json:"task_run_id"`
	Handler   string `json:"handler"`
}

func main() {
	telemetry.ConfigureLogging("runmesh-worker-go")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	shutdownTelemetry, err := telemetry.Setup(ctx, "runmesh-worker-go")
	if err != nil {
		slog.Error("telemetry", "error", err)
		os.Exit(1)
	}
	defer shutdownTelemetry(context.Background())
	brokers := strings.Split(env("RUNMESH_KAFKA_BROKERS", "localhost:19092"), ",")
	workerID := env("RUNMESH_WORKER_ID", defaultWorkerID())
	client := runmesh.NewClient(env("RUNMESH_ENDPOINT", "localhost:7001"), env("RUNMESH_INTERNAL_TOKEN", "local-development-token"), workerID)
	defer client.Close()
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: env("RUNMESH_KAFKA_TOPIC", "runmesh.tasks"), GroupID: "runmesh-workers", MinBytes: 1, MaxBytes: 10e6})
	defer reader.Close()
	go report(ctx, client)
	slog.Info("go worker started", "worker_id", workerID)
	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			slog.Error("fetch task", "error", err)
			continue
		}
		if header(message.Headers, "event_type") != "task.dispatch" {
			_ = reader.CommitMessages(ctx, message)
			continue
		}
		var d dispatch
		if err = json.Unmarshal(message.Value, &d); err != nil {
			slog.Error("decode dispatch", "error", err)
			_ = reader.CommitMessages(ctx, message)
			continue
		}
		taskContext := tracecontext.IntoContext(ctx, header(message.Headers, "traceparent"))
		if err = execute(taskContext, client, d.TaskRunID); err != nil {
			slog.Error("execute task", "task_run_id", d.TaskRunID, "error", err)
		}
		if err = reader.CommitMessages(ctx, message); err != nil {
			slog.Error("commit offset", "error", err)
		}
	}
}
func execute(parent context.Context, client *runmesh.Client, id string) error {
	parent, span := otel.Tracer("runmesh/worker").Start(parent, "task.execute", trace.WithAttributes(attribute.String("task_run_id", id)))
	defer span.End()
	task, err := client.Lease(parent, id)
	if err != nil {
		return err
	}
	if _, err = client.Start(parent, id); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, time.Duration(task.TimeoutSeconds)*time.Second)
	defer cancel()
	heartbeatsDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		defer close(heartbeatsDone)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				h, heartbeatErr := client.Heartbeat(ctx, id)
				if heartbeatErr != nil {
					slog.Warn("heartbeat failed", "error", heartbeatErr)
					continue
				}
				if h.Cancelled {
					cancel()
					return
				}
			}
		}
	}()
	var input map[string]any
	if err = json.Unmarshal(task.Input, &input); err != nil {
		_, _ = client.Fail(parent, id, false, err)
		return err
	}
	var output any
	switch task.Handler {
	case "examples.greet":
		output = map[string]any{"message": fmt.Sprintf("Hello, %v!", input["name"]), "worker": "go"}
	case "examples.upper":
		output = map[string]any{"text": strings.ToUpper(fmt.Sprint(input["text"])), "worker": "go"}
	case "examples.slow":
		delay := 200 * time.Millisecond
		if value, ok := input["delay_ms"].(float64); ok && value >= 0 && value <= 30_000 {
			delay = time.Duration(value) * time.Millisecond
		}
		select {
		case <-time.After(delay):
			output = map[string]any{"slept_ms": delay.Milliseconds(), "worker": "go"}
		case <-ctx.Done():
			return ctx.Err()
		}
	default:
		err = fmt.Errorf("unsupported handler %q", task.Handler)
		_, _ = client.Fail(parent, id, true, err)
		cancel()
		<-heartbeatsDone
		return err
	}
	_, err = client.Complete(ctx, id, output)
	cancel()
	<-heartbeatsDone
	return err
}
func report(ctx context.Context, client *runmesh.Client) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		if err := client.Report(ctx, []string{"examples.greet", "examples.upper", "examples.slow"}, 0); err != nil {
			slog.Warn("worker heartbeat failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func header(headers []kafka.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func defaultWorkerID() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		return "go-worker"
	}
	return "go-worker-" + hostname
}
