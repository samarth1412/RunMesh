package telemetry

import (
	"context"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

func Setup(ctx context.Context, serviceName string) (func(context.Context) error, error) {
	ConfigureLogging(serviceName)
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}
	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(endpoint), otlptracegrpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	// Leave the custom resource schema unset so it can be merged with the SDK's
	// default resource even when dependencies use different semconv versions.
	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes("", semconv.ServiceName(serviceName)))
	if err != nil {
		return nil, err
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(res), sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(1))))
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	return provider.Shutdown, nil
}

func ConfigureLogging(serviceName string) {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	slog.SetDefault(slog.New(handler).With("service", serviceName))
}

// Logger adds the active trace identifiers to structured logs. Callers add
// tenant_id, workflow_run_id, and task_run_id when those dimensions are known.
func Logger(ctx context.Context, attributes ...any) *slog.Logger {
	logger := slog.Default()
	span := trace.SpanContextFromContext(ctx)
	if span.IsValid() {
		attributes = append(attributes, "trace_id", span.TraceID().String(), "span_id", span.SpanID().String())
	}
	return logger.With(attributes...)
}
