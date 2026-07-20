package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	runmeshv1 "github.com/runmesh/runmesh/gen/runmesh/v1"
	"github.com/runmesh/runmesh/internal/api"
	"github.com/runmesh/runmesh/internal/artifact"
	"github.com/runmesh/runmesh/internal/auth"
	"github.com/runmesh/runmesh/internal/config"
	"github.com/runmesh/runmesh/internal/live"
	"github.com/runmesh/runmesh/internal/ratelimit"
	workerrpc "github.com/runmesh/runmesh/internal/rpc"
	"github.com/runmesh/runmesh/internal/storage"
	"github.com/runmesh/runmesh/internal/telemetry"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"google.golang.org/grpc"
)

func main() {
	telemetry.ConfigureLogging("runmesh-control-plane")
	if len(os.Args) > 1 && os.Args[1] == "--healthcheck" {
		client := http.Client{Timeout: 2 * time.Second}
		response, err := client.Get("http://127.0.0.1:8080/health/ready")
		if err != nil || response.StatusCode != http.StatusOK {
			os.Exit(1)
		}
		response.Body.Close()
		return
	}
	cfg, err := config.Load()
	if err != nil {
		slog.Error("configuration", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	shutdownTelemetry, err := telemetry.Setup(ctx, "runmesh-control-plane")
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
	artifactManager, err := artifact.New(ctx, store, artifact.Config{
		Endpoint: cfg.ArtifactEndpoint, Region: cfg.ArtifactRegion, Bucket: cfg.ArtifactBucket,
		AccessKey: cfg.ArtifactAccessKey, SecretKey: cfg.ArtifactSecretKey,
		PathStyle: cfg.ArtifactPathStyle, CreateBucket: cfg.ArtifactCreateBucket, PresignExpiry: cfg.ArtifactPresignExpiry,
	})
	if err != nil {
		slog.Error("artifact storage", "error", err)
		os.Exit(1)
	}
	prometheus.MustRegister(telemetry.NewDatabaseCollector(store))
	authenticator := auth.New(store.Pool, auth.Config{
		Dev: cfg.DevAuth, DevPrincipal: auth.Principal{TenantID: cfg.DevTenantID, UserID: cfg.DevUserID, Role: cfg.DevRole}, DevWorkerToken: cfg.InternalToken,
		Issuer: cfg.OIDCIssuer, Audience: cfg.OIDCAudience, JWKSURL: cfg.OIDCJWKSURL, TenantClaim: cfg.OIDCTenantClaim, APIKeyPepper: cfg.APIKeyPepper,
	})
	limiter, err := ratelimit.New(cfg.RedisURL, cfg.RateLimitRate, cfg.RateLimitBurst, cfg.RateLimitFailOpen, prometheus.DefaultRegisterer)
	if err != nil {
		slog.Error("rate limiter", "error", err)
		os.Exit(1)
	}
	defer limiter.Close()
	liveBroker, err := live.New(cfg.RedisURL, prometheus.DefaultRegisterer)
	if err != nil {
		slog.Error("live notification broker", "error", err)
		os.Exit(1)
	}
	defer liveBroker.Close()
	var redisReady func(context.Context) error
	if !cfg.RateLimitFailOpen {
		redisReady = limiter.Ping
	}
	server := api.New(store, cfg.LeaseDuration, authenticator.Middleware, authenticator.WorkerMiddleware, limiter.Middleware, redisReady, cfg.APIKeyPepper, artifactManager)
	server.Live = liveBroker
	grpcListener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		slog.Error("gRPC listener", "error", err)
		os.Exit(1)
	}
	grpcServer := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
	runmeshv1.RegisterWorkerServiceServer(grpcServer, &workerrpc.WorkerServer{Store: store, LeaseDuration: cfg.LeaseDuration, Authenticator: authenticator, Artifacts: artifactManager})
	go func() {
		if serveErr := grpcServer.Serve(grpcListener); serveErr != nil {
			slog.Error("gRPC server", "error", serveErr)
			stop()
		}
	}()
	httpServer := &http.Server{Addr: cfg.HTTPAddr, Handler: otelhttp.NewHandler(server.Handler(), "runmesh.http"), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		grpcServer.GracefulStop()
		_ = httpServer.Shutdown(shutdown)
	}()
	slog.Info("control plane listening", "addr", cfg.HTTPAddr)
	if err = httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("http server", "error", err)
		os.Exit(1)
	}
}
