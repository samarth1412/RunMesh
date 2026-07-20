package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr              string
	GRPCAddr              string
	SchedulerMetricsAddr  string
	DatabaseURL           string
	KafkaBrokers          []string
	KafkaTopic            string
	InternalToken         string
	LeaseDuration         time.Duration
	SchedulerInterval     time.Duration
	OutboxInterval        time.Duration
	DevAuth               bool
	DevTenantID           string
	DevUserID             string
	DevRole               string
	OIDCIssuer            string
	OIDCAudience          string
	OIDCJWKSURL           string
	OIDCTenantClaim       string
	APIKeyPepper          string
	RedisURL              string
	RateLimitRate         int
	RateLimitBurst        int
	RateLimitFailOpen     bool
	ArtifactEndpoint      string
	ArtifactRegion        string
	ArtifactBucket        string
	ArtifactAccessKey     string
	ArtifactSecretKey     string
	ArtifactPathStyle     bool
	ArtifactCreateBucket  bool
	ArtifactPresignExpiry time.Duration
}

func Load() (Config, error) {
	c := Config{
		HTTPAddr:             env("RUNMESH_HTTP_ADDR", ":8080"),
		GRPCAddr:             env("RUNMESH_GRPC_ADDR", ":7001"),
		SchedulerMetricsAddr: env("RUNMESH_SCHEDULER_METRICS_ADDR", ":9091"),
		DatabaseURL:          env("RUNMESH_DATABASE_URL", "postgres://runmesh:runmesh@localhost:5432/runmesh?sslmode=disable"),
		KafkaBrokers:         strings.Split(env("RUNMESH_KAFKA_BROKERS", "localhost:19092"), ","),
		KafkaTopic:           env("RUNMESH_KAFKA_TOPIC", "runmesh.tasks"),
		InternalToken:        env("RUNMESH_INTERNAL_TOKEN", ""),
		DevAuth:              envBool("RUNMESH_DEV_AUTH", false),
		DevTenantID:          env("RUNMESH_DEV_TENANT_ID", "00000000-0000-0000-0000-000000000001"),
		DevUserID:            env("RUNMESH_DEV_USER_ID", "00000000-0000-0000-0000-000000000001"),
		DevRole:              env("RUNMESH_DEV_ROLE", "admin"),
		OIDCIssuer:           env("RUNMESH_OIDC_ISSUER", ""),
		OIDCAudience:         env("RUNMESH_OIDC_AUDIENCE", "runmesh-web"),
		OIDCJWKSURL:          env("RUNMESH_OIDC_JWKS_URL", ""),
		OIDCTenantClaim:      env("RUNMESH_OIDC_TENANT_CLAIM", "runmesh_tenant_id"),
		APIKeyPepper:         env("RUNMESH_API_KEY_PEPPER", ""),
		RedisURL:             env("RUNMESH_REDIS_URL", "redis://localhost:6379/0"),
		ArtifactEndpoint:     env("RUNMESH_ARTIFACT_ENDPOINT", ""),
		ArtifactRegion:       env("RUNMESH_ARTIFACT_REGION", "us-east-1"),
		ArtifactBucket:       env("RUNMESH_ARTIFACT_BUCKET", "runmesh"),
		ArtifactAccessKey:    env("RUNMESH_ARTIFACT_ACCESS_KEY", ""),
		ArtifactSecretKey:    env("RUNMESH_ARTIFACT_SECRET_KEY", ""),
		ArtifactPathStyle:    envBool("RUNMESH_ARTIFACT_PATH_STYLE", false),
		ArtifactCreateBucket: envBool("RUNMESH_ARTIFACT_CREATE_BUCKET", false),
	}
	var err error
	if c.RateLimitRate, err = envInt("RUNMESH_RATE_LIMIT_RATE", 100); err != nil {
		return Config{}, err
	}
	if c.RateLimitBurst, err = envInt("RUNMESH_RATE_LIMIT_BURST", 200); err != nil {
		return Config{}, err
	}
	c.RateLimitFailOpen = envBool("RUNMESH_RATE_LIMIT_FAIL_OPEN", c.DevAuth)
	if c.LeaseDuration, err = envDuration("RUNMESH_LEASE_DURATION", 30*time.Second); err != nil {
		return Config{}, err
	}
	if c.SchedulerInterval, err = envDuration("RUNMESH_SCHEDULER_INTERVAL", 500*time.Millisecond); err != nil {
		return Config{}, err
	}
	if c.OutboxInterval, err = envDuration("RUNMESH_OUTBOX_INTERVAL", 250*time.Millisecond); err != nil {
		return Config{}, err
	}
	if c.ArtifactPresignExpiry, err = envDuration("RUNMESH_ARTIFACT_PRESIGN_EXPIRY", 15*time.Minute); err != nil {
		return Config{}, err
	}
	if c.InternalToken == "" && !c.DevAuth {
		return Config{}, fmt.Errorf("RUNMESH_INTERNAL_TOKEN is required outside development for administrative tooling")
	}
	if !c.DevAuth && (c.OIDCIssuer == "" || c.OIDCJWKSURL == "" || c.OIDCAudience == "") {
		return Config{}, fmt.Errorf("RUNMESH_OIDC_ISSUER, RUNMESH_OIDC_JWKS_URL, and RUNMESH_OIDC_AUDIENCE are required outside development")
	}
	if !c.DevAuth && c.APIKeyPepper == "" {
		return Config{}, fmt.Errorf("RUNMESH_API_KEY_PEPPER is required outside development")
	}
	if c.RateLimitRate <= 0 || c.RateLimitBurst <= 0 {
		return Config{}, fmt.Errorf("rate limit rate and burst must be positive")
	}
	if !c.DevAuth && c.RateLimitFailOpen {
		return Config{}, fmt.Errorf("RUNMESH_RATE_LIMIT_FAIL_OPEN cannot be enabled outside development")
	}
	if c.ArtifactPresignExpiry <= 0 || c.ArtifactPresignExpiry > time.Hour {
		return Config{}, fmt.Errorf("artifact presign expiry must be between zero and one hour")
	}
	if (c.ArtifactAccessKey == "") != (c.ArtifactSecretKey == "") {
		return Config{}, fmt.Errorf("artifact access key and secret key must be configured together")
	}
	return c, nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func envBool(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	return err == nil && b
}
func envDuration(key string, fallback time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}

func envInt(key string, fallback int) (int, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return parsed, nil
}
