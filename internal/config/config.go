package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr          string
	GRPCAddr          string
	DatabaseURL       string
	KafkaBrokers      []string
	KafkaTopic        string
	InternalToken     string
	LeaseDuration     time.Duration
	SchedulerInterval time.Duration
	OutboxInterval    time.Duration
	DevAuth           bool
	DevTenantID       string
	DevUserID         string
	DevRole           string
}

func Load() (Config, error) {
	c := Config{
		HTTPAddr:      env("RUNMESH_HTTP_ADDR", ":8080"),
		GRPCAddr:      env("RUNMESH_GRPC_ADDR", ":7001"),
		DatabaseURL:   env("RUNMESH_DATABASE_URL", "postgres://runmesh:runmesh@localhost:5432/runmesh?sslmode=disable"),
		KafkaBrokers:  strings.Split(env("RUNMESH_KAFKA_BROKERS", "localhost:19092"), ","),
		KafkaTopic:    env("RUNMESH_KAFKA_TOPIC", "runmesh.tasks"),
		InternalToken: env("RUNMESH_INTERNAL_TOKEN", ""),
		DevAuth:       envBool("RUNMESH_DEV_AUTH", false),
		DevTenantID:   env("RUNMESH_DEV_TENANT_ID", "00000000-0000-0000-0000-000000000001"),
		DevUserID:     env("RUNMESH_DEV_USER_ID", "00000000-0000-0000-0000-000000000001"),
		DevRole:       env("RUNMESH_DEV_ROLE", "admin"),
	}
	var err error
	if c.LeaseDuration, err = envDuration("RUNMESH_LEASE_DURATION", 30*time.Second); err != nil {
		return Config{}, err
	}
	if c.SchedulerInterval, err = envDuration("RUNMESH_SCHEDULER_INTERVAL", 500*time.Millisecond); err != nil {
		return Config{}, err
	}
	if c.OutboxInterval, err = envDuration("RUNMESH_OUTBOX_INTERVAL", 250*time.Millisecond); err != nil {
		return Config{}, err
	}
	if c.InternalToken == "" && !c.DevAuth {
		return Config{}, fmt.Errorf("RUNMESH_INTERNAL_TOKEN is required outside development")
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
