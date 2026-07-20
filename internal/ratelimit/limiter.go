package ratelimit

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/runmesh/runmesh/internal/auth"
)

const tokenBucketScript = `
local current = redis.call('TIME')
local now = tonumber(current[1]) + tonumber(current[2]) / 1000000
local values = redis.call('HMGET', KEYS[1], 'tokens', 'updated')
local tokens = tonumber(values[1]) or tonumber(ARGV[2])
local updated = tonumber(values[2]) or now
tokens = math.min(tonumber(ARGV[2]), tokens + math.max(0, now - updated) * tonumber(ARGV[1]))
local allowed = 0
local retry_ms = 0
if tokens >= 1 then
  allowed = 1
  tokens = tokens - 1
else
  retry_ms = math.ceil((1 - tokens) / tonumber(ARGV[1]) * 1000)
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'updated', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(tonumber(ARGV[2]) / tonumber(ARGV[1]) * 2000))
return {allowed, math.floor(tokens), retry_ms}
`

type Result struct {
	Allowed    bool
	Remaining  int
	RetryAfter time.Duration
}

type Limiter struct {
	client     *redis.Client
	rate       int
	burst      int
	failOpen   bool
	rejections *prometheus.CounterVec
}

func New(rawURL string, rate, burst int, failOpen bool, registerer prometheus.Registerer) (*Limiter, error) {
	options, err := redis.ParseURL(rawURL)
	if err != nil {
		return nil, err
	}
	rejections := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "tenant_rate_limit_rejections_total", Help: "Tenant requests rejected by reason"}, []string{"tenant_id", "reason"})
	if registerer != nil {
		if err = registerer.Register(rejections); err != nil {
			return nil, err
		}
	}
	return &Limiter{client: redis.NewClient(options), rate: rate, burst: burst, failOpen: failOpen, rejections: rejections}, nil
}

func (l *Limiter) Close() error { return l.client.Close() }

func (l *Limiter) Ping(ctx context.Context) error { return l.client.Ping(ctx).Err() }

func (l *Limiter) Allow(ctx context.Context, tenantID string) (Result, error) {
	value, err := l.client.Eval(ctx, tokenBucketScript, []string{"runmesh:rate:" + tenantID}, l.rate, l.burst).Slice()
	if err != nil {
		return Result{}, err
	}
	if len(value) != 3 {
		return Result{}, fmt.Errorf("unexpected Redis rate-limit result")
	}
	allowed, ok := value[0].(int64)
	if !ok {
		return Result{}, fmt.Errorf("invalid Redis allow result")
	}
	remaining, ok := value[1].(int64)
	if !ok {
		return Result{}, fmt.Errorf("invalid Redis remaining result")
	}
	retryMilliseconds, ok := value[2].(int64)
	if !ok {
		return Result{}, fmt.Errorf("invalid Redis retry result")
	}
	return Result{Allowed: allowed == 1, Remaining: int(remaining), RetryAfter: time.Duration(retryMilliseconds) * time.Millisecond}, nil
}

func (l *Limiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal := auth.PrincipalFrom(r.Context())
		result, err := l.Allow(r.Context(), principal.TenantID)
		if err != nil {
			l.rejections.WithLabelValues(principal.TenantID, "redis_unavailable").Inc()
			if l.failOpen {
				slog.Warn("rate limit unavailable; allowing development request", "tenant_id", principal.TenantID, "error", err)
				next.ServeHTTP(w, r)
				return
			}
			writeError(w, http.StatusServiceUnavailable, "rate limiter unavailable")
			return
		}
		w.Header().Set("RateLimit-Limit", strconv.Itoa(l.burst))
		w.Header().Set("RateLimit-Remaining", strconv.Itoa(result.Remaining))
		if !result.Allowed {
			l.rejections.WithLabelValues(principal.TenantID, "limit_exceeded").Inc()
			retrySeconds := int(result.RetryAfter.Round(time.Second) / time.Second)
			if retrySeconds < 1 {
				retrySeconds = 1
			}
			w.Header().Set("Retry-After", strconv.Itoa(retrySeconds))
			writeError(w, http.StatusTooManyRequests, "tenant rate limit exceeded")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `{"error":%q}`, message)
}
