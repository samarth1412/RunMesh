package live

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

const channelPrefix = "runmesh:live:tenant:"

// Event is the non-durable notification envelope sent after an outbox event
// has been published and committed. Consumers must always re-read durable API
// state instead of treating this payload as the source of truth.
type Event struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	AggregateID string          `json:"aggregate_id"`
	Payload     json.RawMessage `json:"payload"`
	OccurredAt  string          `json:"occurred_at"`
}

type Broker struct {
	client   *redis.Client
	failures *prometheus.CounterVec
}

func New(rawURL string, registerer prometheus.Registerer) (*Broker, error) {
	options, err := redis.ParseURL(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse Redis URL: %w", err)
	}
	failures := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "redis_operation_failures_total",
		Help: "Redis operation failures that degraded a non-durable RunMesh capability.",
	}, []string{"operation"})
	if registerer != nil {
		if err = registerer.Register(failures); err != nil {
			return nil, err
		}
	}
	return &Broker{client: redis.NewClient(options), failures: failures}, nil
}

func (b *Broker) Close() error { return b.client.Close() }

func (b *Broker) Ping(ctx context.Context) error {
	operationContext, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := b.client.Ping(operationContext).Err(); err != nil {
		b.failures.WithLabelValues("ping").Inc()
		return err
	}
	return nil
}

func (b *Broker) Publish(ctx context.Context, tenantID string, event Event) error {
	if tenantID == "" {
		return nil
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	operationContext, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	if err = b.client.Publish(operationContext, channelPrefix+tenantID, payload).Err(); err != nil {
		b.failures.WithLabelValues("publish").Inc()
		return err
	}
	return nil
}

// Subscribe returns only messages published for the authenticated tenant.
func (b *Broker) Subscribe(ctx context.Context, tenantID string) (*redis.PubSub, <-chan *redis.Message, error) {
	pubsub := b.client.Subscribe(ctx, channelPrefix+tenantID)
	handshakeContext, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := pubsub.Receive(handshakeContext); err != nil {
		b.failures.WithLabelValues("subscribe").Inc()
		_ = pubsub.Close()
		return nil, nil, err
	}
	return pubsub, pubsub.Channel(), nil
}
