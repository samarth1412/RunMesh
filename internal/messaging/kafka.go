package messaging

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

type Publisher struct {
	writer *kafka.Writer
	topic  string
}

func NewPublisher(brokers []string, topic string) *Publisher {
	return &Publisher{topic: topic, writer: &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, Async: false, BatchTimeout: 10 * time.Millisecond}}
}
func (p *Publisher) Publish(ctx context.Context, key, value []byte, headers map[string]string) error {
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(headers))
	hs := make([]kafka.Header, 0, len(headers))
	for k, v := range headers {
		hs = append(hs, kafka.Header{Key: k, Value: []byte(v)})
	}
	return p.writer.WriteMessages(ctx, kafka.Message{Key: key, Value: value, Headers: hs, Time: time.Now().UTC()})
}
func (p *Publisher) Close() error { return p.writer.Close() }
