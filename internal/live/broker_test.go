package live

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
)

func TestTenantScopedPublishSubscribe(t *testing.T) {
	ctx := context.Background()
	server, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	broker, err := New("redis://"+server.Addr()+"/0", prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = broker.Close() })
	_, tenantA, err := broker.Subscribe(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	_, tenantB, err := broker.Subscribe(ctx, "tenant-b")
	if err != nil {
		t.Fatal(err)
	}
	if err = broker.Publish(ctx, "tenant-a", Event{ID: "event-1", Type: "task.completed"}); err != nil {
		t.Fatal(err)
	}
	select {
	case message := <-tenantA:
		if message == nil || message.Payload == "" {
			t.Fatal("tenant A received an empty message")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("tenant A did not receive its notification")
	}
	select {
	case message := <-tenantB:
		t.Fatalf("tenant B received tenant A notification: %v", message)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestBrokerRecoversWhenRedisReturns(t *testing.T) {
	ctx := context.Background()
	server, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	address := server.Addr()
	broker, err := New("redis://"+address+"/0", prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = broker.Close() })
	if err = broker.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	server.Close()
	if err = broker.Ping(ctx); err == nil {
		t.Fatal("expected Redis outage to be reported")
	}
	server = miniredis.NewMiniRedis()
	if err = server.StartAddr(address); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	deadline := time.Now().Add(2 * time.Second)
	for err = broker.Ping(ctx); err != nil && time.Now().Before(deadline); err = broker.Ping(ctx) {
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("broker did not recover: %v", err)
	}
}
