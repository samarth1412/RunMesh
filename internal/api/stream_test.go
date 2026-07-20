package api

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/samarth1412/RunMesh/internal/auth"
	"github.com/samarth1412/RunMesh/internal/live"
)

func TestAuthenticatedTenantStream(t *testing.T) {
	redisServer, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(redisServer.Close)
	broker, err := live.New("redis://"+redisServer.Addr()+"/0", prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = broker.Close() })
	authenticator := auth.New(nil, auth.Config{Dev: true, DevPrincipal: auth.Principal{TenantID: "tenant-a", UserID: "user-a", Role: "viewer"}})
	server := New(nil, time.Second, authenticator.Middleware, authenticator.WorkerMiddleware, nil, nil, "")
	server.Live = broker
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)

	dialer := websocket.Dialer{Subprotocols: []string{"runmesh"}, HandshakeTimeout: time.Second}
	connection, _, err := dialer.Dial("ws"+strings.TrimPrefix(httpServer.URL, "http")+"/v1/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	_, initial, err := connection.ReadMessage()
	if err != nil || !strings.Contains(string(initial), "stream.connected") {
		t.Fatalf("initial=%s err=%v", initial, err)
	}
	if err = broker.Publish(context.Background(), "tenant-a", live.Event{ID: "event-a", Type: "task.completed"}); err != nil {
		t.Fatal(err)
	}
	_ = connection.SetReadDeadline(time.Now().Add(time.Second))
	_, payload, err := connection.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	var event live.Event
	if err = json.Unmarshal(payload, &event); err != nil || event.ID != "event-a" {
		t.Fatalf("event=%+v err=%v", event, err)
	}
}
