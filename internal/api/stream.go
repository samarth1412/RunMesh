package api

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/websocket"
	"github.com/samarth1412/RunMesh/internal/auth"
)

var streamUpgrader = websocket.Upgrader{
	HandshakeTimeout: 5 * time.Second,
	Subprotocols:     []string{"runmesh"},
	CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		if origin == "" {
			return true
		}
		parsed, err := url.Parse(origin)
		return err == nil && (parsed.Host == r.Host || origin == "http://localhost:3000" || origin == "http://localhost:5173")
	},
}

func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	if s.Live == nil {
		writeError(w, http.StatusServiceUnavailable, "live stream unavailable")
		return
	}
	principal := auth.PrincipalFrom(r.Context())
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	pubsub, messages, err := s.Live.Subscribe(ctx, principal.TenantID)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "live stream unavailable")
		return
	}
	defer pubsub.Close()
	connection, err := streamUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer connection.Close()
	connection.SetReadLimit(1024)
	connection.SetPongHandler(func(string) error {
		return connection.SetReadDeadline(time.Now().Add(45 * time.Second))
	})
	_ = connection.SetReadDeadline(time.Now().Add(45 * time.Second))
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		for {
			if _, _, readErr := connection.ReadMessage(); readErr != nil {
				return
			}
		}
	}()
	if err = connection.WriteJSON(map[string]string{"type": "stream.connected"}); err != nil {
		return
	}
	ping := time.NewTicker(20 * time.Second)
	defer ping.Stop()
	for {
		_ = connection.SetWriteDeadline(time.Now().Add(5 * time.Second))
		select {
		case <-ctx.Done():
			return
		case <-closed:
			return
		case message, ok := <-messages:
			if !ok {
				return
			}
			if err = connection.WriteMessage(websocket.TextMessage, []byte(message.Payload)); err != nil {
				return
			}
		case <-ping.C:
			if err = connection.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)); err != nil {
				return
			}
		}
	}
}
