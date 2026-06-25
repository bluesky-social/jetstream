package oracle

import (
	"context"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

// TestPipeListener_WebsocketInBubble proves the in-process pipeListener carries
// a full websocket handshake (which needs http.Hijacker), frame exchange, and a
// graceful http.Server.Shutdown entirely inside a synctest bubble with the fake
// clock — the load-bearing primitive for running the runtime's public surface
// in-bubble (WI-1).
//
// nolint:paralleltest // synctest.Test forbids t.Parallel inside the bubble.
func TestPipeListener_WebsocketInBubble(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ln := newPipeListener()

		mux := http.NewServeMux()
		mux.HandleFunc("GET /echo", func(w http.ResponseWriter, r *http.Request) {
			c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
			if err != nil {
				return
			}
			defer func() { _ = c.CloseNow() }()
			for {
				typ, data, err := c.Read(r.Context())
				if err != nil {
					return
				}
				if err := c.Write(r.Context(), typ, data); err != nil {
					return
				}
			}
		})

		srv := &http.Server{Handler: mux}
		serveDone := make(chan struct{})
		go func() {
			defer close(serveDone)
			_ = srv.Serve(ln)
		}()

		client := ln.httpClient()
		conn, resp, err := websocket.Dial(t.Context(), "ws://pipe/echo", &websocket.DialOptions{
			HTTPClient: client,
		})
		require.NoError(t, err)
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}

		// Round-trip a frame: proves the handshake hijacked the pipe conn and
		// the bubble settles (durably blocked) between request and response.
		require.NoError(t, conn.Write(t.Context(), websocket.MessageText, []byte("ping")))
		typ, got, err := conn.Read(t.Context())
		require.NoError(t, err)
		require.Equal(t, websocket.MessageText, typ)
		require.Equal(t, "ping", string(got))

		// Fake clock still advances with the connection idle/open: prove a
		// timer fires in zero real time while the websocket is held.
		start := time.Now()
		time.Sleep(time.Hour)
		require.Equal(t, time.Hour, time.Since(start))

		_ = conn.Close(websocket.StatusNormalClosure, "done")

		// Graceful shutdown must complete inside the bubble; every bubble
		// goroutine must exit before the bubble function returns.
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, srv.Shutdown(shutCtx))
		_ = ln.Close()
		<-serveDone
	})
}
