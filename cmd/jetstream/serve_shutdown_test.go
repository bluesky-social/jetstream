package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/lifecycle"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

// TestServe_GracefulShutdownClosesSubscriber is the end-to-end
// regression for the reported bug: a live /subscribe websocket must
// receive a clean StatusGoingAway close frame when the process is asked
// to shut down (ctx cancel == SIGINT), rather than hanging until TCP
// teardown. It also confirms the process exits well within the drain
// budget once the client has departed.
func TestServe_GracefulShutdownClosesSubscriber(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	{
		s, err := store.Open(dataDir, nil)
		require.NoError(t, err)
		require.NoError(t, lifecycle.WritePhase(s, lifecycle.PhaseSteadyState, time.Now().UTC()))
		require.NoError(t, s.Close())
	}

	// Pre-bind a free port so the test knows where /subscribe lives.
	lc := net.ListenConfig{}
	publicLn, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	publicAddr := publicLn.Addr().String()
	require.NoError(t, publicLn.Close())

	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/com.atproto.sync.subscribeRepos") {
			conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
			if err != nil {
				return
			}
			defer func() { _ = conn.CloseNow() }()
			<-r.Context().Done()
			return
		}
		_ = json.NewEncoder(w).Encode(struct {
			Cursor string `json:"cursor,omitempty"`
			Repos  []any  `json:"repos"`
		}{})
	}))
	t.Cleanup(relay.Close)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	done := make(chan error, 1)
	go func() {
		done <- newApp().Run(ctx, []string{
			"jetstream",
			"--log-format=text",
			"--log-level=warn",
			"serve",
			"--addr=" + publicAddr,
			"--debug-addr=127.0.0.1:0",
			"--shutdown-timeout=5s",
			"--client-drain-timeout=10s",
			"--relay-url=" + relay.URL,
			"--data-dir=" + dataDir,
		})
	}()

	// Dial /subscribe until the handler is live (steady-state gate open).
	wsURL := "ws://" + publicAddr + "/subscribe"
	var conn *websocket.Conn
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		dialCtx, dialCancel := context.WithTimeout(ctx, time.Second)
		c, resp, derr := websocket.Dial(dialCtx, wsURL, nil)
		dialCancel()
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		if derr == nil {
			conn = c
			break
		}
		select {
		case err := <-done:
			t.Fatalf("serve exited before /subscribe came up: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
	}
	require.NotNil(t, conn, "/subscribe never accepted a connection")
	defer func() { _ = conn.CloseNow() }()

	// A real client reads continuously; spin up a reader that surfaces
	// the close status. This mirrors websocat's behavior and lets the
	// close handshake complete in milliseconds.
	closeStatus := make(chan websocket.StatusCode, 1)
	go func() {
		readCtx, readCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer readCancel()
		_, _, rerr := conn.Read(readCtx)
		closeStatus <- websocket.CloseStatus(rerr)
	}()

	// Give the subscriber a beat to register in the shutdown registry.
	time.Sleep(200 * time.Millisecond)

	// SIGINT equivalent.
	shutdownStart := time.Now()
	cancel()

	select {
	case status := <-closeStatus:
		require.Equal(t, websocket.StatusGoingAway, status,
			"subscriber must receive a clean StatusGoingAway close frame on shutdown")
	case <-time.After(11 * time.Second):
		t.Fatal("subscriber never received a close frame within the drain budget")
	}

	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("serve exited with unexpected error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not shut down within deadline")
	}

	require.Less(t, time.Since(shutdownStart), 11*time.Second,
		"shutdown should complete promptly once the client has departed")
}
