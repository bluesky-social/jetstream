package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/internal/xrpcapi"
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

	rt, err := jetstreamd.Build(ctx, jetstreamd.Options{
		PublicAddr:                     "127.0.0.1:0",
		DebugAddr:                      "127.0.0.1:0",
		DataDir:                        dataDir,
		RelayURL:                       relay.URL,
		OTelServiceName:                "jetstream-test",
		LogLevel:                       "warn",
		LogFormat:                      "text",
		LogOutput:                      io.Discard,
		ShutdownTimeout:                5 * time.Second,
		ClientDrainTimeout:             10 * time.Second,
		CursorLookback:                 36 * time.Hour,
		PlanMaxDIDs:                    xrpcapi.DefaultPlanMaxDIDs,
		PlanMaxCollections:             xrpcapi.DefaultPlanMaxCollections,
		PlanMaxEntries:                 xrpcapi.DefaultPlanMaxEntries,
		PlanWholeSegmentThreshold:      xrpcapi.DefaultPlanWholeSegmentThreshold,
		SubscribeReadLogRetentionBytes: 1 << 20,
		SubscribeBlockCacheBytes:       1 << 20,
		SubscribeReadBatch:             128,
		SubscribeSlowWindow:            time.Second,
		SubscribeSlowMinRate:           5,
		CursorBlockIndexCacheSize:      32,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		require.NoError(t, rt.Close(closeCtx))
	})

	done := make(chan error, 1)
	go func() {
		done <- rt.Run(ctx)
	}()

	publicAddr := waitRuntimePublicAddr(t, rt, done)

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

func waitRuntimePublicAddr(t *testing.T, rt *jetstreamd.Runtime, done <-chan error) string {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if addr := rt.PublicAddr(); addr != "" {
			return addr
		}
		select {
		case err := <-done:
			t.Fatalf("serve exited before binding public listener: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatal("serve never bound public listener")
	return ""
}

func waitRuntimeSteadyState(t *testing.T, rt *jetstreamd.Runtime, done <-chan error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ready := make(chan error, 1)
	go func() { ready <- rt.WaitSteadyState(ctx) }()
	select {
	case err := <-ready:
		require.NoError(t, err, "runtime did not publish steady-state writer")
	case err := <-done:
		t.Fatalf("serve exited before steady-state writer was published: %v", err)
	}
}
