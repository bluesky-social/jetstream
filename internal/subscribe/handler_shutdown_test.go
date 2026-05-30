package subscribe_test

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/internal/subscribe"
	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestHandler_GracefulShutdownSendsGoingAway is the regression test for
// the original report: a live websocket subscriber must receive a clean
// StatusGoingAway close frame when the broadcaster drains, rather than
// hanging until TCP teardown.
func TestHandler_GracefulShutdownSendsGoingAway(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	makeSteadyState(t, st)

	b, err := subscribe.New(subscribe.Config{Logger: discardLogger()})
	require.NoError(t, err)

	srv := httptest.NewServer(subscribe.NewHandler(subscribe.HandlerDeps{
		Broadcaster: b,
		Store:       st,
		Logger:      discardLogger(),
		Metrics:     subscribe.NewMetrics(prometheus.NewRegistry()),
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn, resp, err := websocket.Dial(context.Background(), wsURL, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	defer func() { _ = conn.CloseNow() }()

	// Let the handler register its connection in the shutdown registry.
	time.Sleep(100 * time.Millisecond)

	// Drain. The handler should send each live subscriber a close frame.
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()
	require.NoError(t, b.Shutdown(shutCtx))

	// The client's next read must surface the server's close frame with
	// StatusGoingAway — not block until our deadline, and not an abrupt
	// abnormal closure.
	readCtx, readCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer readCancel()
	_, _, readErr := conn.Read(readCtx)
	require.Error(t, readErr)
	require.Equal(t, websocket.StatusGoingAway, websocket.CloseStatus(readErr),
		"client must receive a clean StatusGoingAway close frame on shutdown")
}

// TestHandler_RejectsConnectionsDuringDrain proves a connection that
// arrives after drain has begun is closed immediately rather than served
// (it missed the closer snapshot, so it would otherwise hang forever).
func TestHandler_RejectsConnectionsDuringDrain(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	makeSteadyState(t, st)

	b, err := subscribe.New(subscribe.Config{Logger: discardLogger()})
	require.NoError(t, err)

	srv := httptest.NewServer(subscribe.NewHandler(subscribe.HandlerDeps{
		Broadcaster: b,
		Store:       st,
		Logger:      discardLogger(),
		Metrics:     subscribe.NewMetrics(prometheus.NewRegistry()),
	}))
	defer srv.Close()

	// Begin draining before anyone connects.
	shutCtx, shutCancel := context.WithTimeout(context.Background(), time.Second)
	defer shutCancel()
	require.NoError(t, b.Shutdown(shutCtx))

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn, resp, err := websocket.Dial(context.Background(), wsURL, nil)
	if err != nil {
		// Acceptable: handshake refused outright.
		return
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	defer func() { _ = conn.CloseNow() }()

	// If the upgrade succeeded, the connection must be closed promptly
	// rather than left hanging.
	readCtx, readCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer readCancel()
	_, _, readErr := conn.Read(readCtx)
	require.Error(t, readErr, "connection accepted during drain must be closed promptly")
	require.NotErrorIs(t, readErr, context.DeadlineExceeded,
		"connection during drain must not hang until the client's own deadline")
}
