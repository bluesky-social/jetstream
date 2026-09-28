package subscribe

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"syscall"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

// A client that stops reading fails the server's write with the write
// deadline, which is what disconnects_total{reason="write_timeout"} counts
// (specs/notes/2026-09-28-disaggregated-testbed-findings.md finding 9b).
func TestWriteFailureReason_StalledClient(t *testing.T) {
	t.Parallel()
	got := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			got <- "accept: " + err.Error()
			return
		}
		defer func() { _ = conn.CloseNow() }()
		frame := make([]byte, 1<<20)
		for {
			wctx, cancel := context.WithTimeout(r.Context(), 50*time.Millisecond)
			err := conn.Write(wctx, websocket.MessageBinary, frame)
			cancel()
			if err != nil {
				got <- writeFailureReason(r.Context(), err)
				return
			}
		}
	}))
	defer srv.Close()

	conn, resp, err := websocket.Dial(t.Context(), "ws"+srv.URL[len("http"):], nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	require.NoError(t, err)
	defer func() { _ = conn.CloseNow() }()
	select {
	case reason := <-got:
		require.Equal(t, disconnectWriteTimeout, reason)
	case <-time.After(10 * time.Second):
		t.Fatal("server write never timed out")
	}
}

func TestWriteFailureReason(t *testing.T) {
	t.Parallel()
	live := context.Background()
	closed, cancel := context.WithCancel(context.Background())
	cancel()
	require.Equal(t, disconnectWriteTimeout, writeFailureReason(live, context.DeadlineExceeded))
	require.Equal(t, disconnectWriteError, writeFailureReason(live, errors.New("frame too large")))
	for _, gone := range []error{syscall.ECONNRESET, syscall.EPIPE, net.ErrClosed, io.EOF} {
		wrapped := fmt.Errorf("failed to write msg: %w", &net.OpError{Op: "write", Net: "tcp", Err: gone})
		require.Empty(t, writeFailureReason(live, wrapped), "%v is the client going away", gone)
	}
	require.Empty(t, writeFailureReason(closed, context.DeadlineExceeded), "a write failed by the client's close is clean")
}

// A client that drops its socket mid-stream resets the connection, which can
// fail the server's write before the reader cancels ctx. The test bed counted
// 92 such fanout exits as write_error.
func TestWriteFailureReason_ClientReset(t *testing.T) {
	t.Parallel()
	got := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			got <- "accept: " + err.Error()
			return
		}
		defer func() { _ = conn.CloseNow() }()
		frame := make([]byte, 16<<10)
		for {
			if err := conn.Write(r.Context(), websocket.MessageBinary, frame); err != nil {
				got <- writeFailureReason(r.Context(), err)
				return
			}
		}
	}))
	defer srv.Close()

	conn, resp, err := websocket.Dial(t.Context(), "ws"+srv.URL[len("http"):], nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	require.NoError(t, err)
	_, _, err = conn.Read(t.Context())
	require.NoError(t, err)
	require.NoError(t, conn.CloseNow())
	select {
	case reason := <-got:
		require.Empty(t, reason)
	case <-time.After(10 * time.Second):
		t.Fatal("server write never failed")
	}
}
