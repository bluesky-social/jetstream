package live

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// syncBuffer is a goroutine-safe io.Writer for capturing log output
// from the consumer running on its own goroutine.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// TestConsumer_Run_CleanShutdownNoCloseWarning reproduces the reported
// shutdown wart: on a clean ctx-cancel the consumer's deferred
// client.Close races the streaming layer's own conn.CloseNow, logging a
// spurious "client close: use of closed network connection" WARN. A
// clean shutdown must be silent.
func TestConsumer_Run_CleanShutdownNoCloseWarning(t *testing.T) {
	t.Parallel()

	f := &fakeFirehose{t: t, frames: [][]byte{
		encodeIdentityFrame(t, "did:plc:aaa", 1),
	}}
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)

	st := newTestStore(t)
	dir := filepath.Join(t.TempDir(), "live_segments")

	logs := &syncBuffer{}
	c, err := Open(Config{
		SegmentsDir: dir,
		Store:       st,
		SeqKey:      "live_segments/seq/next",
		CursorKey:   "relay/cursor",
		RelayURL:    srv.URL,
		Logger:      slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn})),
		Verifier:    newTestVerifier(t),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	ctx, cancel := context.WithCancel(t.Context())

	runErr := make(chan error, 1)
	go func() { runErr <- c.Run(ctx) }()

	require.Eventually(t, func() bool {
		return c.LastUpstreamSeq() >= 1
	}, 3*time.Second, 10*time.Millisecond, "consumer never processed the upstream event")

	cancel()
	select {
	case err := <-runErr:
		require.True(t, err == nil || errors.Is(err, context.Canceled),
			"clean ctx-cancel shutdown must return nil or context.Canceled, got %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancel")
	}

	require.NotContains(t, logs.String(), "client close",
		"clean shutdown must not log a spurious client-close warning")
	require.NotContains(t, logs.String(), "use of closed network connection",
		"clean shutdown must not surface a use-of-closed-conn error")
}
