package live

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/coder/websocket"
	"github.com/jcalabro/atmos/streaming"
	"github.com/stretchr/testify/require"
)

// memConn feeds queued firehose frames in order, then blocks until Close.
// It lets the consumer run over an in-memory transport with no socket.
type memConn struct {
	frames chan []byte
	closed chan struct{}
	once   sync.Once
}

func newMemConn(frames ...[]byte) *memConn {
	c := &memConn{frames: make(chan []byte, len(frames)), closed: make(chan struct{})}
	for _, f := range frames {
		c.frames <- f
	}
	return c
}

func (c *memConn) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	select {
	case f := <-c.frames:
		return websocket.MessageBinary, f, nil
	case <-c.closed:
		return 0, nil, io.EOF
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
}

func (c *memConn) Close(websocket.StatusCode, string) error { c.closeOnce(); return nil }
func (c *memConn) CloseNow() error                          { c.closeOnce(); return nil }
func (c *memConn) SetReadLimit(int64)                       {}
func (c *memConn) closeOnce()                               { c.once.Do(func() { close(c.closed) }) }

// TestConsumer_Run_InMemoryDial drives the consumer over an injected
// in-memory Conn (no socket) and asserts events are archived through the
// real atmos pipeline, proving the Dial seam reaches streaming.Options.
func TestConsumer_Run_InMemoryDial(t *testing.T) {
	t.Parallel()

	frames := [][]byte{
		encodeIdentityFrame(t, "did:plc:aaa", 1),
		encodeAccountFrame(t, "did:plc:aaa", 2),
		encodeIdentityFrame(t, "did:plc:bbb", 3),
	}
	conn := newMemConn(frames...)

	var dialedURL string
	st := newTestStore(t)
	dir := filepath.Join(t.TempDir(), "live_segments")

	var delivered atomic.Int64
	c, err := Open(Config{
		SegmentsDir:       dir,
		Store:             st,
		SeqKey:            "live_segments/seq/next",
		CursorKey:         "relay/cursor",
		RelayURL:          "https://relay.invalid",
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Verifier:          newTestVerifier(t),
		MaxEventsPerBlock: 2,
		OnEvent:           func(*segment.Event) { delivered.Add(1) },
		Dial: func(_ context.Context, url string) (streaming.Conn, *http.Response, error) {
			dialedURL = url
			return conn, nil, nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	runErr := make(chan error, 1)
	go func() { runErr <- c.Run(ctx) }()

	require.Eventually(t, func() bool {
		return delivered.Load() >= int64(len(frames))
	}, 3*time.Second, 10*time.Millisecond, "consumer never delivered all events over the in-memory dial")

	cancel()
	select {
	case <-runErr:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	require.NoError(t, c.Close())

	require.Equal(t, "wss://relay.invalid/xrpc/com.atproto.sync.subscribeRepos", dialedURL,
		"the injected dialer must receive the derived subscribeRepos URL")

	got := readAllSegmentEvents(t, dir)
	require.Len(t, got, len(frames), "every event fed over the in-memory dial must be archived")
}
