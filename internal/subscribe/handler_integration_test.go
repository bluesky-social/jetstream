package subscribe_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/internal/subscribe"
	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// makeSteadyState writes the steady_state phase marker so the handler's
// IsSteadyState gate passes.
func makeSteadyState(t *testing.T, st *store.Store) {
	t.Helper()
	require.NoError(t, lifecycle.WritePhase(st, lifecycle.PhaseSteadyState, time.Now().UTC()))
}

func TestHandler_ReplaysFromCursor(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	mustWriteSealedSegment(t, filepath.Join(segDir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 0, maxSeq: 9, minIndexedAt: 1_000, maxIndexedAt: 9_999, eventCount: 10,
	})
	m := mustOpenManifest(t, segDir)
	st, w := openWriterAtTip(t, dir, 10)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })
	makeSteadyState(t, st)

	var writerPtr atomic.Pointer[ingest.Writer]
	writerPtr.Store(w)
	cold := subscribe.NewColdReader(subscribe.ColdReaderConfig{
		Manifest:        m,
		WriterRef:       &writerPtr,
		BlockCacheBytes: 1 << 20,
	})
	b, err := subscribe.New(subscribe.Config{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, cold.Read, func() uint64 { return w.NextSeq() })
	require.NoError(t, err)

	srv := httptest.NewServer(subscribe.NewHandler(subscribe.Subscription{
		Tail:     b,
		Store:    st,
		Manifest: m,
		Writer:   w,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:  subscribe.NewMetrics(prometheus.NewRegistry()),
		Lookback: 36 * time.Hour,
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/?cursor=5"
	conn, dialResp, err := websocket.Dial(context.Background(), wsURL, nil)
	require.NoError(t, err)
	if dialResp != nil && dialResp.Body != nil {
		_ = dialResp.Body.Close()
	}
	defer func() { _ = conn.CloseNow() }()

	for want := uint64(5); want <= 9; want++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, body, err := conn.Read(ctx)
		cancel()
		require.NoError(t, err)
		require.Contains(t, string(body), `"cursor":`+strconv.FormatUint(want, 10))
	}
}

// TestHandler_CursorDuringWarmupReturns503 covers the steady-state
// warmup window: the phase marker is durable but the live writer
// pointer hasn't been published yet. A ?cursor= request must get a
// retryable 503 rather than being silently served the live tip (which
// would hand the resuming client a gap of every event between its
// cursor and the live tip).
func TestHandler_CursorDuringWarmupReturns503(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	makeSteadyState(t, st)

	b, err := subscribe.New(subscribe.Config{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, nil, nil)
	require.NoError(t, err)
	m := mustOpenManifest(t, t.TempDir())

	// Note: no Writer and no WriterRef — simulates the warmup window
	// where the steady-state consumer hasn't published its writer yet.
	srv := httptest.NewServer(subscribe.NewHandler(subscribe.Subscription{
		Tail: b, Store: st, Manifest: m,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:  subscribe.NewMetrics(prometheus.NewRegistry()),
		Lookback: 36 * time.Hour,
	}))
	defer srv.Close()

	// A cursor request must be refused with a retryable 503.
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/?cursor=5", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode,
		"cursor request during warmup must be retryable, not silently served live")

	// A NO-cursor (live) request must ALSO be refused during warmup. The
	// Tail's live tip is 0 until the writer publishes; anchoring a live
	// client there makes it dive the whole archive cold once real events
	// arrive at a high seq. Regression guard for that full-replay bug.
	liveReq, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/", nil)
	require.NoError(t, err)
	liveResp, err := http.DefaultClient.Do(liveReq)
	require.NoError(t, err)
	t.Cleanup(func() { _ = liveResp.Body.Close() })
	require.Equal(t, http.StatusServiceUnavailable, liveResp.StatusCode,
		"live request during warmup must be refused: the live tip is not yet known")
}

func TestHandler_RejectsInvalidCursor(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	st, w := openWriterAtTip(t, dir, 0)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })
	makeSteadyState(t, st)

	b, err := subscribe.New(subscribe.Config{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, nil, nil)
	require.NoError(t, err)
	m := mustOpenManifest(t, t.TempDir())

	srv := httptest.NewServer(subscribe.NewHandler(subscribe.Subscription{
		Tail: b, Store: st, Manifest: m, Writer: w,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:  subscribe.NewMetrics(prometheus.NewRegistry()),
		Lookback: 36 * time.Hour,
	}))
	defer srv.Close()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/?cursor=notanumber", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}
