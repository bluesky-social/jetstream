package jetstream

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/require"
)

func TestNormalizeHost(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{name: "bare host", in: "jetstream.us-west.bsky.network", want: "https://jetstream.us-west.bsky.network"},
		{name: "host:port", in: "localhost:8080", want: "https://localhost:8080"},
		{name: "http url", in: "http://localhost:8080", want: "http://localhost:8080"},
		{name: "https url", in: "https://host", want: "https://host"},
		{name: "ws to http", in: "ws://localhost:8080", want: "http://localhost:8080"},
		{name: "wss to https", in: "wss://host", want: "https://host"},
		{name: "strips path", in: "https://host/subscribe", want: "https://host"},
		{name: "trims space", in: "  host  ", want: "https://host"},
		{name: "empty", in: "", wantErr: true},
		{name: "blank", in: "   ", wantErr: true},
		{name: "bad scheme", in: "ftp://host", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := normalizeHost(tc.in)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestOptionsApply(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig()
	require.Equal(t, defaultBatchSize, cfg.batchSize)
	require.Equal(t, defaultDownloadConc, cfg.downloadConc)
	require.False(t, cfg.backfillRequested())

	for _, opt := range []Option{
		WithCollections([]string{"app.bsky.feed.post", "app.bsky.feed.*"}),
		WithDIDs([]string{"did:plc:abc"}),
		WithAfterSeq(10),
		WithBeforeSeq(100),
		WithBatchSize(256),
		WithDownloadConcurrency(4),
	} {
		opt(&cfg)
	}

	require.Equal(t, []string{"app.bsky.feed.post", "app.bsky.feed.*"}, cfg.collections)
	require.Equal(t, []string{"did:plc:abc"}, cfg.dids)
	require.True(t, cfg.hasAfterSeq)
	require.EqualValues(t, 10, cfg.afterSeq)
	require.True(t, cfg.hasBeforeSeq)
	require.EqualValues(t, 100, cfg.beforeSeq)
	require.Equal(t, 256, cfg.batchSize)
	require.Equal(t, 4, cfg.downloadConc)
	require.True(t, cfg.backfillRequested())
}

func TestOptionsRejectNonPositive(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig()
	WithBatchSize(0)(&cfg)
	WithBatchSize(-5)(&cfg)
	WithDownloadConcurrency(0)(&cfg)
	require.Equal(t, defaultBatchSize, cfg.batchSize, "non-positive batch size must be ignored")
	require.Equal(t, defaultDownloadConc, cfg.downloadConc, "non-positive concurrency must be ignored")
}

func TestOptionsCopySlices(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig()
	src := []string{"a", "b"}
	WithCollections(src)(&cfg)
	src[0] = "mutated"
	require.Equal(t, []string{"a", "b"}, cfg.collections, "options must defensively copy slices")
}

func TestSubscribeValidation(t *testing.T) {
	t.Parallel()

	_, err := Subscribe("")
	require.Error(t, err, "empty host must error")

	_, err = Subscribe("host", WithAfterSeq(100), WithBeforeSeq(100))
	require.Error(t, err, "beforeSeq must be strictly greater than afterSeq")

	_, err = Subscribe("host", WithAfterSeq(100), WithBeforeSeq(50))
	require.Error(t, err)

	c, err := Subscribe("host", WithAfterSeq(10), WithBeforeSeq(100))
	require.NoError(t, err)
	require.NoError(t, c.Close())
}

// TestSubscribeNilOption asserts a nil Option yields a constructor error
// rather than panicking on a nil func call — public API robustness.
func TestSubscribeNilOption(t *testing.T) {
	t.Parallel()

	_, err := Subscribe("host", nil)
	require.ErrorContains(t, err, "option 0 is nil")

	_, err = Subscribe("host", WithBatchSize(8), nil)
	require.ErrorContains(t, err, "option 1 is nil")
}

func TestBatchLastCursor(t *testing.T) {
	t.Parallel()
	var empty Batch
	require.EqualValues(t, 0, empty.LastCursor())

	b := Batch{events: []Event{{Seq: 3}, {Seq: 7}, {Seq: 5}}}
	require.EqualValues(t, 7, b.LastCursor())
	require.Len(t, b.Events(), 3)
}

func TestClosedClientEventsErrors(t *testing.T) {
	t.Parallel()
	c, err := Subscribe("host")
	require.NoError(t, err)
	require.NoError(t, c.Close())
	require.NoError(t, c.Close(), "Close is idempotent")

	var gotErr error
	for _, err := range c.Events(context.Background()) {
		gotErr = err
		break
	}
	require.Error(t, gotErr, "Events on a closed client must yield an error")
}

// TestZeroValueClientFailsClosed asserts that calling methods on a Client that
// bypassed the Subscribe constructor (zero value or nil pointer) returns a
// deterministic error instead of a nil-pointer panic. Client is exported, so
// misuse is reachable; failing closed is friendlier than crashing in cleanup.
func TestZeroValueClientFailsClosed(t *testing.T) {
	t.Parallel()

	t.Run("zero-value Close", func(t *testing.T) {
		t.Parallel()
		var c Client
		require.ErrorIs(t, c.Close(), errClientNotInitialized)
	})
	t.Run("nil-pointer Close", func(t *testing.T) {
		t.Parallel()
		var c *Client
		require.ErrorIs(t, c.Close(), errClientNotInitialized)
	})
	t.Run("zero-value Events", func(t *testing.T) {
		t.Parallel()
		var c Client
		var gotErr error
		for _, err := range c.Events(context.Background()) {
			gotErr = err
			break
		}
		require.ErrorIs(t, gotErr, errClientNotInitialized)
	})
	t.Run("nil-pointer Events", func(t *testing.T) {
		t.Parallel()
		var c *Client
		var gotErr error
		for _, err := range c.Events(context.Background()) {
			gotErr = err
			break
		}
		require.ErrorIs(t, gotErr, errClientNotInitialized)
	})
}

// countingEngine records how many times close() is invoked, so tests can
// assert Close drives the engine exactly once even under concurrency.
type countingEngine struct {
	closes  atomic.Int64
	runErr  error
	started atomic.Bool
}

func (e *countingEngine) run(ctx context.Context, yield func(*Batch, error) bool) {
	e.started.Store(true)
	<-ctx.Done()
	yield(nil, e.runErr)
}

func (e *countingEngine) close() error {
	e.closes.Add(1)
	return nil
}

// TestCloseConcurrentClosesEngineOnce asserts Close is idempotent and
// race-free under concurrent callers: the engine is closed exactly once and
// every caller observes nil. Run under -race to catch unsynchronized access
// to the close state.
func TestCloseConcurrentClosesEngineOnce(t *testing.T) {
	t.Parallel()
	eng := &countingEngine{}
	c := &Client{engine: eng}

	const n = 16
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			require.NoError(t, c.Close())
		}()
	}
	wg.Wait()

	require.EqualValues(t, 1, eng.closes.Load(), "engine.close must run exactly once")
}

// TestCloseRacesEvents asserts Close can stop a running Events from another
// goroutine without a data race on the close state — the natural shutdown
// pattern for a live tail. Meaningful under -race.
func TestCloseRacesEvents(t *testing.T) {
	t.Parallel()
	eng := &countingEngine{}
	c := &Client{engine: eng}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range c.Events(ctx) {
		}
	}()

	require.NoError(t, c.Close())
	cancel()
	<-done
	require.EqualValues(t, 1, eng.closes.Load())
}

// TestSubscribeLiveTailEndToEnd exercises the full public API against a real
// /subscribe-v2 websocket server: Subscribe (live-only) -> Events -> decoded
// Event, including record decode and cursor extraction.
func TestSubscribeLiveTailEndToEnd(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/subscribe-v2", r.URL.Path)
		require.Equal(t, "true", r.URL.Query().Get("extended"))
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close(websocket.StatusNormalClosure, "done") }()
		for _, frame := range []string{
			liveCommitFrameJSON(1, "did:plc:a", "app.bsky.feed.post", "r1"),
			liveCommitFrameJSON(2, "did:plc:a", "app.bsky.feed.post", "r2"),
		} {
			if err := conn.Write(r.Context(), websocket.MessageText, []byte(frame)); err != nil {
				return
			}
		}
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)

	c, err := Subscribe(srv.URL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var got []Event
	for batch, err := range c.Events(ctx) {
		if err != nil {
			continue
		}
		got = append(got, batch.Events()...)
		if len(got) >= 2 {
			cancel()
			break
		}
	}

	require.Len(t, got, 2)
	require.Equal(t, KindCommit, got[0].Kind)
	require.EqualValues(t, 1, got[0].Seq)
	require.Equal(t, "app.bsky.feed.post", got[0].Commit.Collection)
	require.Equal(t, "r1", got[0].Commit.Rkey)
	require.NotNil(t, got[0].Commit.Record)
	require.EqualValues(t, 2, got[1].Seq)
}

// TestCloseStopsRunningEventsWithoutCtxCancel is the A2 regression guard: the
// documented contract is that Close is "the natural way to stop a live tail"
// concurrently with a running Events. Before the fix, realEngine.close() only
// closed the buffer and cancelled nothing, so a live tail kept its goroutine
// and network reads alive until the Events ctx was cancelled. Here the ctx is
// deliberately a plain Background (never cancelled by the test): Close alone
// must unwind the iteration.
func TestCloseStopsRunningEventsWithoutCtxCancel(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close(websocket.StatusNormalClosure, "done") }()
		// One frame, then go quiet: the tail stays open with no further events,
		// so only Close (cancelling the run ctx) can unwind the consumer.
		_ = conn.Write(r.Context(), websocket.MessageText,
			[]byte(liveCommitFrameJSON(1, "did:plc:a", "app.bsky.feed.post", "r1")))
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)

	c, err := Subscribe(srv.URL)
	require.NoError(t, err)

	first := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		var once sync.Once
		// Background ctx: NOT cancelled anywhere in this test. The iteration must
		// end because Close cancels the run, per the documented contract.
		for batch, err := range c.Events(context.Background()) {
			if err != nil {
				continue
			}
			if len(batch.Events()) > 0 {
				once.Do(func() { close(first) })
			}
		}
	}()

	select {
	case <-first:
	case <-time.After(5 * time.Second):
		t.Fatal("never received the first event")
	}

	require.NoError(t, c.Close())

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not stop a running Events (no ctx cancel) within 5s")
	}
}

// liveCommitFrameJSON builds an extended commit frame with a minimal CBOR
// record, base64-encoded, matching the /subscribe-v2 extended wire shape.
func liveCommitFrameJSON(seq uint64, did, coll, rkey string) string {
	rec, _ := cbor.Marshal(map[string]any{"$type": coll, "text": "hi " + rkey})
	b64 := base64.StdEncoding.EncodeToString(rec)
	return `{"did":"` + did + `","time_us":1,"cursor":` + strconv.FormatUint(seq, 10) +
		`,"seq":` + strconv.FormatUint(seq, 10) +
		`,"kind":"commit","commit":{"rev":"r","operation":"create","collection":"` + coll +
		`","rkey":"` + rkey + `","cid":"bafytest","record_cbor":"` + b64 + `"}}`
}
