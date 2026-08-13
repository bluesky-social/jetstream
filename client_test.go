package jetstream

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/segment"
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
		{name: "bare host defaults https", in: "jetstream.us-west.bsky.network", want: "https://jetstream.us-west.bsky.network"},
		{name: "bare localhost defaults http", in: "localhost:8080", want: "http://localhost:8080"},
		{name: "bare localhost no port", in: "localhost", want: "http://localhost"},
		{name: "bare 127.0.0.1 defaults http", in: "127.0.0.1:8080", want: "http://127.0.0.1:8080"},
		{name: "bare ipv6 loopback defaults http", in: "[::1]:8080", want: "http://[::1]:8080"},
		{name: "sub.localhost defaults http", in: "foo.localhost:8080", want: "http://foo.localhost:8080"},
		{name: "explicit https localhost honored", in: "https://localhost:8080", want: "https://localhost:8080"},
		{name: "non-loopback ip defaults https", in: "10.0.0.5:8080", want: "https://10.0.0.5:8080"},
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

func TestZstdCompressionDefaultsOnAndCanBeDisabled(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	require.True(t, cfg.zstdCompression, "live dictionary compression should be enabled by default")

	WithZstdCompression(false)(&cfg)
	require.False(t, cfg.zstdCompression)

	WithZstdCompression(true)(&cfg)
	require.True(t, cfg.zstdCompression, "options are applied in order; explicit enable should restore the default")
}

func TestOptionsRejectNonPositive(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig()
	WithBatchSize(0)(&cfg)
	WithBatchSize(-5)(&cfg)
	WithDownloadConcurrency(0)(&cfg)
	WithDownloadConcurrency(-3)(&cfg)
	require.Equal(t, defaultBatchSize, cfg.batchSize, "non-positive batch size must be ignored")
	require.Equal(t, defaultDownloadConc(), cfg.downloadConc, "non-positive concurrency must be ignored (auto-sized default retained)")
}

// Not parallel: mutates the process-global GOMAXPROCS.
//
//nolint:paralleltest // mutates process-global GOMAXPROCS
func TestDefaultDownloadConcClamps(t *testing.T) {
	// The auto-sized default tracks GOMAXPROCS but is clamped to a safe band so a
	// tiny box does not go near-serial and a 256-core box does not spawn 256
	// in-flight downloads.
	got := defaultDownloadConc()
	require.GreaterOrEqual(t, got, minAutoDownloadConc, "must not drop below the floor on a small machine")
	require.LessOrEqual(t, got, maxAutoDownloadConc, "must not exceed the cap on a many-core machine")

	// Drive GOMAXPROCS to the extremes and confirm the clamp, restoring it after.
	prev := runtime.GOMAXPROCS(0)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })

	runtime.GOMAXPROCS(1)
	require.Equal(t, minAutoDownloadConc, defaultDownloadConc(), "1 core clamps up to the floor")

	runtime.GOMAXPROCS(maxAutoDownloadConc * 4)
	require.Equal(t, maxAutoDownloadConc, defaultDownloadConc(), "many cores clamp down to the cap")
}

func TestWithMaxDownloadAttempts(t *testing.T) {
	t.Parallel()

	// Non-positive is ignored: the config stays unset and newXRPCClient
	// leaves xrpc on its default retry policy (Retry unset).
	cfg := defaultConfig()
	WithMaxDownloadAttempts(0)(&cfg)
	WithMaxDownloadAttempts(-3)(&cfg)
	require.Zero(t, cfg.maxDownloadAttempts, "non-positive attempt cap must be ignored")
	c := newXRPCClient("http://h", cfg, nil)
	require.False(t, c.Retry.HasVal(), "unset attempt cap must leave the default retry policy")

	// A positive cap sets the xrpc retry policy's MaxAttempts, on both the
	// default-transport and custom-HTTP-client paths (retry is orthogonal to
	// transport).
	cfg = defaultConfig()
	WithMaxDownloadAttempts(2)(&cfg)
	require.Equal(t, 2, cfg.maxDownloadAttempts)

	c = newXRPCClient("http://h", cfg, nil)
	require.True(t, c.Retry.HasVal(), "attempt cap must set the retry policy")
	require.Equal(t, 2, c.Retry.Val().MaxAttempts.Val())

	cfg.httpClient = &http.Client{}
	c = newXRPCClient("http://h", cfg, nil)
	require.True(t, c.Retry.HasVal(), "attempt cap must apply even with a custom HTTP client")
	require.Equal(t, 2, c.Retry.Val().MaxAttempts.Val())
	require.True(t, c.HTTPClient.HasVal(), "custom HTTP client must still be installed")
}

func TestOptionsCopySlices(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig()
	src := []string{"a", "b"}
	WithCollections(src)(&cfg)
	src[0] = "mutated"
	require.Equal(t, []string{"a", "b"}, cfg.collections, "options must defensively copy slices")

	kinds := []Kind{KindCommit, KindAccount}
	WithKinds(kinds)(&cfg)
	kinds[0] = KindSync
	require.Equal(t, []Kind{KindCommit, KindAccount}, cfg.kinds, "WithKinds must defensively copy its slice")

	dids := []string{"did:plc:a", "did:plc:b"}
	WithDIDs(dids)(&cfg)
	dids[0] = "did:plc:mutated"
	require.Equal(t, []string{"did:plc:a", "did:plc:b"}, cfg.dids, "WithDIDs must defensively copy its slice")
}

func TestSubscribeCanonicalizesFiltersOnce(t *testing.T) {
	t.Parallel()

	c, err := Subscribe("host",
		WithKinds([]Kind{KindCommit, KindAccount, KindCommit}),
		WithDIDs([]string{"did:plc:a", "did:plc:b", "did:plc:a"}),
		WithCollections([]string{"app.bsky.feed.post", "app.bsky.feed.*", "app.bsky.feed.post"}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })

	e, ok := c.engine.(*replayEngine)
	require.True(t, ok)
	require.Equal(t, []Kind{KindCommit, KindAccount}, e.cfg.Request.Kinds)
	require.Equal(t, []string{"did:plc:a", "did:plc:b"}, e.cfg.Request.DIDs)
	require.Equal(t, []string{"app.bsky.feed.post", "app.bsky.feed.*"}, e.cfg.Request.Collections)
}

func TestWithAPIKeyValidation(t *testing.T) {
	t.Parallel()

	for _, apiKey := range []string{"opaque-key", "Bearer included-by-caller", " padded ", "snowman-☃", "base64=="} {
		t.Run(apiKey, func(t *testing.T) {
			t.Parallel()
			cfg := defaultConfig()
			WithAPIKey(apiKey)(&cfg)
			require.True(t, cfg.hasAPIKey)
			require.Equal(t, apiKey, cfg.apiKey, "nonempty API keys are opaque transport values")
			require.NoError(t, validateConfig(&cfg))
		})
	}

	cfg := defaultConfig()
	require.NoError(t, validateConfig(&cfg), "omitting WithAPIKey preserves unauthenticated behavior")
	WithAPIKey("")(&cfg)
	err := validateConfig(&cfg)
	require.ErrorContains(t, err, "API key cannot be empty")
	require.NotContains(t, err.Error(), "Bearer")
}

func TestClientFormattingRedactsAPIKey(t *testing.T) {
	t.Parallel()

	const apiKey = "distinctive-formatting-secret-9fd0"
	c, err := Subscribe("https://host", WithAPIKey(apiKey))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	for _, formatted := range []string{fmt.Sprintf("%v", c), fmt.Sprintf("%+v", c), fmt.Sprintf("%#v", c)} {
		require.NotContains(t, formatted, apiKey)
		require.Less(t, len(formatted), 256, "formatted client summary must stay bounded")
		require.Contains(t, strings.ToLower(formatted), "host")
	}
	require.Equal(t, "<nil>", fmt.Sprintf("%v", (*Client)(nil)))
	require.NotPanics(t, func() { _ = fmt.Sprintf("%#v", &Client{}) })
}

func TestSubscribeValidation(t *testing.T) {
	t.Parallel()

	_, err := Subscribe("")
	require.Error(t, err, "empty host must error")

	_, err = Subscribe("host", WithKinds([]Kind{"wat"}))
	require.ErrorContains(t, err, "invalid kind")

	_, err = Subscribe("host", WithDIDs([]string{"not-a-did"}))
	require.ErrorContains(t, err, "invalid DID")

	_, err = Subscribe("host", WithCollections([]string{"not a collection"}))
	require.ErrorContains(t, err, "invalid collection")

	_, err = Subscribe("host", WithCollections([]string{"app.*"}))
	require.ErrorContains(t, err, "invalid collection wildcard")

	tooManyCollections := make([]string, maxClientCollections+1)
	for i := range tooManyCollections {
		tooManyCollections[i] = "com.example.c" + strconv.Itoa(i)
	}
	_, err = Subscribe("host", WithCollections(tooManyCollections))
	require.ErrorContains(t, err, "too many collections")

	_, err = Subscribe("host", WithKinds([]Kind{KindAccount}), WithCollections([]string{"app.bsky.feed.post"}))
	require.ErrorContains(t, err, "kinds excludes commit")

	_, err = Subscribe("host", WithAfterSeq(100), WithBeforeSeq(100), WithSnapshotOnly())
	require.Error(t, err, "beforeSeq must be strictly greater than afterSeq")

	_, err = Subscribe("host", WithAfterSeq(100), WithBeforeSeq(50), WithSnapshotOnly())
	require.Error(t, err)

	// WithBeforeSeq requires WithSnapshotOnly: on a replay that continues live,
	// the archive upper bound would also gate the live tail and silently drop
	// every event past beforeSeq (F1).
	_, err = Subscribe("host", WithAfterSeq(10), WithBeforeSeq(100))
	require.ErrorContains(t, err, "WithBeforeSeq requires WithSnapshotOnly")

	// With WithSnapshotOnly it is a coherent bounded snapshot.
	c, err := Subscribe("host", WithAfterSeq(10), WithBeforeSeq(100), WithSnapshotOnly())
	require.NoError(t, err)
	require.NoError(t, c.Close())

	// WithSnapshotOnly requires a replay bound: without one there is no
	// archive snapshot to deliver.
	_, err = Subscribe("host", WithSnapshotOnly())
	require.ErrorContains(t, err, "WithSnapshotOnly requires a replay bound")

	// With a bound it is accepted, and Close must not panic despite the engine
	// having no live buffer allocated in dump mode.
	c, err = Subscribe("host", WithAfterSeq(0), WithSnapshotOnly())
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

func TestConcurrentEventsIsRejected(t *testing.T) {
	t.Parallel()
	h := newEngineHarness(t)
	h.as.addSegment(t, "seg_0000000000.jss", []segment.Event{
		makeCreate(t, 1, "did:plc:a", "app.bsky.feed.post", "r1"),
	})
	h.planned = 1
	h.planEntry = []planSeg{{name: "seg_0000000000.jss", index: 0, minSeq: 1, maxSeq: 1}}
	h.installHandlers()

	gate := make(chan struct{})
	h.as.mu.Lock()
	h.as.segGate = gate
	h.as.mu.Unlock()
	cfg := h.cfg()
	cfg.BackfillOnly = true
	c := &Client{engine: newReplayEngine(cfg)}

	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		for range c.Events(context.Background()) {
		}
	}()
	require.Eventually(t, func() bool { return h.as.segReqs.Load() > 0 }, 5*time.Second, time.Millisecond,
		"the first Events iteration never reached the blocked download")

	var gotErr error
	for _, err := range c.Events(context.Background()) {
		gotErr = err
		break
	}
	require.ErrorIs(t, gotErr, ErrFatal)
	require.ErrorContains(t, gotErr, "already running")

	close(gate)
	select {
	case <-firstDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the first Events iteration did not finish")
	}
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
	closes     atomic.Int64
	runErr     error
	started    atomic.Bool
	statsValue Stats
}

func (e *countingEngine) run(ctx context.Context, yield func(*Batch, error) bool) {
	e.started.Store(true)
	<-ctx.Done()
	yield(nil, e.runErr)
}

func (e *countingEngine) stats() Stats { return e.statsValue }

func (e *countingEngine) close() error {
	e.closes.Add(1)
	return nil
}

// TestStatsDelegatesToEngine asserts Client.Stats forwards the engine's
// snapshot, and that a zero-value Client (no engine) reports a zero snapshot
// rather than panicking — the same defensive contract as the other methods.
func TestStatsDelegatesToEngine(t *testing.T) {
	t.Parallel()
	want := Stats{Pages: 3, SealedTip: 60, PlannedThrough: 60, ResidualGap: 0}
	c := &Client{engine: &countingEngine{statsValue: want}}
	require.Equal(t, want, c.Stats(), "Stats must forward the engine snapshot")

	var zero *Client
	require.Equal(t, Stats{}, zero.Stats(), "Stats on a nil Client must be a zero snapshot, not a panic")
	require.Equal(t, Stats{}, (&Client{}).Stats(), "Stats on an engine-less Client must be a zero snapshot")
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

type countingRoundTripper struct {
	base http.RoundTripper
	mu   sync.Mutex
	seen map[string]int64
}

func (rt *countingRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	rt.mu.Lock()
	if rt.seen == nil {
		rt.seen = make(map[string]int64)
	}
	rt.seen[r.URL.Path]++
	rt.mu.Unlock()
	return rt.base.RoundTrip(r)
}

func (rt *countingRoundTripper) count(path string) int64 {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.seen[path]
}

func sealedSegmentFixture(t *testing.T, name string, seqs ...uint64) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: 1})
	require.NoError(t, err)
	for _, seq := range seqs {
		payload, merr := cbor.Marshal(map[string]any{"$type": "app.bsky.feed.post", "text": fmt.Sprintf("event-%d", seq)})
		require.NoError(t, merr)
		_, err = w.Append(segment.Event{
			Seq: seq, WitnessedAt: int64(1_730_000_000_000_000 + seq), Kind: segment.KindCreate,
			DID: "did:plc:auth", Collection: "app.bsky.feed.post", Rkey: fmt.Sprintf("r%d", seq), Rev: fmt.Sprintf("rev%d", seq), Payload: payload,
		})
		require.NoError(t, err)
	}
	_, err = w.Seal()
	require.NoError(t, err)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	return raw
}

func requireExactBearer(t *testing.T, w http.ResponseWriter, r *http.Request, apiKey string) bool {
	t.Helper()
	if got := r.Header.Values("Authorization"); len(got) != 1 || got[0] != "Bearer "+apiKey {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}

func serveSegmentFixture(t *testing.T, w http.ResponseWriter, r *http.Request, raw []byte, name string) {
	t.Helper()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", `"fixture-etag"`)
	http.ServeContent(w, r, name, time.Unix(1_730_000_000, 0), bytes.NewReader(raw))
}

func TestSubscribeAPIKeyAuthenticatesAllArchiveRequests(t *testing.T) {
	t.Parallel()
	const apiKey = "opaque-root-archive-key"
	wholeName, blockName := "seg_0000000000.jss", "seg_0000000001.jss"
	whole := sealedSegmentFixture(t, wholeName, 1)
	blocks := sealedSegmentFixture(t, blockName, 2)
	var planCalls, segmentCalls, blockCalls atomic.Int64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !requireExactBearer(t, w, r, apiKey) {
			return
		}
		switch r.URL.Path {
		case "/xrpc/network.bsky.jetstream.planSnapshot":
			planCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"plannedThroughSeq":2,"sealedTipSeq":2,"segments":[{"name":%q,"index":0,"checksum":"aaaaaaaaaaaaaaaa","minSeq":1,"maxSeq":1,"mode":"segment"},{"name":%q,"index":1,"checksum":"bbbbbbbbbbbbbbbb","minSeq":2,"maxSeq":2,"mode":"blocks","blocks":[{"first":0,"last":0}]}],"stats":{"segmentsExamined":2,"segmentsMatched":2,"blocksMatched":1,"entries":2}}`, wholeName, blockName)
		case "/xrpc/network.bsky.jetstream.getSegment":
			segmentCalls.Add(1)
			require.Equal(t, wholeName, r.URL.Query().Get("name"))
			serveSegmentFixture(t, w, r, whole, wholeName)
		case "/xrpc/network.bsky.jetstream.getBlock":
			blockCalls.Add(1)
			require.Equal(t, blockName, r.URL.Query().Get("segment"))
			hdr, err := segment.ReadSealedHeader(bytes.NewReader(blocks))
			require.NoError(t, err)
			frame, err := segment.ReadBlockFrame(bytes.NewReader(blocks), hdr, 0)
			require.NoError(t, err)
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(frame)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	c, err := Subscribe(srv.URL, WithAPIKey(apiKey), WithAfterSeq(0), WithSnapshotOnly(), WithSegmentStripes(1))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var seqs []uint64
	for batch, eventErr := range c.Events(ctx) {
		require.NoError(t, eventErr)
		for _, event := range batch.Events() {
			seqs = append(seqs, event.Seq)
		}
	}
	require.Equal(t, []uint64{1, 2}, seqs)
	require.Equal(t, int64(1), planCalls.Load())
	require.GreaterOrEqual(t, segmentCalls.Load(), int64(1), "whole-segment probe must execute")
	require.Equal(t, int64(1), blockCalls.Load(), "generated getBlock request must execute")
}

func TestSubscribeAPIKeyDoesNotAuthenticatePublicResources(t *testing.T) {
	t.Parallel()
	const apiKey = "opaque-cutover-key"
	name := "seg_0000000000.jss"
	raw := sealedSegmentFixture(t, name, 1)
	var dictionaryCalls, liveCalls atomic.Int64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/xrpc/network.bsky.jetstream.planSnapshot":
			require.True(t, requireExactBearer(t, w, r, apiKey))
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"plannedThroughSeq":1,"sealedTipSeq":1,"segments":[{"name":%q,"index":0,"checksum":"aaaaaaaaaaaaaaaa","minSeq":1,"maxSeq":1,"mode":"segment"}],"stats":{"segmentsExamined":1,"segmentsMatched":1,"blocksMatched":0,"entries":1}}`, name)
		case "/xrpc/network.bsky.jetstream.getSegment":
			require.True(t, requireExactBearer(t, w, r, apiKey))
			serveSegmentFixture(t, w, r, raw, name)
		case "/xrpc/network.bsky.jetstream.getZstdDictionary":
			dictionaryCalls.Add(1)
			require.Empty(t, r.Header.Values("Authorization"))
			_, _ = w.Write([]byte("invalid dictionary"))
		case "/xrpc/network.bsky.jetstream.subscribeEvents":
			liveCalls.Add(1)
			require.Empty(t, r.Header.Values("Authorization"))
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer func() { _ = conn.Close(websocket.StatusNormalClosure, "done") }()
			_ = conn.Write(r.Context(), websocket.MessageText, []byte(liveCommitFrameJSON(2, "did:plc:auth", "app.bsky.feed.post", "r2")))
			<-r.Context().Done()
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	transport := &countingRoundTripper{base: srv.Client().Transport}
	hc := &http.Client{Transport: transport}
	c, err := Subscribe(srv.URL, WithAPIKey(apiKey), WithAfterSeq(0), WithSegmentStripes(1), WithHTTPClient(hc))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var seqs []uint64
	for batch, eventErr := range c.Events(ctx) {
		if eventErr != nil {
			continue
		}
		for _, event := range batch.Events() {
			seqs = append(seqs, event.Seq)
		}
		if len(seqs) >= 2 {
			cancel()
			break
		}
	}
	require.Equal(t, []uint64{1, 2}, seqs, "archive event must precede live event across cutover")
	require.GreaterOrEqual(t, dictionaryCalls.Load(), int64(1))
	require.GreaterOrEqual(t, liveCalls.Load(), int64(1))
	for _, path := range []string{
		"/xrpc/network.bsky.jetstream.planSnapshot",
		"/xrpc/network.bsky.jetstream.getSegment",
		"/xrpc/network.bsky.jetstream.getZstdDictionary",
		"/xrpc/network.bsky.jetstream.subscribeEvents",
	} {
		require.GreaterOrEqual(t, transport.count(path), int64(1), "custom WithHTTPClient transport must handle %s", path)
	}
}

// TestSubscribeLiveTailEndToEnd exercises the full public API against a real
// live websocket server: Subscribe (live-only) -> Events -> decoded
// Event, including record decode and cursor extraction.
func TestSubscribeLiveTailEndToEnd(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/xrpc/network.bsky.jetstream.subscribeEvents", r.URL.Path)
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

	c, err := Subscribe(srv.URL, WithZstdCompression(false))
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
// concurrently with a running Events. Before the fix, replayEngine.close() only
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

	c, err := Subscribe(srv.URL, WithZstdCompression(false))
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

// liveCommitFrameJSON builds an xrpc.v1.json #commit message frame matching
// the network.bsky.jetstream.subscribeEvents wire.
func liveCommitFrameJSON(seq uint64, did, coll, rkey string) string {
	s := strconv.FormatUint(seq, 10)
	return `{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#commit"` +
		`,"seq":` + s + `,"did":"` + did + `","time":"1970-01-01T00:00:00.000001Z"` +
		`,"rev":"r","operation":"create","collection":"` + coll +
		`","rkey":"` + rkey + `","cid":"bafytest","record":{"$type":"` + coll + `","text":"hi ` + rkey + `"}}}`
}
