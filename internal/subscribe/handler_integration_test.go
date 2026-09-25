package subscribe_test

import (
	"context"
	"encoding/json"
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
	"github.com/bluesky-social/jetstream/internal/metastore/pebblestore"
	"github.com/bluesky-social/jetstream/internal/subscribe"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// makeSteadyState writes the steady_state phase marker so the handler's
// IsSteadyState gate passes.
func makeSteadyState(t *testing.T, st *pebblestore.Store) {
	t.Helper()
	require.NoError(t, lifecycle.WritePhase(t.Context(), st, lifecycle.PhaseSteadyState, time.Now().UTC()))
}

func TestHandler_ReplaysFromCursor(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	mustWriteSealedSegment(t, filepath.Join(segDir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 0, maxSeq: 9, minWitnessedAt: 1_000, maxWitnessedAt: 9_999, eventCount: 10,
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
	st, err := pebblestore.Open(dir, nil)
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

// recentSealedSegment writes a single sealed segment whose events are recent
// (within the lookback window) so its MinSeq is the lookback floor. minSeq is
// returned for the caller to choose a below-floor cursor.
func recentSealedSegment(t *testing.T, dir string, minSeq, maxSeq uint64) {
	t.Helper()
	now := time.Now().UnixMicro()
	mustWriteSealedSegment(t, filepath.Join(dir, "seg_0000000000.jss"), sealedFixture{
		minSeq: minSeq, maxSeq: maxSeq,
		minWitnessedAt: now - int64(10*time.Hour/time.Microsecond),
		maxWitnessedAt: now - int64(1*time.Hour/time.Microsecond),
		eventCount:     10,
	})
}

// newCursorReplaySubscription builds a steady-state /subscribe handler wired
// for seq-cursor replay against a recent single-segment archive (floor =
// minSeq). rejectBelowFloor selects the v2 policy.
func newCursorReplaySubscription(t *testing.T, minSeq, maxSeq uint64, rejectBelowFloor bool) *httptest.Server {
	t.Helper()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	recentSealedSegment(t, segDir, minSeq, maxSeq)
	m := mustOpenManifest(t, segDir)
	st, w := openWriterAtTip(t, dir, maxSeq+1)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })
	makeSteadyState(t, st)

	b, err := subscribe.New(subscribe.Config{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, nil, nil)
	require.NoError(t, err)

	srv := httptest.NewServer(subscribe.NewHandler(subscribe.Subscription{
		Tail: b, Store: st, Manifest: m, Writer: w,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:  subscribe.NewMetrics(prometheus.NewRegistry()),
		Lookback: 36 * time.Hour,
		V2:       rejectBelowFloor,
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestHandler_V2TooOldCursorReturns400 is the §14/D5 v2 contract end-to-end: a
// seq cursor below the lookback floor returns a pre-upgrade HTTP 400 whose body
// carries the floor seq, so the client can re-backfill from its last seq rather
// than be silently clamped (skipping the gap).
func TestHandler_V2TooOldCursorReturns400(t *testing.T) {
	t.Parallel()
	srv := newCursorReplaySubscription(t, 200, 299, true)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/?cursor=50", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))
	body, _ := io.ReadAll(resp.Body)
	var envelope struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(body, &envelope), "v2 pre-upgrade errors are XRPC envelopes, got %q", body)
	require.Equal(t, "CursorTooOld", envelope.Error, "clients match the structured error name")
	require.Contains(t, envelope.Message, "200", "the message must carry the lookback floor seq")
}

// TestHandler_V2ClampedTimestampCursorEmitsOutdatedCursorInfo pins design
// decision 6: a unix-µs timestamp cursor below the retention floor is
// clamped (the legacy v1 translation contract), and on v2 the clamp is
// announced in-band — the FIRST frame is an #info OutdatedCursor naming
// the seq actually resumed from, replacing the silent clamp.
func TestHandler_V2ClampedTimestampCursorEmitsOutdatedCursorInfo(t *testing.T) {
	t.Parallel()
	srv := newCursorReplaySubscription(t, 200, 299, true)

	// A timestamp (>= 1e15 ⇒ unix-µs mode) older than every archived
	// event: translates to the first segment's MinSeq with clamped=true.
	tooOld := time.Now().Add(-20 * time.Hour).UnixMicro()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/?cursor=" + strconv.FormatInt(tooOld, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, dialResp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err, "a clamped timestamp cursor must upgrade, not 400")
	if dialResp != nil && dialResp.Body != nil {
		_ = dialResp.Body.Close()
	}
	defer func() { _ = conn.CloseNow() }()

	_, frame, err := conn.Read(ctx)
	require.NoError(t, err)
	var envelope struct {
		Type    string `json:"$type"`
		Payload struct {
			Type    string `json:"$type"`
			Name    string `json:"name"`
			Message string `json:"message"`
		} `json:"payload"`
	}
	require.NoError(t, json.Unmarshal(frame, &envelope))
	require.Equal(t, "message", envelope.Type)
	require.Equal(t, "network.bsky.jetstream.subscribeEvents#info", envelope.Payload.Type)
	require.Equal(t, "OutdatedCursor", envelope.Payload.Name)
	require.Contains(t, envelope.Payload.Message, "starting at seq 200",
		"the info message names the seq actually resumed from")
}

// TestHandler_V1TooOldCursorClampsAndUpgrades pins v1 parity: with the reject
// flag unset, the same below-floor cursor is silently clamped to the floor and
// the connection upgrades to a websocket (no 400), matching legacy jetstream.
func TestHandler_V1TooOldCursorClampsAndUpgrades(t *testing.T) {
	t.Parallel()
	srv := newCursorReplaySubscription(t, 200, 299, false)

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/?cursor=50"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, dialResp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err, "v1 must clamp and upgrade, not 400")
	if dialResp != nil && dialResp.Body != nil {
		_ = dialResp.Body.Close()
	}
	_ = conn.CloseNow()
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

func newActiveTimestampServer(t *testing.T, v2 bool, blockSize int) (*httptest.Server, *ingest.Writer) {
	t.Helper()
	dir := t.TempDir()
	st, err := pebblestore.Open(dir, pebblestore.NewMetrics(prometheus.NewRegistry()))
	require.NoError(t, err)
	segDir := filepath.Join(dir, "segments")
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:           segDir,
		Store:                 st,
		Logger:                slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:               ingest.NewMetrics(prometheus.NewRegistry()),
		MaxEventsPerBlock:     blockSize,
		MaxSegmentBytes:       1 << 30,
		ReadLogRetentionBytes: 0, // force flushed blocks through active cold replay
	})
	require.NoError(t, err)
	m := mustOpenManifest(t, segDir)
	makeSteadyState(t, st)

	var writerPtr atomic.Pointer[ingest.Writer]
	writerPtr.Store(w)
	cold := subscribe.NewColdReader(subscribe.ColdReaderConfig{
		Manifest:        m,
		WriterRef:       &writerPtr,
		BlockCacheBytes: 1 << 20,
	})
	metrics := subscribe.NewMetrics(prometheus.NewRegistry())
	tail, err := subscribe.New(subscribe.Config{
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics: metrics,
	}, cold.Read, w.NextSeq)
	require.NoError(t, err)
	tail.SetReadLogSource(func() *ingest.ReadableLog { return w.ReadLog() })

	srv := httptest.NewServer(subscribe.NewHandler(subscribe.Subscription{
		Tail: tail, Store: st, Manifest: m, Writer: w,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:  metrics,
		Lookback: 36 * time.Hour,
		V2:       v2,
	}))
	t.Cleanup(func() {
		srv.Close()
		_ = w.Close()
		_ = st.Close()
	})
	return srv, w
}

func appendTimestampEvent(t *testing.T, w *ingest.Writer, witnessedAt int64, did string) uint64 {
	t.Helper()
	ev := &segment.Event{
		WitnessedAt: witnessedAt,
		Kind:        segment.KindDelete,
		DID:         did,
		Collection:  "app.bsky.feed.post",
		Rkey:        "rkey",
		Rev:         "rev",
	}
	require.NoError(t, w.Append(t.Context(), ev))
	return ev.Seq
}

func dialTimestampCursor(t *testing.T, srv *httptest.Server, path string, cursor int64) *websocket.Conn {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + path + "?cursor=" + strconv.FormatInt(cursor, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	return conn
}

func readV1Cursor(t *testing.T, conn *websocket.Conn) (uint64, int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, body, err := conn.Read(ctx)
	require.NoError(t, err)
	var frame struct {
		Cursor uint64 `json:"cursor"`
		TimeUS int64  `json:"time_us"`
	}
	require.NoError(t, json.Unmarshal(body, &frame))
	return frame.Cursor, frame.TimeUS
}

func TestHandler_TimeCursorSeeksInsideActiveFlushedBlock(t *testing.T) {
	t.Parallel()
	srv, w := newActiveTimestampServer(t, false, 2)
	base := time.Now().Add(-10 * time.Minute).UnixMicro()
	for i := range 5 {
		appendTimestampEvent(t, w, base+int64(i)*1_000, "did:plc:active")
	}

	requested := base + 2_500
	conn := dialTimestampCursor(t, srv, "/subscribe", requested)
	cursor, timeUS := readV1Cursor(t, conn)
	require.Equal(t, uint64(4), cursor)
	require.GreaterOrEqual(t, timeUS, requested)
}

func TestHandler_TimeCursorSeeksInsidePendingBlock(t *testing.T) {
	t.Parallel()
	srv, w := newActiveTimestampServer(t, false, 2)
	base := time.Now().Add(-10 * time.Minute).UnixMicro()
	for i := range 5 {
		appendTimestampEvent(t, w, base+int64(i)*1_000, "did:plc:pending")
	}

	conn := dialTimestampCursor(t, srv, "/subscribe", base+3_500)
	cursor, _ := readV1Cursor(t, conn)
	require.Equal(t, uint64(5), cursor)
}

func TestHandler_TimeCursorAfterTipWaitsForQualifyingEvent(t *testing.T) {
	t.Parallel()
	srv, w := newActiveTimestampServer(t, false, 2)
	base := time.Now().Add(-10 * time.Minute).UnixMicro()
	appendTimestampEvent(t, w, base, "did:plc:wait")
	appendTimestampEvent(t, w, base+1_000, "did:plc:wait")
	requested := time.Now().Add(-time.Minute).UnixMicro()
	conn := dialTimestampCursor(t, srv, "/subscribe", requested)

	type readResult struct {
		cursor uint64
		err    error
	}
	result := make(chan readResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, body, err := conn.Read(ctx)
		if err != nil {
			result <- readResult{err: err}
			return
		}
		var frame struct {
			Cursor uint64 `json:"cursor"`
		}
		err = json.Unmarshal(body, &frame)
		result <- readResult{cursor: frame.Cursor, err: err}
	}()

	appendTimestampEvent(t, w, requested-1, "did:plc:wait")
	select {
	case got := <-result:
		require.Failf(t, "early event delivered", "cursor=%d err=%v", got.cursor, got.err)
	case <-time.After(150 * time.Millisecond):
	}
	qualifyingSeq := appendTimestampEvent(t, w, requested, "did:plc:wait")
	select {
	case got := <-result:
		require.NoError(t, got.err)
		require.Equal(t, qualifyingSeq, got.cursor)
	case <-time.After(5 * time.Second):
		require.Fail(t, "qualifying event was not delivered")
	}
}

func TestHandler_TimeGateRunsBeforeFilter(t *testing.T) {
	t.Parallel()
	srv, w := newActiveTimestampServer(t, false, 4)
	base := time.Now().Add(-10 * time.Minute).UnixMicro()
	requested := base + 1_000
	appendTimestampEvent(t, w, base, "did:plc:other")
	appendTimestampEvent(t, w, requested, "did:plc:other")
	wantSeq := appendTimestampEvent(t, w, base+500, "did:plc:wanted")

	path := "/subscribe?wantedDids=did%3Aplc%3Awanted&cursor=" + strconv.FormatInt(requested, 10)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + path
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	defer func() { _ = conn.CloseNow() }()
	cursor, _ := readV1Cursor(t, conn)
	require.Equal(t, wantSeq, cursor, "a filtered qualifying row must still unlock the timestamp gate")
}

func TestHandler_V2ActiveTimeCursorStartsWithEventNotInfo(t *testing.T) {
	t.Parallel()
	srv, w := newActiveTimestampServer(t, true, 2)
	base := time.Now().Add(-10 * time.Minute).UnixMicro()
	appendTimestampEvent(t, w, base, "did:plc:v2")
	wantSeq := appendTimestampEvent(t, w, base+1_000, "did:plc:v2")

	conn := dialTimestampCursor(t, srv, "/xrpc/network.bsky.jetstream.subscribeEvents", base+500)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, body, err := conn.Read(ctx)
	require.NoError(t, err)
	var frame struct {
		Type    string `json:"$type"`
		Payload struct {
			Type string `json:"$type"`
			Seq  int64  `json:"seq"`
		} `json:"payload"`
	}
	require.NoError(t, json.Unmarshal(body, &frame))
	require.Equal(t, "message", frame.Type)
	require.Equal(t, "network.bsky.jetstream.subscribeEvents#commit", frame.Payload.Type)
	require.Equal(t, int64(wantSeq), frame.Payload.Seq)
}
