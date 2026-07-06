package oracle

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/internal/subscribe"
	"github.com/bluesky-social/jetstream/internal/xrpcapi"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// partb_harness_test.go builds the hermetic, real-socket fixture the §16 Part-B
// oracle scenarios drive: a single httptest server that serves BOTH the
// paginated archive XRPC (planBackfill/getSegment/getBlock, mounted at /xrpc/)
// and the live /subscribe-v2 websocket (with the v2 reject-below-floor
// too-old policy), backed by one sealed-segment archive + manifest + writer +
// writer-readable-log Tail. It deliberately avoids the synctest bubble — the oracle package
// allows exactly one bubble per process (owned by TestOracle_DefaultLifecycle),
// so every other server-driving test runs on real sockets (see serveArchive in
// foldconvergence_gate_test.go).
//
// Unlike the full jetstreamd runtime, this harness lets a test deterministically
// control the three Part-B variables the scenarios need:
//
//   - MaxEntries (the planner per-page cap) → multi-page / mid-segment cuts;
//   - the lookback floor (via segment WitnessedAt vs the configured Lookback) →
//     the §14 too-old 400 / stale-cursor / fell-off-live cases;
//   - sealing new segments + appending live events mid-flight (SealMore /
//     AppendLive) → mid-download seal and exhaust-sealed cold-replay backstop.

// pagedCutoverServer is a running hermetic archive+live server. Construct it
// with newPagedCutoverServer; URL is the base the public jetstream client dials
// (both /xrpc/ and /subscribe-v2 hang off it).
type pagedCutoverServer struct {
	t        *testing.T
	URL      string
	dataDir  string
	segDir   string
	manifest *manifest.Manifest
	store    *store.Store
	writer   *ingest.Writer
	tail     *subscribe.Tail

	mu        sync.Mutex
	planCalls atomic.Int64

	// onPlanServed, when set, is invoked AFTER each planBackfill response is
	// written, with the running call count. It lets a test seal a segment at a
	// deterministic point in the pagination loop (e.g. after page 1 pins S) so a
	// mid-download seal is reproducible rather than timing-dependent.
	onPlanServed func(n int64)
}

// pagedCutoverConfig parameterizes the harness.
type pagedCutoverConfig struct {
	// MaxEntries is the planner's per-page work-unit cap (0 = unlimited). Small
	// values force multi-page / mid-segment truncation.
	MaxEntries int
	// WholeSegmentThreshold selects whole-segment vs block-mode planning. 1
	// always plans whole segments; 0 always plans blocks (forcing mid-segment
	// block-range cuts). Mirrors xrpcapi.PlanConfig.
	WholeSegmentThreshold float64
	// Lookback is the cursor-replay window. A seq cursor resolving below the
	// floor (the MinSeq of the freshest in-window segment) gets a v2 400.
	Lookback time.Duration
	// InitialSegments are the segments sealed before the server starts serving,
	// in ascending order. Each inner slice is one segment's events (one block per
	// event, mirroring the production per-block collection index).
	InitialSegments [][]segment.Event
}

// recentMicros returns a timestamp `ago` before now, in microseconds — the unit
// segment WitnessedAt and the lookback floor compare in.
func recentMicros(ago time.Duration) int64 {
	return time.Now().UnixMicro() - ago.Microseconds()
}

// newPagedCutoverServer stands up the harness. It seals InitialSegments, opens
// the manifest/store/writer/tail, seeds the writer tip past the highest sealed
// seq, marks steady-state, and serves /xrpc/ + /subscribe-v2 on one mux.
func newPagedCutoverServer(t *testing.T, cfg pagedCutoverConfig) *pagedCutoverServer {
	t.Helper()
	if cfg.Lookback <= 0 {
		cfg.Lookback = 36 * time.Hour
	}
	dataDir := t.TempDir()
	segDir := filepath.Join(dataDir, "segments")
	require.NoError(t, os.MkdirAll(segDir, 0o755))

	s := &pagedCutoverServer{t: t, dataDir: dataDir, segDir: segDir}

	// Seal the initial segments and learn the highest sealed seq.
	var maxSeq uint64
	for i, evs := range cfg.InitialSegments {
		writeSealedSegment(t, segDir, uint64(i), evs...)
		for _, ev := range evs {
			if ev.Seq > maxSeq {
				maxSeq = ev.Seq
			}
		}
	}
	nextSeq := maxSeq + 1
	if nextSeq == 0 {
		nextSeq = 1 // empty archive: first live event is seq 1 (§R8 1-based seqs)
	}

	logger := slog.New(slog.NewTextHandler(testWriter{t: t}, &slog.HandlerOptions{Level: slog.LevelWarn}))

	m, err := manifest.Open(manifest.Options{SegmentsDir: segDir, Logger: logger})
	require.NoError(t, err)
	require.NoError(t, m.Wait(context.Background()))
	s.manifest = m

	st, err := store.Open(dataDir, store.NewMetrics(prometheus.NewRegistry()))
	require.NoError(t, err)
	require.NoError(t, st.Set([]byte("seq/next"), encodeUint64LEOracle(nextSeq), store.SyncWrites))
	s.store = st

	w, err := ingest.Open(ingest.Config{
		SegmentsDir: segDir,
		Store:       st,
		Logger:      logger,
		Metrics:     ingest.NewMetrics(prometheus.NewRegistry()),
		OnAfterSeal: m.OnSegmentSealed,
	})
	require.NoError(t, err)
	s.writer = w

	require.NoError(t, lifecycle.WritePhase(st, lifecycle.PhaseSteadyState, time.Now().UTC()))

	var writerPtr atomic.Pointer[ingest.Writer]
	writerPtr.Store(w)
	cold := subscribe.NewColdReader(subscribe.ColdReaderConfig{
		Manifest:        m,
		WriterRef:       &writerPtr,
		BlockCacheBytes: 1 << 20,
	})
	tail, err := subscribe.New(subscribe.Config{
		Logger:  logger,
		Metrics: subscribe.NewMetrics(prometheus.NewRegistry()),
	}, cold.Read, w.NextSeq)
	require.NoError(t, err)
	tail.SetReadLogSource(func() *ingest.ReadableLog { return w.ReadLog() })
	s.tail = tail

	t.Cleanup(func() {
		_ = w.Close()
		_ = st.Close()
	})

	xs := xrpcapi.New(xrpcapi.Config{
		Src:    m,
		Logger: logger,
		Plan: xrpcapi.PlanConfig{
			MaxDIDs:               xrpcapi.DefaultPlanMaxDIDs,
			MaxCollections:        xrpcapi.DefaultPlanMaxCollections,
			MaxEntries:            cfg.MaxEntries,
			WholeSegmentThreshold: cfg.WholeSegmentThreshold,
		},
	})

	mux := http.NewServeMux()
	// Count planBackfill calls and fire onPlanServed after each, so a test can
	// seal a segment at a deterministic point in the pagination loop.
	archive := xs.Handler()
	mux.Handle("/xrpc/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		isPlan := strings.HasSuffix(r.URL.Path, "network.bsky.jetstream.planBackfill")
		archive.ServeHTTP(w, r)
		if isPlan {
			n := s.planCalls.Add(1)
			if s.onPlanServed != nil {
				s.onPlanServed(n)
			}
		}
	}))
	mux.Handle("GET /subscribe-v2", subscribe.NewHandler(subscribe.Subscription{
		Tail:     tail,
		Store:    st,
		Manifest: m,
		Writer:   w,
		Logger:   logger,
		Metrics:  subscribe.NewMetrics(prometheus.NewRegistry()),
		Lookback: cfg.Lookback,
		V2:       true,
	}))

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	s.URL = ts.URL
	return s
}

// SealMore appends evs through the real writer and force-rotates the active
// segment, modelling a segment sealed during a paged download or after the loop
// has reached the tip. The seqs in evs must match the writer's next seq.
func (s *pagedCutoverServer) SealMore(evs ...segment.Event) {
	s.t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ev := range evs {
		wantSeq := ev.Seq
		require.Equalf(s.t, s.writer.NextSeq(), wantSeq, "SealMore seq %d must match the writer tip", wantSeq)
		require.NoError(s.t, s.writer.Append(context.Background(), &ev))
		require.Equal(s.t, wantSeq, ev.Seq)
	}
	require.NoError(s.t, s.writer.ForceRotate(context.Background()))
}

// AppendLive appends events through the real writer, modelling steady-state
// ingest above the sealed tip.
func (s *pagedCutoverServer) AppendLive(evs ...segment.Event) {
	s.t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range evs {
		ev := evs[i]
		wantSeq := ev.Seq
		require.Equalf(s.t, s.writer.NextSeq(), wantSeq, "AppendLive seq %d must match the writer tip", wantSeq)
		require.NoError(s.t, s.writer.Append(context.Background(), &ev))
		require.Equal(s.t, wantSeq, ev.Seq)
	}
}

// dialSubscribeV2 performs a raw /subscribe-v2 websocket handshake at the given
// cursor and reports the HTTP status the server returned pre-upgrade, plus the
// response body. A successful upgrade returns 101 (and the connection is closed
// immediately); a too-old cursor returns 400 with the floor in the body. This
// is the raw §14 signal the client's re-backfill keys on.
func dialSubscribeV2(t *testing.T, baseURL string, cursor uint64) (status int, body string) {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(baseURL, "http") + "/subscribe-v2?cursor=" + strconv.FormatUint(cursor, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	if resp != nil && resp.Body != nil {
		b, _ := io.ReadAll(resp.Body)
		body = string(b)
		_ = resp.Body.Close()
	}
	if err == nil {
		_ = conn.Close(websocket.StatusNormalClosure, "")
	}
	if resp != nil {
		return resp.StatusCode, body
	}
	// No HTTP response at all (transport failure): surface as a synthetic 0.
	return 0, body
}

// encodeUint64LEOracle encodes v little-endian for the store's seq/next seed.
func encodeUint64LEOracle(v uint64) []byte {
	b := make([]byte, 8)
	for i := range b {
		b[i] = byte(v >> (8 * i))
	}
	return b
}

// makeOracleCreate builds a create event at seq with a recent timestamp (within
// any reasonable lookback). The timestamp is derived from seq so events stay in
// timestamp order, which the manifest's lookback-floor search relies on.
func makeOracleCreate(seq uint64, did, collection, rkey string) segment.Event {
	return segment.Event{
		Seq:         seq,
		WitnessedAt: recentMicros(time.Hour) + int64(seq), // recent, monotonic in seq
		Kind:        segment.KindCreate,
		DID:         did,
		Collection:  collection,
		Rkey:        rkey,
		Rev:         "rev" + rkey,
		Payload:     []byte{0xa0}, // empty DAG-CBOR map; decodes cleanly in map mode
	}
}

// makeOracleCreateAged is like makeOracleCreate but stamps the event `age`
// before now, so a test can place a segment below a short lookback floor.
func makeOracleCreateAged(seq uint64, did, collection, rkey string, age time.Duration) segment.Event {
	ev := makeOracleCreate(seq, did, collection, rkey)
	ev.WitnessedAt = recentMicros(age) + int64(seq)
	return ev
}
