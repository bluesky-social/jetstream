package live

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/bluesky-social/jetstream/internal/tombstone"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/coder/websocket"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/api/lextypes"
	"github.com/jcalabro/atmos/car"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/mst"
	atmosrepo "github.com/jcalabro/atmos/repo"
	"github.com/jcalabro/atmos/streaming"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// fakeFirehose is a minimal subscribeRepos server: it upgrades to
// a WebSocket and writes a scripted sequence of CBOR frames with
// {op:1, t:"<type>"} headers, exactly the wire format atmos's
// decoder consumes.
type fakeFirehose struct {
	t               *testing.T
	frames          [][]byte     // pre-encoded frames to send
	connWG          atomic.Int32 // tracks live connections
	receivedCursors []string     // cursors observed across reconnects
	cursorsMu       sync.Mutex
}

func (f *fakeFirehose) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/com.atproto.sync.subscribeRepos") {
			http.NotFound(w, r)
			return
		}
		f.cursorsMu.Lock()
		f.receivedCursors = append(f.receivedCursors, r.URL.Query().Get("cursor"))
		f.cursorsMu.Unlock()

		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			InsecureSkipVerify: true,
		})
		if err != nil {
			f.t.Logf("fake firehose accept: %v", err)
			return
		}
		f.connWG.Add(1)
		defer f.connWG.Add(-1)
		defer func() { _ = conn.CloseNow() }()

		ctx := r.Context()
		for _, frame := range f.frames {
			if err := conn.Write(ctx, websocket.MessageBinary, frame); err != nil {
				return
			}
		}
		// Hold open until client closes.
		<-ctx.Done()
	})
}

// encodeFrame builds the CBOR frame format atmos expects:
// {op:1, t:"<type>"} concatenated with the body CBOR.
func encodeFrame(t *testing.T, typ string, body []byte) []byte {
	t.Helper()
	hdr := cbor.AppendMapHeader(nil, 2)
	hdr = append(hdr, cbor.AppendTextKey(nil, "op")...)
	hdr = cbor.AppendInt(hdr, 1)
	hdr = append(hdr, cbor.AppendTextKey(nil, "t")...)
	hdr = cbor.AppendText(hdr, typ)
	return append(hdr, body...)
}

func encodeIdentityFrame(t *testing.T, did string, seq int64) []byte {
	t.Helper()
	id := &comatproto.SyncSubscribeRepos_Identity{
		DID:    did,
		Handle: gt.Some("h.test"),
		Seq:    seq,
		Time:   "2026-05-21T00:00:00Z",
	}
	body, err := id.MarshalCBOR()
	require.NoError(t, err)
	return encodeFrame(t, "#identity", body)
}

// TestProcessBatch_UnknownEventDoesNotAdvanceCursor pins the
// archival-correctness invariant that drives the sentinel-error
// branch in ConvertEvent: a frame whose kind we don't recognize must
// leave lastUpstream pointing at the last RECOGNIZED event so a
// future build that learns to decode the new kind can resume from
// the gap. Without the guard, the cursor jumps past data the archive
// will never contain.
func TestProcessBatch_UnknownEventDoesNotAdvanceCursor(t *testing.T) {
	t.Parallel()

	st := newTestStore(t)
	dir := filepath.Join(t.TempDir(), "live_segments")

	c, err := Open(Config{
		SegmentsDir: dir,
		Store:       st,
		SeqKey:      "live_segments/seq/next",
		CursorKey:   "relay/cursor",
		RelayURL:    "https://example.invalid",
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Verifier:    newTestVerifier(t),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	batch := []streaming.Event{
		{Seq: 5, Identity: &comatproto.SyncSubscribeRepos_Identity{
			DID: "did:plc:aaa", Time: "2026-05-21T00:00:00Z",
		}},
		// Event 6 has no recognized field — emulates a future relay
		// type the current build cannot decode.
		{Seq: 6},
		{Seq: 7, Identity: &comatproto.SyncSubscribeRepos_Identity{
			DID: "did:plc:bbb", Time: "2026-05-21T00:00:00Z",
		}},
	}

	require.NoError(t, c.processBatch(t.Context(), batch))
	require.Equal(t, int64(7), c.LastUpstreamSeq(),
		"recognized events should advance lastUpstream past the unknown one")
	// And after the unknown event arrives LAST, the cursor must
	// stop at the previous recognized seq, not skip past it.
	c.lastUpstream.Store(0)
	require.NoError(t, c.processBatch(t.Context(), []streaming.Event{
		{Seq: 100, Identity: &comatproto.SyncSubscribeRepos_Identity{
			DID: "did:plc:ccc", Time: "2026-05-21T00:00:00Z",
		}},
		{Seq: 101},
	}))
	require.Equal(t, int64(100), c.LastUpstreamSeq(),
		"unknown trailing event must not advance the cursor past it")
}

func TestMaybeTriggerCompactionReportsCoalescedTriggers(t *testing.T) {
	t.Parallel()

	trigger := make(chan struct{}, 1)
	var skipped atomic.Int32
	c := &Consumer{cfg: Config{
		Tombstones:                   tombstoneSetWithOneEntry(t),
		TombstoneCap:                 1,
		CompactionTrigger:            trigger,
		OnCompactionTriggerCoalesced: func() { skipped.Add(1) },
	}}

	c.maybeTriggerCompaction()
	require.Len(t, trigger, 1)
	require.EqualValues(t, 0, skipped.Load())

	c.maybeTriggerCompaction()
	require.Len(t, trigger, 1)
	require.EqualValues(t, 1, skipped.Load())
}

func tombstoneSetWithOneEntry(t *testing.T) *tombstone.Set {
	t.Helper()
	set := tombstone.New()
	require.NoError(t, set.Observe(&segment.Event{Seq: 1, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "r"}))
	return set
}

func TestOpen_ForwardsWriterMetricsToOwnedIngestWriter(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	writerMetrics := ingest.NewMetrics(reg)
	c, err := Open(Config{
		SegmentsDir:    filepath.Join(t.TempDir(), "segments"),
		Store:          newTestStore(t),
		SeqKey:         SteadySeqKey,
		CursorKey:      CursorKey,
		RelayURL:       "https://example.invalid",
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		Verifier:       newTestVerifier(t),
		WriterMetrics:  writerMetrics,
		SegmentMetrics: obs.NewSegmentMetrics(prometheus.NewRegistry()),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "x.y", Rkey: "r", Rev: "1", Payload: []byte{0x01}}
	require.NoError(t, c.Writer().Append(t.Context(), &ev))

	require.InDelta(t, 1.0, testutil.ToFloat64(writerMetrics.EventsAppended), 0)
	require.InDelta(t, 2.0, testutil.ToFloat64(writerMetrics.NextSeq), 0)
}

// TestProcessBatch_MissingBlockOpDoesNotShutDownConsumer pins the
// system-level guarantee that motivated this fix: a #commit whose
// CAR diff omits the record block for one of its create/update ops
// (a partial CAR from a non-canonical PDS, e.g.
// did:web:atpub.social.clipsymphony.com observed in production)
// must NOT propagate an error out of processBatch. Pre-fix, this
// returned a non-nil error, Consumer.Run returned it, the
// orchestrator's errgroup tore down the entire process, and a
// single misbehaving PDS could DoS the bootstrap backfill of the
// whole network.
//
// This test drives processBatch directly so we can pin the
// "no propagation, surviving ops still archived" property without
// spinning up a fake firehose.
func TestProcessBatch_MissingBlockOpDoesNotShutDownConsumer(t *testing.T) {
	t.Parallel()

	st := newTestStore(t)
	dir := filepath.Join(t.TempDir(), "live_segments")

	// Real metrics registry so we can assert the dropped-ops counter
	// was bumped — both that the typed-error arm fired AND that the
	// AsType extraction inside that arm reached len(dme.Dropped).
	reg := prometheus.NewRegistry()
	dropMetrics := ingest.NewDropMetrics(reg)
	writerMetrics := ingest.NewMetrics(reg)

	c, err := Open(Config{
		SegmentsDir:   dir,
		Store:         st,
		SeqKey:        "live_segments/seq/next",
		CursorKey:     "relay/cursor",
		RelayURL:      "https://example.invalid",
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		Verifier:      newTestVerifier(t),
		DropMetrics:   dropMetrics,
		WriterMetrics: writerMetrics,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	// Build a #commit with one valid create plus one orphan create
	// whose CID parses but isn't in the CAR.
	did := "did:plc:partialcar"
	evt, _ := buildCommit(t, did, "3l3qo2vutsw2b",
		struct{ Coll, Rkey string }{"app.bsky.feed.post", "good"},
	)
	orphanCID := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("not-in-the-car"))
	evt.Commit.Ops = append(evt.Commit.Ops, comatproto.SyncSubscribeRepos_RepoOp{
		Action: "create",
		Path:   "app.bsky.feed.post/orphan",
		CID:    gt.Some(lextypes.LexCIDLink{Link: orphanCID.String()}),
	})
	evt.Seq = 42

	require.NoError(t, c.processBatch(t.Context(), []streaming.Event{evt}),
		"missing-block op must not propagate an error out of processBatch")
	require.Equal(t, int64(42), c.LastUpstreamSeq(),
		"the upstream cursor must advance past a partial-CAR commit; "+
			"otherwise restart hits the same commit and re-crashes")
	require.InDelta(t, 1.0, testutil.ToFloat64(dropMetrics.Counter(ingest.DropSourceLive, ingest.DropReasonMissingBlock)), 0,
		"the consumer must extract per-op detail from the typed error and bump the counter")
	// The drop is per-OP, not per-event: the well-formed sibling op in
	// the same commit must still be archived. Without this the arm could
	// silently discard survivors (count the drop, skip the append) and
	// every assertion above would still pass.
	require.InDelta(t, 1.0, testutil.ToFloat64(writerMetrics.EventsAppended), 0,
		"the surviving sibling op must be archived, not discarded with the dropped one")
}

func TestProcessBatch_MalformedCommitDoesNotShutDownConsumer(t *testing.T) {
	t.Parallel()

	st := newTestStore(t)
	dir := filepath.Join(t.TempDir(), "live_segments")
	metrics := NewMetrics(prometheus.NewRegistry())

	c, err := Open(Config{
		SegmentsDir: dir,
		Store:       st,
		SeqKey:      "live_segments/seq/next",
		CursorKey:   "relay/cursor",
		RelayURL:    "https://example.invalid",
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Verifier:    newTestVerifier(t),
		Metrics:     metrics,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	evt, _ := buildCommit(t, "did:plc:badcommit", "3l3qo2vutsw2f",
		struct{ Coll, Rkey string }{"app.bsky.feed.post", "rec0"},
	)
	evt.Seq = 43
	evt.Commit.Ops[0].Action = "not-a-real-action"

	require.NoError(t, c.processBatch(t.Context(), []streaming.Event{evt}),
		"malformed upstream commit data must not propagate an error out of processBatch")
	require.Equal(t, int64(43), c.LastUpstreamSeq(),
		"malformed-but-recognized external data should advance past the bad upstream cursor")
	require.InDelta(t, 1.0, testutil.ToFloat64(metrics.DecodeErrors), 0,
		"the skipped malformed event must be visible in decode_errors_total")
}

func TestProcessBatch_OverwideRecordKeyDoesNotShutDownConsumer(t *testing.T) {
	t.Parallel()

	st := newTestStore(t)
	dir := filepath.Join(t.TempDir(), "live_segments")
	dropMetrics := ingest.NewDropMetrics(prometheus.NewRegistry())

	c, err := Open(Config{
		SegmentsDir: dir,
		Store:       st,
		SeqKey:      "live_segments/seq/next",
		CursorKey:   "relay/cursor",
		RelayURL:    "https://example.invalid",
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Verifier:    newTestVerifier(t),
		DropMetrics: dropMetrics,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	// A spec-valid rkey can be up to 512 bytes, but our segment rkey
	// column holds 255 — this is the field_too_long path, distinct
	// from spec-invalid input.
	evt, _ := buildCommit(t, "did:plc:widerkey", "3l3qo2vutsw2b",
		struct{ Coll, Rkey string }{"app.bsky.feed.post", strings.Repeat("x", 256)},
	)
	evt.Seq = 44

	require.NoError(t, c.processBatch(t.Context(), []streaming.Event{evt}),
		"overwide upstream record fields must not propagate an append error out of processBatch")
	require.Equal(t, int64(44), c.LastUpstreamSeq(),
		"the upstream cursor must advance past recognized-but-unarchivable external data")
	require.Equal(t, uint64(1), c.Writer().NextSeq(), "unarchivable records must not be written (NextSeq stays at the fresh-dir seed)")
	require.InDelta(t, 1.0, testutil.ToFloat64(dropMetrics.Counter(ingest.DropSourceLive, ingest.DropReasonFieldTooLong)), 0,
		"the skipped event must be visible in the shared drop family under field_too_long")
}

// TestProcessBatch_InvalidRevDropsEventAndAdvancesCursor pins the #197
// whole-event gate at the consumer level: a #commit with a non-TID rev
// is counted under invalid_rev, nothing is archived, and the cursor
// advances past it (the input can never become valid, so there is
// nothing to replay).
func TestProcessBatch_InvalidRevDropsEventAndAdvancesCursor(t *testing.T) {
	t.Parallel()

	st := newTestStore(t)
	dir := filepath.Join(t.TempDir(), "live_segments")
	dropMetrics := ingest.NewDropMetrics(prometheus.NewRegistry())

	c, err := Open(Config{
		SegmentsDir: dir,
		Store:       st,
		SeqKey:      "live_segments/seq/next",
		CursorKey:   "relay/cursor",
		RelayURL:    "https://example.invalid",
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Verifier:    newTestVerifier(t),
		DropMetrics: dropMetrics,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	evt, _ := buildCommit(t, "did:plc:badrev", "not-a-tid",
		struct{ Coll, Rkey string }{"app.bsky.feed.post", "rec0"},
	)
	evt.Seq = 45

	require.NoError(t, c.processBatch(t.Context(), []streaming.Event{evt}),
		"a spec-invalid rev must not propagate an error out of processBatch")
	require.Equal(t, int64(45), c.LastUpstreamSeq(),
		"the cursor must advance past a permanently-invalid event")
	require.Equal(t, uint64(1), c.Writer().NextSeq(),
		"no row of an invalid-rev commit may be archived")
	require.InDelta(t, 1.0, testutil.ToFloat64(dropMetrics.Counter(ingest.DropSourceLive, ingest.DropReasonInvalidRev)), 0)
}

// TestProcessBatch_InvalidOpPathDropsOpArchivesSiblings pins the
// per-op gate end-to-end: the hostile op is counted under its reason
// and the well-formed sibling still lands in the archive.
func TestProcessBatch_InvalidOpPathDropsOpArchivesSiblings(t *testing.T) {
	t.Parallel()

	st := newTestStore(t)
	dir := filepath.Join(t.TempDir(), "live_segments")
	dropMetrics := ingest.NewDropMetrics(prometheus.NewRegistry())

	c, err := Open(Config{
		SegmentsDir: dir,
		Store:       st,
		SeqKey:      "live_segments/seq/next",
		CursorKey:   "relay/cursor",
		RelayURL:    "https://example.invalid",
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Verifier:    newTestVerifier(t),
		DropMetrics: dropMetrics,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	evt, _ := buildCommit(t, "did:plc:evilpath", "3l3qo2vutsw2b",
		struct{ Coll, Rkey string }{"app.bsky.feed.post", "good"},
	)
	// Sentinel-shadowing collection on a spliced op that reuses the
	// valid record's CID so only the path check can reject it.
	evil := evt.Commit.Ops[0]
	evil.Path = "$account/evil"
	evt.Commit.Ops = append(evt.Commit.Ops, evil)
	evt.Seq = 46

	require.NoError(t, c.processBatch(t.Context(), []streaming.Event{evt}))
	require.Equal(t, int64(46), c.LastUpstreamSeq())
	require.Equal(t, uint64(2), c.Writer().NextSeq(),
		"exactly the surviving sibling must be archived")
	require.InDelta(t, 1.0, testutil.ToFloat64(dropMetrics.Counter(ingest.DropSourceLive, ingest.DropReasonInvalidCollection)), 0)
}

func encodeAccountFrame(t *testing.T, did string, seq int64) []byte {
	t.Helper()
	acc := &comatproto.SyncSubscribeRepos_Account{
		DID:    did,
		Active: true,
		Seq:    seq,
		Time:   "2026-05-21T00:00:00Z",
	}
	body, err := acc.MarshalCBOR()
	require.NoError(t, err)
	return encodeFrame(t, "#account", body)
}

// TestConsumer_Run_HappyPath drives a fake firehose end-to-end and
// asserts on segment contents — not just file size and a counter.
// Specifically: every upstream event must show up in the on-disk
// segment file with the right Kind, DID, and a non-empty CBOR payload
// (where applicable). A regression in ConvertEvent (e.g. mapping
// Identity to KindAccount) would fail this test, where the prior
// version checked only LastUpstreamSeq and "file > 256 bytes" and
// would have passed.
func TestConsumer_Run_HappyPath(t *testing.T) {
	t.Parallel()

	upstream := []struct {
		seq  int64
		kind segment.Kind
		did  string
		make func() []byte
	}{
		{1, segment.KindIdentity, "did:plc:aaa", func() []byte { return encodeIdentityFrame(t, "did:plc:aaa", 1) }},
		{2, segment.KindAccount, "did:plc:aaa", func() []byte { return encodeAccountFrame(t, "did:plc:aaa", 2) }},
		{3, segment.KindIdentity, "did:plc:bbb", func() []byte { return encodeIdentityFrame(t, "did:plc:bbb", 3) }},
		{4, segment.KindAccount, "did:plc:ccc", func() []byte { return encodeAccountFrame(t, "did:plc:ccc", 4) }},
	}
	frames := make([][]byte, 0, len(upstream))
	for _, u := range upstream {
		frames = append(frames, u.make())
	}

	f := &fakeFirehose{t: t, frames: frames}
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)

	st := newTestStore(t)
	dir := filepath.Join(t.TempDir(), "live_segments")

	// Count durable appends via OnEvent so we can gate on ALL upstream
	// events landing — not just the highest seq. Under atmos's default
	// Parallelism>1, events are delivered in verification-completion
	// order, so the highest seq can finish (bumping LastUpstreamSeq)
	// while a lower seq on another DID is still in-flight. On ctx-cancel
	// atmos flushes only the current batch and returns WITHOUT draining
	// in-flight work, so gating on LastUpstreamSeq let us cancel before
	// a trailing event was appended, dropping it from the segment and
	// flaking the readback below. Gating on the delivered count instead
	// guarantees every event is durable before we cancel.
	var delivered atomic.Int64

	c, err := Open(Config{
		SegmentsDir:       dir,
		Store:             st,
		SeqKey:            "live_segments/seq/next",
		CursorKey:         "relay/cursor",
		RelayURL:          srv.URL,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Verifier:          newTestVerifier(t),
		MaxEventsPerBlock: 2, // force a block flush after every 2 events
		OnEvent:           func(*segment.Event) { delivered.Add(1) },
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	runErr := make(chan error, 1)
	go func() { runErr <- c.Run(ctx) }()

	// Gate on every upstream event being durably appended. This both
	// fixes the cross-DID interleave race described above AND ensures
	// the cursor watermark has settled before we read it: once all
	// events are delivered, atmos's inflight set is empty so the
	// cursor reflects its final value.
	lastSeq := upstream[len(upstream)-1].seq
	require.Eventually(t, func() bool {
		return delivered.Load() >= int64(len(upstream))
	}, 3*time.Second, 10*time.Millisecond, "consumer never delivered all upstream events")

	cancel()
	select {
	case err := <-runErr:
		require.True(t, err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded),
			"Run returned %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	require.NoError(t, c.Close())

	// The persisted cursor must be a valid safe-replay watermark, NOT
	// necessarily the highest seq. atmos derives the cursor as
	// min(in-flight)-1, falling back to the highest seq in the
	// just-flushed batch once nothing is in-flight (streaming/client.go).
	// Under Parallelism>1 with BatchSize=1, the final cursor lands on
	// the seq of whichever event finished verifying LAST, which may be
	// any of our seqs — e.g. completion order 1,3,4,2 settles at 2.
	// Asserting == lastSeq here is what made this test flaky. The real
	// invariant: the cursor is positive, never exceeds the highest seq
	// we sent (which would mean skipping un-archived data on replay),
	// and matches what cursorValue() reports in-memory.
	finalCursor, err := LoadUpstreamCursor(st, "relay/cursor")
	require.NoError(t, err)
	require.Positive(t, finalCursor, "a cursor must be persisted once events flow")
	require.LessOrEqual(t, finalCursor, lastSeq,
		"the safe watermark must never advance past the highest seq we archived")
	require.Equal(t, c.cursorValue(), finalCursor,
		"the persisted cursor must equal the in-memory watermark Close flushed")

	// Decode every event from the on-disk segment files. Under
	// atmos's default Parallelism>1 cross-DID seq order is NOT
	// preserved (events for different DIDs may interleave by
	// verification completion time), so we assert on (DID, Kind,
	// Payload-non-empty) as a multiset rather than positionally.
	// Per-DID order across DIDs is preserved by the per-DID FIFO
	// scheduler, but each upstream DID here only has 1-2 events,
	// so cross-DID interleave is the only thing this test would
	// false-fail on.
	got := readAllSegmentEvents(t, dir)
	require.Len(t, got, len(upstream),
		"segment files must contain exactly the events we sent")

	type segKey struct {
		did  string
		kind segment.Kind
	}
	wantSet := map[segKey]int{}
	for _, u := range upstream {
		wantSet[segKey{did: u.did, kind: u.kind}]++
	}
	gotSet := map[segKey]int{}
	for _, ev := range got {
		gotSet[segKey{did: ev.DID, kind: ev.Kind}]++
		require.NotEmpty(t, ev.Payload,
			"non-commit kinds carry a CBOR payload (DID=%s Kind=%v)", ev.DID, ev.Kind)
	}
	require.Equal(t, wantSet, gotSet,
		"segment events as a (DID, Kind) multiset must match upstream")

	// ingest.Writer allocates Seq monotonically across the events
	// in append order, regardless of upstream order. Two adjacent
	// events with the same DID must still be in upstream-relative
	// order in the segment, which is guaranteed by atmos's per-DID
	// FIFO scheduler — but with only 1-2 events per DID here we
	// just sanity-check that the writer assigned a contiguous range.
	gotSeqs := make([]uint64, len(got))
	for i, ev := range got {
		gotSeqs[i] = ev.Seq
	}
	for i := range gotSeqs {
		require.Equal(t, uint64(i+1), gotSeqs[i],
			"ingest.Writer allocates seq monotonically")
	}
}

// readAllSegmentEvents returns every event durably written across all
// segment files in dir, in on-disk order. Active segments are sealed
// in place (the same code path production uses on rotation) so the
// public segment.Reader API can decode them.
func readAllSegmentEvents(t *testing.T, dir string) []segment.Event {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "seg_*.jss"))
	require.NoError(t, err)
	require.NotEmpty(t, matches, "segments dir must have at least one seg_*.jss")

	var events []segment.Event
	for _, path := range matches {
		// Seal in place if the file is still active. segment.New
		// resumes an unsealed file; Seal makes it readable via Open.
		sw, err := segment.New(segment.Config{Path: path})
		switch {
		case err == nil:
			_, sealErr := sw.Seal()
			require.NoError(t, sealErr, "seal %s", path)
		case errors.Is(err, segment.ErrSegmentSealed):
			// already sealed — fine
		default:
			t.Fatalf("open %s for sealing: %v", path, err)
		}

		r, err := segment.Open(segment.ReaderConfig{Path: path})
		require.NoError(t, err, "open %s", path)
		for i := range r.Blocks() {
			block, err := r.DecodeBlock(i)
			require.NoError(t, err, "decode block %d of %s", i, path)
			events = append(events, block...)
		}
		require.NoError(t, r.Close())
	}
	return events
}

// TestConsumer_Run_ResumesFromPersistedCursor verifies the crash
// recovery story: kill the consumer mid-stream, reopen, and assert
// the second connection requests a cursor at or before the last
// durable seq.
func TestConsumer_Run_ResumesFromPersistedCursor(t *testing.T) {
	t.Parallel()

	f := &fakeFirehose{
		t: t,
		frames: [][]byte{
			encodeIdentityFrame(t, "did:plc:aaa", 10),
			encodeAccountFrame(t, "did:plc:aaa", 11),
			encodeIdentityFrame(t, "did:plc:bbb", 12),
			encodeIdentityFrame(t, "did:plc:ccc", 13),
		},
	}
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)

	st := newTestStore(t)
	dir := filepath.Join(t.TempDir(), "live_segments")

	cfg := Config{
		SegmentsDir:       dir,
		Store:             st,
		SeqKey:            "live_segments/seq/next",
		CursorKey:         "relay/cursor",
		RelayURL:          srv.URL,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Verifier:          newTestVerifier(t),
		MaxEventsPerBlock: 2,
	}

	// First run — drain at least one block, then cancel.
	c1, err := Open(cfg)
	require.NoError(t, err)

	ctx1, cancel1 := context.WithCancel(t.Context())
	go func() { _ = c1.Run(ctx1) }()

	// Gate on the safe-to-persist watermark — the exact value Close
	// will write to relay/cursor — not on LastUpstreamSeq(). Under
	// atmos's default Parallelism>1, a higher seq for one DID can finish
	// verification (bumping LastUpstreamSeq) while a lower seq is still
	// in-flight on another worker, pinning the watermark (min(inflight)-1)
	// below 11. Gating on LastUpstreamSeq here let us cancel while the
	// watermark was still at 9, so Close persisted 9 and the assertion
	// below flaked. cursorValue() is exactly what Close persists, and it
	// only advances monotonically, so once it reaches 11 the assertion holds.
	require.Eventually(t, func() bool { return c1.cursorValue() >= 11 }, 3*time.Second, 10*time.Millisecond)
	cancel1()
	require.NoError(t, c1.Close())

	persistedAfterFirst, err := LoadUpstreamCursor(st, "relay/cursor")
	require.NoError(t, err)
	require.GreaterOrEqual(t, persistedAfterFirst, int64(11))

	// Second run — must request a cursor in its handshake.
	c2, err := Open(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c2.Close() })

	ctx2, cancel2 := context.WithTimeout(t.Context(), 3*time.Second)
	t.Cleanup(cancel2)
	go func() { _ = c2.Run(ctx2) }()

	require.Eventually(t, func() bool {
		f.cursorsMu.Lock()
		defer f.cursorsMu.Unlock()
		return len(f.receivedCursors) >= 2
	}, 3*time.Second, 10*time.Millisecond)

	f.cursorsMu.Lock()
	defer f.cursorsMu.Unlock()
	require.NotEmpty(t, f.receivedCursors[1], "second connection must include a cursor")
	parsed, err := strconv.ParseInt(f.receivedCursors[1], 10, 64)
	require.NoError(t, err)
	require.GreaterOrEqual(t, parsed, int64(11), "second cursor advances from at least 11 (got %d)", parsed)
}

// stubResolver is an in-memory identity.Resolver returning a fixed
// DID document per DID. It mirrors atmos's internal/testutil
// TrackingResolver in shape (a public copy is unavailable since
// internal/testutil is, well, internal).
type stubResolver struct {
	mu   sync.Mutex
	docs map[atmos.DID]*identity.DIDDocument
}

func (r *stubResolver) ResolveDID(_ context.Context, did atmos.DID) (*identity.DIDDocument, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	doc, ok := r.docs[did]
	if !ok {
		return nil, fmt.Errorf("stubResolver: not found: %s", did)
	}
	return doc, nil
}

func (r *stubResolver) ResolveHandle(_ context.Context, _ atmos.Handle) (atmos.DID, error) {
	return "", fmt.Errorf("stubResolver: ResolveHandle not implemented")
}

// buildSyntheticChainedCommit ports the essence of atmos's
// internal/testutil.BuildSyntheticCommit so we can construct a real
// signed #commit body that the verifier accepts. Each call mutates
// r.Tree, signs a commit pointing at the post-state root, and returns
// the wire-shaped #commit and the inner commit's data CID (the value
// the next chained commit's PrevData must reference).
func buildSyntheticChainedCommit(
	t *testing.T,
	r *atmosrepo.Repo,
	key crypto.PrivateKey,
	prevData cbor.CID,
	op struct{ Coll, Rkey string },
) (*comatproto.SyncSubscribeRepos_Commit, cbor.CID) {
	t.Helper()

	// Apply a single Create op (sufficient for the wire-up test;
	// chain-break / multi-op variants live in atmos's verifier_test).
	require.NoError(t, r.Create(op.Coll, op.Rkey, map[string]any{"text": op.Rkey}))
	postCID, _, err := r.Get(op.Coll, op.Rkey)
	require.NoError(t, err)

	// Persist the post-state MST blocks; postRoot is the new data CID.
	postRoot, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	// Build, sign, and store the inner commit block.
	rev := r.Clock.Next()
	c := &atmosrepo.Commit{
		DID:     string(r.DID),
		Version: atmossync.CommitVersion,
		Data:    postRoot,
		Rev:     string(rev),
	}
	require.NoError(t, c.Sign(key))
	commitBytes, err := c.EncodeCBOR()
	require.NoError(t, err)
	commitCID := cbor.ComputeCID(cbor.CodecDagCBOR, commitBytes)
	require.NoError(t, r.Store.PutBlock(commitCID, commitBytes))

	// Pack the entire MemBlockStore into a CAR. The verifier's
	// inversion path only needs touched nodes, but dumping all blocks
	// is always sufficient (testutil.BuildSyntheticCommit takes the
	// same shortcut).
	memStore, ok := r.Store.(*mst.MemBlockStore)
	require.True(t, ok, "buildSyntheticChainedCommit requires MemBlockStore")
	var carBuf bytes.Buffer
	cw, err := car.NewWriter(&carBuf, []cbor.CID{commitCID})
	require.NoError(t, err)
	for cid, data := range memStore.All() {
		require.NoError(t, cw.WriteBlock(cid, data))
	}

	commit := &comatproto.SyncSubscribeRepos_Commit{
		Repo:     string(r.DID),
		Rev:      string(rev),
		Commit:   lextypes.LexCIDLink{Link: commitCID.String()},
		Blocks:   carBuf.Bytes(),
		PrevData: gt.Some(lextypes.LexCIDLink{Link: prevData.String()}),
		Ops: []comatproto.SyncSubscribeRepos_RepoOp{{
			Action: "create",
			Path:   op.Coll + "/" + op.Rkey,
			CID:    gt.Some(lextypes.LexCIDLink{Link: postCID.String()}),
		}},
	}
	return commit, postRoot
}

// buildDIDDoc constructs a minimal DID document carrying the given
// signing key as the "atproto" verification method. Mirrors
// atmos/internal/testutil.BuildDIDDoc.
func buildDIDDoc(did atmos.DID, key crypto.PublicKey) *identity.DIDDocument {
	return &identity.DIDDocument{
		ID: string(did),
		VerificationMethod: []identity.VerificationMethod{{
			ID:                 string(did) + "#atproto",
			Type:               "Multikey",
			Controller:         string(did),
			PublicKeyMultibase: key.Multibase(),
		}},
	}
}

// TestConsumer_Run_VerifierAcceptsValidChain pins the wire-up that a
// real *sync.Verifier runs inside Consumer.Run end-to-end against a
// scripted firehose. Two cryptographically chained commits for a
// single DID are signed with a fresh P256 key, served via fakeFirehose,
// and the consumer is asserted to land both commits' Create ops in the
// segment.
//
// A regression that drops the verifier on the floor, wires it with the
// wrong directory, or breaks the segment append path on verified
// commits would either error inside VerifyCommit (signature /
// chain-break) or fail the segment readback. The per-component tests
// (syncstate, identitycache, ConvertEvent) already pin the substantive
// behavior; this is end-to-end glue insurance.
func TestConsumer_Run_VerifierAcceptsValidChain(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:vchain1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	// Build a fresh empty repo and capture the empty-MST root as the
	// PrevData for commit #1. Subsequent commits chain off the
	// previous commit's post-state data CID.
	mstore := mst.NewMemBlockStore()
	repo := &atmosrepo.Repo{
		DID:   did,
		Clock: atmos.NewTIDClock(0),
		Store: mstore,
		Tree:  mst.NewTree(mstore),
	}
	prevData, err := repo.Tree.WriteBlocks(repo.Store)
	require.NoError(t, err)

	// Commit 1: Create app.bsky.feed.post/rec1.
	c1, postRoot1 := buildSyntheticChainedCommit(t, repo, key, prevData,
		struct{ Coll, Rkey string }{"app.bsky.feed.post", "rec1"})
	c1.Seq = 1
	body1, err := c1.MarshalCBOR()
	require.NoError(t, err)

	// Commit 2: Create app.bsky.feed.like/rec2, chained on c1's data.
	c2, _ := buildSyntheticChainedCommit(t, repo, key, postRoot1,
		struct{ Coll, Rkey string }{"app.bsky.feed.like", "rec2"})
	c2.Seq = 2
	body2, err := c2.MarshalCBOR()
	require.NoError(t, err)

	frames := [][]byte{
		encodeFrame(t, "#commit", body1),
		encodeFrame(t, "#commit", body2),
	}

	f := &fakeFirehose{t: t, frames: frames}
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)

	// Stub identity Resolver returns a DID document carrying the
	// signing key's atproto verification method. The verifier consults
	// this on first sighting then caches per-DID; commit #2 reuses the
	// cached entry.
	resolver := &stubResolver{
		docs: map[atmos.DID]*identity.DIDDocument{
			did: buildDIDDoc(did, key.PublicKey()),
		},
	}
	dir := &identity.Directory{Resolver: resolver}

	verifier, err := atmossync.NewVerifier(atmossync.VerifierOptions{
		Directory:  dir,
		StateStore: atmossync.NewMemStateStore(),
		// PolicyError: a chain break surfaces as ChainBreakError on the
		// stream rather than enqueuing an async resync — gives us a
		// deterministic test without needing a fake getRepo server.
		Policy: gt.Some(atmossync.PolicyError),
		SyncClient: gt.Some(atmossync.NewClient(atmossync.Options{
			Client: &xrpc.Client{Host: "http://example.invalid"},
		})),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = verifier.Close() })

	st := newTestStore(t)
	dir2 := filepath.Join(t.TempDir(), "live_segments")

	c, err := Open(Config{
		SegmentsDir:       dir2,
		Store:             st,
		SeqKey:            "live_segments/seq/next",
		CursorKey:         "relay/cursor",
		RelayURL:          srv.URL,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Verifier:          verifier,
		MaxEventsPerBlock: 2,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	runErr := make(chan error, 1)
	go func() { runErr <- c.Run(ctx) }()

	require.Eventually(t, func() bool {
		return c.LastUpstreamSeq() >= 2
	}, 4*time.Second, 10*time.Millisecond,
		"consumer never reached the second chained commit")

	cancel()
	select {
	case err := <-runErr:
		require.True(t, err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded),
			"Run returned %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	require.NoError(t, c.Close())

	// Verifier stats must show exactly two events verified and zero
	// chain breaks / signature failures. A regression that swapped in
	// the wrong key or mis-wired the directory would surface here.
	stats := verifier.Stats()
	require.Equal(t, uint64(2), stats.EventsVerified,
		"both commits must reach the verifier and pass")
	require.Equal(t, uint64(0), stats.SignatureFailures,
		"chained commits with the matching pubkey must not fail signature")
	require.Equal(t, uint64(0), stats.ChainBreaks,
		"properly chained commits must not register a chain break")

	// relay/cursor must reflect the last verified seq. This proves the writer's
	// per-block durable batch fires through to pebble — a regression that broke
	// the cursor-write path on verified commits would not be caught by
	// LastUpstreamSeq alone.
	persisted, err := LoadUpstreamCursor(st, "relay/cursor")
	require.NoError(t, err)
	require.Equal(t, int64(2), persisted,
		"relay/cursor must advance to the last verified seq")

	// Both commits' Create ops must land in the on-disk segment.
	got := readAllSegmentEvents(t, dir2)
	require.Len(t, got, 2, "both verified commits' single op should be archived")
	require.Equal(t, segment.KindCreate, got[0].Kind)
	require.Equal(t, "app.bsky.feed.post", got[0].Collection)
	require.Equal(t, "rec1", got[0].Rkey)
	require.NotEmpty(t, got[0].Payload)
	require.Equal(t, segment.KindCreate, got[1].Kind)
	require.Equal(t, "app.bsky.feed.like", got[1].Collection)
	require.Equal(t, "rec2", got[1].Rkey)
	require.NotEmpty(t, got[1].Payload)
}

// TestConsumer_Run_VerifierRejectsChainBreak pins the rejection path:
// when a commit's prevData doesn't match locally-tracked state, the
// verifier surfaces a ChainBreakError under PolicyError, the bad
// commit's ops do NOT reach the segment, and the cursor stays at
// the last verified seq. A regression that bypassed verification
// (or that broke the streaming layer's wiring) would let the bad
// commit through and corrupt the archive.
func TestConsumer_Run_VerifierRejectsChainBreak(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:vchainbreak1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	// Fresh repo, MST, block store — all calls share this state.
	mstore := mst.NewMemBlockStore()
	repo := &atmosrepo.Repo{
		DID:   did,
		Clock: atmos.NewTIDClock(0),
		Store: mstore,
		Tree:  mst.NewTree(mstore),
	}
	prevData, err := repo.Tree.WriteBlocks(repo.Store)
	require.NoError(t, err)

	// Commit 1: legitimate.
	c1, _ := buildSyntheticChainedCommit(t, repo, key, prevData,
		struct{ Coll, Rkey string }{"app.bsky.feed.post", "rec1"})
	c1.Seq = 1
	body1, err := c1.MarshalCBOR()
	require.NoError(t, err)

	// Commit 2: chain-broken. We deliberately pass the empty-tree root
	// as prevData even though commit 1 already advanced the chain past
	// it. The verifier sees prevData mismatch and emits a
	// ChainBreakError under PolicyError.
	bogusPrev, err := mst.NewTree(mstore).WriteBlocks(mstore)
	require.NoError(t, err)
	c2, _ := buildSyntheticChainedCommit(t, repo, key, bogusPrev,
		struct{ Coll, Rkey string }{"app.bsky.feed.like", "rec2"})
	c2.Seq = 2
	body2, err := c2.MarshalCBOR()
	require.NoError(t, err)

	frames := [][]byte{
		encodeFrame(t, "#commit", body1),
		encodeFrame(t, "#commit", body2),
	}

	f := &fakeFirehose{t: t, frames: frames}
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)

	resolver := &stubResolver{
		docs: map[atmos.DID]*identity.DIDDocument{
			did: buildDIDDoc(did, key.PublicKey()),
		},
	}
	dir := &identity.Directory{Resolver: resolver}

	verifier, err := atmossync.NewVerifier(atmossync.VerifierOptions{
		Directory:  dir,
		StateStore: atmossync.NewMemStateStore(),
		// PolicyError makes verification failures synchronous: the
		// verifier returns ChainBreakError on the stream rather than
		// enqueuing an async resync. Gives a deterministic test signal
		// without needing a fake getRepo server.
		Policy: gt.Some(atmossync.PolicyError),
		SyncClient: gt.Some(atmossync.NewClient(atmossync.Options{
			Client: &xrpc.Client{Host: "http://example.invalid"},
		})),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = verifier.Close() })

	st := newTestStore(t)
	dir2 := filepath.Join(t.TempDir(), "live_segments")

	c, err := Open(Config{
		SegmentsDir:       dir2,
		Store:             st,
		SeqKey:            "live_segments/seq/next",
		CursorKey:         "relay/cursor",
		RelayURL:          srv.URL,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Verifier:          verifier,
		MaxEventsPerBlock: 1, // flush after each accepted op
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	runErr := make(chan error, 1)
	go func() { runErr <- c.Run(ctx) }()

	// Wait for the verifier to process both commits (one accepted, one
	// rejected). Stats give a deterministic signal independent of how
	// the streaming layer surfaces the chain-break error to our loop.
	require.Eventually(t, func() bool {
		s := verifier.Stats()
		return s.EventsVerified >= 1 && s.ChainBreaks >= 1
	}, 4*time.Second, 10*time.Millisecond,
		"verifier never processed both commits (verified=%d chain_breaks=%d)",
		verifier.Stats().EventsVerified, verifier.Stats().ChainBreaks)

	cancel()
	select {
	case err := <-runErr:
		require.True(t, err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded),
			"Run returned %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	require.NoError(t, c.Close())

	stats := verifier.Stats()
	require.Equal(t, uint64(1), stats.EventsVerified,
		"only commit 1 should pass verification; commit 2 has bad prevData")
	require.Equal(t, uint64(1), stats.ChainBreaks,
		"commit 2's chain break must be counted")
	require.Equal(t, uint64(0), stats.SignatureFailures,
		"both commits are properly signed; only the chain breaks")

	// Segment has commit 1's op only.
	got := readAllSegmentEvents(t, dir2)
	require.Len(t, got, 1, "only commit 1's op should land in the segment")
	require.Equal(t, segment.KindCreate, got[0].Kind)
	require.Equal(t, "app.bsky.feed.post", got[0].Collection)
	require.Equal(t, "rec1", got[0].Rkey)

	// Cursor at the last VERIFIED seq, not the last seen. A regression
	// that advanced the cursor on rejected commits would surface here.
	persisted, err := LoadUpstreamCursor(st, "relay/cursor")
	require.NoError(t, err)
	require.Equal(t, int64(1), persisted,
		"relay/cursor must NOT advance past the last verified seq")
}

// TestProcessBatch_OnEventCalledAfterAppend pins the invariant that
// the OnEvent hook fires once per segment.Event AFTER writer.Append
// has succeeded (so the seq is populated and the data is durable).
func TestProcessBatch_OnEventCalledAfterAppend(t *testing.T) {
	t.Parallel()

	st := newTestStore(t)
	dir := filepath.Join(t.TempDir(), "live_segments")

	var received []segment.Event
	var mu sync.Mutex

	c, err := Open(Config{
		SegmentsDir: dir,
		Store:       st,
		SeqKey:      "seq/next",
		CursorKey:   "relay/cursor",
		RelayURL:    "http://example.invalid",
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Verifier:    newTestVerifier(t),
		OnEvent: func(evt *segment.Event) {
			mu.Lock()
			defer mu.Unlock()
			cp := *evt
			cp.Payload = append([]byte(nil), evt.Payload...)
			received = append(received, cp)
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	// Drive a single identity batch through processBatch directly.
	idEvt := &comatproto.SyncSubscribeRepos_Identity{
		DID:  "did:plc:test",
		Seq:  42,
		Time: "2026-05-25T00:00:00Z",
	}
	streamEvt := streaming.Event{
		Seq:      42,
		Identity: idEvt,
	}
	require.NoError(t, c.processBatch(context.Background(), []streaming.Event{streamEvt}))

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, received, 1)
	require.Equal(t, segment.KindIdentity, received[0].Kind)
	require.Equal(t, uint64(1), received[0].Seq, "first event in a fresh writer must be Seq=1")
}
