package oracle

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// partb_scenarios_test.go implements the design §16 Part-B oracle scenarios:
// end-to-end exercises of the paginated, bufferless backfill→live cutover over
// the hermetic pagedCutoverServer (partb_harness_test.go). Each asserts an
// observable property of the loop — gap-free pagination, the pinned-S
// mid-download-seal channel, the §14 too-old 400 + re-backfill, the residual-gap
// metric — against an independent ground truth folded from the on-disk archive
// plus the live events the test fed in.
//
// These run on real sockets (NOT the synctest bubble: one bubble per process is
// owned by TestOracle_DefaultLifecycle).

// drainResult is the outcome of draining the public client through the archive
// + live path against a pagedCutoverServer.
type drainResult struct {
	emitted      []ObservedEvent
	downloadErrs int
	stats        jetstream.Stats
}

// drainClientToConvergence drives the real public jetstream client (full
// backfill → cutover → live) against the harness, draining until the folded
// emitted stream converges to want (the independent ground truth) or ctx fires.
// The client is an OBSERVATION SURFACE only; want is always derived
// independently by the caller. Recoverable errors are counted, never swallowed.
func drainClientToConvergence(t *testing.T, srv *pagedCutoverServer, opts []jetstream.Option, want map[RecordKey]uint64, collections []string) drainResult {
	t.Helper()

	client, err := jetstream.Subscribe(srv.URL, opts...)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	res := drainResult{}
	for batch, berr := range client.Events(ctx) {
		if berr != nil {
			if errors.Is(berr, jetstream.ErrFatal) {
				t.Fatalf("unexpected fatal client error: %v", berr)
			}
			res.downloadErrs++
			continue
		}
		for _, ev := range batch.Events() {
			res.emitted = append(res.emitted, observedEventFromClient(t, ev))
		}
		// Converge once the folded emitted stream (restricted by the query's
		// collection filter) equals ground truth. Final state is the load-bearing
		// check under the eventually-consistent contract (§R1/§R7).
		emittedLive, err := groundTruthLive(res.emitted)
		require.NoError(t, err)
		got := restrictByCollection(emittedLive, collections)
		if mapsEqualU64(got, want) {
			break
		}
	}
	res.stats = client.Stats()
	return res
}

// mapsEqualU64 reports whether two RecordKey→seq maps are identical.
func mapsEqualU64(a, b map[RecordKey]uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// foldOracle folds a flat event slice into live ground truth (the oracle's
// independent model), the want side of a convergence check.
func foldOracle(t *testing.T, events []segment.Event) map[RecordKey]uint64 {
	t.Helper()
	obs := make([]ObservedEvent, 0, len(events))
	for _, ev := range events {
		obs = append(obs, observedFromSegment(ev))
	}
	live, err := groundTruthLive(obs)
	require.NoError(t, err)
	return live
}

// observedFromSegment adapts a segment.Event into the oracle's ObservedEvent
// (the harness builds events directly as segment.Event).
func observedFromSegment(ev segment.Event) ObservedEvent {
	return ObservedEvent{
		Seq:        ev.Seq,
		IndexedAt:  ev.IndexedAt,
		Kind:       ev.Kind,
		DID:        ev.DID,
		Collection: ev.Collection,
		Rkey:       ev.Rkey,
		Rev:        ev.Rev,
		Payload:    ev.Payload,
	}
}

// emittedSeqs returns the sorted, deduped seqs in the emitted stream.
func emittedSeqs(events []ObservedEvent) []uint64 {
	seen := map[uint64]bool{}
	var out []uint64
	for _, e := range events {
		if !seen[e.Seq] {
			seen[e.Seq] = true
			out = append(out, e.Seq)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// rangeCreates builds a contiguous run of create events [lo,hi] for one DID and
// collection, rkey = "r<seq>".
func rangeCreates(lo, hi uint64, did, collection string) []segment.Event {
	var out []segment.Event
	for s := lo; s <= hi; s++ {
		out = append(out, makeOracleCreate(s, did, collection, "r"+itoaOracle(s)))
	}
	return out
}

func itoaOracle(n uint64) string {
	const digits = "0123456789"
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = digits[n%10]
		n /= 10
	}
	return string(b[i:])
}

const (
	pbDID  = "did:plc:partb"
	pbColl = "app.bsky.feed.post"
)

// TestPartB_MultiPageBackfillCutover (§16 multi-page backfill correctness): with
// a small MaxEntries the plan truncates repeatedly. The union of all pages,
// folded in seq order, must equal ground truth; no row skipped at a page
// boundary; the loop pages more than once and converges, then cuts over to the
// live tail.
func TestPartB_MultiPageBackfillCutover(t *testing.T) {
	t.Parallel()

	// 6 single-event segments, seqs 1..6: with MaxEntries=2 (whole-segment mode)
	// the planner emits 2 segments per page → 3 pages.
	var segs [][]segment.Event
	var all []segment.Event
	for s := uint64(1); s <= 6; s++ {
		ev := makeOracleCreate(s, pbDID, pbColl, "r"+itoaOracle(s))
		segs = append(segs, []segment.Event{ev})
		all = append(all, ev)
	}
	srv := newPagedCutoverServer(t, pagedCutoverConfig{
		MaxEntries:            2,
		WholeSegmentThreshold: 1,
		InitialSegments:       segs,
	})

	// Live tail above the sealed tip (seqs 7..9): the active segment + steady state.
	live := rangeCreates(7, 9, pbDID, pbColl)
	all = append(all, live...)
	go func() {
		// Feed the live events shortly after the drain starts so the client has
		// cut over to /subscribe-v2 by the time they arrive.
		time.Sleep(50 * time.Millisecond)
		srv.AppendLive(live...)
	}()

	want := foldOracle(t, all)
	res := drainClientToConvergence(t, srv,
		[]jetstream.Option{jetstream.WithAfterSeq(0), jetstream.WithBatchSize(4)},
		want, nil)

	require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9}, emittedSeqs(res.emitted),
		"every page + the live tail must be delivered, no boundary skip")
	require.Zero(t, res.downloadErrs, "no recoverable error expected on a clean archive")
	require.GreaterOrEqual(t, res.stats.Pages, uint64(3), "small MaxEntries must force multi-page paging")
}

// TestPartB_MidSegmentTruncation (§16 mid-segment truncation §12.1): a single
// block-mode segment whose matched block ranges exceed MaxEntries forces a cut
// INSIDE the segment. The union of pages must still fold to ground truth with no
// skipped block, and the loop must page more than once (proving the cut landed
// mid-segment, not at the segment boundary).
func TestPartB_MidSegmentTruncation(t *testing.T) {
	t.Parallel()

	// One segment, 6 blocks (one event each via writeSealedSegment's
	// MaxEventsPerBlock=1), with the target DID interleaved with another so its
	// matched blocks are NON-contiguous (blocks 0, 2, 4, 5) — three coalesced
	// ranges. Block mode (threshold 0) + MaxEntries=1 then cuts INSIDE the
	// segment, page by page. A single contiguous run would coalesce into one
	// range and never exercise a mid-segment cut.
	const otherDID = "did:plc:other"
	one := []segment.Event{
		makeOracleCreate(1, pbDID, pbColl, "r1"),    // block 0 — target
		makeOracleCreate(2, otherDID, pbColl, "r2"), // block 1
		makeOracleCreate(3, pbDID, pbColl, "r3"),    // block 2 — target
		makeOracleCreate(4, otherDID, pbColl, "r4"), // block 3
		makeOracleCreate(5, pbDID, pbColl, "r5"),    // block 4 — target
		makeOracleCreate(6, pbDID, pbColl, "r6"),    // block 5 — target
	}
	srv := newPagedCutoverServer(t, pagedCutoverConfig{
		MaxEntries:            1,
		WholeSegmentThreshold: 0, // force block-mode planning
		InitialSegments:       [][]segment.Event{one},
	})

	// Ground truth restricted to the target DID: only seqs 1,3,5,6 are in scope.
	var target []segment.Event
	for _, ev := range one {
		if ev.DID == pbDID {
			target = append(target, ev)
		}
	}
	want := foldOracle(t, target)
	res := drainClientToConvergence(t, srv,
		[]jetstream.Option{
			jetstream.WithDIDs([]string{pbDID}),
			jetstream.WithAfterSeq(0), jetstream.WithBeforeSeq(6), jetstream.WithBackfillOnly(),
		},
		want, nil)

	require.Equal(t, []uint64{1, 3, 5, 6}, emittedSeqs(res.emitted),
		"a mid-segment cut must still deliver every matched block, no skip")
	require.Zero(t, res.downloadErrs)
	require.GreaterOrEqual(t, res.stats.Pages, uint64(2),
		"matched block ranges over MaxEntries must paginate inside the segment")
	require.EqualValues(t, 6, res.stats.SealedTip, "sealed tip is the segment's MaxSeq")
}

// TestPartB_MidDownloadSeal (§16 mid-download seal): segments sealed DURING the
// paged download carry seqs > S (the page-1 sealed tip, pinned as beforeSeq), so
// they fall outside every page's (afterSeq, S] range and are NOT picked up by a
// later page. They must instead arrive via the terminal /subscribe cold replay
// at cutover (WalkFromCursor re-reads the manifest at connect, §14.1) —
// losslessly, with no client buffer.
func TestPartB_MidDownloadSeal(t *testing.T) {
	t.Parallel()

	// Initial sealed archive: seqs 1..4 across 2 segments. Page-1 tip S = 4.
	segs := [][]segment.Event{
		rangeCreates(1, 2, pbDID, pbColl),
		rangeCreates(3, 4, pbDID, pbColl),
	}
	srv := newPagedCutoverServer(t, pagedCutoverConfig{
		MaxEntries:            1, // page one segment at a time, so the seal lands mid-sweep
		WholeSegmentThreshold: 1,
		InitialSegments:       segs,
	})

	// A segment sealed DURING the download (seqs 5..6, above S=4), fired
	// deterministically right after page 1 pins S=4 — so it is genuinely a
	// mid-download seal, not present when S was learned. Pinning beforeSeq=4
	// keeps it outside every page's range; it must instead reach the client via
	// /subscribe cold replay at cutover.
	handoffSealed := rangeCreates(5, 6, pbDID, pbColl)
	var sealOnce sync.Once
	srv.onPlanServed = func(n int64) {
		if n == 1 {
			sealOnce.Do(func() { srv.SealMore(handoffSealed...) })
		}
	}

	var all []segment.Event
	all = append(all, rangeCreates(1, 4, pbDID, pbColl)...)
	all = append(all, handoffSealed...)
	want := foldOracle(t, all)

	res := drainClientToConvergence(t, srv,
		[]jetstream.Option{jetstream.WithAfterSeq(0), jetstream.WithBatchSize(4)},
		want, nil)

	require.Equal(t, []uint64{1, 2, 3, 4, 5, 6}, emittedSeqs(res.emitted),
		"segments sealed mid-download must arrive via /subscribe cold replay, losslessly")
	require.Zero(t, res.downloadErrs)
	require.EqualValues(t, 4, res.stats.SealedTip,
		"beforeSeq stays PINNED to the page-1 tip (4); the handoff seal rides cold replay, not a page")
}

// TestPartB_StaleCursorSignal (§16 stale-cursor signal / §10.1 regression): with
// a tiny lookback, a /subscribe-v2 seq cursor below the floor must get an
// explicit "too old" HTTP 400 (not a silently truncated stream). Driven at the
// handler level so the assertion is on the raw signal the client's re-backfill
// keys on.
func TestPartB_StaleCursorSignal(t *testing.T) {
	t.Parallel()

	// Two segments. The fresh one (seqs 100..101) is within the lookback window;
	// the old one (seqs 1..2) is far outside it, so the lookback floor is 100.
	srv := newPagedCutoverServer(t, pagedCutoverConfig{
		WholeSegmentThreshold: 1,
		Lookback:              time.Hour,
		InitialSegments: [][]segment.Event{
			{makeOracleCreateAged(1, pbDID, pbColl, "r1", 48*time.Hour), makeOracleCreateAged(2, pbDID, pbColl, "r2", 48*time.Hour)},
			{makeOracleCreate(100, pbDID, pbColl, "r100"), makeOracleCreate(101, pbDID, pbColl, "r101")},
		},
	})

	floor, _ := srv.manifest.LookbackFloor(time.Hour)
	require.EqualValues(t, 100, floor, "the lookback floor must be the fresh segment's MinSeq")

	// A cursor below the floor must get a pre-upgrade 400 carrying the floor seq.
	status, body := dialSubscribeV2(t, srv.URL, 5)
	require.Equal(t, 400, status, "a below-floor v2 cursor must return HTTP 400, not a truncated stream")
	require.Contains(t, body, "too old")
	require.Contains(t, body, "100", "the 400 body must carry the lookback floor seq")

	// A cursor at/above the floor upgrades cleanly (101 then live-tail).
	status2, _ := dialSubscribeV2(t, srv.URL, 100)
	require.Equal(t, 101, status2, "an in-window cursor must upgrade to the websocket (101 Switching Protocols)")
}

// TestPartB_CaughtUpHandoffBelowFloorReBackfills (§16 caught-up handoff +
// fell-off-live recovery): when the client finishes paging and connects
// /subscribe at the sealed tip but that cursor has aged below the lookback
// floor, the §14 400 fires and the client re-enters the pagination loop. The
// re-backfill re-learns the current (in-window) tip, downloads the newer
// segment, and converges — transparently, never fatally.
func TestPartB_CaughtUpHandoffBelowFloorReBackfills(t *testing.T) {
	t.Parallel()

	// Page-1 archive: ONLY an OLD segment (seqs 1..2). Because it is the sole
	// segment, the lookback floor is its own MinSeq (1) when page 1 is served, so
	// S is pinned at the old tip (2) and the sweep finishes at cursor=2.
	old := []segment.Event{
		makeOracleCreateAged(1, pbDID, pbColl, "r1", 48*time.Hour),
		makeOracleCreateAged(2, pbDID, pbColl, "r2", 48*time.Hour),
	}
	srv := newPagedCutoverServer(t, pagedCutoverConfig{
		WholeSegmentThreshold: 1,
		Lookback:              time.Hour,
		InitialSegments:       [][]segment.Event{old},
	})

	// A fresh, IN-WINDOW segment (seqs 100..101) seals right AFTER page 1 — i.e.
	// during the slow handoff, after S was pinned at the old tip. It advances the
	// lookback floor to 100, so when the client connects /subscribe at cursor=2
	// (>= S=2, caught up to the pinned tip) that cursor is now BELOW the floor and
	// the §14 400 fires. The re-backfill's fresh sweep (afterSeq=2) re-learns
	// tip=101, downloads the fresh segment, and connects at 101 (in-window) — the
	// transparent recovery, never fatal.
	fresh := []segment.Event{
		makeOracleCreate(100, pbDID, pbColl, "r100"),
		makeOracleCreate(101, pbDID, pbColl, "r101"),
	}
	var sealOnce sync.Once
	srv.onPlanServed = func(n int64) {
		if n == 1 {
			sealOnce.Do(func() { srv.SealMore(fresh...) })
		}
	}

	var all []segment.Event
	all = append(all, old...)
	all = append(all, fresh...)
	want := foldOracle(t, all)

	res := drainClientToConvergence(t, srv,
		[]jetstream.Option{jetstream.WithAfterSeq(0), jetstream.WithBatchSize(4)},
		want, nil)

	require.Equal(t, []uint64{1, 2, 100, 101}, emittedSeqs(res.emitted),
		"a below-floor handoff must re-backfill and converge, not lose the fresh segment")
	require.Zero(t, res.downloadErrs)
	require.GreaterOrEqual(t, res.stats.RebackfillCycles, uint64(1),
		"the below-floor handoff must trigger at least one §14 re-backfill cycle")
}

// TestPartB_ExhaustSealedThenColdReplay (§16 exhaust-sealed termination): with
// ingest paused the loop pages until plannedThroughSeq == sealedTipSeq, then
// connects /subscribe. Resuming ingest (sealing a new segment above the tip)
// then delivers the just-sealed segment via /subscribe's cold replay backstop
// (§14.1), losslessly — the client never re-backfills because the connect cursor
// is in-window.
func TestPartB_ExhaustSealedThenColdReplay(t *testing.T) {
	t.Parallel()

	initial := rangeCreates(1, 4, pbDID, pbColl)
	srv := newPagedCutoverServer(t, pagedCutoverConfig{
		MaxEntries:            2,
		WholeSegmentThreshold: 1,
		InitialSegments:       [][]segment.Event{initial[:2], initial[2:]},
	})

	// Resume ingest after the client has had time to exhaust the sealed archive
	// and connect: a new sealed segment (seqs 5..6) above the tip. It arrives via
	// /subscribe cold replay (WalkFromCursor re-reads the manifest at connect).
	resumed := rangeCreates(5, 6, pbDID, pbColl)
	go func() {
		time.Sleep(100 * time.Millisecond)
		srv.SealMore(resumed...)
		// Nudge the live tail so a subscriber blocked at the tip re-reads the
		// manifest and picks up the cold segment.
		srv.AppendLive(makeOracleCreate(7, pbDID, pbColl, "r7"))
	}()

	var all []segment.Event
	all = append(all, initial...)
	all = append(all, resumed...)
	all = append(all, makeOracleCreate(7, pbDID, pbColl, "r7"))
	want := foldOracle(t, all)

	res := drainClientToConvergence(t, srv,
		[]jetstream.Option{jetstream.WithAfterSeq(0), jetstream.WithBatchSize(4)},
		want, nil)

	require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7}, emittedSeqs(res.emitted),
		"segments sealed after exhaustion must arrive via cold replay, losslessly")
	require.Zero(t, res.downloadErrs)
	require.Zero(t, res.stats.RebackfillCycles,
		"an in-window exhaust-sealed handoff must NOT re-backfill")
}

// TestPartB_SustainedIngestConvergence (§16 sustained-ingest convergence): with
// continuous ingest the loop still reaches the sealed tip and hands off — the
// residual gap trends to zero rather than diverging. Asserts the residual-gap
// metric (Stats().ResidualGap) is observable and converges.
func TestPartB_SustainedIngestConvergence(t *testing.T) {
	t.Parallel()

	initial := rangeCreates(1, 8, pbDID, pbColl)
	// Two segments of 4 events each; MaxEntries=2 → multi-page.
	srv := newPagedCutoverServer(t, pagedCutoverConfig{
		MaxEntries:            2,
		WholeSegmentThreshold: 1,
		InitialSegments:       [][]segment.Event{initial[:4], initial[4:]},
	})

	// Sustained live ingest above the sealed tip, fed in small bursts.
	live := rangeCreates(9, 14, pbDID, pbColl)
	go func() {
		for _, ev := range live {
			time.Sleep(15 * time.Millisecond)
			srv.AppendLive(ev)
		}
	}()

	var all []segment.Event
	all = append(all, initial...)
	all = append(all, live...)
	want := foldOracle(t, all)

	res := drainClientToConvergence(t, srv,
		[]jetstream.Option{jetstream.WithAfterSeq(0), jetstream.WithBatchSize(4)},
		want, nil)

	require.Equal(t, rangeSeqs(1, 14), emittedSeqs(res.emitted),
		"sustained ingest must converge: every sealed + live event delivered")
	require.Zero(t, res.downloadErrs)
	// The loop reached the tip and handed off: the residual gap closed to zero.
	require.Zero(t, res.stats.ResidualGap,
		"the residual gap must converge to zero once the sweep reaches the tip")
	require.GreaterOrEqual(t, res.stats.SealedTip, uint64(8), "the sealed tip must be observable")
}

// rangeSeqs returns [lo, lo+1, ..., hi].
func rangeSeqs(lo, hi uint64) []uint64 {
	var out []uint64
	for s := lo; s <= hi; s++ {
		out = append(out, s)
	}
	return out
}
