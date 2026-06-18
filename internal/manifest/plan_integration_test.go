package manifest_test

import (
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// matchesFilter is the exact, brute-force row predicate a correct client
// applies after decoding. The planner is allowed to over-include (false
// positives) but must never drop a row this predicate accepts (no false
// negatives).
func matchesFilter(ev segment.Event, req manifest.PlanBackfillRequest, dids, collections map[string]struct{}) bool {
	if len(dids) > 0 {
		if _, ok := dids[ev.DID]; !ok {
			return false
		}
	}
	if len(collections) > 0 {
		if _, ok := collections[ev.Collection]; !ok {
			return false
		}
	}
	if req.HasAfterSeq && ev.Seq <= req.AfterSeq {
		return false
	}
	if req.HasBeforeSeq && ev.Seq > req.BeforeSeq {
		return false
	}
	return true
}

func toSet(vals []string) map[string]struct{} {
	if len(vals) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(vals))
	for _, v := range vals {
		out[v] = struct{}{}
	}
	return out
}

// coveredSeqs decodes exactly the rows a client would download given the plan:
// for mode=segment, every block in the segment; for mode=blocks, only the
// listed inclusive ranges. It returns the set of seqs the plan's transport
// footprint actually delivers to the client.
func coveredSeqs(t *testing.T, dir string, plan manifest.PlanBackfillResult) map[uint64]struct{} {
	t.Helper()
	covered := make(map[uint64]struct{})
	for _, seg := range plan.Segments {
		path := filepath.Join(dir, ingest.SegmentFilename(seg.Idx))
		r, err := segment.Open(segment.ReaderConfig{Path: path})
		require.NoError(t, err)

		blockCount := len(r.Blocks())
		var blockIdxs []int
		switch seg.Mode {
		case manifest.PlanModeSegment:
			require.Empty(t, seg.Blocks, "mode=segment must not carry block ranges")
			for i := range blockCount {
				blockIdxs = append(blockIdxs, i)
			}
		case manifest.PlanModeBlocks:
			require.NotEmpty(t, seg.Blocks, "mode=blocks must carry block ranges")
			for _, br := range seg.Blocks {
				require.GreaterOrEqual(t, br.First, 0)
				require.GreaterOrEqual(t, br.Last, br.First)
				require.Less(t, br.Last, blockCount)
				for i := br.First; i <= br.Last; i++ {
					blockIdxs = append(blockIdxs, i)
				}
			}
		default:
			t.Fatalf("unknown plan mode %q", seg.Mode)
		}

		for _, bi := range blockIdxs {
			events, err := r.DecodeBlock(bi)
			require.NoError(t, err)
			for _, ev := range events {
				covered[ev.Seq] = struct{}{}
			}
		}
		require.NoError(t, r.Close())
	}
	return covered
}

// TestPlanBackfill_Integration_NoFalseNegatives is the DoD integration test
// (issue #71): build representative multi-DID/multi-collection sealed segments,
// plan, then prove the planned transport footprint is a SUPERSET of every row
// the exact filter accepts. This catches any false negative regardless of the
// planner's internal block-selection implementation, because the expected set
// is derived from ground-truth row membership rather than hand-pinned indices.
func TestPlanBackfill_Integration_NoFalseNegatives(t *testing.T) {
	t.Parallel()

	const (
		didA = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
		didB = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"
		didC = "did:plc:cccccccccccccccccccccccc"
		didD = "did:plc:dddddddddddddddddddddddd"
	)
	collections := []string{postNSID, likeNSID, repostNSID}
	dids := []string{didA, didB, didC, didD}

	// Two segments, deterministic interleave of DIDs and collections, one
	// event per block so block boundaries align with rows and the planner has
	// to make real per-block selection decisions.
	dir := t.TempDir()
	var allEvents []segment.Event
	seq := uint64(1)
	mk := func(idx uint64, n int) {
		evs := make([]segment.Event, 0, n)
		for range n {
			ev := planEvent(seq, dids[int(seq)%len(dids)], collections[int(seq)%len(collections)])
			evs = append(evs, ev)
			allEvents = append(allEvents, ev)
			seq++
		}
		writePlanSegment(t, dir, idx, 1, evs...)
	}
	mk(0, 12)
	mk(1, 12)

	m := openManifestDir(t, dir)

	// Exercise a representative cross-product of filters and seq windows. For
	// each, brute-force the ground-truth matching seqs and assert the plan
	// covers all of them.
	type tc struct {
		name string
		req  manifest.PlanBackfillRequest
	}
	cases := []tc{
		{"match-all", planReq()},
		{"did-only", withDIDs(planReq(), didA)},
		{"two-dids", withDIDs(planReq(), didA, didC)},
		{"collection-only", withCollections(planReq(), likeNSID)},
		{"did+collection", withCollections(withDIDs(planReq(), didB), postNSID, repostNSID)},
		{"window-mid", withWindow(planReq(), 6, 18)},
		{"did+window", withWindow(withDIDs(planReq(), didD), 3, 20)},
		{"after-only", withAfter(planReq(), 10)},
		{"before-only", withBefore(planReq(), 14)},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			// Use a threshold that allows both modes to occur across cases.
			req := c.req
			req.WholeSegmentThreshold = 0.75

			plan, err := m.PlanBackfill(req)
			require.NoError(t, err)

			didSet := toSet(req.DIDs)
			collSet := toSet(req.Collections)
			want := make(map[uint64]struct{})
			for _, ev := range allEvents {
				if matchesFilter(ev, req, didSet, collSet) {
					want[ev.Seq] = struct{}{}
				}
			}

			covered := coveredSeqs(t, dir, plan)
			for s := range want {
				require.Contains(t, covered, s,
					"plan dropped seq %d that matches filter %q (false negative)", s, c.name)
			}

			// Sanity: the plan never claims to cover seqs outside the window.
			if req.HasBeforeSeq {
				require.LessOrEqual(t, plan.PlannedThroughSeq, req.BeforeSeq)
			}
		})
	}
}

// TestPlanBackfill_MultiSegment_OrderingAndPruning exercises the multi-segment
// loop: ascending result order, whole-segment skip via seq window, and
// plannedThroughSeq taken from the last segment's tip (capped by beforeSeq).
func TestPlanBackfill_MultiSegment_OrderingAndPruning(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	// seg 0: seq 1..4, seg 1: seq 5..8, seg 2: seq 9..12. One event per block.
	writePlanSegment(t, dir, 0, 1,
		planEvent(1, otherDID, postNSID), planEvent(2, otherDID, postNSID),
		planEvent(3, otherDID, postNSID), planEvent(4, otherDID, postNSID),
	)
	writePlanSegment(t, dir, 1, 1,
		planEvent(5, planDID, postNSID), planEvent(6, otherDID, postNSID),
		planEvent(7, planDID, postNSID), planEvent(8, otherDID, postNSID),
	)
	writePlanSegment(t, dir, 2, 1,
		planEvent(9, otherDID, postNSID), planEvent(10, otherDID, postNSID),
		planEvent(11, otherDID, postNSID), planEvent(12, otherDID, postNSID),
	)
	m := openManifestDir(t, dir)

	// Window (4, 8] selects only segment 1; segments 0 and 2 are pruned at the
	// segment-envelope check.
	req := withWindow(planReq(), 4, 8)
	got, err := m.PlanBackfill(req)
	require.NoError(t, err)
	require.Len(t, got.Segments, 1)
	require.EqualValues(t, 1, got.Segments[0].Idx)
	require.EqualValues(t, 3, got.Stats.SegmentsExamined, "all three segments are examined")
	require.EqualValues(t, 1, got.Stats.SegmentsMatched, "only segment 1 overlaps the window")
	require.EqualValues(t, 8, got.PlannedThroughSeq, "capped by beforeSeq")

	// No window, DID filter present in segments 0(no),1(yes),2(no): result is a
	// single segment, and result.Segments is ascending by Idx.
	req2 := withDIDs(planReq(), planDID)
	got2, err := m.PlanBackfill(req2)
	require.NoError(t, err)
	require.NotEmpty(t, got2.Segments)
	idxs := make([]uint64, len(got2.Segments))
	for i, s := range got2.Segments {
		idxs[i] = s.Idx
	}
	require.True(t, sort.SliceIsSorted(idxs, func(i, j int) bool { return idxs[i] < idxs[j] }),
		"segments must be ascending by index, got %v", idxs)
	require.EqualValues(t, 12, got2.PlannedThroughSeq, "uncapped tip is the last segment's maxSeq")
}

// TestPlanBackfill_MultiSegment_PerSegmentModeDiffers proves two segments in
// the same response can independently choose mode=segment vs mode=blocks based
// on their own density.
func TestPlanBackfill_MultiSegment_PerSegmentModeDiffers(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	// seg 0: planDID dense (3 of 4 blocks) -> mode=segment at threshold 0.75.
	writePlanSegment(t, dir, 0, 1,
		planEvent(1, planDID, postNSID), planEvent(2, planDID, postNSID),
		planEvent(3, planDID, postNSID), planEvent(4, otherDID, postNSID),
	)
	// seg 1: planDID sparse (1 of 4 blocks) -> mode=blocks.
	writePlanSegment(t, dir, 1, 1,
		planEvent(5, otherDID, postNSID), planEvent(6, otherDID, postNSID),
		planEvent(7, planDID, postNSID), planEvent(8, otherDID, postNSID),
	)
	m := openManifestDir(t, dir)

	req := withDIDs(planReq(), planDID)
	req.WholeSegmentThreshold = 0.75
	got, err := m.PlanBackfill(req)
	require.NoError(t, err)
	require.Len(t, got.Segments, 2)

	byIdx := map[uint64]manifest.PlannedSegment{}
	for _, s := range got.Segments {
		byIdx[s.Idx] = s
	}
	require.Equal(t, manifest.PlanModeSegment, byIdx[0].Mode, "dense segment 0 -> whole segment")
	require.Empty(t, byIdx[0].Blocks)
	require.Equal(t, manifest.PlanModeBlocks, byIdx[1].Mode, "sparse segment 1 -> block ranges")
	require.Equal(t, []manifest.BlockRange{{First: 2, Last: 2}}, byIdx[1].Blocks)
}

// TestPlanBackfill_SeqWindowBoundaries pins the exclusive-lower / inclusive-upper
// semantics of (afterSeq, beforeSeq] at the exact block boundaries.
func TestPlanBackfill_SeqWindowBoundaries(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	// One event per block, seq == blockIndex+1.
	writePlanSegment(t, dir, 0, 1,
		planEvent(1, otherDID, postNSID), planEvent(2, otherDID, postNSID),
		planEvent(3, otherDID, postNSID), planEvent(4, otherDID, postNSID),
		planEvent(5, otherDID, postNSID),
	)
	m := openManifestDir(t, dir)

	t.Run("after-only excludes the boundary block", func(t *testing.T) {
		t.Parallel()
		req := withAfter(planReq(), 2) // exclude seq<=2; blocks 0,1 dropped.
		got, err := m.PlanBackfill(req)
		require.NoError(t, err)
		require.Len(t, got.Segments, 1)
		require.Equal(t, []manifest.BlockRange{{First: 2, Last: 4}}, got.Segments[0].Blocks)
		require.EqualValues(t, 5, got.PlannedThroughSeq)
	})

	t.Run("before-only includes the boundary block", func(t *testing.T) {
		t.Parallel()
		req := withBefore(planReq(), 3) // keep seq<=3; blocks 0,1,2.
		got, err := m.PlanBackfill(req)
		require.NoError(t, err)
		require.Len(t, got.Segments, 1)
		require.Equal(t, []manifest.BlockRange{{First: 0, Last: 2}}, got.Segments[0].Blocks)
		require.EqualValues(t, 3, got.PlannedThroughSeq, "capped by beforeSeq")
	})

	t.Run("window excludes everything", func(t *testing.T) {
		t.Parallel()
		req := withWindow(planReq(), 100, 200) // wholly above the archive.
		got, err := m.PlanBackfill(req)
		require.NoError(t, err)
		require.Empty(t, got.Segments)
		require.Zero(t, got.Stats.SegmentsMatched)
		require.EqualValues(t, 5, got.PlannedThroughSeq, "tip is below beforeSeq, so uncapped tip")
	})
}

// TestPlanBackfill_ThresholdFlip isolates the density decision: the same sparse
// selection is mode=blocks below the threshold and mode=segment at/above it.
func TestPlanBackfill_ThresholdFlip(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	// planDID in 1 of 3 blocks -> density 1/3.
	writePlanSegment(t, dir, 0, 1,
		planEvent(1, planDID, postNSID),
		planEvent(2, otherDID, postNSID),
		planEvent(3, otherDID, postNSID),
	)
	m := openManifestDir(t, dir)

	sparse := withDIDs(planReq(), planDID)
	sparse.WholeSegmentThreshold = 0.75
	got, err := m.PlanBackfill(sparse)
	require.NoError(t, err)
	require.Len(t, got.Segments, 1)
	require.Equal(t, manifest.PlanModeBlocks, got.Segments[0].Mode, "1/3 < 0.75 -> blocks")
	require.Equal(t, []manifest.BlockRange{{First: 0, Last: 0}}, got.Segments[0].Blocks)

	// Lower the threshold so 1/3 >= threshold and the same selection flips to a
	// whole-segment entry.
	dense := withDIDs(planReq(), planDID)
	dense.WholeSegmentThreshold = 0.3
	got2, err := m.PlanBackfill(dense)
	require.NoError(t, err)
	require.Len(t, got2.Segments, 1)
	require.Equal(t, manifest.PlanModeSegment, got2.Segments[0].Mode, "1/3 >= 0.3 -> segment")
	require.Empty(t, got2.Segments[0].Blocks)
}

// TestPlanBackfill_PlannedThroughSeqWhenNothingMatches pins that a non-empty
// archive reports the sealed tip even when the filter matches nothing.
func TestPlanBackfill_PlannedThroughSeqWhenNothingMatches(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writePlanSegment(t, dir, 0, 1,
		planEvent(1, otherDID, postNSID),
		planEvent(2, otherDID, postNSID),
		planEvent(3, otherDID, postNSID),
	)
	m := openManifestDir(t, dir)

	req := withDIDs(planReq(), "did:plc:zzzzzzzzzzzzzzzzzzzzzzzz") // absent.
	got, err := m.PlanBackfill(req)
	require.NoError(t, err)
	require.Empty(t, got.Segments)
	require.Zero(t, got.Stats.SegmentsMatched)
	require.EqualValues(t, 3, got.PlannedThroughSeq, "tip is reported even with no matches")

	// Same, but capped by a smaller beforeSeq.
	req2 := withBefore(withDIDs(planReq(), "did:plc:zzzzzzzzzzzzzzzzzzzzzzzz"), 2)
	got2, err := m.PlanBackfill(req2)
	require.NoError(t, err)
	require.Empty(t, got2.Segments)
	require.EqualValues(t, 2, got2.PlannedThroughSeq)
}

// TestPlanBackfill_CoalesceManyDisjointRanges covers the N-range fold including
// a range that starts at block index 0.
func TestPlanBackfill_CoalesceManyDisjointRanges(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	// planDID in blocks 0, 2, 4 -> three disjoint single-block ranges.
	writePlanSegment(t, dir, 0, 1,
		planEvent(1, planDID, postNSID),
		planEvent(2, otherDID, postNSID),
		planEvent(3, planDID, postNSID),
		planEvent(4, otherDID, postNSID),
		planEvent(5, planDID, postNSID),
	)
	m := openManifestDir(t, dir)

	req := withDIDs(planReq(), planDID)
	req.WholeSegmentThreshold = 1 // keep it in blocks mode (3/5 < 1).
	got, err := m.PlanBackfill(req)
	require.NoError(t, err)
	require.Len(t, got.Segments, 1)
	require.Equal(t, manifest.PlanModeBlocks, got.Segments[0].Mode)
	require.Equal(t,
		[]manifest.BlockRange{{First: 0, Last: 0}, {First: 2, Last: 2}, {First: 4, Last: 4}},
		got.Segments[0].Blocks)
	require.Equal(t, 3, got.Stats.Entries)
}

// TestPlanBackfill_ConcurrentWithCompactionSwap runs the planner under -race
// while a compaction rewrite re-publishes a segment, pinning the lock/copy
// invariant: PlanBackfill must never observe a torn segment, and
// plannedThroughSeq must stay within bounds.
func TestPlanBackfill_ConcurrentWithCompactionSwap(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writePlanSegment(t, dir, 0, 1,
		planEvent(1, planDID, postNSID), planEvent(2, otherDID, postNSID),
		planEvent(3, planDID, likeNSID), planEvent(4, otherDID, repostNSID),
	)
	writePlanSegment(t, dir, 1, 1,
		planEvent(5, planDID, postNSID), planEvent(6, otherDID, postNSID),
	)
	m := openManifestDir(t, dir)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			// Re-publish segment 0 from its (unchanged) file. This drives the
			// m.mu.Lock() swap path concurrently with planning.
			require.NoError(t, m.OnSegmentCompacted(0, filepath.Join(dir, ingest.SegmentFilename(0))))
		}
	})

	req := withDIDs(planReq(), planDID)
	for range 500 {
		got, err := m.PlanBackfill(req)
		require.NoError(t, err)
		require.LessOrEqual(t, got.PlannedThroughSeq, uint64(6))
		for _, s := range got.Segments {
			require.GreaterOrEqual(t, s.MaxSeq, s.MinSeq)
		}
	}
	close(stop)
	wg.Wait()
}

// --- small request builders to keep the table cases readable ---

func withDIDs(req manifest.PlanBackfillRequest, dids ...string) manifest.PlanBackfillRequest {
	req.DIDs = dids
	return req
}

func withCollections(req manifest.PlanBackfillRequest, collections ...string) manifest.PlanBackfillRequest {
	req.Collections = collections
	return req
}

func withAfter(req manifest.PlanBackfillRequest, after uint64) manifest.PlanBackfillRequest {
	req.AfterSeq = after
	req.HasAfterSeq = true
	return req
}

func withBefore(req manifest.PlanBackfillRequest, before uint64) manifest.PlanBackfillRequest {
	req.BeforeSeq = before
	req.HasBeforeSeq = true
	return req
}

func withWindow(req manifest.PlanBackfillRequest, after, before uint64) manifest.PlanBackfillRequest {
	return withBefore(withAfter(req, after), before)
}
