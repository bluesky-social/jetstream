package manifest_test

import (
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

const (
	planDID    = "did:plc:plan"
	otherDID   = "did:plc:other"
	postNSID   = "app.bsky.feed.post"
	likeNSID   = "app.bsky.feed.like"
	repostNSID = "app.bsky.feed.repost"
)

func planEvent(seq uint64, did, collection string) segment.Event {
	ev := ev(seq, did)
	ev.Collection = collection
	return ev
}

func writePlanSegment(t *testing.T, dir string, idx uint64, maxPerBlock int, events ...segment.Event) {
	t.Helper()
	mustWriteSealedSegmentWithEvents(t, filepath.Join(dir, ingest.SegmentFilename(idx)), maxPerBlock, events)
}

func planReq() manifest.PlanBackfillRequest {
	return manifest.PlanBackfillRequest{
		MaxEntries:            100,
		WholeSegmentThreshold: 1,
	}
}

func TestPlanBackfill_EmptyArchive(t *testing.T) {
	t.Parallel()

	m := openManifestDir(t, t.TempDir())
	got, err := m.PlanBackfill(planReq())
	require.NoError(t, err)
	require.Zero(t, got.PlannedThroughSeq)
	require.Empty(t, got.Segments)
	require.Zero(t, got.Stats)
}

func TestPlanBackfill_EmptyFiltersMatchAllSealedBlocks(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writePlanSegment(t, dir, 0, 1,
		planEvent(1, "did:plc:a", postNSID),
		planEvent(2, "did:plc:b", likeNSID),
		planEvent(3, "did:plc:c", repostNSID),
	)
	m := openManifestDir(t, dir)

	got, err := m.PlanBackfill(planReq())
	require.NoError(t, err)
	require.EqualValues(t, 3, got.PlannedThroughSeq)
	require.Len(t, got.Segments, 1)
	require.Equal(t, manifest.PlanModeSegment, got.Segments[0].Mode)
	require.Empty(t, got.Segments[0].Blocks)
	require.Equal(t, manifest.PlanBackfillStats{
		SegmentsExamined: 1,
		SegmentsMatched:  1,
		BlocksMatched:    3,
		Entries:          1,
	}, got.Stats)
}

func TestPlanBackfill_DIDFilterCoalescesSparseBlocks(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writePlanSegment(t, dir, 0, 1,
		planEvent(1, otherDID, postNSID),
		planEvent(2, planDID, postNSID),
		planEvent(3, planDID, likeNSID),
		planEvent(4, otherDID, likeNSID),
	)
	m := openManifestDir(t, dir)
	req := planReq()
	req.DIDs = []string{planDID}

	got, err := m.PlanBackfill(req)
	require.NoError(t, err)
	require.Len(t, got.Segments, 1)
	require.Equal(t, manifest.PlanModeBlocks, got.Segments[0].Mode)
	require.Equal(t, []manifest.BlockRange{{First: 1, Last: 2}}, got.Segments[0].Blocks)
	require.Equal(t, 2, got.Stats.BlocksMatched)
	require.Equal(t, 1, got.Stats.Entries)
}

// markerEvent builds a DID-level marker event (empty collection) of the given
// kind for the planner sentinel-selection tests.
func markerEvent(seq uint64, did string, kind segment.Kind) segment.Event {
	return segment.Event{Seq: seq, Kind: kind, DID: did, Payload: []byte{0xa0}}
}

// TestPlanBackfill_CollectionFilterSelectsDIDMarkerBlocks is the planner-level
// guard for the §R3 gap fix: a collection-filtered plan must select the blocks
// holding DID-level markers (#account/#identity/#sync) even though those markers
// carry no real collection, because the seal index tags their blocks with a
// reserved sentinel collection that the planner always admits. Without the
// sentinel union the marker block (seq 2) would be dropped and the killer never
// downloaded.
func TestPlanBackfill_CollectionFilterSelectsDIDMarkerBlocks(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writePlanSegment(t, dir, 0, 1,
		planEvent(1, planDID, postNSID),                // in-filter create
		markerEvent(2, planDID, segment.KindAccount),   // killer, empty collection
		planEvent(3, otherDID, likeNSID),               // out-of-filter create
		markerEvent(4, otherDID, segment.KindIdentity), // identity marker
		markerEvent(5, planDID, segment.KindSync),      // sync marker
	)
	m := openManifestDir(t, dir)
	req := planReq()
	req.Collections = []string{postNSID}

	got, err := m.PlanBackfill(req)
	require.NoError(t, err)
	require.Len(t, got.Segments, 1)
	require.Equal(t, manifest.PlanModeBlocks, got.Segments[0].Mode)

	// Blocks selected: 0 (post create), and the three marker blocks 1, 3, 4
	// (account/identity/sync sentinels), regardless of the post filter. Block 2
	// (other DID's like, out of filter, no marker) is the only one dropped.
	require.Equal(t, []manifest.BlockRange{{First: 0, Last: 1}, {First: 3, Last: 4}}, got.Segments[0].Blocks)
}

// TestPlanBackfill_CollectionAndDIDFilterNarrowsMarkerBlocks proves the DID
// bloom still narrows sentinel-selected blocks: a collection+DID-filtered plan
// pulls only marker blocks whose block bloom may contain the requested DID, not
// every marker block in the segment.
func TestPlanBackfill_CollectionAndDIDFilterNarrowsMarkerBlocks(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writePlanSegment(t, dir, 0, 1,
		planEvent(1, planDID, postNSID),               // block 0: wanted DID + collection
		markerEvent(2, planDID, segment.KindAccount),  // block 1: wanted DID marker
		markerEvent(3, otherDID, segment.KindAccount), // block 2: other DID marker
	)
	m := openManifestDir(t, dir)
	req := planReq()
	req.Collections = []string{postNSID}
	req.DIDs = []string{planDID}

	got, err := m.PlanBackfill(req)
	require.NoError(t, err)
	require.Len(t, got.Segments, 1)
	// Block 2 (otherDID's marker) is pruned by the DID bloom; blocks 0 and 1
	// (planDID) survive.
	require.Equal(t, []manifest.BlockRange{{First: 0, Last: 1}}, got.Segments[0].Blocks)
}

func TestPlanBackfill_CollectionFilterUsesResidentCollectionIndex(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writePlanSegment(t, dir, 0, 1,
		planEvent(1, otherDID, postNSID),
		planEvent(2, otherDID, likeNSID),
		planEvent(3, otherDID, repostNSID),
		planEvent(4, otherDID, likeNSID),
	)
	m := openManifestDir(t, dir)
	req := planReq()
	req.Collections = []string{likeNSID}

	got, err := m.PlanBackfill(req)
	require.NoError(t, err)
	require.Len(t, got.Segments, 1)
	require.Equal(t, manifest.PlanModeBlocks, got.Segments[0].Mode)
	require.Equal(t, []manifest.BlockRange{{First: 1, Last: 1}, {First: 3, Last: 3}}, got.Segments[0].Blocks)
	require.Equal(t, 2, got.Stats.BlocksMatched)
	require.Equal(t, 2, got.Stats.Entries)
}

func TestPlanBackfill_CollectionPrefixMatchesNamespace(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writePlanSegment(t, dir, 0, 1,
		planEvent(1, otherDID, postNSID), // app.bsky.feed.post
		planEvent(2, otherDID, "app.bsky.graph.follow"),
		planEvent(3, otherDID, likeNSID), // app.bsky.feed.like
		planEvent(4, otherDID, "app.bsky.graph.block"),
	)
	m := openManifestDir(t, dir)
	req := planReq()
	req.CollectionPrefixes = []string{"app.bsky.feed."}

	got, err := m.PlanBackfill(req)
	require.NoError(t, err)
	require.Len(t, got.Segments, 1)
	require.Equal(t, manifest.PlanModeBlocks, got.Segments[0].Mode)
	// Only the two app.bsky.feed.* blocks (0-indexed 0 and 2) match.
	require.Equal(t, []manifest.BlockRange{{First: 0, Last: 0}, {First: 2, Last: 2}}, got.Segments[0].Blocks)
	require.Equal(t, 2, got.Stats.BlocksMatched)
}

func TestPlanBackfill_CollectionPrefixAndExactUnion(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writePlanSegment(t, dir, 0, 1,
		planEvent(1, otherDID, postNSID),                // matched by exact
		planEvent(2, otherDID, "app.bsky.graph.follow"), // matched by prefix
		planEvent(3, otherDID, "com.example.thing"),     // matched by neither
		planEvent(4, otherDID, "app.bsky.graph.block"),  // matched by prefix
	)
	m := openManifestDir(t, dir)
	req := planReq()
	req.Collections = []string{postNSID}
	req.CollectionPrefixes = []string{"app.bsky.graph."}

	got, err := m.PlanBackfill(req)
	require.NoError(t, err)
	require.Len(t, got.Segments, 1)
	require.Equal(t, []manifest.BlockRange{{First: 0, Last: 1}, {First: 3, Last: 3}}, got.Segments[0].Blocks)
	require.Equal(t, 3, got.Stats.BlocksMatched)
}

func TestPlanBackfill_CollectionPrefixMatchesNothing(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writePlanSegment(t, dir, 0, 1,
		planEvent(1, otherDID, postNSID),
		planEvent(2, otherDID, likeNSID),
	)
	m := openManifestDir(t, dir)
	req := planReq()
	req.CollectionPrefixes = []string{"com.example."}

	got, err := m.PlanBackfill(req)
	require.NoError(t, err)
	// Coverage horizon still reported even though nothing matched: both the
	// continuation cursor and the tip jump straight to the sealed tip, so a
	// paginating client connects live at the tip without an empty-page loop.
	require.EqualValues(t, 2, got.PlannedThroughSeq)
	require.EqualValues(t, 2, got.SealedTipSeq)
	require.Empty(t, got.Segments)
	require.Equal(t, 1, got.Stats.SegmentsExamined)
	require.Zero(t, got.Stats.SegmentsMatched)
}

func TestPlanBackfill_DIDAndCollectionFiltersIntersect(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writePlanSegment(t, dir, 0, 1,
		planEvent(1, planDID, postNSID),
		planEvent(2, otherDID, likeNSID),
		planEvent(3, planDID, likeNSID),
		planEvent(4, otherDID, postNSID),
	)
	m := openManifestDir(t, dir)
	req := planReq()
	req.DIDs = []string{planDID}
	req.Collections = []string{likeNSID}

	got, err := m.PlanBackfill(req)
	require.NoError(t, err)
	require.Len(t, got.Segments, 1)
	require.Equal(t, []manifest.BlockRange{{First: 2, Last: 2}}, got.Segments[0].Blocks)
	require.Equal(t, 1, got.Stats.BlocksMatched)
}

func TestPlanBackfill_SeqWindowPrunesBlocksAndCapsPlannedThrough(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writePlanSegment(t, dir, 0, 1,
		planEvent(1, otherDID, postNSID),
		planEvent(2, otherDID, postNSID),
		planEvent(3, otherDID, postNSID),
		planEvent(4, otherDID, postNSID),
		planEvent(5, otherDID, postNSID),
		planEvent(6, otherDID, postNSID),
	)
	m := openManifestDir(t, dir)
	req := planReq()
	req.AfterSeq = 2
	req.HasAfterSeq = true
	req.BeforeSeq = 5
	req.HasBeforeSeq = true

	got, err := m.PlanBackfill(req)
	require.NoError(t, err)
	require.EqualValues(t, 5, got.PlannedThroughSeq)
	require.EqualValues(t, 5, got.SealedTipSeq, "tip capped by beforeSeq=5")
	require.Len(t, got.Segments, 1)
	require.Equal(t, []manifest.BlockRange{{First: 2, Last: 4}}, got.Segments[0].Blocks)
	require.Equal(t, 3, got.Stats.BlocksMatched)
}

func TestPlanBackfill_DenseSelectionUsesWholeSegment(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writePlanSegment(t, dir, 0, 1,
		planEvent(1, planDID, postNSID),
		planEvent(2, planDID, postNSID),
		planEvent(3, planDID, postNSID),
		planEvent(4, otherDID, postNSID),
	)
	m := openManifestDir(t, dir)
	req := planReq()
	req.DIDs = []string{planDID}
	req.WholeSegmentThreshold = 0.75

	got, err := m.PlanBackfill(req)
	require.NoError(t, err)
	require.Len(t, got.Segments, 1)
	require.Equal(t, manifest.PlanModeSegment, got.Segments[0].Mode)
	require.Empty(t, got.Segments[0].Blocks)
	require.Equal(t, 3, got.Stats.BlocksMatched)
	require.Equal(t, 1, got.Stats.Entries)
}

// TestPlanBackfill_TruncatesAtUnitBoundary is the core pagination property: a
// plan whose matched work exceeds MaxEntries truncates at a work-unit boundary
// (here: coalesced block ranges) rather than erroring, advancing the
// continuation cursor (PlannedThroughSeq) to the MaxSeq of the last included
// unit. SealedTipSeq stays pinned at the true sealed tip so the client knows
// the pagination goal.
func TestPlanBackfill_TruncatesAtUnitBoundary(t *testing.T) {
	t.Parallel()

	// One block per event, planDID on odd seqs only. The DID filter yields
	// sparse non-adjacent blocks (0,2,4) → three coalesced ranges → three
	// work entries. MaxEntries=2 truncates after the second range.
	dir := t.TempDir()
	writePlanSegment(t, dir, 0, 1,
		planEvent(1, planDID, postNSID),  // block 0
		planEvent(2, otherDID, postNSID), // block 1
		planEvent(3, planDID, postNSID),  // block 2
		planEvent(4, otherDID, postNSID), // block 3
		planEvent(5, planDID, postNSID),  // block 4
	)
	m := openManifestDir(t, dir)
	req := planReq()
	req.DIDs = []string{planDID}
	req.MaxEntries = 2

	got, err := m.PlanBackfill(req)
	require.NoError(t, err)
	require.Len(t, got.Segments, 1)
	require.Equal(t, manifest.PlanModeBlocks, got.Segments[0].Mode)
	// Only the first two ranges (blocks 0 and 2) are included.
	require.Equal(t, []manifest.BlockRange{{First: 0, Last: 0}, {First: 2, Last: 2}}, got.Segments[0].Blocks)
	require.Equal(t, 2, got.Stats.Entries, "per-page entry count is capped")
	// Continuation cursor = last included block's MaxSeq (seq 3, block 2), NOT
	// the segment's MaxSeq (5) — the un-included tail block 4 must survive to
	// the next page.
	require.EqualValues(t, 3, got.PlannedThroughSeq)
	// SealedTipSeq stays at the true sealed tip regardless of truncation.
	require.EqualValues(t, 5, got.SealedTipSeq)

	// Page 2: resume from the continuation cursor; the tail block (seq 5) is
	// re-planned and the union of both pages covers every matching block.
	req.AfterSeq = got.PlannedThroughSeq
	req.HasAfterSeq = true
	got2, err := m.PlanBackfill(req)
	require.NoError(t, err)
	require.Len(t, got2.Segments, 1)
	require.Equal(t, []manifest.BlockRange{{First: 4, Last: 4}}, got2.Segments[0].Blocks)
	require.EqualValues(t, 5, got2.PlannedThroughSeq, "second page completes at the tip")
	require.EqualValues(t, 5, got2.SealedTipSeq)
	// No block skipped across the boundary, no block double-counted: pages
	// cover blocks {0,2} ∪ {4} = all three matching blocks.
}

// TestPlanBackfill_MidSegmentCutCursorIsBlockMaxSeq guards the §12.1 hazard:
// when truncation happens partway through a single segment, the continuation
// cursor must be the last included BLOCK range's MaxSeq (strictly inside the
// segment), never the enclosing segment's MaxSeq, or the next page's
// exclusive afterSeq would skip the segment's un-included tail blocks forever.
func TestPlanBackfill_MidSegmentCutCursorIsBlockMaxSeq(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writePlanSegment(t, dir, 0, 1,
		planEvent(1, planDID, postNSID),  // block 0
		planEvent(2, otherDID, postNSID), // block 1
		planEvent(3, planDID, postNSID),  // block 2
		planEvent(4, otherDID, postNSID), // block 3
		planEvent(5, planDID, postNSID),  // block 4
		planEvent(6, planDID, postNSID),  // block 5
	)
	m := openManifestDir(t, dir)
	req := planReq()
	req.DIDs = []string{planDID}
	req.MaxEntries = 1

	got, err := m.PlanBackfill(req)
	require.NoError(t, err)
	require.Len(t, got.Segments, 1)
	require.Equal(t, []manifest.BlockRange{{First: 0, Last: 0}}, got.Segments[0].Blocks)
	require.EqualValues(t, 1, got.PlannedThroughSeq, "cursor is block 0's MaxSeq (1), strictly inside the segment")
	require.NotEqualValues(t, got.Segments[0].MaxSeq, got.PlannedThroughSeq, "must NOT be the enclosing segment MaxSeq")
	require.EqualValues(t, 6, got.SealedTipSeq)

	// Walk the rest of the segment page by page; assert every matching block
	// (0,2,4,5) is delivered exactly once and the cursor strictly advances.
	seen := []manifest.BlockRange{got.Segments[0].Blocks[0]}
	cursor := got.PlannedThroughSeq
	for cursor < got.SealedTipSeq {
		req.AfterSeq = cursor
		req.HasAfterSeq = true
		page, err := m.PlanBackfill(req)
		require.NoError(t, err)
		require.Greater(t, page.PlannedThroughSeq, cursor, "cursor must strictly advance")
		if len(page.Segments) > 0 {
			seen = append(seen, page.Segments[0].Blocks...)
		}
		cursor = page.PlannedThroughSeq
	}
	require.Equal(t, []manifest.BlockRange{{First: 0, Last: 0}, {First: 2, Last: 2}, {First: 4, Last: 5}}, seen)
}

// TestPlanBackfill_OneUnitOverCapStillAdvances proves the always-admit-≥1-unit
// rule: when a single work unit alone exceeds MaxEntries, the planner includes
// it anyway and advances the cursor, rather than returning zero units with the
// cursor unmoved (which would livelock the client's pagination loop).
func TestPlanBackfill_OneUnitOverCapStillAdvances(t *testing.T) {
	t.Parallel()

	// A contiguous run of planDID blocks coalesces into ONE range (one entry).
	// With MaxEntries=... below 1 is impossible (0 = unlimited), so the cap is
	// exercised by a whole-segment unit: density 1.0 → one segment-mode entry.
	dir := t.TempDir()
	writePlanSegment(t, dir, 0, 1,
		planEvent(1, planDID, postNSID),
		planEvent(2, planDID, postNSID),
		planEvent(3, planDID, postNSID),
	)
	m := openManifestDir(t, dir)
	req := planReq()
	req.DIDs = []string{planDID}
	req.WholeSegmentThreshold = 1
	req.MaxEntries = 1

	got, err := m.PlanBackfill(req)
	require.NoError(t, err)
	require.Len(t, got.Segments, 1, "the single over-cap unit is admitted anyway")
	require.Equal(t, manifest.PlanModeSegment, got.Segments[0].Mode)
	require.EqualValues(t, 3, got.PlannedThroughSeq)
	require.EqualValues(t, 3, got.SealedTipSeq)
}

// TestPlanBackfill_TruncatesAtSegmentBoundary covers multi-segment truncation:
// each whole segment is one unit, so MaxEntries=1 truncates after the first
// segment and the cursor = that segment's MaxSeq.
func TestPlanBackfill_TruncatesAtSegmentBoundary(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writePlanSegment(t, dir, 0, 1,
		planEvent(1, planDID, postNSID),
		planEvent(2, planDID, postNSID),
	)
	writePlanSegment(t, dir, 1, 1,
		planEvent(3, planDID, postNSID),
		planEvent(4, planDID, postNSID),
	)
	m := openManifestDir(t, dir)
	req := planReq()
	req.DIDs = []string{planDID}
	req.WholeSegmentThreshold = 1
	req.MaxEntries = 1

	got, err := m.PlanBackfill(req)
	require.NoError(t, err)
	require.Len(t, got.Segments, 1, "truncated after the first whole-segment unit")
	require.EqualValues(t, 0, got.Segments[0].Idx)
	require.EqualValues(t, 2, got.PlannedThroughSeq, "cursor = first segment MaxSeq")
	require.EqualValues(t, 4, got.SealedTipSeq, "tip is the second segment's MaxSeq")

	req.AfterSeq = got.PlannedThroughSeq
	req.HasAfterSeq = true
	got2, err := m.PlanBackfill(req)
	require.NoError(t, err)
	require.Len(t, got2.Segments, 1)
	require.EqualValues(t, 1, got2.Segments[0].Idx)
	require.EqualValues(t, 4, got2.PlannedThroughSeq)
}

func TestPlanBackfill_InvalidRequest(t *testing.T) {
	t.Parallel()

	m := openManifestDir(t, t.TempDir())

	req := planReq()
	req.AfterSeq = 10
	req.HasAfterSeq = true
	req.BeforeSeq = 10
	req.HasBeforeSeq = true
	_, err := m.PlanBackfill(req)
	require.ErrorIs(t, err, manifest.ErrInvalidPlanRequest)

	req = planReq()
	req.WholeSegmentThreshold = 0
	_, err = m.PlanBackfill(req)
	require.ErrorIs(t, err, manifest.ErrInvalidPlanRequest)

	req = planReq()
	req.MaxEntries = -1
	_, err = m.PlanBackfill(req)
	require.ErrorIs(t, err, manifest.ErrInvalidPlanRequest)
}

func TestPlanBackfill_ZeroMaxEntriesIsUnlimited(t *testing.T) {
	t.Parallel()

	// MaxEntries == 0 disables the cap: a plan that would exceed any positive
	// limit must still succeed and return all matched work.
	dir := t.TempDir()
	writePlanSegment(t, dir, 0, 1,
		planEvent(1, planDID, postNSID),
		planEvent(2, otherDID, postNSID),
		planEvent(3, planDID, postNSID),
		planEvent(4, otherDID, postNSID),
		planEvent(5, planDID, postNSID),
	)
	m := openManifestDir(t, dir)
	req := planReq()
	req.DIDs = []string{planDID}
	req.MaxEntries = 0

	got, err := m.PlanBackfill(req)
	require.NoError(t, err)
	require.NotEmpty(t, got.Segments)
	require.Equal(t, 3, got.Stats.Entries)
}
