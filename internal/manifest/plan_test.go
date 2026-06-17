package manifest_test

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/manifest"
	"github.com/bluesky-social/jetstream-v2/segment"
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

func TestPlanBackfill_PlanTooLargeDoesNotReturnTruncatedResult(t *testing.T) {
	t.Parallel()

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
	req.MaxEntries = 2

	got, err := m.PlanBackfill(req)
	require.ErrorIs(t, err, manifest.ErrPlanTooLarge)
	require.Empty(t, got.Segments)
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
	req.MaxEntries = 0
	_, err = m.PlanBackfill(req)
	require.True(t, errors.Is(err, manifest.ErrPlanTooLarge))
}
