package subscribe_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/catalog/local"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/seqspace"
	"github.com/bluesky-social/jetstream/internal/subscribe"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func openGapReplayFixture(t *testing.T) (*subscribeFixture, *seqspace.Gaps) {
	t.Helper()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	mustWriteSealedSegment(t, filepath.Join(segDir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 1, maxSeq: 2, minWitnessedAt: 1_000, maxWitnessedAt: 2_000, eventCount: 2,
	})
	mustWriteSealedSegment(t, filepath.Join(segDir, "seg_0000000001.jss"), sealedFixture{
		minSeq: 5, maxSeq: 6, minWitnessedAt: 5_000, maxWitnessedAt: 6_000, eventCount: 2,
	})
	st, w := openWriterAtTip(t, dir, 7)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })
	cat := mustCatalog(t, segDir, w)
	gaps := mustSeqGaps(t, seqspace.Gap{Start: 3, End: 5})
	return &subscribeFixture{cat: cat}, gaps
}

type subscribeFixture struct {
	cat *local.Catalog
}

func TestWalkFromCursor_RegisteredGapCrossesSegmentBoundary(t *testing.T) {
	t.Parallel()
	fixture, gaps := openGapReplayFixture(t)
	var emitted []uint64
	var jumps []seqspace.Gap
	err := subscribe.WalkFromCursor(t.Context(), subscribe.WalkInput{
		StartSeq: 1,
		StopSeq:  7,
		Catalog:  fixture.cat,
		Fetcher:  fixture.cat.Fetcher(),
		Gaps:     gaps,
		OnGapJump: func(start, end uint64) {
			jumps = append(jumps, seqspace.Gap{Start: start, End: end})
		},
	}, func(entry *subscribe.Entry) error {
		emitted = append(emitted, entry.Event.Seq)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 5, 6}, emitted)
	require.Equal(t, []seqspace.Gap{{Start: 3, End: 5}}, jumps)
}

func TestWalkFromCursor_UnregisteredGapStillFailsLoud(t *testing.T) {
	t.Parallel()
	fixture, _ := openGapReplayFixture(t)
	err := subscribe.WalkFromCursor(context.Background(), subscribe.WalkInput{
		StartSeq: 1,
		StopSeq:  7,
		Catalog:  fixture.cat,
		Fetcher:  fixture.cat.Fetcher(),
	}, func(*subscribe.Entry) error { return nil })
	require.ErrorContains(t, err, "unregistered sequence hole [3,5) before segment 1 block 0")
}

func TestWalkFromCursor_GapContainingStartAndEndingAtFloor(t *testing.T) {
	t.Parallel()
	fixture, gaps := openGapReplayFixture(t)
	for _, tc := range []struct {
		name  string
		start uint64
		stop  uint64
		want  []uint64
	}{
		{name: "start in gap", start: 4, stop: 7, want: []uint64{5, 6}},
		{name: "gap ends at floor", start: 3, stop: 5},
		{name: "start at end", start: 5, stop: 7, want: []uint64{5, 6}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []uint64
			err := subscribe.WalkFromCursor(t.Context(), subscribe.WalkInput{
				StartSeq: tc.start,
				StopSeq:  tc.stop,
				Catalog:  fixture.cat,
				Fetcher:  fixture.cat.Fetcher(),
				Gaps:     gaps,
			}, func(entry *subscribe.Entry) error {
				got = append(got, entry.Event.Seq)
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestWalkFromCursor_MultipleRegisteredGaps(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	for idx, bounds := range [][2]uint64{{1, 2}, {5, 6}, {9, 10}} {
		mustWriteSealedSegment(t, filepath.Join(segDir, ingest.SegmentFilename(uint64(idx))), sealedFixture{
			minSeq: bounds[0], maxSeq: bounds[1],
			minWitnessedAt: int64(bounds[0] * 1_000), maxWitnessedAt: int64(bounds[1] * 1_000),
			eventCount: 2,
		})
	}
	st, w := openWriterAtTip(t, dir, 11)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })
	cat := mustCatalog(t, segDir, w)
	gaps := mustSeqGaps(t,
		seqspace.Gap{Start: 3, End: 5},
		seqspace.Gap{Start: 7, End: 9},
	)

	var got []uint64
	err := subscribe.WalkFromCursor(t.Context(), subscribe.WalkInput{
		StartSeq: 1, StopSeq: 11, Catalog: cat, Fetcher: cat.Fetcher(), Gaps: gaps,
	}, func(entry *subscribe.Entry) error {
		got = append(got, entry.Event.Seq)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 5, 6, 9, 10}, got)
}

// TestWalkFromCursor_RegisteredGapDoesNotMaskMissingTailSegment pins the
// end-of-catalog check: crossing a registered vacancy is progress, but a view
// that then ends below the floor is missing durable data. Segment 1 is on
// disk but not in the catalog, as if its seal had never been published.
func TestWalkFromCursor_RegisteredGapDoesNotMaskMissingTailSegment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	mustWriteSealedSegment(t, filepath.Join(segDir, ingest.SegmentFilename(0)), sealedFixture{
		minSeq: 1, maxSeq: 2, minWitnessedAt: 1_000, maxWitnessedAt: 2_000, eventCount: 2,
	})
	cat := mustCatalog(t, segDir, nil)
	mustWriteSealedSegment(t, filepath.Join(segDir, ingest.SegmentFilename(1)), sealedFixture{
		minSeq: 5, maxSeq: 6, minWitnessedAt: 5_000, maxWitnessedAt: 6_000, eventCount: 2,
	})

	var got []uint64
	var jumps []seqspace.Gap
	err := subscribe.WalkFromCursor(t.Context(), subscribe.WalkInput{
		StartSeq: 1,
		StopSeq:  7,
		Catalog:  cat,
		Fetcher:  cat.Fetcher(),
		Gaps:     mustSeqGaps(t, seqspace.Gap{Start: 3, End: 5}),
		OnGapJump: func(start, end uint64) {
			jumps = append(jumps, seqspace.Gap{Start: start, End: end})
		},
	}, func(entry *subscribe.Entry) error {
		got = append(got, entry.Event.Seq)
		return nil
	})
	require.ErrorContains(t, err, "reached the end of the catalog at seq 5 before readable-log floor 7")
	require.Equal(t, []uint64{1, 2}, got)
	require.Equal(t, []seqspace.Gap{{Start: 3, End: 5}}, jumps)

	// Once the segment is in the catalog the same walk completes.
	require.NoError(t, cat.Refresh(t.Context()))
	got = got[:0]
	err = subscribe.WalkFromCursor(t.Context(), subscribe.WalkInput{
		StartSeq: 1, StopSeq: 7, Catalog: cat, Fetcher: cat.Fetcher(),
		Gaps: mustSeqGaps(t, seqspace.Gap{Start: 3, End: 5}),
	}, func(entry *subscribe.Entry) error {
		got = append(got, entry.Event.Seq)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 5, 6}, got)
}

func TestWalkFromCursor_ObservesRegisteredGapBetweenBlocksInOneSegment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	require.NoError(t, os.MkdirAll(segDir, 0o755))
	path := filepath.Join(segDir, ingest.SegmentFilename(0))
	wseg, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	for _, seq := range []uint64{1, 2, 5, 6} {
		full, err := wseg.Append(segment.Event{
			Seq: seq, WitnessedAt: int64(seq * 1_000), Kind: segment.KindCreate,
			DID: "did:plc:one-segment", Collection: "app.bsky.feed.post",
			Rkey: "r", Rev: "rev", Payload: []byte{0xa0},
		})
		require.NoError(t, err)
		if full {
			require.NoError(t, wseg.Flush())
		}
	}
	_, err = wseg.Seal()
	require.NoError(t, err)
	st, w := openWriterAtTip(t, dir, 7)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })
	cat := mustCatalog(t, segDir, w)

	var jumps []seqspace.Gap
	err = subscribe.WalkFromCursor(t.Context(), subscribe.WalkInput{
		StartSeq: 1, StopSeq: 7, Catalog: cat, Fetcher: cat.Fetcher(),
		Gaps: mustSeqGaps(t, seqspace.Gap{Start: 3, End: 5}),
		OnGapJump: func(start, end uint64) {
			jumps = append(jumps, seqspace.Gap{Start: start, End: end})
		},
	}, func(*subscribe.Entry) error { return nil })
	require.NoError(t, err)
	require.Equal(t, []seqspace.Gap{{Start: 3, End: 5}}, jumps)
}

func TestWalkFromCursor_UnregisteredGapBetweenBlocksInOneSegmentFailsLoud(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	require.NoError(t, os.MkdirAll(segDir, 0o755))
	path := filepath.Join(segDir, ingest.SegmentFilename(0))
	wseg, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	for _, seq := range []uint64{1, 2, 5, 6} {
		full, err := wseg.Append(segment.Event{
			Seq: seq, WitnessedAt: int64(seq * 1_000), Kind: segment.KindCreate,
			DID: "did:plc:one-segment", Collection: "app.bsky.feed.post",
			Rkey: "r", Rev: "rev", Payload: []byte{0xa0},
		})
		require.NoError(t, err)
		if full {
			require.NoError(t, wseg.Flush())
		}
	}
	_, err = wseg.Seal()
	require.NoError(t, err)
	st, w := openWriterAtTip(t, dir, 7)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })
	cat := mustCatalog(t, segDir, w)

	err = subscribe.WalkFromCursor(t.Context(), subscribe.WalkInput{
		StartSeq: 1, StopSeq: 7, Catalog: cat, Fetcher: cat.Fetcher(),
	}, func(*subscribe.Entry) error { return nil })
	require.ErrorContains(t, err, "unregistered sequence hole [3,5) before segment 0 block 1")
}

func TestWalkFromCursor_CompactionSparseBlocksDoNotRequireRegisteredGap(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	require.NoError(t, os.MkdirAll(segDir, 0o755))
	path := filepath.Join(segDir, ingest.SegmentFilename(0))
	wseg, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	for seq := uint64(1); seq <= 4; seq++ {
		full, err := wseg.Append(segment.Event{
			Seq: seq, WitnessedAt: int64(seq * 1_000), Kind: segment.KindCreate,
			DID: "did:plc:compacted", Collection: "app.bsky.feed.post",
			Rkey: "r", Rev: "rev", Payload: []byte{0xa0},
		})
		require.NoError(t, err)
		if full {
			require.NoError(t, wseg.Flush())
		}
	}
	_, err = wseg.Seal()
	require.NoError(t, err)
	_, err = segment.Rewrite(path, func(ev *segment.Event) segment.RowDecision {
		if ev.Seq == 2 || ev.Seq == 3 {
			return segment.RowDrop
		}
		return segment.RowKeep
	}, segment.RewriteOptions{})
	require.NoError(t, err)

	st, w := openWriterAtTip(t, dir, 5)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })
	cat := mustCatalog(t, segDir, w)
	var got []uint64
	err = subscribe.WalkFromCursor(t.Context(), subscribe.WalkInput{
		StartSeq: 1, StopSeq: 5, Catalog: cat, Fetcher: cat.Fetcher(),
	}, func(entry *subscribe.Entry) error {
		got = append(got, entry.Event.Seq)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 4}, got)
}

func TestWalkFromCursor_UnregisteredActiveGapFailsLoud(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	require.NoError(t, os.MkdirAll(segDir, 0o755))
	wseg, err := segment.New(segment.Config{
		Path: filepath.Join(segDir, ingest.SegmentFilename(0)), MaxEventsPerBlock: 2,
	})
	require.NoError(t, err)
	for _, seq := range []uint64{1, 2, 5, 6} {
		full, err := wseg.Append(segment.Event{
			Seq: seq, WitnessedAt: int64(seq * 1_000), Kind: segment.KindCreate,
			DID: "did:plc:active-gap", Collection: "app.bsky.feed.post",
			Rkey: "r", Rev: "rev", Payload: []byte{0xa0},
		})
		require.NoError(t, err)
		if full {
			require.NoError(t, wseg.Flush())
		}
	}
	require.NoError(t, wseg.Close())
	st, w := openWriterAtTip(t, dir, 7)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })
	cat := mustCatalog(t, segDir, w)

	err = subscribe.WalkFromCursor(t.Context(), subscribe.WalkInput{
		StartSeq: 1, StopSeq: 7, Catalog: cat, Fetcher: cat.Fetcher(),
	}, func(*subscribe.Entry) error { return nil })
	require.ErrorContains(t, err, "unregistered sequence hole [3,5) before segment 0 block 1")
}
