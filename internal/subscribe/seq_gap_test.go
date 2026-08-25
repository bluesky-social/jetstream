package subscribe_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/manifest"
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
	m := mustOpenManifest(t, segDir)
	st, w := openWriterAtTip(t, dir, 7)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })
	gaps := mustSeqGaps(t, seqspace.Gap{Start: 3, End: 5})
	return &subscribeFixture{manifest: m, writer: w}, gaps
}

type subscribeFixture struct {
	manifest *manifest.Manifest
	writer   *ingest.Writer
}

func TestWalkFromCursor_RegisteredGapCrossesSegmentBoundary(t *testing.T) {
	t.Parallel()
	fixture, gaps := openGapReplayFixture(t)
	var emitted []uint64
	var jumps []seqspace.Gap
	err := subscribe.WalkFromCursor(t.Context(), subscribe.WalkInput{
		StartSeq: 1,
		StopSeq:  7,
		Manifest: fixture.manifest,
		Writer:   fixture.writer,
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
		Manifest: fixture.manifest,
		Writer:   fixture.writer,
	}, func(*subscribe.Entry) error { return nil })
	require.Error(t, err)
	require.Contains(t, err.Error(), "rotation seam invariant violated")
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
				Manifest: fixture.manifest,
				Writer:   fixture.writer,
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
	m := mustOpenManifest(t, segDir)
	st, w := openWriterAtTip(t, dir, 11)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })
	gaps := mustSeqGaps(t,
		seqspace.Gap{Start: 3, End: 5},
		seqspace.Gap{Start: 7, End: 9},
	)

	var got []uint64
	err := subscribe.WalkFromCursor(t.Context(), subscribe.WalkInput{
		StartSeq: 1, StopSeq: 11, Manifest: m, Writer: w, Gaps: gaps,
	}, func(entry *subscribe.Entry) error {
		got = append(got, entry.Event.Seq)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 5, 6, 9, 10}, got)
}

func TestWalkFromCursor_RegisteredGapDoesNotMaskAdjacentRotationSeam(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	firstPath := filepath.Join(segDir, ingest.SegmentFilename(0))
	mustWriteSealedSegment(t, firstPath, sealedFixture{
		minSeq: 1, maxSeq: 2, minWitnessedAt: 1_000, maxWitnessedAt: 2_000, eventCount: 2,
	})
	m := mustOpenManifest(t, segDir)

	// Create the post-gap segment only after the manifest's initial scan. It is
	// published from OnSeamRetry, exactly modelling a rotation that becomes
	// visible one pass after the durable registered vacancy is crossed.
	secondPath := filepath.Join(segDir, ingest.SegmentFilename(1))
	mustWriteSealedSegment(t, secondPath, sealedFixture{
		minSeq: 5, maxSeq: 6, minWitnessedAt: 5_000, maxWitnessedAt: 6_000, eventCount: 2,
	})
	st, w := openWriterAtTip(t, dir, 7)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })

	var retries []uint64
	var got []uint64
	err := subscribe.WalkFromCursor(t.Context(), subscribe.WalkInput{
		StartSeq: 1,
		StopSeq:  7,
		Manifest: m,
		Writer:   w,
		Gaps:     mustSeqGaps(t, seqspace.Gap{Start: 3, End: 5}),
		OnSeamRetry: func(seq uint64) {
			retries = append(retries, seq)
			if len(retries) == 1 {
				require.NoError(t, m.OnSegmentSealed(1, secondPath))
			}
		},
	}, func(entry *subscribe.Entry) error {
		got = append(got, entry.Event.Seq)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 5, 6}, got)
	require.Equal(t, []uint64{5}, retries, "the registered gap is progress; only the adjacent unpublished segment is a seam")
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
	m := mustOpenManifest(t, segDir)
	st, w := openWriterAtTip(t, dir, 7)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })

	var jumps []seqspace.Gap
	err = subscribe.WalkFromCursor(t.Context(), subscribe.WalkInput{
		StartSeq: 1, StopSeq: 7, Manifest: m, Writer: w,
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
	m := mustOpenManifest(t, segDir)
	st, w := openWriterAtTip(t, dir, 7)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })

	err = subscribe.WalkFromCursor(t.Context(), subscribe.WalkInput{
		StartSeq: 1, StopSeq: 7, Manifest: m, Writer: w,
	}, func(*subscribe.Entry) error { return nil })
	require.ErrorContains(t, err, "unregistered sequence hole")
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

	m := mustOpenManifest(t, segDir)
	st, w := openWriterAtTip(t, dir, 5)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })
	var got []uint64
	err = subscribe.WalkFromCursor(t.Context(), subscribe.WalkInput{
		StartSeq: 1, StopSeq: 5, Manifest: m, Writer: w,
	}, func(entry *subscribe.Entry) error {
		got = append(got, entry.Event.Seq)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 4}, got)
}

func TestWalkFromCursor_UnregisteredActiveGapWithoutManifestFailsLoud(t *testing.T) {
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

	err = subscribe.WalkFromCursor(t.Context(), subscribe.WalkInput{
		StartSeq: 1, StopSeq: 7, Writer: w,
	}, func(*subscribe.Entry) error { return nil })
	require.ErrorContains(t, err, "unregistered sequence hole")
}
