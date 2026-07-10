package subscribe_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/internal/subscribe"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestWalkFromCursor_SingleSealedSegment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	mustWriteSealedSegment(t, filepath.Join(segDir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 0, maxSeq: 9, minWitnessedAt: 1_000, maxWitnessedAt: 9_999, eventCount: 10,
	})
	m := mustOpenManifest(t, segDir)

	st, w := openWriterAtTip(t, dir, 10)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })

	var got []uint64
	err := subscribe.WalkFromCursor(context.Background(), subscribe.WalkInput{
		StartSeq: 5,
		Manifest: m,
		Writer:   w,
	}, func(e *subscribe.Entry) error {
		got = append(got, e.Event.Seq)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{5, 6, 7, 8, 9}, got)
}

func TestWalkFromCursor_SealedThenActive(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	mustWriteSealedSegment(t, filepath.Join(segDir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 0, maxSeq: 9, minWitnessedAt: 1_000, maxWitnessedAt: 9_999, eventCount: 10,
	})
	m := mustOpenManifest(t, segDir)

	st, w := openWriterAtTip(t, dir, 10)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })

	// 5 events appended into the active segment and flushed so cold replay can
	// serve them from the active file without reading pending memory.
	for i := 10; i < 15; i++ {
		require.NoError(t, w.Append(context.Background(), &segment.Event{
			WitnessedAt: int64(i * 1000), Kind: segment.KindCreate,
			DID: "did:plc:active", Payload: []byte{0xa0},
		}))
	}
	require.NoError(t, w.Flush(context.Background()))

	var got []uint64
	err := subscribe.WalkFromCursor(context.Background(), subscribe.WalkInput{
		StartSeq: 8,
		Manifest: m,
		Writer:   w,
	}, func(e *subscribe.Entry) error {
		got = append(got, e.Event.Seq)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{8, 9, 10, 11, 12, 13, 14}, got)
}

func TestWalkFromCursor_HaltsOnCallbackError(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	mustWriteSealedSegment(t, filepath.Join(segDir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 0, maxSeq: 9, minWitnessedAt: 1_000, maxWitnessedAt: 9_999, eventCount: 10,
	})
	m := mustOpenManifest(t, segDir)
	st, w := openWriterAtTip(t, dir, 10)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })

	stop := errors.New("stop")
	count := 0
	err := subscribe.WalkFromCursor(context.Background(), subscribe.WalkInput{
		StartSeq: 0,
		Manifest: m,
		Writer:   w,
	}, func(e *subscribe.Entry) error {
		count++
		if count == 3 {
			return stop
		}
		return nil
	})
	require.ErrorIs(t, err, stop)
	require.Equal(t, 3, count)
}

// openWriterAtTip is a test helper that opens a fresh ingest.Writer
// with seq/next preset to the given value (so the next Append starts
// allocating from there).
func openWriterAtTip(t *testing.T, dir string, nextSeq uint64) (*store.Store, *ingest.Writer) {
	t.Helper()
	st, err := store.Open(dir, store.NewMetrics(prometheus.NewRegistry()))
	require.NoError(t, err)

	// Seed seq/next BEFORE opening the writer; ingest.Open reads it
	// during its reconciliation pass.
	require.NoError(t, st.Set([]byte("seq/next"), encodeUint64LE(nextSeq), store.SyncWrites))

	w, err := ingest.Open(ingest.Config{
		SegmentsDir: filepath.Join(dir, "segments"),
		Store:       st,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:     ingest.NewMetrics(prometheus.NewRegistry()),
	})
	require.NoError(t, err)
	return st, w
}

func encodeUint64LE(v uint64) []byte {
	b := make([]byte, 8)
	for i := range b {
		b[i] = byte(v >> (8 * i))
	}
	return b
}

// TestWalkFromCursor_CompactedTrailingGapTerminates pins the
// compaction interaction: a rewritten segment keeps its historical
// seq envelope while its trailing rows may be gone, so the walk must
// advance past the envelope instead of re-querying the same segment
// forever.
func TestWalkFromCursor_CompactedTrailingGapTerminates(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	mustWriteSealedSegment(t, filepath.Join(segDir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 0, maxSeq: 9, minWitnessedAt: 1_000, maxWitnessedAt: 9_999, eventCount: 10,
	})
	mustWriteSealedSegment(t, filepath.Join(segDir, "seg_0000000001.jss"), sealedFixture{
		minSeq: 10, maxSeq: 19, minWitnessedAt: 10_000, maxWitnessedAt: 19_999, eventCount: 10,
	})

	// Compact away seg0's trailing rows (8 and 9). The envelope
	// (MaxSeq=9) is preserved by design.
	res, err := segment.Rewrite(filepath.Join(segDir, "seg_0000000000.jss"), func(ev *segment.Event) segment.RowDecision {
		if ev.Seq >= 8 {
			return segment.RowDrop
		}
		return segment.RowKeep
	}, segment.RewriteOptions{})
	require.NoError(t, err)
	require.True(t, res.Rewritten)

	m := mustOpenManifest(t, segDir)
	st, w := openWriterAtTip(t, dir, 20)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })

	// Walk across the gap.
	var got []uint64
	err = subscribe.WalkFromCursor(context.Background(), subscribe.WalkInput{
		StartSeq: 0, Manifest: m, Writer: w,
	}, func(e *subscribe.Entry) error {
		got = append(got, e.Event.Seq)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}, got)

	// Cursor landing inside the trailing gap.
	got = got[:0]
	err = subscribe.WalkFromCursor(context.Background(), subscribe.WalkInput{
		StartSeq: 9, Manifest: m, Writer: w,
	}, func(e *subscribe.Entry) error {
		got = append(got, e.Event.Seq)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{10, 11, 12, 13, 14, 15, 16, 17, 18, 19}, got)
}

func TestWalkFromCursor_FullyEmptiedSegmentTerminates(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	mustWriteSealedSegment(t, filepath.Join(segDir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 0, maxSeq: 9, minWitnessedAt: 1_000, maxWitnessedAt: 9_999, eventCount: 10,
	})
	mustWriteSealedSegment(t, filepath.Join(segDir, "seg_0000000001.jss"), sealedFixture{
		minSeq: 10, maxSeq: 19, minWitnessedAt: 10_000, maxWitnessedAt: 19_999, eventCount: 10,
	})
	res, err := segment.Rewrite(filepath.Join(segDir, "seg_0000000000.jss"),
		func(*segment.Event) segment.RowDecision { return segment.RowDrop }, segment.RewriteOptions{})
	require.NoError(t, err)
	require.True(t, res.Rewritten)

	m := mustOpenManifest(t, segDir)
	st, w := openWriterAtTip(t, dir, 20)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })

	var got []uint64
	err = subscribe.WalkFromCursor(context.Background(), subscribe.WalkInput{
		StartSeq: 0, Manifest: m, Writer: w,
	}, func(e *subscribe.Entry) error {
		got = append(got, e.Event.Seq)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{10, 11, 12, 13, 14, 15, 16, 17, 18, 19}, got)
}
