package subscribe_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/internal/subscribe"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestWalkFromCursor_SingleSealedSegment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	mustWriteSealedSegment(t, filepath.Join(segDir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 0, maxSeq: 9, minIndexedAt: 1_000, maxIndexedAt: 9_999, eventCount: 10,
	})
	m := mustOpenManifest(t, segDir)

	st, w := openWriterAtTip(t, dir, 10)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })

	var got []uint64
	err := subscribe.WalkFromCursor(context.Background(), subscribe.WalkInput{
		StartSeq: 5,
		Manifest: m,
		Writer:   w,
	}, func(ev *segment.Event) error {
		got = append(got, ev.Seq)
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
		minSeq: 0, maxSeq: 9, minIndexedAt: 1_000, maxIndexedAt: 9_999, eventCount: 10,
	})
	m := mustOpenManifest(t, segDir)

	st, w := openWriterAtTip(t, dir, 10)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })

	// 5 events appended into the active segment's pending block.
	for i := 10; i < 15; i++ {
		require.NoError(t, w.Append(context.Background(), &segment.Event{
			IndexedAt: int64(i * 1000), Kind: segment.KindCreate,
			DID: "did:plc:active", Payload: []byte{0xa0},
		}))
	}

	var got []uint64
	err := subscribe.WalkFromCursor(context.Background(), subscribe.WalkInput{
		StartSeq: 8,
		Manifest: m,
		Writer:   w,
	}, func(ev *segment.Event) error {
		got = append(got, ev.Seq)
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
		minSeq: 0, maxSeq: 9, minIndexedAt: 1_000, maxIndexedAt: 9_999, eventCount: 10,
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
	}, func(ev *segment.Event) error {
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
