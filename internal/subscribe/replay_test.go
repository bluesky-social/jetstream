package subscribe_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

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

func TestReplayer_DeliversSealedThenLive(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	mustWriteSealedSegment(t, filepath.Join(segDir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 0, maxSeq: 9, minIndexedAt: 1_000, maxIndexedAt: 9_999, eventCount: 10,
	})
	m := mustOpenManifest(t, segDir)

	st, w := openWriterAtTip(t, dir, 10)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })

	b, err := subscribe.New(subscribe.Config{
		Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
		LookbackRingSize: 16,
	})
	require.NoError(t, err)

	out := make(chan *segment.Event, 64)
	ctx := t.Context()

	r := subscribe.NewReplayer(subscribe.ReplayerInput{
		Broadcaster: b,
		Manifest:    m,
		Writer:      w,
		StartSeq:    5,
		RingSize:    16,
		MaxIters:    3,
	})

	go func() {
		defer close(out)
		_ = r.Run(ctx, func(ev *segment.Event) error {
			select {
			case out <- ev:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		})
	}()

	for i := uint64(5); i <= 9; i++ {
		select {
		case ev := <-out:
			require.Equal(t, i, ev.Seq)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for seq %d", i)
		}
	}

	b.Publish(&segment.Event{Seq: 10, DID: "did:plc:live"})
	select {
	case ev := <-out:
		require.Equal(t, uint64(10), ev.Seq)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for live event")
	}
}

func TestReplayer_TerminatesOnTooManyOverflows(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	mustWriteSealedSegment(t, filepath.Join(segDir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 0, maxSeq: 99, minIndexedAt: 1_000, maxIndexedAt: 99_999, eventCount: 100,
	})
	m := mustOpenManifest(t, segDir)

	st, w := openWriterAtTip(t, dir, 100)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })

	b, err := subscribe.New(subscribe.Config{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)

	r := subscribe.NewReplayer(subscribe.ReplayerInput{
		Broadcaster: b,
		Manifest:    m,
		Writer:      w,
		StartSeq:    0,
		RingSize:    1, // 1-slot ring guarantees overflow on every concurrent publish
		MaxIters:    2, // tight cap so the test terminates quickly
	})

	ctx := t.Context()

	go func() {
		for i := uint64(100); i < 1_000_000; i++ {
			select {
			case <-ctx.Done():
				return
			default:
			}
			b.Publish(&segment.Event{Seq: i})
		}
	}()

	err = r.Run(ctx, func(ev *segment.Event) error {
		time.Sleep(1 * time.Millisecond)
		return nil
	})
	require.ErrorIs(t, err, subscribe.ErrLookbackTooSlow)
}
