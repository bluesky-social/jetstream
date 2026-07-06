package subscribe_test

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/internal/subscribe"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func openFloorReplayFixture(t *testing.T, onSeal func(*manifest.Manifest) func(uint64, string) error) (*manifest.Manifest, *ingest.Writer) {
	t.Helper()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	require.NoError(t, os.MkdirAll(segDir, 0o755))

	st, err := store.Open(dir, store.NewMetrics(prometheus.NewRegistry()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	m, err := manifest.Open(manifest.Options{
		SegmentsDir: segDir,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	require.NoError(t, m.Wait(context.Background()))

	sealHook := m.OnSegmentSealed
	if onSeal != nil {
		sealHook = onSeal(m)
	}
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:           segDir,
		Store:                 st,
		MaxEventsPerBlock:     4,
		MaxSegmentBytes:       512,
		Logger:                slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:               ingest.NewMetrics(prometheus.NewRegistry()),
		OnAfterSeal:           sealHook,
		ReadLogRetentionBytes: 0,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return m, w
}

func appendReplayEvent(t *testing.T, w *ingest.Writer, did string) uint64 {
	t.Helper()
	ev := segment.Event{
		WitnessedAt: time.Now().UnixMicro(),
		Kind:        segment.KindCreate,
		DID:         did,
		Collection:  "app.bsky.feed.post",
		Rkey:        "rkey",
		Rev:         "rev",
		Payload:     []byte{0xa0},
	}
	require.NoError(t, w.Append(context.Background(), &ev))
	return ev.Seq
}

func firstOr(s []uint64, def uint64) uint64 {
	if len(s) == 0 {
		return def
	}
	return s[0]
}

func lastOr(s []uint64, def uint64) uint64 {
	if len(s) == 0 {
		return def
	}
	return s[len(s)-1]
}

func TestWalkFromCursor_ReadLogFloorConcurrentRotation(t *testing.T) {
	t.Parallel()
	m, w := openFloorReplayFixture(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const targetRotations = 300
	var (
		wg          sync.WaitGroup
		producerErr atomic.Pointer[error]
		walkRuns    atomic.Uint64
		holeFound   atomic.Bool
	)

	wg.Go(func() {
		for w.ActiveIndex() < targetRotations {
			ev := segment.Event{
				WitnessedAt: time.Now().UnixMicro(),
				Kind:        segment.KindCreate,
				DID:         "did:plc:floor",
				Collection:  "app.bsky.feed.post",
				Rkey:        "rkey",
				Rev:         "rev",
				Payload:     []byte{0xa0},
			}
			if err := w.Append(ctx, &ev); err != nil {
				producerErr.Store(&err)
				return
			}
		}
	})

	checkWalk := func() {
		floor := w.ReadLog().FloorSeq()
		if floor <= 2 {
			return
		}
		start := uint64(1)
		if floor > 24 {
			start = floor - 24
		}

		var emitted []uint64
		err := subscribe.WalkFromCursor(ctx, subscribe.WalkInput{
			StartSeq: start,
			StopSeq:  floor,
			Manifest: m,
			Writer:   w,
		}, func(ev *segment.Event) error {
			emitted = append(emitted, ev.Seq)
			return nil
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// A "made no progress / rotation seam invariant violated" error must
			// NEVER fire in this fixture: every seq below the floor is dense and
			// durable (no compaction), so the convergence loop must always fill a
			// seam gap. Any other error is the documented benign concurrent-seal
			// transient (WalkActive racing a Seal reads footer bytes and fails
			// loud with a zstd magic mismatch); a real subscriber just reconnects
			// and retries from the same cursor, so the test does too — it never
			// advances a cursor, so retrying loses no coverage.
			if strings.Contains(err.Error(), "rotation seam invariant violated") {
				holeFound.Store(true)
				t.Errorf("floor-bounded walk failed to converge below the floor: %v", err)
			}
			return
		}
		walkRuns.Add(1)
		// Completeness: the walk must serve EXACTLY [start, floor) — contiguous,
		// no holes, and reaching floor-1. The single-pass seam bug (issue #190
		// regression) manifests as an early clean stop below floor-1, which a
		// contiguity-only check cannot see.
		want := make([]uint64, 0, floor-start)
		for s := start; s < floor; s++ {
			want = append(want, s)
		}
		if !slices.Equal(emitted, want) {
			holeFound.Store(true)
			t.Errorf("floor-bounded walk incomplete: got %d..%d (len %d), want %d..%d (len %d)",
				firstOr(emitted, 0), lastOr(emitted, 0), len(emitted), start, floor-1, len(want))
		}
	}

	const walkers = 16
	for range walkers {
		wg.Go(func() {
			for !holeFound.Load() && w.ActiveIndex() < targetRotations {
				if err := ctx.Err(); err != nil {
					return
				}
				checkWalk()
			}
			checkWalk()
		})
	}

	wg.Wait()
	cancel()
	if perr := producerErr.Load(); perr != nil {
		require.NoError(t, *perr)
	}
	require.Positive(t, walkRuns.Load(), "no floor-bounded walks ran concurrently with rotations")
	require.False(t, holeFound.Load(), "floor-bounded replay emitted a hole below the readable-log floor")
}

func TestWalkFromCursor_GapBelowReadLogFloorFailsLoud(t *testing.T) {
	t.Parallel()
	type sealEvent struct {
		idx  uint64
		path string
	}
	var seals []sealEvent
	m, w := openFloorReplayFixture(t, func(*manifest.Manifest) func(uint64, string) error {
		return func(idx uint64, path string) error {
			seals = append(seals, sealEvent{idx: idx, path: path})
			return nil
		}
	})

	for w.ActiveIndex() < 3 {
		appendReplayEvent(t, w, "did:plc:gap")
	}
	for range 3 {
		appendReplayEvent(t, w, "did:plc:gap")
	}
	require.NoError(t, w.Flush(context.Background()))
	require.GreaterOrEqual(t, len(seals), 3)

	withheld := seals[1]
	for _, s := range seals {
		if s.idx == withheld.idx {
			continue
		}
		require.NoError(t, m.OnSegmentSealed(s.idx, s.path))
	}

	r, err := segment.Open(segment.ReaderConfig{Path: withheld.path})
	require.NoError(t, err)
	start := r.Header().MinSeq
	require.NoError(t, r.Close())

	var emitted []uint64
	err = subscribe.WalkFromCursor(context.Background(), subscribe.WalkInput{
		StartSeq: start,
		StopSeq:  w.ReadLog().FloorSeq(),
		Manifest: m,
		Writer:   w,
	}, func(ev *segment.Event) error {
		emitted = append(emitted, ev.Seq)
		return nil
	})
	require.Error(t, err, "missing data below the readable-log floor must not be skipped")
	require.Contains(t, err.Error(), "made no progress")
	require.Empty(t, emitted, "walk must not emit past the missing segment")
}

// TestWalkFromCursor_SeamConvergesWhenSegmentPublishedLate deterministically
// models the rotation seam issue #190 guards: a sealed segment is present on
// disk and owns seqs below the floor, but the walk's first manifest snapshot
// predates its publish. Without the convergence loop the single-pass walk would
// stop below the floor and the cold reader would jump the cursor to the floor,
// silently dropping the segment. Here we publish the withheld segment on the
// first seam retry (standing in for rotateLocked's publish-before-bump
// happens-before) and assert the walk then serves the full range.
func TestWalkFromCursor_SeamConvergesWhenSegmentPublishedLate(t *testing.T) {
	t.Parallel()
	type sealEvent struct {
		idx  uint64
		path string
	}
	var seals []sealEvent
	m, w := openFloorReplayFixture(t, func(*manifest.Manifest) func(uint64, string) error {
		return func(idx uint64, path string) error {
			seals = append(seals, sealEvent{idx: idx, path: path})
			return nil
		}
	})

	// Fill and seal several segments (MaxEventsPerBlock=4, MaxSegmentBytes=512
	// rotate quickly), then flush the tail so every seq below the floor is
	// durable and file-visible.
	for w.ActiveIndex() < 3 {
		appendReplayEvent(t, w, "did:plc:seam")
	}
	for range 4 {
		appendReplayEvent(t, w, "did:plc:seam")
	}
	require.NoError(t, w.Flush(context.Background()))
	require.GreaterOrEqual(t, len(seals), 3)

	// Publish every sealed segment EXCEPT the first into the manifest. The
	// first segment is the one whose publish "races" the walk: it is absent
	// from the initial snapshot and only becomes visible on the seam retry.
	withheld := seals[0]
	for _, s := range seals[1:] {
		require.NoError(t, m.OnSegmentSealed(s.idx, s.path))
	}

	r, err := segment.Open(segment.ReaderConfig{Path: withheld.path})
	require.NoError(t, err)
	start := r.Header().MinSeq
	require.NoError(t, r.Close())

	floor := w.ReadLog().FloorSeq()
	require.Greater(t, floor, start)

	var retries int
	var emitted []uint64
	err = subscribe.WalkFromCursor(context.Background(), subscribe.WalkInput{
		StartSeq: start,
		StopSeq:  floor,
		Manifest: m,
		Writer:   w,
		OnSeamRetry: func(uint64) {
			// Publish the withheld segment exactly once, on the first retry —
			// the deterministic analogue of rotateLocked publishing N before
			// the next manifest read.
			if retries == 0 {
				require.NoError(t, m.OnSegmentSealed(withheld.idx, withheld.path))
			}
			retries++
		},
	}, func(ev *segment.Event) error {
		emitted = append(emitted, ev.Seq)
		return nil
	})
	require.NoError(t, err)
	require.Positive(t, retries, "the seam retry path must be exercised")

	want := make([]uint64, 0, floor-start)
	for s := start; s < floor; s++ {
		want = append(want, s)
	}
	require.Equal(t, want, emitted, "seam convergence must serve the full [start, floor) range gap-free")
}

func TestWalkFromCursor_DoesNotReplayPendingMemory(t *testing.T) {
	t.Parallel()
	m, w := openFloorReplayFixture(t, nil)

	pendingSeq := appendReplayEvent(t, w, "did:plc:pending")
	require.Equal(t, pendingSeq, w.ReadLog().TipSeq()-1)
	require.Equal(t, pendingSeq, w.ReadLog().FloorSeq(), "unflushed pending event must remain at or above the floor")

	var before []uint64
	err := subscribe.WalkFromCursor(context.Background(), subscribe.WalkInput{
		StartSeq: pendingSeq,
		StopSeq:  w.ReadLog().FloorSeq(),
		Manifest: m,
		Writer:   w,
	}, func(ev *segment.Event) error {
		before = append(before, ev.Seq)
		return nil
	})
	require.NoError(t, err)
	require.Empty(t, before, "cold replay must not serve pending in-memory events")

	require.NoError(t, w.Flush(context.Background()))
	floor := w.ReadLog().FloorSeq()
	require.Greater(t, floor, pendingSeq)

	var after []uint64
	err = subscribe.WalkFromCursor(context.Background(), subscribe.WalkInput{
		StartSeq: pendingSeq,
		StopSeq:  floor,
		Manifest: m,
		Writer:   w,
	}, func(ev *segment.Event) error {
		after = append(after, ev.Seq)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{pendingSeq}, after)
}
