package subscribe_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/catalog/local"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/metastore/pebblestore"
	"github.com/bluesky-social/jetstream/internal/subscribe"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// withholdingCatalog forwards a writer's seals to the catalog except those
// withhold picks, which it records instead: the test's way of making a
// sealed segment missing from the catalog.
type withholdingCatalog struct {
	*local.Catalog
	withhold func(catalog.SegmentView) bool
	withheld []catalog.SegmentView
}

func (c *withholdingCatalog) Sealed(v catalog.SegmentView) error {
	if c.withhold != nil && c.withhold(v) {
		c.withheld = append(c.withheld, v)
		return nil
	}
	return c.Catalog.Sealed(v)
}

func openFloorReplayFixture(t *testing.T, withhold func(catalog.SegmentView) bool) (*withholdingCatalog, *ingest.Writer) {
	t.Helper()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	require.NoError(t, os.MkdirAll(segDir, 0o755))

	st, err := pebblestore.Open(dir, pebblestore.NewMetrics(prometheus.NewRegistry()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	cat := &withholdingCatalog{Catalog: mustCatalog(t, segDir, nil), withhold: withhold}
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:           segDir,
		Store:                 st,
		MaxEventsPerBlock:     4,
		MaxSegmentBytes:       512,
		Logger:                slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:               ingest.NewMetrics(prometheus.NewRegistry()),
		Catalog:               cat,
		ReadLogRetentionBytes: 0,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return cat, w
}

// hookFetcher runs before ahead of each fetch it delegates.
type hookFetcher struct {
	catalog.Fetcher
	before func(catalog.BlockRef)
}

func (f hookFetcher) Fetch(ctx context.Context, ref catalog.BlockRef) ([]byte, error) {
	f.before(ref)
	return f.Fetcher.Fetch(ctx, ref)
}

func seqRange(start, stop uint64) []uint64 {
	out := make([]uint64, 0, stop-start)
	for s := start; s < stop; s++ {
		out = append(out, s)
	}
	return out
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
	cat, w := openFloorReplayFixture(t, nil)
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
			Catalog:  cat,
			Fetcher:  cat.Fetcher(),
		}, func(e *subscribe.Entry) error {
			emitted = append(emitted, e.Event.Seq)
			return nil
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// Every seq below the floor is dense and durable (no compaction),
			// and every view taken after the floor covers it, so no walk may
			// fail. The one tolerated error is a walk that lost the seal race
			// maxStaleRetries times in a row under this fixture's rotation
			// rate; a real subscriber reconnects from the same cursor, and so
			// does the test, which never advances one.
			if errors.Is(err, catalog.ErrStaleRef) {
				return
			}
			holeFound.Store(true)
			t.Errorf("floor-bounded walk failed below the floor: %v", err)
			return
		}
		walkRuns.Add(1)
		// Completeness: the walk must serve EXACTLY [start, floor) — contiguous,
		// no holes, and reaching floor-1. The single-pass seam bug (issue #190
		// regression) manifests as an early clean stop below floor-1, which a
		// contiguity-only check cannot see.
		want := seqRange(start, floor)
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
	cat, w := openFloorReplayFixture(t, func(v catalog.SegmentView) bool { return v.Index == 1 })

	for w.ActiveIndex() < 3 {
		appendReplayEvent(t, w, "did:plc:gap")
	}
	for range 3 {
		appendReplayEvent(t, w, "did:plc:gap")
	}
	require.NoError(t, w.Flush(context.Background()))
	require.Len(t, cat.withheld, 1)
	withheld := cat.withheld[0]

	var emitted []uint64
	err := subscribe.WalkFromCursor(context.Background(), subscribe.WalkInput{
		StartSeq: withheld.MinSeq(),
		StopSeq:  w.ReadLog().FloorSeq(),
		Catalog:  cat,
		Fetcher:  cat.Fetcher(),
	}, func(e *subscribe.Entry) error {
		emitted = append(emitted, e.Event.Seq)
		return nil
	})
	require.Error(t, err, "missing data below the readable-log floor must not be skipped")
	require.ErrorContains(t, err, fmt.Sprintf("unregistered sequence hole [%d,", withheld.MinSeq()))
	require.Empty(t, emitted, "walk must not emit past the missing segment")
}

// TestWalkFromCursor_ActiveSealedMidWalkRetries deterministically lands a
// seal between a view and the fetch of its active blocks: the fetch sees the
// sealed generation and fails stale, and the walk resumes on a fresh view,
// where the segment is sealed, without skipping or repeating a seq.
func TestWalkFromCursor_ActiveSealedMidWalkRetries(t *testing.T) {
	t.Parallel()
	cat, w := openFloorReplayFixture(t, nil)

	for w.ActiveIndex() < 2 {
		appendReplayEvent(t, w, "did:plc:stale")
	}
	for range 6 {
		appendReplayEvent(t, w, "did:plc:stale")
	}
	require.NoError(t, w.Flush(context.Background()))
	floor := w.ReadLog().FloorSeq()
	start := cat.Snapshot().Segments(catalog.Main)[0].MinSeq()
	require.Greater(t, floor, start)

	sealed := false
	fetcher := hookFetcher{Fetcher: cat.Fetcher(), before: func(ref catalog.BlockRef) {
		if ref.Generation == 0 && !sealed {
			sealed = true
			require.NoError(t, w.ForceRotate(context.Background()))
		}
	}}
	var retries []uint64
	var emitted []uint64
	err := subscribe.WalkFromCursor(context.Background(), subscribe.WalkInput{
		StartSeq:     start,
		StopSeq:      floor,
		Catalog:      cat,
		Fetcher:      fetcher,
		OnStaleRetry: func(seq uint64) { retries = append(retries, seq) },
	}, func(e *subscribe.Entry) error {
		emitted = append(emitted, e.Event.Seq)
		return nil
	})
	require.NoError(t, err)
	require.True(t, sealed, "the walk must reach the active segment")
	require.Len(t, retries, 1)
	require.Equal(t, seqRange(start, floor), emitted)
}

// TestWalkFromCursor_StaleRefsThatNeverConvergeFail bounds the retry loop: a
// catalog whose refs are always stale is broken, and the walk says so.
func TestWalkFromCursor_StaleRefsThatNeverConvergeFail(t *testing.T) {
	t.Parallel()
	cat, w := openFloorReplayFixture(t, nil)
	seq := appendReplayEvent(t, w, "did:plc:stale")
	require.NoError(t, w.Flush(context.Background()))

	retries := 0
	err := subscribe.WalkFromCursor(context.Background(), subscribe.WalkInput{
		StartSeq:     seq,
		StopSeq:      w.ReadLog().FloorSeq(),
		Catalog:      cat,
		Fetcher:      staleFetcher{},
		OnStaleRetry: func(uint64) { retries++ },
	}, func(*subscribe.Entry) error { return nil })
	require.ErrorIs(t, err, catalog.ErrStaleRef)
	require.ErrorContains(t, err, "stale views in a row")
	require.Positive(t, retries)
}

type staleFetcher struct{}

func (staleFetcher) Fetch(context.Context, catalog.BlockRef) ([]byte, error) {
	return nil, catalog.ErrStaleRef
}

func TestWalkFromCursor_DoesNotReplayPendingMemory(t *testing.T) {
	t.Parallel()
	cat, w := openFloorReplayFixture(t, nil)

	pendingSeq := appendReplayEvent(t, w, "did:plc:pending")
	require.Equal(t, pendingSeq, w.ReadLog().TipSeq()-1)
	require.Equal(t, pendingSeq, w.ReadLog().FloorSeq(), "unflushed pending event must remain at or above the floor")

	var before []uint64
	err := subscribe.WalkFromCursor(context.Background(), subscribe.WalkInput{
		StartSeq: pendingSeq,
		StopSeq:  w.ReadLog().FloorSeq(),
		Catalog:  cat,
		Fetcher:  cat.Fetcher(),
	}, func(e *subscribe.Entry) error {
		before = append(before, e.Event.Seq)
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
		Catalog:  cat,
		Fetcher:  cat.Fetcher(),
	}, func(e *subscribe.Entry) error {
		after = append(after, e.Event.Seq)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{pendingSeq}, after)
}
