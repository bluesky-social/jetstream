package subscribe_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
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

// TestWalkFromCursor_ConcurrentRotationSeam stresses the hypothesis that
// WalkFromCursor can silently drop events when ingest rotates/flushes the
// active segment concurrently, because the walk reads its three sources
// (sealed manifest, active flushed blocks, in-memory pending) non-atomically.
//
// Invariant under test: with strictly contiguous seqs and no compaction, a
// WalkFromCursor(StartSeq=S) must emit S, S+1, ..., up to some tip with NO
// holes. Any hole is a dropped durable/pending event.
//
// Regression test for the cold-read rotation seam (issue #190), fixed by the
// convergence loop in WalkFromCursor.
func TestWalkFromCursor_ConcurrentRotationSeam(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	require.NoError(t, os.MkdirAll(segDir, 0o755))

	st, err := store.Open(dir, store.NewMetrics(prometheus.NewRegistry()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	// Open the manifest on the (empty) segments dir BEFORE any segment
	// exists; OnSegmentSealed publishes each segment as ingest seals it.
	m, err := manifest.Open(manifest.Options{
		SegmentsDir: segDir,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	require.NoError(t, m.Wait(context.Background()))

	// Tiny block + segment thresholds so the writer rotates and flushes
	// constantly, maximizing the chance of hitting the seam windows.
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       segDir,
		Store:             st,
		MaxEventsPerBlock: 4,
		MaxSegmentBytes:   512, // rotate every few blocks
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:           ingest.NewMetrics(prometheus.NewRegistry()),
		OnAfterSeal:       m.OnSegmentSealed,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Bound by WORK, not wall-clock: the producer appends until the writer
	// has rotated targetRotations times, then stops. Each rotation is one
	// seal->publish->bump->open cycle — exactly the seam window — so a few
	// hundred of them, hammered concurrently by the walkers, gives dense
	// coverage and completes in a fraction of a second. (The pre-fix bug
	// reproduced within the first ~5 rotations, 100% of runs.)
	const targetRotations = 300

	var (
		wg          sync.WaitGroup
		producerErr atomic.Pointer[error]
		highestSeq  atomic.Uint64 // highest seq durably appended (NextSeq-1)
		walkRuns    atomic.Uint64
		holeFound   atomic.Bool
		holeMessage atomic.Pointer[string]
	)

	// Producer: append contiguous events until the writer has rotated
	// targetRotations times (activeIdx counts rotations, starting at 0).
	wg.Go(func() {
		for w.ActiveIndex() < targetRotations {
			ev := segment.Event{
				IndexedAt:  time.Now().UnixMicro(),
				Kind:       segment.KindCreate,
				DID:        "did:plc:seamtest",
				Collection: "app.bsky.feed.post",
				Rkey:       "rkey",
				Rev:        "rev",
				Payload:    []byte{0xa0},
			}
			if err := w.Append(ctx, &ev); err != nil {
				producerErr.Store(&err)
				return
			}
			highestSeq.Store(ev.Seq)
		}
	})

	// Walkers: repeatedly walk from a trailing cursor that straddles the
	// sealed -> active boundary and assert contiguity of the emitted seqs.
	// The trailing window is kept short (a couple of segments' worth) so each
	// walk is cheap and many interleave with live rotations rather than a few
	// long cold scans dominating the run.
	const trailingWindow = 24 // ~1.5 segments at 16 events/segment
	checkWalk := func() {
		tip := highestSeq.Load()
		if tip < 2 {
			return
		}
		start := uint64(1)
		if tip > trailingWindow {
			start = tip - trailingWindow // straddle the active boundary
		}

		var emitted []uint64
		err := subscribe.WalkFromCursor(ctx, subscribe.WalkInput{
			StartSeq: start,
			Manifest: m,
			Writer:   w,
		}, func(ev *segment.Event) error {
			emitted = append(emitted, ev.Seq)
			return nil
		})
		if err != nil {
			return // ctx cancel / transient; not the property under test
		}
		walkRuns.Add(1)

		for i := 1; i < len(emitted); i++ {
			// Strictly increasing is a separate invariant; the bug shows
			// as a forward hole (gap > 1) where the missing seqs exist.
			if emitted[i] != emitted[i-1]+1 {
				// Only a hole BELOW a value we know was durably appended
				// counts: the missing seqs provably existed.
				if emitted[i-1]+1 <= highestSeq.Load() {
					msg := fmt.Sprintf(
						"HOLE: walk(start=%d) emitted ...%v -> %d (skipped %d..%d); writerNextSeq=%d activeIdx=%d",
						start, tailOf(emitted, i), emitted[i],
						emitted[i-1]+1, emitted[i]-1,
						w.NextSeq(), w.ActiveIndex(),
					)
					holeMessage.Store(&msg)
					holeFound.Store(true)
					return
				}
			}
		}
	}

	// Walkers run until the producer is done (activeIdx reached the target)
	// or a hole is found. They keep pace with the producer so walks interleave
	// with live rotations across the whole run.
	const walkers = 16
	for range walkers {
		wg.Go(func() {
			for !holeFound.Load() && w.ActiveIndex() < targetRotations {
				if err := ctx.Err(); err != nil {
					return
				}
				checkWalk()
			}
			// A final sweep after the producer stopped, so a hole created by
			// the very last rotation is still exercised.
			checkWalk()
		})
	}

	wg.Wait()
	cancel()

	if perr := producerErr.Load(); perr != nil {
		require.NoError(t, *perr, "producer append failed")
	}
	t.Logf("walk runs completed: %d, highest seq appended: %d, rotations: %d",
		walkRuns.Load(), highestSeq.Load(), w.ActiveIndex())
	require.Positive(t, walkRuns.Load(), "no walks ran concurrently with rotations")

	if holeFound.Load() {
		t.Fatalf("data-loss gap reproduced: %s", *holeMessage.Load())
	}
}

// TestWalkFromCursor_RotationSeamDeterministic proves the exact mechanism
// behind the stress-test failure WITHOUT relying on timing.
//
// WalkFromCursor samples its sources in this order:
//  1. the manifest (sealed segments), then
//  2. the writer's ActiveIndex() + its flushed/pending events.
//
// Between those two reads, ingest's rotateLocked does, all under w.mu:
//
//	seal(N) -> OnAfterSeal publishes N to the manifest -> activeIdx = N+1.
//
// So a walk can observe the manifest BEFORE N is published (tier 1 sees
// nothing at/after the cursor and stops) yet observe the writer AFTER the
// rotation (tier 2 reads activeIdx=N+1, whose events are all > the cursor and
// get emitted) — segment N is read by neither tier. We reconstruct exactly
// that torn state: a manifest that has not yet learned about segment N, and a
// writer that has already rotated to N+1.
//
// Regression test for the cold-read rotation seam (issue #190), fixed by the
// convergence loop in WalkFromCursor.
func TestWalkFromCursor_RotationSeamDeterministic(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	require.NoError(t, os.MkdirAll(segDir, 0o755))

	st, err := store.Open(dir, store.NewMetrics(prometheus.NewRegistry()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	// Capture seal callbacks instead of forwarding them to the manifest,
	// so we can deterministically WITHHOLD the publish of the last segment
	// — modeling the instant after rotateLocked sealed it but before the
	// walk's tier-1 manifest read observed it.
	m, err := manifest.Open(manifest.Options{
		SegmentsDir: segDir,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	require.NoError(t, m.Wait(context.Background()))

	type sealEvent struct {
		idx  uint64
		path string
	}
	var seals []sealEvent
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       segDir,
		Store:             st,
		MaxEventsPerBlock: 4,
		MaxSegmentBytes:   512,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:           ingest.NewMetrics(prometheus.NewRegistry()),
		OnAfterSeal: func(idx uint64, path string) error {
			seals = append(seals, sealEvent{idx, path})
			return nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	// Append until the writer has rotated several times AND the current
	// active segment holds events, so there are durable seqs BOTH inside a
	// middle withheld segment AND above it (later sealed segments + the
	// active file). That "events exist above the hole" condition is what
	// makes the pre-fix silent JUMP possible — without it the walk merely
	// runs dry at the hole.
	for w.ActiveIndex() < 3 {
		ev := segment.Event{
			IndexedAt: time.Now().UnixMicro(), Kind: segment.KindCreate,
			DID: "did:plc:seam", Collection: "app.bsky.feed.post",
			Rkey: "r", Rev: "v", Payload: []byte{0xa0},
		}
		require.NoError(t, w.Append(context.Background(), &ev))
	}
	// Add a few more events into the (now active) segment 3 so the active
	// file is non-empty: these are the "events above the hole" served from
	// the active region.
	for range 3 {
		ev := segment.Event{
			IndexedAt: time.Now().UnixMicro(), Kind: segment.KindCreate,
			DID: "did:plc:seam", Collection: "app.bsky.feed.post",
			Rkey: "r", Rev: "v", Payload: []byte{0xa0},
		}
		require.NoError(t, w.Append(context.Background(), &ev))
	}

	require.GreaterOrEqual(t, len(seals), 3, "need >= 3 sealed segments")

	// Publish every sealed segment EXCEPT a MIDDLE one (segment 1). The
	// manifest is missing exactly that middle segment, while segments above
	// it AND the active file are fully reachable — the torn read in which a
	// rotation published higher segments but the walk's manifest snapshot
	// still lacks the gap segment.
	withheld := seals[1]
	for _, s := range seals {
		if s.idx == withheld.idx {
			continue
		}
		require.NoError(t, m.OnSegmentSealed(s.idx, s.path))
	}

	// Determine the seq range of the withheld segment by reading it.
	r, err := segment.Open(segment.ReaderConfig{Path: withheld.path})
	require.NoError(t, err)
	withheldMin := r.Header().MinSeq
	withheldMax := r.Header().MaxSeq
	require.NoError(t, r.Close())
	t.Logf("withheld MIDDLE segment idx=%d covers seqs [%d..%d]; writer activeIdx=%d nextSeq=%d",
		withheld.idx, withheldMin, withheldMax, w.ActiveIndex(), w.NextSeq())

	// Walk from a cursor that lands at the start of the withheld segment,
	// while it stays permanently withheld (an artificial break of the
	// publish-before-bump invariant). The PRE-FIX bug silently jumped the
	// gap and emitted the active segment's seqs (> withheldMax). The FIX's
	// strict contiguity must NEVER emit past the hole; with the manifest
	// permanently unable to fill it, the bounded convergence guard then
	// surfaces a loud error rather than silently dropping or spinning.
	var emitted []uint64
	err = subscribe.WalkFromCursor(context.Background(), subscribe.WalkInput{
		StartSeq: withheldMin,
		Manifest: m,
		Writer:   w,
	}, func(ev *segment.Event) error {
		emitted = append(emitted, ev.Seq)
		return nil
	})

	t.Logf("walk(start=%d) err=%v emitted %d events: first=%v", withheldMin, err, len(emitted), firstN(emitted, 8))

	// Safety property: no silent jump. The walk must not emit ANY seq from
	// the active segment (everything > withheldMax) while the hole at
	// withheldMin is unfilled. That is the exact data loss issue #190
	// described.
	for _, s := range emitted {
		require.LessOrEqualf(t, s, withheldMax,
			"SILENT JUMP: walk emitted seq %d past the unfilled hole [%d..%d] (issue #190 regression)",
			s, withheldMin, withheldMax)
	}

	// Liveness property: with the gap permanently unfillable, the walk must
	// terminate loudly (bounded convergence guard), not hang and not return
	// a clean nil that would hide the drop.
	require.Error(t, err, "permanently-withheld segment must surface a loud non-convergence error, not silent success")
	require.Contains(t, err.Error(), "converge")
}

// TestWalkFromCursor_RotationSeamConverges proves the convergence loop's
// success path: a segment that is missing from the manifest at walk start but
// becomes visible DURING the walk is recovered, yielding a gap-free stream.
// This is the realistic shape of the seam — the rotation's manifest publish
// lands a moment after the walk began — and exercises the re-sweep that fills
// the hole instead of dropping it.
func TestWalkFromCursor_RotationSeamConverges(t *testing.T) {
	t.Parallel()
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

	type sealEvent struct {
		idx  uint64
		path string
	}
	var seals []sealEvent
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       segDir,
		Store:             st,
		MaxEventsPerBlock: 4,
		MaxSegmentBytes:   512,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:           ingest.NewMetrics(prometheus.NewRegistry()),
		OnAfterSeal: func(idx uint64, path string) error {
			seals = append(seals, sealEvent{idx, path})
			return nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	for w.ActiveIndex() < 3 {
		ev := segment.Event{
			IndexedAt: time.Now().UnixMicro(), Kind: segment.KindCreate,
			DID: "did:plc:seam", Collection: "app.bsky.feed.post",
			Rkey: "r", Rev: "v", Payload: []byte{0xa0},
		}
		require.NoError(t, w.Append(context.Background(), &ev))
	}
	require.GreaterOrEqual(t, len(seals), 3, "need >= 3 sealed segments")

	// Publish segment 0 immediately; withhold segment 1 (the "gap"). We will
	// publish segment 1 mid-walk, the first time the emit callback fires, to
	// model the manifest catching up while the walk is in flight.
	seg0 := seals[0]
	seg1 := seals[1]
	require.NoError(t, m.OnSegmentSealed(seg0.idx, seg0.path))
	for _, s := range seals[2:] { // publish everything above the gap too
		require.NoError(t, m.OnSegmentSealed(s.idx, s.path))
	}

	r, err := segment.Open(segment.ReaderConfig{Path: seg0.path})
	require.NoError(t, err)
	seg0Min := r.Header().MinSeq
	require.NoError(t, r.Close())

	highest := w.NextSeq() - 1

	var published atomic.Bool
	var emitted []uint64
	err = subscribe.WalkFromCursor(context.Background(), subscribe.WalkInput{
		StartSeq: seg0Min,
		Manifest: m,
		Writer:   w,
	}, func(ev *segment.Event) error {
		// Publish the withheld gap segment exactly once, as soon as the walk
		// starts producing — the manifest "catches up" mid-walk.
		if published.CompareAndSwap(false, true) {
			require.NoError(t, m.OnSegmentSealed(seg1.idx, seg1.path))
		}
		emitted = append(emitted, ev.Seq)
		return nil
	})
	require.NoError(t, err)

	// Must be the full contiguous run seg0Min..highest with no holes.
	require.NotEmpty(t, emitted)
	require.Equal(t, seg0Min, emitted[0])
	for i := 1; i < len(emitted); i++ {
		require.Equalf(t, emitted[i-1]+1, emitted[i],
			"hole in converged walk at index %d: %v", i, tailOf(emitted, i+1))
	}
	require.Equalf(t, highest, emitted[len(emitted)-1],
		"converged walk stopped short: last emitted %d, highest durable %d", emitted[len(emitted)-1], highest)
	t.Logf("converged: emitted contiguous [%d..%d] (%d events)", emitted[0], emitted[len(emitted)-1], len(emitted))
}

// TestWalkFromCursor_SeamRetryFillsHoleViaActiveRegion deterministically drives
// the ACTIVE-REGION hole -> retry -> sealed-fill path (distinct from the
// converge test, where tier 1 fills the gap before the active sweep runs).
//
// We withhold a middle segment so that on pass 1 the active region reads
// events ABOVE the gap and reports a hole. The gap segment is published from
// inside the onSeamRetry hook — i.e. exactly at the moment the walk decides to
// retry — so the SECOND sealed sweep is what recovers it. This proves the
// retry mechanic itself fills holes, contiguously, with no jump.
func TestWalkFromCursor_SeamRetryFillsHoleViaActiveRegion(t *testing.T) {
	t.Parallel()
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

	type sealEvent struct {
		idx  uint64
		path string
	}
	var seals []sealEvent
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       segDir,
		Store:             st,
		MaxEventsPerBlock: 4,
		MaxSegmentBytes:   512,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:           ingest.NewMetrics(prometheus.NewRegistry()),
		OnAfterSeal: func(idx uint64, path string) error {
			seals = append(seals, sealEvent{idx, path})
			return nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	for w.ActiveIndex() < 3 {
		ev := segment.Event{
			IndexedAt: time.Now().UnixMicro(), Kind: segment.KindCreate,
			DID: "did:plc:seam", Collection: "app.bsky.feed.post",
			Rkey: "r", Rev: "v", Payload: []byte{0xa0},
		}
		require.NoError(t, w.Append(context.Background(), &ev))
	}
	for range 3 { // non-empty active segment: events above the gap
		ev := segment.Event{
			IndexedAt: time.Now().UnixMicro(), Kind: segment.KindCreate,
			DID: "did:plc:seam", Collection: "app.bsky.feed.post",
			Rkey: "r", Rev: "v", Payload: []byte{0xa0},
		}
		require.NoError(t, w.Append(context.Background(), &ev))
	}
	require.GreaterOrEqual(t, len(seals), 3)

	// Publish everything EXCEPT middle segment 1.
	gap := seals[1]
	for _, s := range seals {
		if s.idx != gap.idx {
			require.NoError(t, m.OnSegmentSealed(s.idx, s.path))
		}
	}

	r, err := segment.Open(segment.ReaderConfig{Path: seals[0].path})
	require.NoError(t, err)
	startSeq := r.Header().MinSeq
	require.NoError(t, r.Close())
	highest := w.NextSeq() - 1

	var retries []uint64
	input := subscribe.WithSeamRetryObserver(
		subscribe.WalkInput{StartSeq: startSeq, Manifest: m, Writer: w},
		func(holeSeq uint64) {
			retries = append(retries, holeSeq)
			// Publish the gap segment the first time the walk retries, so the
			// SECOND sealed sweep recovers it. Idempotent re-publish is safe.
			require.NoError(t, m.OnSegmentSealed(gap.idx, gap.path))
		},
	)

	var emitted []uint64
	err = subscribe.WalkFromCursor(context.Background(), input, func(ev *segment.Event) error {
		emitted = append(emitted, ev.Seq)
		return nil
	})
	require.NoError(t, err)

	require.NotEmpty(t, retries, "active-region hole -> retry path was not exercised")
	t.Logf("seam retries observed at seqs %v; emitted [%d..%d] (%d events)",
		retries, emitted[0], emitted[len(emitted)-1], len(emitted))

	// Gap-free, full coverage.
	require.Equal(t, startSeq, emitted[0])
	for i := 1; i < len(emitted); i++ {
		require.Equalf(t, emitted[i-1]+1, emitted[i], "hole at index %d: %v", i, tailOf(emitted, i+1))
	}
	require.Equal(t, highest, emitted[len(emitted)-1])
}

// TestWalkFromCursor_NilManifestLenient pins the nil-manifest contract that
// the convergence restructuring must preserve: with no manifest there are no
// sealed segments and nothing that could ever fill a hole, so the active
// region is read leniently (every event >= cursor, no strict-contiguity stop)
// in a single pass. A strict walk here would wedge: it would stop at the first
// event above the start cursor and never resume.
func TestWalkFromCursor_NilManifestLenient(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	require.NoError(t, os.MkdirAll(segDir, 0o755))

	st, w := openWriterAtTip(t, dir, 1)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })

	// Append events into the active segment, flushing some to disk and
	// leaving the rest pending, so both the flushed-block and pending paths
	// are exercised under a nil manifest.
	const total = 10
	for range total {
		ev := segment.Event{
			IndexedAt: time.Now().UnixMicro(), Kind: segment.KindCreate,
			DID: "did:plc:nilman", Collection: "app.bsky.feed.post",
			Rkey: "r", Rev: "v", Payload: []byte{0xa0},
		}
		require.NoError(t, w.Append(context.Background(), &ev))
	}
	require.NoError(t, w.Flush(context.Background())) // flush some; later appends stay pending
	for range 4 {
		ev := segment.Event{
			IndexedAt: time.Now().UnixMicro(), Kind: segment.KindCreate,
			DID: "did:plc:nilman", Collection: "app.bsky.feed.post",
			Rkey: "r", Rev: "v", Payload: []byte{0xa0},
		}
		require.NoError(t, w.Append(context.Background(), &ev))
	}
	highest := w.NextSeq() - 1

	var emitted []uint64
	err := subscribe.WalkFromCursor(context.Background(), subscribe.WalkInput{
		StartSeq: 3, // start mid-stream to confirm the lenient skip below start
		Manifest: nil,
		Writer:   w,
	}, func(ev *segment.Event) error {
		emitted = append(emitted, ev.Seq)
		return nil
	})
	require.NoError(t, err)

	require.NotEmpty(t, emitted)
	require.Equal(t, uint64(3), emitted[0])
	for i := 1; i < len(emitted); i++ {
		require.Equalf(t, emitted[i-1]+1, emitted[i], "hole in nil-manifest walk at %d: %v", i, tailOf(emitted, i+1))
	}
	require.Equalf(t, highest, emitted[len(emitted)-1],
		"nil-manifest walk stopped short: last %d, highest %d", emitted[len(emitted)-1], highest)
}

// TestWalkFromCursor_EmptyActiveSuccessorSeam pins the rotation seam variant
// with NO in-band hole signal: rotation sealed+published segment N, bumped
// activeIdx to N+1, and opened an EMPTY N+1 with no events yet appended. A walk
// whose sealed sweep snapshotted the manifest BEFORE N was published stops at
// the start of N's range, then reads the empty active successor — which emits
// nothing and reports hole=false because no event sits ABOVE the cursor.
//
// The other seam tests all keep the active segment NON-empty (events above the
// gap drive hole=true); none exercise this empty-successor state. Without the
// convergence check on Writer.NextSeq(), WalkFromCursor returns here with the
// cursor unchanged, so ColdReader.Read hands the subscriber loop a
// non-advancing (next==cursor) batch and handler.go disconnects it — even
// though N is durable and recoverable from the manifest. This test withholds N,
// publishes it from the onSeamRetry hook (the only callback that fires, since
// nothing is emitted on pass 1), and asserts the walk converges to the full
// contiguous range. Regression test for the cold-read rotation seam (issue
// #190), empty-active-successor variant.
func TestWalkFromCursor_EmptyActiveSuccessorSeam(t *testing.T) {
	t.Parallel()
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

	type sealEvent struct {
		idx  uint64
		path string
	}
	var seals []sealEvent
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       segDir,
		Store:             st,
		MaxEventsPerBlock: 4,
		MaxSegmentBytes:   512,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:           ingest.NewMetrics(prometheus.NewRegistry()),
		OnAfterSeal: func(idx uint64, path string) error {
			seals = append(seals, sealEvent{idx, path})
			return nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	// Append until the writer has rotated to active index 3. The append that
	// triggers each rotation seals the just-filled segment and opens an empty
	// successor; we stop the instant ActiveIndex first reaches 3, so segment 3
	// (the active file) holds ZERO events and ZERO pending — the empty
	// successor this test is about. seals == [0,1,2].
	for w.ActiveIndex() < 3 {
		ev := segment.Event{
			IndexedAt: time.Now().UnixMicro(), Kind: segment.KindCreate,
			DID: "did:plc:emptyseam", Collection: "app.bsky.feed.post",
			Rkey: "r", Rev: "v", Payload: []byte{0xa0},
		}
		require.NoError(t, w.Append(context.Background(), &ev))
	}
	require.GreaterOrEqual(t, len(seals), 3, "need segments 0,1,2 sealed with 3 active+empty")
	require.Empty(t, w.SnapshotPending(), "active successor must be empty for this seam")

	// Publish segments 0 and 1; WITHHOLD segment 2 (the gap N). The walk's
	// first sealed sweep therefore stops at the start of segment 2's range,
	// and the empty active successor (index 3) cannot fill it.
	require.NoError(t, m.OnSegmentSealed(seals[0].idx, seals[0].path))
	require.NoError(t, m.OnSegmentSealed(seals[1].idx, seals[1].path))
	segN := seals[2]

	rN, err := segment.Open(segment.ReaderConfig{Path: segN.path})
	require.NoError(t, err)
	segNMin := rN.Header().MinSeq
	require.NoError(t, rN.Close())

	// With segment 2 the last sealed and the active successor empty, the live
	// tip is exactly one past segment 2's max.
	highest := w.NextSeq() - 1
	require.Greater(t, highest, segNMin, "withheld segment must hold >= 1 event above the start cursor")

	var published atomic.Bool
	var retries []uint64
	var emitted []uint64
	input := subscribe.WithSeamRetryObserver(subscribe.WalkInput{
		StartSeq: segNMin,
		Manifest: m,
		Writer:   w,
	}, func(holeSeq uint64) {
		retries = append(retries, holeSeq)
		// Publish the withheld gap segment exactly once, at the moment the walk
		// detects the sub-tip gap and decides to retry. This is the ONLY
		// callback that fires on pass 1 — the empty active emits nothing — so
		// the seam is invisible to the emit callback the converge test relies
		// on. A walk that returned at !hole would never reach this hook.
		if published.CompareAndSwap(false, true) {
			require.NoError(t, m.OnSegmentSealed(segN.idx, segN.path))
		}
	})
	err = subscribe.WalkFromCursor(context.Background(), input, func(ev *segment.Event) error {
		emitted = append(emitted, ev.Seq)
		return nil
	})
	require.NoError(t, err)

	// The seam must have been detected with no in-band hole signal: the only
	// way to observe it is the NextSeq() convergence check.
	require.NotEmpty(t, retries, "empty-active-successor seam was not detected (no retry fired)")

	// The walk must converge to the full contiguous run [segNMin..highest].
	// Without the fix, emitted is empty (the walk returns at the unchanged
	// cursor) and these assertions fail.
	require.NotEmpty(t, emitted, "walk emitted nothing: empty-active seam dropped the withheld segment")
	require.Equal(t, segNMin, emitted[0])
	for i := 1; i < len(emitted); i++ {
		require.Equalf(t, emitted[i-1]+1, emitted[i],
			"hole in converged walk at index %d: %v", i, tailOf(emitted, i+1))
	}
	require.Equalf(t, highest, emitted[len(emitted)-1],
		"converged walk stopped short: last emitted %d, highest durable %d", emitted[len(emitted)-1], highest)
	t.Logf("empty-active seam converged: retries at %v, emitted contiguous [%d..%d] (%d events)",
		retries, emitted[0], emitted[len(emitted)-1], len(emitted))
}

func firstN(s []uint64, n int) []uint64 {
	if len(s) < n {
		return s
	}
	return s[:n]
}

func tailOf(s []uint64, i int) []uint64 {
	return s[max(i-4, 0):i]
}
