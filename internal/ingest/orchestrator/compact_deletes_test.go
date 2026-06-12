package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/crashpoint"
	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/internal/tombstone"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/stretchr/testify/require"
)

func TestRunDeleteCompactionCallsPassHook(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dataDir, "segments"), 0o755))
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	var got []CompactionPassResult
	o := &Orchestrator{cfg: Config{
		DataDir:            dataDir,
		Store:              st,
		Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
		CompactionInterval: time.Hour,
		OnCompactionPass: func(result CompactionPassResult) {
			got = append(got, result)
		},
	}}

	require.NoError(t, o.runDeleteCompaction(t.Context(), compactionSteady))
	require.Len(t, got, 1)
	require.Equal(t, uint64(0), got[0].Watermark)
	require.NoError(t, got[0].Err)
}

func TestCompactionCandidateDIDs(t *testing.T) {
	t.Parallel()

	got := compactionCandidateDIDs(tombstone.Snapshot{
		Records: map[tombstone.RecordKey]uint64{
			{DID: "did:plc:b", Collection: "c", Rkey: "r1"}: 1,
			{DID: "did:plc:a", Collection: "c", Rkey: "r2"}: 2,
			{DID: "did:plc:b", Collection: "c", Rkey: "r3"}: 3,
			{DID: "", Collection: "c", Rkey: "r4"}:          4,
		},
		DIDs: map[string]tombstone.DIDTombstone{
			"did:plc:c": {Seq: 5, Reason: "sync"},
			"did:plc:a": {Seq: 6, Reason: "account"},
			"":          {Seq: 7, Reason: "sync"},
		},
	})

	require.ElementsMatch(t, []string{"did:plc:a", "did:plc:b", "did:plc:c"}, got)
}

func TestRunDeleteCompaction_RewriteBeforeWatermarkCrashIsIdempotent(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	segmentsDir := filepath.Join(dataDir, "segments")
	require.NoError(t, os.MkdirAll(segmentsDir, 0o755))
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	path := writeCompactionSegment(t, segmentsDir, 0, []segment.Event{
		{Seq: 1, IndexedAt: 10, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r", Rev: "1", Payload: []byte("old")},
		{Seq: 2, IndexedAt: 20, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r", Rev: "2"},
	})

	crashErr := errors.New("simulated compaction crash")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	o := &Orchestrator{cfg: Config{
		DataDir:            dataDir,
		Store:              st,
		Logger:             logger,
		CompactionInterval: time.Hour,
		CrashInjector: pointErrorInjector{
			point: crashpoint.AfterCompactionRewriteBeforeWatermark,
			err:   crashErr,
		},
	}, logger: logger}

	err = o.runDeleteCompaction(t.Context(), compactionMergeTail)
	require.ErrorIs(t, err, crashErr)
	watermark, _, err := loadCompactionWatermark(st)
	require.NoError(t, err)
	require.Zero(t, watermark, "watermark must not advance when crash fires after rewrite")

	events := readCompactionSegment(t, path)
	require.Len(t, events, 1, "rewrite may have completed before the crash")
	require.Equal(t, segment.KindDelete, events[0].Kind)

	o.cfg.CrashInjector = nil
	require.NoError(t, o.runDeleteCompaction(t.Context(), compactionMergeTail))
	watermark, _, err = loadCompactionWatermark(st)
	require.NoError(t, err)
	require.Equal(t, uint64(2), watermark)
	events = readCompactionSegment(t, path)
	require.Len(t, events, 1)
	require.Equal(t, segment.KindDelete, events[0].Kind)
}

func TestRunDeleteCompaction_ManifestRefreshFailureReconcilesOnRetry(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	segmentsDir := filepath.Join(dataDir, "segments")
	require.NoError(t, os.MkdirAll(segmentsDir, 0o755))
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	path := writeCompactionSegment(t, segmentsDir, 0, []segment.Event{
		{Seq: 1, IndexedAt: 10, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r", Rev: "1", Payload: []byte("old")},
		{Seq: 2, IndexedAt: 20, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r", Rev: "2"},
	})
	ts := tombstone.New()
	require.NoError(t, ts.Observe(&segment.Event{Seq: 2, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r"}))

	refreshErr := errors.New("manifest refresh failed")
	var calls int
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	o := &Orchestrator{cfg: Config{
		DataDir:            dataDir,
		Store:              st,
		Logger:             logger,
		Tombstones:         ts,
		CompactionInterval: time.Hour,
		OnSegmentCompacted: func(idx uint64, gotPath string) error {
			require.Equal(t, uint64(0), idx)
			require.Equal(t, path, gotPath)
			calls++
			if calls == 2 {
				return refreshErr
			}
			return nil
		},
	}, logger: logger}

	err = o.runDeleteCompaction(t.Context(), compactionSteady)
	require.ErrorIs(t, err, refreshErr)
	require.Equal(t, 2, calls, "first call reconciles at pass start; second call is the failed post-rewrite refresh")
	watermark, _, err := loadCompactionWatermark(st)
	require.NoError(t, err)
	require.Zero(t, watermark)
	events := readCompactionSegment(t, path)
	require.Len(t, events, 1)
	require.Equal(t, segment.KindDelete, events[0].Kind)
	require.Equal(t, 1, ts.Len(), "failed pass must not evict tombstones")

	require.NoError(t, o.runDeleteCompaction(t.Context(), compactionSteady))
	require.Equal(t, 3, calls, "retry must reconcile the rewritten segment even though it is already clean")
	watermark, _, err = loadCompactionWatermark(st)
	require.NoError(t, err)
	require.Equal(t, uint64(2), watermark)
	require.Zero(t, ts.Len(), "successful retry evicts applied tombstones")
}

func TestRunDeleteCompaction_ChunkWatermarkCrashResumesAtNextChunk(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	segmentsDir := filepath.Join(dataDir, "segments")
	require.NoError(t, os.MkdirAll(segmentsDir, 0o755))
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	path0 := writeCompactionSegment(t, segmentsDir, 0, []segment.Event{
		{Seq: 1, IndexedAt: 10, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r", Rev: "1", Payload: []byte("old-a")},
		{Seq: 2, IndexedAt: 20, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r", Rev: "2"},
	})
	path1 := writeCompactionSegment(t, segmentsDir, 1, []segment.Event{
		{Seq: 3, IndexedAt: 30, Kind: segment.KindCreate, DID: "did:plc:b", Collection: "app.bsky.feed.post", Rkey: "r", Rev: "3", Payload: []byte("old-b")},
		{Seq: 4, IndexedAt: 40, Kind: segment.KindDelete, DID: "did:plc:b", Collection: "app.bsky.feed.post", Rkey: "r", Rev: "4"},
	})

	crashErr := errors.New("simulated chunk crash")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	o := &Orchestrator{cfg: Config{
		DataDir:                dataDir,
		Store:                  st,
		Logger:                 logger,
		CompactionInterval:     time.Hour,
		CompactionTombstoneCap: 1,
		CrashInjector:          pointErrorInjector{point: crashpoint.AfterCompactionChunkWatermark, err: crashErr},
	}, logger: logger}

	err = o.runDeleteCompaction(t.Context(), compactionMergeTail)
	require.ErrorIs(t, err, crashErr)
	watermark, _, err := loadCompactionWatermark(st)
	require.NoError(t, err)
	require.Equal(t, uint64(2), watermark)
	require.Len(t, readCompactionSegment(t, path0), 1)
	require.Len(t, readCompactionSegment(t, path1), 2)

	o.cfg.CrashInjector = nil
	require.NoError(t, o.runDeleteCompaction(t.Context(), compactionMergeTail))
	watermark, _, err = loadCompactionWatermark(st)
	require.NoError(t, err)
	require.Equal(t, uint64(4), watermark)
	require.Len(t, readCompactionSegment(t, path0), 1)
	require.Len(t, readCompactionSegment(t, path1), 1)
}

func TestRunDeleteCompaction_SteadyLiveSetMatchesScanFold(t *testing.T) {
	t.Parallel()

	events := []segment.Event{
		{Seq: 1, IndexedAt: 10, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r", Rev: "1", Payload: []byte("old-a")},
		{Seq: 2, IndexedAt: 20, Kind: segment.KindCreate, DID: "did:plc:b", Collection: "app.bsky.feed.post", Rkey: "r", Rev: "1", Payload: []byte("old-b")},
		{Seq: 3, IndexedAt: 30, Kind: segment.KindUpdate, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r", Rev: "2", Payload: []byte("new-a")},
		{Seq: 4, IndexedAt: 40, Kind: segment.KindDelete, DID: "did:plc:b", Collection: "app.bsky.feed.post", Rkey: "r", Rev: "2"},
	}

	scanDir, scanStore, scanPath := newCompactionDataDir(t, events)
	scanLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	scan := &Orchestrator{cfg: Config{
		DataDir:            scanDir,
		Store:              scanStore,
		Logger:             scanLogger,
		CompactionInterval: time.Hour,
	}, logger: scanLogger}
	require.NoError(t, scan.runDeleteCompaction(t.Context(), compactionMergeTail))

	steadyDir, steadyStore, steadyPath := newCompactionDataDir(t, events)
	liveSet := tombstone.New()
	for i := range events {
		require.NoError(t, liveSet.Observe(&events[i]))
	}
	steadyLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	steady := &Orchestrator{cfg: Config{
		DataDir:            steadyDir,
		Store:              steadyStore,
		Logger:             steadyLogger,
		Tombstones:         liveSet,
		CompactionInterval: time.Hour,
	}, logger: steadyLogger}
	require.NoError(t, steady.runDeleteCompaction(t.Context(), compactionSteady))

	scanWatermark, _, err := loadCompactionWatermark(scanStore)
	require.NoError(t, err)
	steadyWatermark, _, err := loadCompactionWatermark(steadyStore)
	require.NoError(t, err)
	require.Equal(t, scanWatermark, steadyWatermark)
	require.Equal(t, readCompactionSegment(t, scanPath), readCompactionSegment(t, steadyPath))
}

func BenchmarkDeleteCompactionSyntheticArchive(b *testing.B) {
	const (
		segments         = 8
		eventsPerSegment = 512
	)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dataDir := b.TempDir()
		segmentsDir := filepath.Join(dataDir, "segments")
		if err := os.MkdirAll(segmentsDir, 0o755); err != nil {
			b.Fatal(err)
		}
		st, err := store.Open(dataDir, nil)
		if err != nil {
			b.Fatal(err)
		}
		var seq uint64
		for segmentIdx := range segments {
			events := make([]segment.Event, 0, eventsPerSegment)
			for eventIdx := range eventsPerSegment / 2 {
				did := fmt.Sprintf("did:plc:%04d", eventIdx%128)
				rkey := fmt.Sprintf("r-%d-%d", segmentIdx, eventIdx)
				seq++
				events = append(events, segment.Event{
					Seq:        seq,
					IndexedAt:  int64(seq),
					Kind:       segment.KindCreate,
					DID:        did,
					Collection: "app.bsky.feed.post",
					Rkey:       rkey,
					Rev:        fmt.Sprintf("%d-a", seq),
					Payload:    []byte(`{"text":"old"}`),
				})
				seq++
				events = append(events, segment.Event{
					Seq:        seq,
					IndexedAt:  int64(seq),
					Kind:       segment.KindDelete,
					DID:        did,
					Collection: "app.bsky.feed.post",
					Rkey:       rkey,
					Rev:        fmt.Sprintf("%d-b", seq),
				})
			}
			writeCompactionSegmentB(b, segmentsDir, uint64(segmentIdx), events)
		}
		o := &Orchestrator{cfg: Config{
			DataDir:                  dataDir,
			Store:                    st,
			Logger:                   logger,
			CompactionInterval:       time.Hour,
			CompactionRewriteWorkers: 4,
		}, logger: logger}

		b.StartTimer()
		if err := o.runDeleteCompaction(b.Context(), compactionMergeTail); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if err := st.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func newCompactionDataDir(t *testing.T, events []segment.Event) (string, *store.Store, string) {
	t.Helper()
	dataDir := t.TempDir()
	segmentsDir := filepath.Join(dataDir, "segments")
	require.NoError(t, os.MkdirAll(segmentsDir, 0o755))
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	path := writeCompactionSegment(t, segmentsDir, 0, events)
	return dataDir, st, path
}

func writeCompactionSegment(t *testing.T, dir string, idx uint64, events []segment.Event) string {
	t.Helper()
	path := filepath.Join(dir, ingest.SegmentFilename(idx))
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	for _, ev := range events {
		full, err := w.Append(ev)
		require.NoError(t, err)
		if full {
			require.NoError(t, w.Flush())
		}
	}
	_, err = w.Seal()
	require.NoError(t, err)
	return path
}

func writeCompactionSegmentB(b *testing.B, dir string, idx uint64, events []segment.Event) string {
	b.Helper()
	path := filepath.Join(dir, ingest.SegmentFilename(idx))
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: 128})
	if err != nil {
		b.Fatal(err)
	}
	for _, ev := range events {
		full, err := w.Append(ev)
		if err != nil {
			b.Fatal(err)
		}
		if full {
			if err := w.Flush(); err != nil {
				b.Fatal(err)
			}
		}
	}
	if _, err := w.Seal(); err != nil {
		b.Fatal(err)
	}
	return path
}

func readCompactionSegment(t *testing.T, path string) []segment.Event {
	t.Helper()
	r, err := segment.Open(segment.ReaderConfig{Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })
	var out []segment.Event
	for i := range int(r.Header().BlockCount) {
		events, err := r.DecodeBlock(i)
		require.NoError(t, err)
		out = append(out, events...)
	}
	return out
}

// TestReconcileCompactionManifestRefreshesOnlyMismatches pins the spec
// §5 step-2 reconcile primitive: entries whose resident checksum
// matches the on-disk header are skipped; mismatched and missing
// entries re-fire the refresh path.
func TestReconcileCompactionManifestRefreshesOnlyMismatches(t *testing.T) {
	t.Parallel()

	var refreshed []uint64
	o := &Orchestrator{cfg: Config{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		OnSegmentCompacted: func(idx uint64, path string) error {
			refreshed = append(refreshed, idx)
			return nil
		},
		SegmentManifestChecksums: func() map[uint64]uint64 {
			return map[uint64]uint64{
				0: 0xAAAA, // matches -> skipped
				1: 0xDEAD, // stale -> refreshed
				// 2 missing -> refreshed
			}
		},
	}}
	o.logger = o.cfg.Logger

	sealed := []sealedCompactionSegment{
		{SegmentFile: ingest.SegmentFile{Idx: 0, Path: "seg0"}, header: segment.Header{Checksum: 0xAAAA}},
		{SegmentFile: ingest.SegmentFile{Idx: 1, Path: "seg1"}, header: segment.Header{Checksum: 0xBBBB}},
		{SegmentFile: ingest.SegmentFile{Idx: 2, Path: "seg2"}, header: segment.Header{Checksum: 0xCCCC}},
	}
	require.NoError(t, o.reconcileCompactionManifest(sealed))
	require.Equal(t, []uint64{1, 2}, refreshed)

	// Without a checksum source, reconcile must refresh everything
	// (conservative fallback).
	refreshed = nil
	o.cfg.SegmentManifestChecksums = nil
	require.NoError(t, o.reconcileCompactionManifest(sealed))
	require.Equal(t, []uint64{0, 1, 2}, refreshed)
}

// TestRunSteadyCompactor_PassErrorDoesNotExit pins the spec §5 failure
// policy: a failing pass is logged and retried on the next trigger;
// it must never propagate into the errgroup and take the daemon down.
func TestRunSteadyCompactor_PassErrorDoesNotExit(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	// Make every pass fail: "segments" is a file, so the pass's
	// directory listing errors.
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "segments"), []byte("not a dir"), 0o644))
	st, err := store.Open(filepath.Join(dataDir, "meta"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	var passes []CompactionPassResult
	var mu sync.Mutex
	o := &Orchestrator{
		cfg: Config{
			DataDir:            dataDir,
			Store:              st,
			Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
			CompactionInterval: 5 * time.Millisecond,
			OnCompactionPass: func(result CompactionPassResult) {
				mu.Lock()
				passes = append(passes, result)
				mu.Unlock()
			},
		},
		compactionTrigger: make(chan struct{}, 1),
	}
	o.logger = o.cfg.Logger

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	err = o.runSteadyCompactor(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded,
		"the compactor must outlive failing passes and exit only on ctx")

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(passes), 2, "failing passes must keep retrying on the interval")
	for _, p := range passes {
		require.Error(t, p.Err)
	}
}

// TestRebuildLiveTombstones_BoundedByWatermark pins the spec §3.4
// restart rebuild: the rebuilt set equals the fold over
// (compaction/seq, tip] — segments entirely at or below the watermark
// contribute nothing — and the result matches an incrementally
// Observe()-built set over the same window.
func TestRebuildLiveTombstones_BoundedByWatermark(t *testing.T) {
	t.Parallel()

	segA := []segment.Event{
		{Seq: 1, IndexedAt: 10, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Rev: "1", Payload: []byte("x")},
		{Seq: 2, IndexedAt: 20, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "r", Rev: "2"},
	}
	segB := []segment.Event{
		{Seq: 3, IndexedAt: 30, Kind: segment.KindDelete, DID: "did:plc:b", Collection: "c", Rkey: "r", Rev: "3"},
		{Seq: 4, IndexedAt: 40, Kind: segment.KindSync, DID: "did:plc:c", Rev: "4", Payload: []byte{0xa0}},
	}

	dataDir := t.TempDir()
	segmentsDir := filepath.Join(dataDir, "segments")
	require.NoError(t, os.MkdirAll(segmentsDir, 0o755))
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	writeCompactionSegment(t, segmentsDir, 0, segA)
	writeCompactionSegment(t, segmentsDir, 1, segB)

	// Watermark covers all of segment A.
	require.NoError(t, saveCompactionWatermark(st, 2))

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	set := tombstone.New()
	o := &Orchestrator{cfg: Config{
		DataDir:            dataDir,
		Store:              st,
		Logger:             logger,
		Tombstones:         set,
		CompactionInterval: time.Hour,
	}, logger: logger}
	require.NoError(t, o.rebuildLiveTombstones(t.Context()))

	want := tombstone.New()
	for i := range segB {
		require.NoError(t, want.Observe(&segB[i]))
	}
	maxSeq := ^uint64(0)
	require.Equal(t, want.SnapshotRange(0, maxSeq).Records, set.SnapshotRange(0, maxSeq).Records)
	require.Equal(t, want.SnapshotRange(0, maxSeq).DIDs, set.SnapshotRange(0, maxSeq).DIDs)
	require.NotContains(t, set.SnapshotRange(0, maxSeq).Records,
		tombstone.RecordKey{DID: "did:plc:a", Collection: "c", Rkey: "r"},
		"tombstones at or below the watermark are already applied and must not rebuild")
}

// TestRebuildLiveTombstones_DisabledWhenCompactionOff: with
// --compaction-interval=0 nothing ever evicts the set, so the rebuild
// must not populate it (unbounded growth otherwise).
func TestRebuildLiveTombstones_DisabledWhenCompactionOff(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	segmentsDir := filepath.Join(dataDir, "segments")
	require.NoError(t, os.MkdirAll(segmentsDir, 0o755))
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	writeCompactionSegment(t, segmentsDir, 0, []segment.Event{
		{Seq: 1, IndexedAt: 10, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "r", Rev: "1"},
	})

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	set := tombstone.New()
	o := &Orchestrator{cfg: Config{
		DataDir:            dataDir,
		Store:              st,
		Logger:             logger,
		Tombstones:         set,
		CompactionInterval: 0,
	}, logger: logger}
	require.NoError(t, o.rebuildLiveTombstones(t.Context()))
	require.Zero(t, set.Len())
}
