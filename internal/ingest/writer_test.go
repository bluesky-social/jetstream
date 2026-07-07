package ingest

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	mathrand "math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// newTestStore opens a fresh metadata pebble db rooted at t.TempDir.
func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// newTestWriter is the standard Writer fixture: fresh segments dir, a
// fresh pebble store, the provided overrides applied last.
func newTestWriter(t *testing.T, overrides Config) *Writer {
	t.Helper()
	segDir := filepath.Join(t.TempDir(), "segments")

	cfg := Config{
		SegmentsDir: segDir,
		Store:       newTestStore(t),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:     NewMetrics(prometheus.NewRegistry()),
	}
	if overrides.MaxSegmentBytes != 0 {
		cfg.MaxSegmentBytes = overrides.MaxSegmentBytes
	}
	if overrides.MaxEventsPerBlock != 0 {
		cfg.MaxEventsPerBlock = overrides.MaxEventsPerBlock
	}
	if overrides.AsyncFlushWorkers != 0 {
		cfg.AsyncFlushWorkers = overrides.AsyncFlushWorkers
	}
	if overrides.DataDir != "" {
		cfg.DataDir = overrides.DataDir
	}
	if overrides.Metrics != nil {
		cfg.Metrics = overrides.Metrics
	}
	if overrides.SegmentIOFaultInjector != nil {
		cfg.SegmentIOFaultInjector = overrides.SegmentIOFaultInjector
	}

	w, err := Open(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return w
}

type durableOrderRecorder struct {
	mu  sync.Mutex
	ops []string
}

func (r *durableOrderRecorder) add(op string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ops = append(r.ops, op)
}

func (r *durableOrderRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ops...)
}

type seqCommitRecorder struct {
	rec *durableOrderRecorder
}

func (r seqCommitRecorder) BeforeWrite(op store.WriteOp, keys [][]byte) error {
	if op != store.WriteOpBatchCommit {
		return nil
	}
	for _, key := range keys {
		if string(key) == seqNextKey {
			r.rec.add("store-seq-commit")
			break
		}
	}
	return nil
}

func TestWriterFlushOrdersSegmentSyncBeforeStoreCommit(t *testing.T) {
	t.Parallel()

	baseFS := vfs.NewStrictMem()
	dataDir := "/data"
	segmentsDir := "/data/segments"
	require.NoError(t, baseFS.MkdirAll(dataDir, 0o755))
	syncStrictTestDir(t, baseFS, "/")

	rec := &durableOrderRecorder{}
	fs := vfs.WithLogging(baseFS, func(format string, args ...any) {
		switch format {
		case "write-at(%d, %d): %s":
			offset, ok := args[0].(int64)
			if ok && offset >= int64(segment.ReservedHeaderBytes) {
				if path, ok := args[2].(string); ok && strings.Contains(path, ".jss") {
					rec.add("segment-data-write")
				}
			}
		case "sync: %s":
			if path, ok := args[0].(string); ok && strings.Contains(path, ".jss") {
				rec.add("segment-sync")
			}
		}
	})

	st, err := store.Open(dataDir, nil,
		store.WithFS(fs),
		store.WithFaultInjector(seqCommitRecorder{rec: rec}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })

	w, err := Open(Config{
		DataDir:           dataDir,
		SegmentsDir:       segmentsDir,
		FS:                fs,
		Store:             st,
		MaxEventsPerBlock: 1,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:order"}
	require.NoError(t, w.Append(t.Context(), &ev))

	ops := rec.snapshot()
	storeCommit := slices.Index(ops, "store-seq-commit")
	require.NotEqual(t, -1, storeCommit, "expected seq/next commit in op stream: %v", ops)
	dataWrite := slices.Index(ops, "segment-data-write")
	require.NotEqual(t, -1, dataWrite, "expected segment data write in op stream: %v", ops)
	syncAfterWrite := slices.Index(ops[dataWrite:storeCommit], "segment-sync")
	require.NotEqual(t, -1, syncAfterWrite, "segment data fsync must happen after block write and before seq/next commit: %v", ops)
}

func TestWriterStrictMemPowerLossDropsUnsyncedSegmentAndStoreState(t *testing.T) {
	t.Parallel()

	fs := vfs.NewStrictMem()
	dataDir := "/data"
	segmentsDir := "/data/segments"
	require.NoError(t, fs.MkdirAll(dataDir, 0o755))
	syncStrictTestDir(t, fs, "/")

	st, err := store.Open(dataDir, nil, store.WithFS(fs))
	require.NoError(t, err)
	w, err := Open(Config{
		DataDir:           dataDir,
		SegmentsDir:       segmentsDir,
		FS:                fs,
		Store:             st,
		MaxEventsPerBlock: 1,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)

	first := segment.Event{Kind: segment.KindCreate, DID: "did:plc:first"}
	require.NoError(t, w.Append(t.Context(), &first))
	require.Equal(t, uint64(1), first.Seq)
	require.Equal(t, []uint64{1}, collectActiveSeqs(t, fs, filepath.Join(segmentsDir, SegmentFilename(0))))

	fs.SetIgnoreSyncs(true)
	second := segment.Event{Kind: segment.KindCreate, DID: "did:plc:second"}
	require.NoError(t, w.Append(t.Context(), &second))
	require.Equal(t, uint64(2), second.Seq)
	require.NoError(t, w.Close())
	require.NoError(t, st.Close())

	fs.ResetToSyncedState()
	fs.SetIgnoreSyncs(false)
	require.Equal(t, []uint64{1}, collectActiveSeqs(t, fs, filepath.Join(segmentsDir, SegmentFilename(0))))

	st, err = store.Open(dataDir, nil, store.WithFS(fs))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	w, err = Open(Config{
		DataDir:           dataDir,
		SegmentsDir:       segmentsDir,
		FS:                fs,
		Store:             st,
		MaxEventsPerBlock: 1,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, w.Close()) })
	require.Equal(t, uint64(2), w.NextSeq())

	require.Equal(t, []uint64{1}, collectActiveSeqs(t, fs, filepath.Join(segmentsDir, SegmentFilename(0))))
}

func collectActiveSeqs(t *testing.T, fs vfs.FS, path string) []uint64 {
	t.Helper()
	var got []uint64
	require.NoError(t, segment.WalkActiveFS(fs, path, func(events []segment.Event) error {
		for i := range events {
			got = append(got, events[i].Seq)
		}
		return nil
	}))
	return got
}

func syncStrictTestDir(t *testing.T, fs *vfs.MemFS, dir string) {
	t.Helper()
	f, err := fs.OpenDir(dir)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())
}

type segmentIOFault struct {
	op      segment.IOOp
	ordinal int
	err     error
	seen    atomic.Int64
}

func (f *segmentIOFault) BeforeSegmentIO(_ string, op segment.IOOp) error {
	if op != f.op {
		return nil
	}
	if int(f.seen.Add(1)) == f.ordinal {
		return f.err
	}
	return nil
}

func TestWriter_ENOSPCSyncFlushReturnsFatalOperatorMessage(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	w, err := Open(Config{
		DataDir:                dataDir,
		SegmentsDir:            filepath.Join(dataDir, "segments"),
		Store:                  st,
		MaxEventsPerBlock:      2,
		Logger:                 slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:                NewMetrics(prometheus.NewRegistry()),
		SegmentIOFaultInjector: &segmentIOFault{op: segment.IOOpWrite, ordinal: 2, err: syscall.ENOSPC},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	for i := 0; i < 2; i++ {
		ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a"}
		err = w.Append(t.Context(), &ev)
		if err != nil {
			break
		}
	}
	require.ErrorIs(t, err, syscall.ENOSPC)
	require.ErrorContains(t, err, "fatal persistence error")
	require.ErrorContains(t, err, "disk full")
	require.ErrorContains(t, err, dataDir)
	require.ErrorContains(t, err, "restart jetstream")
}

func TestWriter_ENOSPCAsyncFlushReturnsFatalOperatorMessage(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	w, err := Open(Config{
		DataDir:                dataDir,
		SegmentsDir:            filepath.Join(dataDir, "segments"),
		Store:                  st,
		MaxEventsPerBlock:      2,
		AsyncFlushWorkers:      1,
		Logger:                 slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:                NewMetrics(prometheus.NewRegistry()),
		SegmentIOFaultInjector: &segmentIOFault{op: segment.IOOpSync, ordinal: 3, err: syscall.ENOSPC},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	for i := 0; i < 2; i++ {
		ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a"}
		err = w.Append(t.Context(), &ev)
		if err != nil {
			break
		}
	}
	require.ErrorIs(t, err, syscall.ENOSPC)
	require.ErrorContains(t, err, "fatal persistence error")
	require.ErrorContains(t, err, "disk full")
	require.ErrorContains(t, err, dataDir)
	require.ErrorContains(t, err, "restart jetstream")
}

// TestOpen_FreshDir creates a fresh segments dir and confirms Open
// initializes seg_0000000000.jss with the 256-byte reserved header
// and seeds nextSeq=1 in memory (seq 0 is a reserved "nothing yet"
// sentinel; the first-ever event is seq 1, design §R8).
func TestOpen_FreshDir(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{})

	require.Equal(t, uint64(1), w.NextSeq())
	require.Equal(t, uint64(0), w.ActiveIndex())

	path := filepath.Join(w.cfg.SegmentsDir, "seg_0000000000.jss")
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, int64(256), info.Size(), "fresh segment is exactly the reserved header")

	// seq/next must not be set yet — Open never writes pebble for
	// a fresh dir (defaults read as 0).
	_, _, err = w.cfg.Store.Get([]byte(seqNextKey))
	require.ErrorIs(t, err, pebble.ErrNotFound)
}

// TestOpen_FloorsPersistedZeroSeq pins the nextSeq-never-0 invariant
// against an illegal persisted seq/next=0 with no recovered events. No
// current build writes a 0 counter (a fresh dir seeds 1 in memory and
// the first flush/Close persists >= 1), so this state is only reachable
// via a pre-seed build or on-disk corruption. Open must floor nextSeq to
// 1 regardless, so the first event is seq 1 and seq 0 stays the pure
// "nothing yet" sentinel (design §R8) — otherwise the first event would
// be allocated seq 0 and the client dedup floor would silently drop it.
func TestOpen_FloorsPersistedZeroSeq(t *testing.T) {
	t.Parallel()
	segDir := filepath.Join(t.TempDir(), "segments")
	st := newTestStore(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := Config{
		SegmentsDir:       segDir,
		Store:             st,
		Logger:            logger,
		MaxEventsPerBlock: 64,
		MaxSegmentBytes:   1 << 30,
	}

	// Pre-seed an illegal persisted counter of 0 before any event exists.
	require.NoError(t, saveNextSeq(st, seqNextKey, 0))

	w, err := Open(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	require.Equal(t, uint64(1), w.NextSeq(),
		"Open must floor a persisted seq/next=0 (no events) up to 1")

	ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a"}
	require.NoError(t, w.Append(t.Context(), &ev))
	require.Equal(t, uint64(1), ev.Seq,
		"first event after flooring must be seq 1, not the seq-0 sentinel")
}

// TestOpen_SealedSegmentReconcilesPastZeroSeq is the sealed-segment
// companion to the floor test above. When the highest-index segment is
// already sealed (the orchestrator's cutover state) AND pebble carries an
// illegal seq/next=0, Open must read the sealed header's MaxSeq and
// reconcile nextSeq past it — not blindly floor to 1, which would hand the
// next append a seq the sealed segment already contains, corrupting replay
// ordering. The flooring test alone would pass even with this bug, since it
// has no on-disk events.
func TestOpen_SealedSegmentReconcilesPastZeroSeq(t *testing.T) {
	t.Parallel()
	segDir := filepath.Join(t.TempDir(), "segments")
	st := newTestStore(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := Config{
		SegmentsDir:       segDir,
		Store:             st,
		Logger:            logger,
		MaxEventsPerBlock: 64,
		MaxSegmentBytes:   1 << 30,
	}

	// Write a few events and seal the segment, mirroring the cutover state.
	w1, err := Open(cfg)
	require.NoError(t, err)
	const n = 5
	for range n {
		ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a"}
		require.NoError(t, w1.Append(t.Context(), &ev))
	}
	require.NoError(t, w1.SealActiveAndClose())

	path := filepath.Join(segDir, SegmentFilename(0))
	ins, err := segment.Inspect(path)
	require.NoError(t, err)
	require.True(t, ins.Sealed, "precondition: trailing segment must be sealed")
	require.Equal(t, uint64(n), ins.Header.MaxSeq)

	// Corrupt the persisted counter to the illegal 0 value.
	require.NoError(t, saveNextSeq(st, seqNextKey, 0))

	w2, err := Open(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w2.Close() })

	require.Equal(t, uint64(n+1), w2.NextSeq(),
		"Open must reconcile nextSeq past the sealed segment's MaxSeq, not floor to 1")

	ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:b"}
	require.NoError(t, w2.Append(t.Context(), &ev))
	require.Equal(t, uint64(n+1), ev.Seq,
		"first append after reopen must not reuse a sealed-segment seq")
}

// TestOpen_EmptyHighestSealedSegmentReconcilesPastLowerSegment pins the
// sealed-TAIL recovery floor. The highest segment can be sealed-but-empty
// (MaxSeq==0) while a LOWER segment holds real seqs — the orchestrator's
// bootstrap rolls forward to a fresh seg_<N+1> and SealActiveAndClose seals that
// empty file (bootstrap.go). Inspecting only the highest segment's header would
// see MaxSeq==0, find no envelope, and (with an illegal seq/next=0) floor
// nextSeq to 1 — reusing seqs the lower segment already owns. Open must walk the
// sealed tail back to the highest non-empty segment and floor past ITS MaxSeq.
func TestOpen_EmptyHighestSealedSegmentReconcilesPastLowerSegment(t *testing.T) {
	t.Parallel()
	segDir := filepath.Join(t.TempDir(), "segments")
	st := newTestStore(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := Config{
		SegmentsDir:       segDir,
		Store:             st,
		Logger:            logger,
		MaxEventsPerBlock: 64,
		MaxSegmentBytes:   1 << 30,
	}
	require.NoError(t, os.MkdirAll(segDir, 0o755))

	// seg0: a sealed segment holding seqs 1..n (the real envelope).
	const n = 5
	seg0Path := filepath.Join(segDir, SegmentFilename(0))
	sw0, err := segment.New(segment.Config{Path: seg0Path, MaxEventsPerBlock: 64})
	require.NoError(t, err)
	for i := uint64(1); i <= n; i++ {
		_, err := sw0.Append(segment.Event{Seq: i, Kind: segment.KindCreate, DID: "did:plc:a"})
		require.NoError(t, err)
	}
	require.NoError(t, sw0.Flush())
	_, err = sw0.Seal()
	require.NoError(t, err)

	// seg1: the HIGHEST segment, sealed but EMPTY (MaxSeq==0) — the rolled-forward
	// trailing segment the orchestrator seals at bootstrap.
	seg1Path := filepath.Join(segDir, SegmentFilename(1))
	sw1, err := segment.New(segment.Config{Path: seg1Path, MaxEventsPerBlock: 64})
	require.NoError(t, err)
	_, err = sw1.Seal()
	require.NoError(t, err)

	ins0, err := segment.Inspect(seg0Path)
	require.NoError(t, err)
	require.EqualValues(t, n, ins0.Header.MaxSeq, "precondition: seg0 holds the real envelope")
	ins1, err := segment.Inspect(seg1Path)
	require.NoError(t, err)
	require.True(t, ins1.Sealed, "precondition: highest segment sealed")
	require.EqualValues(t, 0, ins1.Header.MaxSeq, "precondition: highest segment is empty (MaxSeq==0)")

	// Illegal seq/next=0 forces the recovery floor to do real work.
	require.NoError(t, saveNextSeq(st, seqNextKey, 0))

	w, err := Open(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	require.Equal(t, uint64(n+1), w.NextSeq(),
		"Open must walk the sealed tail past an empty highest segment and floor past seg0's MaxSeq, not to 1")

	ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:b"}
	require.NoError(t, w.Append(t.Context(), &ev))
	require.Equal(t, uint64(n+1), ev.Seq,
		"first append after reopen must not reuse a seq the lower sealed segment owns")
}

// TestOpen_CompactedEmptySealedSegmentReconcilesPastZeroSeq pins the
// compacted-empty edge of the sealed-segment recovery floor. A fully-compacted
// sealed segment has EventCount==0 (every row dropped) but PRESERVES its
// historical MaxSeq envelope (segment.Rewrite restores MinSeq/MaxSeq even when
// all rows drop). Those seqs are still owned by the segment, so when it is the
// highest segment AND pebble carries an illegal seq/next=0, Open must reconcile
// nextSeq past the envelope's MaxSeq — gating on MaxSeq, not EventCount. Gating
// on EventCount (the obvious-but-wrong choice) would leave foundEvents=false and
// floor nextSeq to 1, reusing seqs the compacted segment already owns.
func TestOpen_CompactedEmptySealedSegmentReconcilesPastZeroSeq(t *testing.T) {
	t.Parallel()
	segDir := filepath.Join(t.TempDir(), "segments")
	st := newTestStore(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := Config{
		SegmentsDir:       segDir,
		Store:             st,
		Logger:            logger,
		MaxEventsPerBlock: 64,
		MaxSegmentBytes:   1 << 30,
	}

	// Write + seal a segment with n events, then compact every row out of it so
	// it becomes EventCount==0 / MaxSeq==n — the post-compaction envelope state.
	w1, err := Open(cfg)
	require.NoError(t, err)
	const n = 5
	for range n {
		ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a"}
		require.NoError(t, w1.Append(t.Context(), &ev))
	}
	require.NoError(t, w1.SealActiveAndClose())

	path := filepath.Join(segDir, SegmentFilename(0))
	res, err := segment.Rewrite(path, func(*segment.Event) segment.RowDecision { return segment.RowDrop }, segment.RewriteOptions{})
	require.NoError(t, err)
	require.EqualValues(t, n, res.RowsDropped, "precondition: all rows dropped")

	ins, err := segment.Inspect(path)
	require.NoError(t, err)
	require.True(t, ins.Sealed, "precondition: trailing segment must be sealed")
	require.EqualValues(t, 0, ins.Header.EventCount, "precondition: compacted-empty (EventCount==0)")
	require.EqualValues(t, n, ins.Header.MaxSeq, "precondition: historical MaxSeq envelope preserved")

	// Corrupt the persisted counter to the illegal 0 value.
	require.NoError(t, saveNextSeq(st, seqNextKey, 0))

	w2, err := Open(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w2.Close() })

	require.Equal(t, uint64(n+1), w2.NextSeq(),
		"Open must reconcile nextSeq past a COMPACTED-empty segment's MaxSeq envelope, not floor to 1")

	ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:b"}
	require.NoError(t, w2.Append(t.Context(), &ev))
	require.Equal(t, uint64(n+1), ev.Seq,
		"first append after reopen must not reuse a seq the compacted envelope owns")
}

// TestOpen_SealedSegmentRejectsCorruptHeader pins that the sealed-segment
// nextSeq recovery does NOT trust an unverified header. The recovery branch
// exists as a corruption safety net (illegal seq/next=0), so it must itself
// verify the segment's xxh3 checksum — which covers MaxSeq/EventCount —
// rather than flooring nextSeq off a header field read blindly. Here we
// corrupt MaxSeq in a sealed segment WITHOUT recomputing the checksum;
// Open must fail loud (ErrChecksumMismatch) instead of silently seeding
// nextSeq from the garbage value (which would gap or reuse seqs). Crash >
// corruption.
func TestOpen_SealedSegmentRejectsCorruptHeader(t *testing.T) {
	t.Parallel()
	segDir := filepath.Join(t.TempDir(), "segments")
	st := newTestStore(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := Config{
		SegmentsDir:       segDir,
		Store:             st,
		Logger:            logger,
		MaxEventsPerBlock: 64,
		MaxSegmentBytes:   1 << 30,
	}

	// Write + seal a segment, mirroring the cutover state.
	w1, err := Open(cfg)
	require.NoError(t, err)
	const n = 5
	for range n {
		ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a"}
		require.NoError(t, w1.Append(t.Context(), &ev))
	}
	require.NoError(t, w1.SealActiveAndClose())

	path := filepath.Join(segDir, SegmentFilename(0))
	ins, err := segment.Inspect(path)
	require.NoError(t, err)
	require.True(t, ins.Sealed, "precondition: trailing segment must be sealed")
	require.Equal(t, uint64(n), ins.Header.MaxSeq)

	// Corrupt MaxSeq (header bytes 34..42) to a large bogus value WITHOUT
	// fixing the checksum. ReadSealedHeader would happily return this; only
	// the checksum-verifying Reader catches it.
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	var bogus [8]byte
	binary.LittleEndian.PutUint64(bogus[:], 1<<40)
	_, err = f.WriteAt(bogus[:], 34)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// An illegal seq/next=0 forces the recovery branch to actually consult
	// the sealed header — otherwise the floor would never read MaxSeq.
	require.NoError(t, saveNextSeq(st, seqNextKey, 0))

	_, err = Open(cfg)
	require.Error(t, err, "Open must reject a sealed segment whose header fails checksum, not floor nextSeq off garbage")
	require.ErrorIs(t, err, segment.ErrChecksumMismatch,
		"the corruption must surface as a checksum mismatch, not a silent recovery")
}

// TestAppend_AllocatesMonotonicSeq pins the seq-allocation contract:
// N appends produce ev.Seq values in [1, N] in call order (seq 0 is a
// reserved "nothing yet" sentinel; the first-ever event is seq 1).
func TestAppend_AllocatesMonotonicSeq(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{MaxEventsPerBlock: 64})

	for i := range 10 {
		ev := segment.Event{
			WitnessedAt: 1,
			Kind:        segment.KindCreate,
			DID:         "did:plc:a",
		}
		require.NoError(t, w.Append(t.Context(), &ev))
		require.Equal(t, uint64(i+1), ev.Seq, "append %d", i)
	}
	require.Equal(t, uint64(11), w.NextSeq())
}

// TestAppend_RejectsClosed pins the closed-writer behavior.
func TestAppend_RejectsClosed(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{})
	require.NoError(t, w.Close())

	ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a"}
	err := w.Append(t.Context(), &ev)
	require.ErrorIs(t, err, ErrClosed)
}

// TestAppend_LeavesSeqUntouchedOnError pins the API contract that a
// failed Append leaves ev.Seq untouched, so callers retrying with
// the same struct don't observe a phantom allocation.
func TestAppend_LeavesSeqUntouchedOnError(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{})
	require.NoError(t, w.Close())

	ev := segment.Event{Seq: 0xDEAD, Kind: segment.KindCreate, DID: "did:plc:a"}
	err := w.Append(t.Context(), &ev)
	require.ErrorIs(t, err, ErrClosed)
	require.Equal(t, uint64(0xDEAD), ev.Seq,
		"failed Append must not mutate ev.Seq")
}

// TestClose_PersistsNextSeq pins the contract that Close commits
// the latest in-memory nextSeq to pebble, so a Close → crash →
// Reopen sequence does not regress nextSeq even when the last
// Append did not trigger a block flush.
func TestClose_PersistsNextSeq(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{MaxEventsPerBlock: 1024})

	for range 3 {
		ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a"}
		require.NoError(t, w.Append(t.Context(), &ev))
	}
	store := w.cfg.Store
	require.NoError(t, w.Close())

	got, err := loadNextSeq(store, seqNextKey)
	require.NoError(t, err)
	require.Equal(t, uint64(4), got, "Close must persist nextSeq")
}

// TestBlockFlush_AdvancesPebbleSeq confirms the durability ordering
// from docs/README.md §3.1.1: after a block flush, seq/next in pebble
// equals the in-memory nextSeq.
func TestBlockFlush_AdvancesPebbleSeq(t *testing.T) {
	t.Parallel()
	const blockSize = 4
	w := newTestWriter(t, Config{MaxEventsPerBlock: blockSize})

	for range blockSize {
		ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a"}
		require.NoError(t, w.Append(t.Context(), &ev))
	}

	val, closer, err := w.cfg.Store.Get([]byte(seqNextKey))
	require.NoError(t, err)
	defer func() { _ = closer.Close() }()
	require.Equal(t, uint64(blockSize+1), binary.LittleEndian.Uint64(val))
	require.Equal(t, uint64(blockSize+1), w.NextSeq())
}

// TestBlockFlush_SegmentBytesGrow confirms activeBytes advances after
// a block flush. The exact size depends on zstd compression of the
// fixture events, but it must be > 0.
func TestBlockFlush_SegmentBytesGrow(t *testing.T) {
	t.Parallel()
	const blockSize = 4
	w := newTestWriter(t, Config{MaxEventsPerBlock: blockSize})

	for range blockSize {
		ev := segment.Event{
			Kind:    segment.KindCreate,
			DID:     "did:plc:a",
			Payload: []byte("hello"),
		}
		require.NoError(t, w.Append(t.Context(), &ev))
	}
	w.mu.Lock()
	got := w.activeBytes
	w.mu.Unlock()
	require.Greater(t, got, int64(0), "activeBytes must grow after a block flush")
}

// TestRotation_ByteThreshold pins rotation behavior. Setting
// MaxSegmentBytes to a tiny value forces a rotation after the first
// block flush. The original seg_*0000.jss must be sealed (open via
// segment.Open) and seg_*0001.jss must be active.
func TestRotation_ByteThreshold(t *testing.T) {
	t.Parallel()
	const blockSize = 4
	w := newTestWriter(t, Config{
		MaxEventsPerBlock: blockSize,
		MaxSegmentBytes:   1,
	})

	for range blockSize {
		ev := segment.Event{
			Kind:    segment.KindCreate,
			DID:     "did:plc:a",
			Payload: []byte("hello"),
		}
		require.NoError(t, w.Append(t.Context(), &ev))
	}

	require.Equal(t, uint64(1), w.ActiveIndex())

	first := filepath.Join(w.cfg.SegmentsDir, "seg_0000000000.jss")
	r, err := segment.Open(segment.ReaderConfig{Path: first})
	require.NoError(t, err, "first segment must be sealed")
	require.NoError(t, r.Close())

	second := filepath.Join(w.cfg.SegmentsDir, "seg_0000000001.jss")
	info, err := os.Stat(second)
	require.NoError(t, err)
	require.Equal(t, int64(segment.ReservedHeaderBytes), info.Size(),
		"new active segment is exactly the reserved header")

	// metrics should record the rotation.
	require.InDelta(t, 1.0,
		testutil.ToFloat64(w.cfg.Metrics.SegmentsRotated), 0,
		"a rotation must increment the counter")
}

// TestResume_ExistingActive confirms a Close() then Open() picks up
// where the previous run left off. Seq numbers continue monotonically
// without duplication; both blocks read back via segment.Reader.
func TestResume_ExistingActive(t *testing.T) {
	t.Parallel()
	const blockSize = 4
	segDir := filepath.Join(t.TempDir(), "segments")
	st := newTestStore(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	cfg := Config{
		SegmentsDir:       segDir,
		Store:             st,
		Logger:            logger,
		MaxEventsPerBlock: blockSize,
		MaxSegmentBytes:   1 << 30, // do not rotate
	}

	// Run 1: append blockSize events, flush, close.
	w1, err := Open(cfg)
	require.NoError(t, err)
	for range blockSize {
		ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a"}
		require.NoError(t, w1.Append(t.Context(), &ev))
	}
	require.Equal(t, uint64(blockSize+1), w1.NextSeq())
	require.NoError(t, w1.Close())

	// Run 2: same dir, same store. Open must resume.
	w2, err := Open(cfg)
	require.NoError(t, err)
	require.Equal(t, uint64(blockSize+1), w2.NextSeq(),
		"resumed nextSeq must match the last block's high water mark")
	require.Equal(t, uint64(0), w2.ActiveIndex(),
		"still on segment 0; we have not rotated")

	// Append more; allocator picks up from the right spot.
	for i := range blockSize {
		ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:b"}
		require.NoError(t, w2.Append(t.Context(), &ev))
		require.Equal(t, uint64(blockSize+1+i), ev.Seq)
	}
	require.NoError(t, w2.Close())
}

// TestResume_SealedSkipsToNext confirms that if the highest segment
// is sealed at Open time, Open creates seg_<idx+1>.jss instead of
// trying to reopen the sealed file.
func TestResume_SealedSkipsToNext(t *testing.T) {
	t.Parallel()
	const blockSize = 4
	segDir := filepath.Join(t.TempDir(), "segments")
	st := newTestStore(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	cfg := Config{
		SegmentsDir:       segDir,
		Store:             st,
		Logger:            logger,
		MaxEventsPerBlock: blockSize,
		MaxSegmentBytes:   1, // force rotation after one block
	}

	w1, err := Open(cfg)
	require.NoError(t, err)
	for range blockSize {
		ev := segment.Event{
			Kind: segment.KindCreate, DID: "did:plc:a",
			Payload: []byte("hello"),
		}
		require.NoError(t, w1.Append(t.Context(), &ev))
	}
	require.Equal(t, uint64(1), w1.ActiveIndex(), "rotated to segment 1")
	require.NoError(t, w1.Close())

	// Manually seal segment 1 by writing one block then closing — the
	// in-memory writer will not auto-seal on close, so we need a
	// helper. Simulate the pre-conditions by sealing via the segment
	// package directly:
	seg1Path := filepath.Join(segDir, "seg_0000000001.jss")
	sw, err := segment.New(segment.Config{Path: seg1Path, MaxEventsPerBlock: blockSize})
	require.NoError(t, err)
	for range blockSize {
		_, err := sw.Append(segment.Event{Kind: segment.KindCreate, DID: "did:plc:b"})
		require.NoError(t, err)
	}
	require.NoError(t, sw.Flush())
	_, err = sw.Seal()
	require.NoError(t, err)

	w2, err := Open(cfg)
	require.NoError(t, err)
	require.Equal(t, uint64(2), w2.ActiveIndex(),
		"highest is sealed; Open opens idx+1")
	t.Cleanup(func() { _ = w2.Close() })

	seg2Path := filepath.Join(segDir, "seg_0000000002.jss")
	info, err := os.Stat(seg2Path)
	require.NoError(t, err)
	require.Equal(t, int64(segment.ReservedHeaderBytes), info.Size(),
		"new active segment is exactly the reserved header")
}

// TestOpen_ReconcilesDriftedPebble simulates the crash mode from
// docs/README.md §3.1.1: block fsynced, pebble batch lost. Open must read
// max(seq) from the segment, advance nextSeq to max+1, and rewrite
// pebble. Otherwise the next Append would reuse a seq.
func TestOpen_ReconcilesDriftedPebble(t *testing.T) {
	t.Parallel()
	const blockSize = 4
	segDir := filepath.Join(t.TempDir(), "segments")
	st := newTestStore(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := Config{
		SegmentsDir:       segDir,
		Store:             st,
		Logger:            logger,
		MaxEventsPerBlock: blockSize,
		MaxSegmentBytes:   1 << 30,
	}

	w1, err := Open(cfg)
	require.NoError(t, err)
	for range blockSize {
		ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a"}
		require.NoError(t, w1.Append(t.Context(), &ev))
	}
	require.NoError(t, w1.Close())

	// Simulate "pebble batch lost after segment fsync" by rewriting
	// seq/next backwards.
	require.NoError(t, saveNextSeq(st, seqNextKey, 1))

	w2, err := Open(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w2.Close() })
	require.Equal(t, uint64(blockSize+1), w2.NextSeq(),
		"reconcile: nextSeq must advance past the segment's max seq")

	got, err := loadNextSeq(st, seqNextKey)
	require.NoError(t, err)
	require.Equal(t, uint64(blockSize+1), got,
		"reconcile must persist the corrected value")
}

// TestOpen_RecoversFromTornTail simulates a crash with bytes past the
// last good frame. segment.New's resumeExistingSegment truncates the
// torn tail; ingest.Writer must then reconcile nextSeq cleanly.
func TestOpen_RecoversFromTornTail(t *testing.T) {
	t.Parallel()
	const blockSize = 2
	segDir := filepath.Join(t.TempDir(), "segments")
	st := newTestStore(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := Config{
		SegmentsDir:       segDir,
		Store:             st,
		Logger:            logger,
		MaxEventsPerBlock: blockSize,
		MaxSegmentBytes:   1 << 30,
	}

	w1, err := Open(cfg)
	require.NoError(t, err)
	for range blockSize {
		ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a"}
		require.NoError(t, w1.Append(t.Context(), &ev))
	}
	require.NoError(t, w1.Close())

	// Inject a torn-tail by appending raw bytes after the last good frame.
	path := filepath.Join(segDir, "seg_0000000000.jss")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], 1<<20) // promises 1MiB
	_, err = f.Write(lenBuf[:])
	require.NoError(t, err)
	_, err = f.Write([]byte{0xff, 0xff, 0xff, 0xff})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	w2, err := Open(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w2.Close() })

	require.Equal(t, uint64(blockSize+1), w2.NextSeq())

	info, err := os.Stat(path)
	require.NoError(t, err)
	w2.mu.Lock()
	require.Equal(t, info.Size()-int64(segment.ReservedHeaderBytes), w2.activeBytes,
		"activeBytes mirrors post-truncate size")
	w2.mu.Unlock()
}

// TestAppend_Concurrent confirms ingest.Writer is goroutine-safe and
// that concurrent appends produce a contiguous unique seq range.
// Run under -race to catch any locking gaps.
func TestAppend_Concurrent(t *testing.T) {
	t.Parallel()
	const goroutines = 16
	const perGoroutine = 64
	w := newTestWriter(t, Config{
		MaxEventsPerBlock: 4,
		MaxSegmentBytes:   1 << 30,
	})

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range perGoroutine {
				ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a"}
				require.NoError(t, w.Append(t.Context(), &ev))
			}
		}()
	}
	wg.Wait()

	require.Equal(t, uint64(goroutines*perGoroutine+1), w.NextSeq())
}

// TestOpen_HonorsCustomSeqKey verifies that two Writers with
// different SeqKey values maintain independent counters in the same
// pebble store. This is what enables the live_segments consumer to
// share a metadata db with the backfill writer without their seq
// counters colliding.
func TestOpen_HonorsCustomSeqKey(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	mkWriter := func(subdir, key string) *Writer {
		w, err := Open(Config{
			SegmentsDir: filepath.Join(t.TempDir(), subdir),
			Store:       st,
			SeqKey:      key,
			Logger:      logger,
			Metrics:     NewMetrics(prometheus.NewRegistry()),
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = w.Close() })
		return w
	}

	wA := mkWriter("a", "seq/next")
	wB := mkWriter("b", "live_segments/seq/next")

	for i := range 5 {
		ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "x.y", Rkey: "r", Rev: "1", Payload: []byte{0x01}}
		require.NoError(t, wA.Append(t.Context(), &ev))
		require.Equal(t, uint64(i+1), ev.Seq)
	}
	for i := range 3 {
		ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:b", Collection: "x.y", Rkey: "r", Rev: "1", Payload: []byte{0x02}}
		require.NoError(t, wB.Append(t.Context(), &ev))
		require.Equal(t, uint64(i+1), ev.Seq, "live writer's seq is independent of backfill writer's")
	}

	// Close the writers so their final nextSeq values are persisted
	// to pebble, then read both keys back to confirm the two seq
	// counters were durably stored under disjoint pebble keys.
	require.NoError(t, wA.Close())
	require.NoError(t, wB.Close())

	persistedA, err := loadNextSeq(st, "seq/next")
	require.NoError(t, err)
	require.Equal(t, uint64(6), persistedA)

	persistedB, err := loadNextSeq(st, "live_segments/seq/next")
	require.NoError(t, err)
	require.Equal(t, uint64(4), persistedB)
}

// TestOpen_DefaultSeqKey pins back-compat: zero-value SeqKey resolves
// to "seq/next", which is what every existing caller relies on.
func TestOpen_DefaultSeqKey(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{}) // SeqKey left zero
	require.Equal(t, "seq/next", w.cfg.SeqKey)
}

// TestFlush_InvokesDurableBatchHook pins the durability hook contract: after
// each block flush the writer calls OnDurableBatch exactly once, in the same
// synced Pebble batch as seq/next.
func TestFlush_InvokesDurableBatchHook(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	w, err := Open(Config{
		SegmentsDir:       filepath.Join(t.TempDir(), "segments"),
		Store:             newTestStore(t),
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:           NewMetrics(prometheus.NewRegistry()),
		MaxEventsPerBlock: 2,
		OnDurableBatch: func(_ context.Context, _ *pebble.Batch, _ uint64, force bool, _ any) (func(), func(error), error) {
			if force {
				return nil, nil, nil
			}
			calls.Add(1)
			return nil, nil, nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	// Two events fill the block, triggering one flush.
	for range 2 {
		ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "x.y", Rkey: "r", Rev: "1", Payload: []byte{0x01}}
		require.NoError(t, w.Append(t.Context(), &ev))
	}
	require.Equal(t, int32(1), calls.Load(), "exactly one flush hook fired")

	// Two more events trigger a second flush.
	for range 2 {
		ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "x.y", Rkey: "r", Rev: "1", Payload: []byte{0x02}}
		require.NoError(t, w.Append(t.Context(), &ev))
	}
	require.Equal(t, int32(2), calls.Load())
}

// TestAppendBatch_DurableCommitNotAbortedByCanceledContext pins the contract
// that a post-fsync durability commit must run to completion even when the
// caller's context is cancelled. The sync flush path threads the engine's
// (cancellable) run context all the way into the per-block durable commit; when
// the backfill MaxRepos limit trips and cancels that context, a concurrent
// worker that happens to fill a block must NOT have its already-fsynced block's
// metadata commit turned into a spurious "on_durable_batch: context canceled"
// error (which the handler escalates to a fatal run abort). This matches the
// async flush path, which commits durable batches under context.Background().
func TestAppendBatch_DurableCommitNotAbortedByCanceledContext(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	var hookCtxErr error
	var hookCalls int
	w, err := Open(Config{
		SegmentsDir:       filepath.Join(dir, "segments"),
		Store:             st,
		MaxEventsPerBlock: 1, // every append fills a block -> durable commit
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		OnDurableBatch: func(ctx context.Context, b *pebble.Batch, _ uint64, _ bool, _ any) (func(), func(error), error) {
			hookCalls++
			// Mirror the completion batcher's leading guard: a cancelled
			// context here would abort the post-fsync durable commit.
			if err := ctx.Err(); err != nil {
				hookCtxErr = err
				return nil, nil, err
			}
			return nil, nil, b.Set([]byte("durable/ok"), []byte("yes"), nil)
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the run was cancelled (e.g. MaxRepos tripped) before this flush

	err = w.AppendBatch(ctx, []segment.Event{
		{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Rev: "1"},
	})
	require.NoError(t, err, "a post-fsync durable commit must not be aborted by a cancelled context")
	require.NoError(t, hookCtxErr, "OnDurableBatch must not observe a cancelled context")
	require.Equal(t, 1, hookCalls)

	got, closer, err := st.Get([]byte("durable/ok"))
	require.NoError(t, err)
	require.Equal(t, "yes", string(got))
	require.NoError(t, closer.Close())

	persisted, err := loadNextSeq(st, w.cfg.SeqKey)
	require.NoError(t, err)
	require.Equal(t, uint64(2), persisted)
}

func TestFlush_StagesDurableBatchHookWithSeq(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	var hookSeq uint64
	w, err := Open(Config{
		SegmentsDir:       filepath.Join(dir, "segments"),
		Store:             st,
		MaxEventsPerBlock: 2,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		OnDurableBatch: func(ctx context.Context, b *pebble.Batch, nextSeq uint64, force bool, _ any) (func(), func(error), error) {
			if force {
				return nil, nil, nil
			}
			hookSeq = nextSeq
			return nil, nil, b.Set([]byte("hook/ran"), []byte("yes"), nil)
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	require.NoError(t, w.AppendBatch(t.Context(), []segment.Event{
		{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r1", Rev: "1"},
		{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r2", Rev: "1"},
	}))

	require.Equal(t, uint64(3), hookSeq)
	got, closer, err := st.Get([]byte("hook/ran"))
	require.NoError(t, err)
	require.Equal(t, "yes", string(got))
	require.NoError(t, closer.Close())
	persisted, err := loadNextSeq(st, w.cfg.SeqKey)
	require.NoError(t, err)
	require.Equal(t, uint64(3), persisted)
}

// TestFlush_OnDurableBatchErrorPropagates verifies that an error from
// the hook surfaces back through Append so the errgroup can tear
// the process down. AGENTS.md: crashing > silent corruption.
func TestFlush_OnDurableBatchErrorPropagates(t *testing.T) {
	t.Parallel()

	want := errors.New("hook boom")
	w, err := Open(Config{
		SegmentsDir:       filepath.Join(t.TempDir(), "segments"),
		Store:             newTestStore(t),
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:           NewMetrics(prometheus.NewRegistry()),
		MaxEventsPerBlock: 1,
		OnDurableBatch: func(_ context.Context, _ *pebble.Batch, _ uint64, _ bool, _ any) (func(), func(error), error) {
			return nil, nil, want
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "x.y", Rkey: "r", Rev: "1", Payload: []byte{0xab}}
	err = w.Append(t.Context(), &ev)
	require.ErrorIs(t, err, want)
}

// TestSealActiveAndClose_SealsAndCloses verifies the cutover-time
// teardown path: after SealActiveAndClose, the trailing segment
// file has a non-zero header checksum (sealed) and the writer
// rejects further appends. seq/next is persisted.
func TestSealActiveAndClose_SealsAndCloses(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{MaxEventsPerBlock: 2})

	for i := range 3 {
		ev := segment.Event{
			WitnessedAt: int64(i + 1),
			Kind:        segment.KindCreate,
			DID:         "did:plc:a",
		}
		require.NoError(t, w.Append(t.Context(), &ev))
	}

	require.NoError(t, w.SealActiveAndClose())

	// Subsequent appends fail with ErrClosed.
	err := w.Append(t.Context(), &segment.Event{
		WitnessedAt: 4, Kind: segment.KindCreate, DID: "did:plc:a",
	})
	require.ErrorIs(t, err, ErrClosed)

	path := filepath.Join(w.cfg.SegmentsDir, "seg_0000000000.jss")
	ins, err := segment.Inspect(path)
	require.NoError(t, err)
	require.True(t, ins.Sealed, "expected the trailing segment to be sealed")

	// nextSeq must be persisted to pebble at SeqKey. Reading it
	// directly (rather than reopening the Writer, which would mask a
	// bug via ScanMaxSeq reconciliation) is what locks in that
	// SealActiveAndClose actually called saveNextSeq.
	persisted, closer, err := w.cfg.Store.Get([]byte(seqNextKey))
	require.NoError(t, err)
	defer func() { _ = closer.Close() }()
	require.Equal(t, uint64(4), binary.LittleEndian.Uint64(persisted))
}

// TestSealActiveAndClose_Idempotent verifies the second call is a
// true no-op: returns nil and does not mutate the on-disk file.
// Without this stronger assertion, an implementation that re-sealed
// every call (e.g. forgot the closed flag) would still pass.
func TestSealActiveAndClose_Idempotent(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{})

	require.NoError(t, w.SealActiveAndClose())

	path := filepath.Join(w.cfg.SegmentsDir, "seg_0000000000.jss")
	before, err := os.ReadFile(path)
	require.NoError(t, err)

	require.NoError(t, w.SealActiveAndClose())

	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after, "second SealActiveAndClose must not modify the file")
}

// TestSealActiveAndClose_FreshDir seals an empty active segment.
// The seal path must handle a writer that never accepted any events.
func TestSealActiveAndClose_FreshDir(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{})

	require.NoError(t, w.SealActiveAndClose())

	path := filepath.Join(w.cfg.SegmentsDir, "seg_0000000000.jss")
	ins, err := segment.Inspect(path)
	require.NoError(t, err)
	require.True(t, ins.Sealed)
	require.Zero(t, ins.Header.EventCount, "sealed empty segment carries no events")
}

func TestSealActiveAndClose_OnAfterSealFiresOnce(t *testing.T) {
	t.Parallel()
	segDir := filepath.Join(t.TempDir(), "segments")
	st := newTestStore(t)

	var calls int
	var gotIdx uint64
	var gotPath string
	w, err := Open(Config{
		SegmentsDir:       segDir,
		Store:             st,
		MaxEventsPerBlock: 2,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:           NewMetrics(prometheus.NewRegistry()),
		OnAfterSeal: func(idx uint64, path string) error {
			calls++
			gotIdx = idx
			gotPath = path

			ins, err := segment.Inspect(path)
			require.NoError(t, err)
			require.True(t, ins.Sealed, "callback must observe a sealed segment")
			require.Equal(t, uint64(1), ins.TotalEvents)
			return nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	ev := segment.Event{
		WitnessedAt: 1, Kind: segment.KindCreate,
		DID: "did:plc:a", Payload: []byte{0xa0},
	}
	require.NoError(t, w.Append(t.Context(), &ev))
	require.NoError(t, w.SealActiveAndClose())
	require.NoError(t, w.SealActiveAndClose(), "second terminal seal is an idempotent no-op")

	require.Equal(t, 1, calls)
	require.Equal(t, uint64(0), gotIdx)
	require.Equal(t, filepath.Join(segDir, SegmentFilename(0)), gotPath)
}

func TestSealActiveAndClose_OnAfterSealErrorPropagatesAfterDurableSeal(t *testing.T) {
	t.Parallel()
	segDir := filepath.Join(t.TempDir(), "segments")
	st := newTestStore(t)

	wantErr := errors.New("publish failed")
	var calls int
	w, err := Open(Config{
		SegmentsDir:       segDir,
		Store:             st,
		MaxEventsPerBlock: 2,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:           NewMetrics(prometheus.NewRegistry()),
		OnAfterSeal: func(uint64, string) error {
			calls++
			return wantErr
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	for i := range 3 {
		ev := segment.Event{
			WitnessedAt: int64(i + 1),
			Kind:        segment.KindCreate,
			DID:         "did:plc:a",
		}
		require.NoError(t, w.Append(t.Context(), &ev))
	}

	err = w.SealActiveAndClose()
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, 1, calls)

	path := filepath.Join(segDir, SegmentFilename(0))
	ins, inspectErr := segment.Inspect(path)
	require.NoError(t, inspectErr)
	require.True(t, ins.Sealed, "hook failure happens after the segment is sealed")
	require.Equal(t, uint64(3), ins.TotalEvents)

	persisted, closer, getErr := st.Get([]byte(seqNextKey))
	require.NoError(t, getErr)
	defer func() { _ = closer.Close() }()
	require.Equal(t, uint64(4), binary.LittleEndian.Uint64(persisted),
		"hook failure happens after seq/next is persisted")
	require.ErrorIs(t, w.Append(t.Context(), &segment.Event{WitnessedAt: 4, Kind: segment.KindCreate, DID: "did:plc:a"}), ErrClosed)
}

// TestForceRotate_SealsAndOpensNext pins the compaction-time forced
// rotation: pending events are flushed and sealed into the current
// segment, a fresh active segment is opened at idx+1, and appends
// continue with monotonic seqs.
func TestForceRotate_SealsAndOpensNext(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{MaxEventsPerBlock: 64})

	for i := range 3 {
		ev := segment.Event{WitnessedAt: int64(i + 1), Kind: segment.KindCreate, DID: "did:plc:a"}
		require.NoError(t, w.Append(t.Context(), &ev))
	}

	require.NoError(t, w.ForceRotate(t.Context()))

	require.Equal(t, uint64(1), w.ActiveIndex())
	ins, err := segment.Inspect(filepath.Join(w.cfg.SegmentsDir, SegmentFilename(0)))
	require.NoError(t, err)
	require.True(t, ins.Sealed)
	require.Equal(t, uint64(3), ins.TotalEvents)

	// seq/next was persisted before the seal (same ordering as the
	// size-based rotation path).
	persisted, closer, err := w.cfg.Store.Get([]byte(seqNextKey))
	require.NoError(t, err)
	defer func() { _ = closer.Close() }()
	require.Equal(t, uint64(4), binary.LittleEndian.Uint64(persisted))

	// The writer remains usable on the next segment.
	ev := segment.Event{WitnessedAt: 4, Kind: segment.KindCreate, DID: "did:plc:b"}
	require.NoError(t, w.Append(t.Context(), &ev))
	require.Equal(t, uint64(4), ev.Seq)
}

// TestForceRotate_EmptyActiveIsNoOp: rotating an empty active segment
// would generate churn (empty sealed files, manifest publishes) with
// zero compliance benefit — e.g. every compaction interval while the
// upstream relay is down. It must do nothing.
func TestForceRotate_EmptyActiveIsNoOp(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{})

	require.NoError(t, w.ForceRotate(t.Context()))
	require.Equal(t, uint64(0), w.ActiveIndex())

	info, err := os.Stat(filepath.Join(w.cfg.SegmentsDir, SegmentFilename(0)))
	require.NoError(t, err)
	require.Equal(t, int64(segment.ReservedHeaderBytes), info.Size(),
		"empty active segment must be left untouched")
}

func TestDrainDurability_CommitsHookWithoutPendingEvents(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	var gotForce bool
	var gotNextSeq uint64
	afterDone := make(chan error, 1)
	w, err := Open(Config{
		SegmentsDir: filepath.Join(dir, "segments"),
		Store:       st,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		OnDurableBatch: func(_ context.Context, b *pebble.Batch, nextSeq uint64, force bool, _ any) (func(), func(error), error) {
			gotForce = force
			gotNextSeq = nextSeq
			return nil, func(err error) { afterDone <- err }, b.Set([]byte("metadata/only"), []byte("ok"), nil)
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	require.NoError(t, w.DrainDurability(t.Context()))
	require.True(t, gotForce)
	require.Equal(t, uint64(1), gotNextSeq)
	select {
	case err := <-afterDone:
		require.NoError(t, err)
	default:
		require.Fail(t, "afterDone did not run")
	}

	got, closer, err := st.Get([]byte("metadata/only"))
	require.NoError(t, err)
	require.Equal(t, "ok", string(got))
	require.NoError(t, closer.Close())
}

func TestDrainDurability_AsyncCommitsHookWithoutPendingEvents(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	type durableCall struct {
		nextSeq uint64
		force   bool
	}
	calls := make(chan durableCall, 4)
	w, err := Open(Config{
		SegmentsDir:       filepath.Join(dir, "segments"),
		Store:             st,
		AsyncFlushWorkers: 2,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		OnDurableBatch: func(_ context.Context, b *pebble.Batch, nextSeq uint64, force bool, _ any) (func(), func(error), error) {
			calls <- durableCall{nextSeq: nextSeq, force: force}
			return nil, nil, b.Set([]byte("metadata/async-only"), []byte("ok"), nil)
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	require.NoError(t, w.DrainDurability(t.Context()))

	select {
	case got := <-calls:
		require.True(t, got.force)
		require.Equal(t, uint64(1), got.nextSeq)
	default:
		require.Fail(t, "durable hook did not run")
	}
	require.Empty(t, calls)

	got, closer, err := st.Get([]byte("metadata/async-only"))
	require.NoError(t, err)
	require.Equal(t, "ok", string(got))
	require.NoError(t, closer.Close())
}

func TestDrainDurability_AsyncFlushesPendingEventsBeforeForcedHook(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	type durableCall struct {
		nextSeq uint64
		force   bool
	}
	calls := make(chan durableCall, 4)
	w, err := Open(Config{
		SegmentsDir:       filepath.Join(dir, "segments"),
		Store:             st,
		MaxEventsPerBlock: 64,
		AsyncFlushWorkers: 2,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		OnDurableBatch: func(_ context.Context, b *pebble.Batch, nextSeq uint64, force bool, _ any) (func(), func(error), error) {
			calls <- durableCall{nextSeq: nextSeq, force: force}
			key := fmt.Sprintf("metadata/async-pending/%t", force)
			return nil, nil, b.Set([]byte(key), []byte("ok"), nil)
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	ev := segment.Event{WitnessedAt: 1, Kind: segment.KindCreate, DID: "did:plc:a"}
	require.NoError(t, w.Append(t.Context(), &ev))

	require.NoError(t, w.DrainDurability(t.Context()))

	gotCalls := []durableCall{<-calls, <-calls}
	require.ElementsMatch(t, []durableCall{
		{nextSeq: 2, force: false},
		{nextSeq: 2, force: true},
	}, gotCalls)
	require.Empty(t, calls)

	for _, key := range []string{"metadata/async-pending/false", "metadata/async-pending/true"} {
		got, closer, err := st.Get([]byte(key))
		require.NoError(t, err)
		require.Equal(t, "ok", string(got))
		require.NoError(t, closer.Close())
	}
	persisted, err := loadNextSeq(st, seqNextKey)
	require.NoError(t, err)
	require.Equal(t, uint64(2), persisted)

	path := filepath.Join(w.cfg.SegmentsDir, SegmentFilename(0))
	var gotEvents []segment.Event
	require.NoError(t, segment.WalkActive(path, func(events []segment.Event) error {
		gotEvents = append(gotEvents, events...)
		return nil
	}))
	require.Len(t, gotEvents, 1)
	require.Equal(t, uint64(1), gotEvents[0].Seq)
}

func TestDurableBatchClose_RunsAfterPendingEvents(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	type durableCall struct {
		nextSeq uint64
		force   bool
	}
	calls := make(chan durableCall, 1)
	w, err := Open(Config{
		SegmentsDir:       filepath.Join(dir, "segments"),
		Store:             st,
		MaxEventsPerBlock: 64,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		OnDurableBatch: func(_ context.Context, b *pebble.Batch, nextSeq uint64, force bool, _ any) (func(), func(error), error) {
			calls <- durableCall{nextSeq: nextSeq, force: force}
			return nil, nil, b.Set([]byte("metadata/close"), []byte("ok"), nil)
		},
	})
	require.NoError(t, err)

	ev := segment.Event{WitnessedAt: 1, Kind: segment.KindCreate, DID: "did:plc:close"}
	require.NoError(t, w.Append(t.Context(), &ev))
	require.NoError(t, w.Close())

	require.Equal(t, durableCall{nextSeq: 2, force: true}, requireDurableCall(t, calls))
	got, closer, err := st.Get([]byte("metadata/close"))
	require.NoError(t, err)
	require.Equal(t, "ok", string(got))
	require.NoError(t, closer.Close())
	persisted, err := loadNextSeq(st, seqNextKey)
	require.NoError(t, err)
	require.Equal(t, uint64(2), persisted)
}

func TestSealActiveAndClose_RunsDurableBatchHookAfterPendingEvents(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	type durableCall struct {
		nextSeq uint64
		force   bool
	}
	calls := make(chan durableCall, 1)
	w, err := Open(Config{
		SegmentsDir:       filepath.Join(dir, "segments"),
		Store:             st,
		MaxEventsPerBlock: 64,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		OnDurableBatch: func(_ context.Context, b *pebble.Batch, nextSeq uint64, force bool, _ any) (func(), func(error), error) {
			calls <- durableCall{nextSeq: nextSeq, force: force}
			return nil, nil, b.Set([]byte("metadata/seal-close"), []byte("ok"), nil)
		},
	})
	require.NoError(t, err)

	ev := segment.Event{WitnessedAt: 1, Kind: segment.KindCreate, DID: "did:plc:seal-close"}
	require.NoError(t, w.Append(t.Context(), &ev))
	require.NoError(t, w.SealActiveAndClose())

	require.Equal(t, durableCall{nextSeq: 2, force: true}, requireDurableCall(t, calls))
	got, closer, err := st.Get([]byte("metadata/seal-close"))
	require.NoError(t, err)
	require.Equal(t, "ok", string(got))
	require.NoError(t, closer.Close())
	ins, err := segment.Inspect(filepath.Join(dir, "segments", SegmentFilename(0)))
	require.NoError(t, err)
	require.True(t, ins.Sealed)
	require.Equal(t, uint64(1), ins.TotalEvents)
}

func TestWriter_DurableBatchAsyncCloseRunsAfterPendingEvents(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	type durableCall struct {
		nextSeq uint64
		force   bool
	}
	calls := make(chan durableCall, 2)
	w, err := Open(Config{
		SegmentsDir:       filepath.Join(dir, "segments"),
		Store:             st,
		MaxEventsPerBlock: 64,
		AsyncFlushWorkers: 2,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		OnDurableBatch: func(_ context.Context, b *pebble.Batch, nextSeq uint64, force bool, _ any) (func(), func(error), error) {
			calls <- durableCall{nextSeq: nextSeq, force: force}
			key := fmt.Sprintf("metadata/async-close/%t", force)
			return nil, nil, b.Set([]byte(key), []byte("ok"), nil)
		},
	})
	require.NoError(t, err)

	ev := segment.Event{WitnessedAt: 1, Kind: segment.KindCreate, DID: "did:plc:async-close"}
	require.NoError(t, w.Append(t.Context(), &ev))
	require.NoError(t, w.Close())

	gotCalls := []durableCall{requireDurableCall(t, calls), requireDurableCall(t, calls)}
	require.ElementsMatch(t, []durableCall{
		{nextSeq: 2, force: false},
		{nextSeq: 2, force: true},
	}, gotCalls)
	for _, key := range []string{"metadata/async-close/false", "metadata/async-close/true"} {
		got, closer, err := st.Get([]byte(key))
		require.NoError(t, err)
		require.Equal(t, "ok", string(got))
		require.NoError(t, closer.Close())
	}
}

func TestWriter_AsyncSealActiveAndCloseRunsDurableBatchHookAfterPendingEvents(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	type durableCall struct {
		nextSeq uint64
		force   bool
	}
	calls := make(chan durableCall, 2)
	w, err := Open(Config{
		SegmentsDir:       filepath.Join(dir, "segments"),
		Store:             st,
		MaxEventsPerBlock: 64,
		AsyncFlushWorkers: 2,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		OnDurableBatch: func(_ context.Context, b *pebble.Batch, nextSeq uint64, force bool, _ any) (func(), func(error), error) {
			calls <- durableCall{nextSeq: nextSeq, force: force}
			key := fmt.Sprintf("metadata/async-seal-close/%t", force)
			return nil, nil, b.Set([]byte(key), []byte("ok"), nil)
		},
	})
	require.NoError(t, err)

	ev := segment.Event{WitnessedAt: 1, Kind: segment.KindCreate, DID: "did:plc:async-seal-close"}
	require.NoError(t, w.Append(t.Context(), &ev))
	require.NoError(t, w.SealActiveAndClose())

	gotCalls := []durableCall{requireDurableCall(t, calls), requireDurableCall(t, calls)}
	require.ElementsMatch(t, []durableCall{
		{nextSeq: 2, force: false},
		{nextSeq: 2, force: true},
	}, gotCalls)
	ins, err := segment.Inspect(filepath.Join(dir, "segments", SegmentFilename(0)))
	require.NoError(t, err)
	require.True(t, ins.Sealed)
}

func requireDurableCall[T any](t *testing.T, calls <-chan T) T {
	t.Helper()

	select {
	case got := <-calls:
		return got
	case <-time.After(time.Second):
		require.Fail(t, "durable hook did not run")
		var zero T
		return zero
	}
}

// TestForceRotate_FlushedButUnsealedRotates: events already flushed to
// disk (no pending block) must still rotate — emptiness is about the
// segment having zero events, not zero buffered events.
func TestForceRotate_FlushedButUnsealedRotates(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{MaxEventsPerBlock: 64})

	ev := segment.Event{WitnessedAt: 1, Kind: segment.KindCreate, DID: "did:plc:a"}
	require.NoError(t, w.Append(t.Context(), &ev))
	require.NoError(t, w.Flush(t.Context()))

	require.NoError(t, w.ForceRotate(t.Context()))
	require.Equal(t, uint64(1), w.ActiveIndex())
}

// TestForceRotate_RejectsClosed pins the closed-writer behavior.
func TestForceRotate_RejectsClosed(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{})
	require.NoError(t, w.Close())
	require.ErrorIs(t, w.ForceRotate(t.Context()), ErrClosed)
}

// TestForceRotate_FiresOnAfterSeal: the manifest publish hook must see
// a forced rotation exactly like a size-based one.
func TestForceRotate_FiresOnAfterSeal(t *testing.T) {
	t.Parallel()
	segDir := filepath.Join(t.TempDir(), "segments")

	var calls int
	var gotIdx uint64
	w, err := Open(Config{
		SegmentsDir:       segDir,
		Store:             newTestStore(t),
		MaxEventsPerBlock: 64,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:           NewMetrics(prometheus.NewRegistry()),
		OnAfterSeal: func(idx uint64, path string) error {
			calls++
			gotIdx = idx
			return nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	ev := segment.Event{WitnessedAt: 1, Kind: segment.KindCreate, DID: "did:plc:a"}
	require.NoError(t, w.Append(t.Context(), &ev))
	require.NoError(t, w.ForceRotate(t.Context()))
	require.Equal(t, 1, calls)
	require.Equal(t, uint64(0), gotIdx)
}

// TestForceRotate_ConcurrentWithAppends: forced rotation must compose
// with concurrent appenders — no lost events, no duplicate seqs, and
// every event lands in exactly one segment.
func TestForceRotate_ConcurrentWithAppends(t *testing.T) {
	t.Parallel()
	const goroutines = 8
	const perGoroutine = 64
	w := newTestWriter(t, Config{
		MaxEventsPerBlock: 4,
		MaxSegmentBytes:   1 << 30,
	})

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range perGoroutine {
				ev := segment.Event{WitnessedAt: 1, Kind: segment.KindCreate, DID: "did:plc:a"}
				require.NoError(t, w.Append(t.Context(), &ev))
			}
		}()
	}
	for range 4 {
		require.NoError(t, w.ForceRotate(t.Context()))
	}
	wg.Wait()
	require.NoError(t, w.Close())

	require.Equal(t, uint64(goroutines*perGoroutine+1), w.NextSeq())

	// Count events across all segments (sealed + trailing active).
	files, err := SegmentFiles(w.cfg.SegmentsDir)
	require.NoError(t, err)
	seen := make(map[uint64]bool)
	note := func(events []segment.Event) {
		for i := range events {
			require.False(t, seen[events[i].Seq], "duplicate seq %d", events[i].Seq)
			seen[events[i].Seq] = true
		}
	}
	for _, f := range files {
		r, err := segment.Open(segment.ReaderConfig{Path: f.Path})
		if err == nil {
			for i := range int(r.Header().BlockCount) {
				events, err := r.DecodeBlock(i)
				require.NoError(t, err)
				note(events)
			}
			require.NoError(t, r.Close())
			continue
		}
		require.ErrorIs(t, err, segment.ErrActiveSegment)
		require.NoError(t, segment.WalkActive(f.Path, func(events []segment.Event) error {
			note(events)
			return nil
		}))
	}
	require.Len(t, seen, goroutines*perGoroutine)
}

func TestSegmentFiles_Empty(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	got, err := SegmentFiles(dir)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestSegmentFiles_SortedAscending(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// Create out-of-order to confirm the helper sorts.
	for _, idx := range []uint64{2, 0, 5, 1} {
		path := filepath.Join(dir, SegmentFilename(idx))
		require.NoError(t, os.WriteFile(path, []byte("placeholder"), 0o644))
	}

	got, err := SegmentFiles(dir)
	require.NoError(t, err)
	require.Len(t, got, 4)
	require.Equal(t, []uint64{0, 1, 2, 5}, []uint64{got[0].Idx, got[1].Idx, got[2].Idx, got[3].Idx})
	require.Equal(t, filepath.Join(dir, SegmentFilename(0)), got[0].Path)
	require.Equal(t, filepath.Join(dir, SegmentFilename(5)), got[3].Path)
}

func TestSegmentFiles_IgnoresNonSegmentFiles(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, SegmentFilename(3)), []byte("x"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "README.md"), []byte("x"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "subdir"), 0o755))

	got, err := SegmentFiles(dir)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, uint64(3), got[0].Idx)
}

func TestWriter_OnAfterSeal_FiresOnRotation(t *testing.T) {
	t.Parallel()
	segDir := filepath.Join(t.TempDir(), "segments")
	st := newTestStore(t)

	type sealedEvent struct {
		idx  uint64
		path string
	}
	var got []sealedEvent
	var gotMu sync.Mutex

	w, err := Open(Config{
		SegmentsDir:       segDir,
		Store:             st,
		MaxSegmentBytes:   1, // rotate on every flush
		MaxEventsPerBlock: 1,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:           NewMetrics(prometheus.NewRegistry()),
		OnAfterSeal: func(idx uint64, path string) error {
			gotMu.Lock()
			defer gotMu.Unlock()
			got = append(got, sealedEvent{idx: idx, path: path})
			return nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	// MaxEventsPerBlock=1 + MaxSegmentBytes=1 means each Append fills
	// the block, flushes, rotates, and seals. Three appends → three
	// seals at idx 0, 1, 2.
	for i := 1; i <= 3; i++ {
		ev := segment.Event{
			WitnessedAt: int64(i), Kind: segment.KindCreate,
			DID: "did:plc:a", Payload: []byte{0xa0},
		}
		require.NoError(t, w.Append(t.Context(), &ev))
	}

	gotMu.Lock()
	defer gotMu.Unlock()
	require.GreaterOrEqual(t, len(got), 2, "expected at least two seal callbacks across three rotations")
	for i, ev := range got {
		require.Equal(t, uint64(i), ev.idx, "callback %d: idx mismatch", i)
		require.Contains(t, ev.path, fmt.Sprintf("seg_%010d.jss", i),
			"callback %d: path should reference its sealed file", i)
	}
}

func TestWriter_OnAfterSeal_ErrorPropagates(t *testing.T) {
	t.Parallel()
	segDir := filepath.Join(t.TempDir(), "segments")
	st := newTestStore(t)

	wantErr := errors.New("boom")
	w, err := Open(Config{
		SegmentsDir:       segDir,
		Store:             st,
		MaxSegmentBytes:   1,
		MaxEventsPerBlock: 1,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:           NewMetrics(prometheus.NewRegistry()),
		OnAfterSeal:       func(idx uint64, path string) error { return wantErr },
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	// First append fills the block + triggers a rotation; the seal hook
	// returns wantErr, which Append must surface.
	ev := segment.Event{
		WitnessedAt: 1, Kind: segment.KindCreate,
		DID: "did:plc:a", Payload: []byte{0xa0},
	}
	err = w.Append(t.Context(), &ev)
	require.ErrorIs(t, err, wantErr)

	// Subsequent Appends must also fail: the hook error left the writer
	// with no usable active segment (Seal already closed the file).
	// We don't pin the exact error class — what matters is that a
	// caller can't accidentally write into limbo state.
	ev2 := segment.Event{
		WitnessedAt: 2, Kind: segment.KindCreate,
		DID: "did:plc:a", Payload: []byte{0xa0},
	}
	require.Error(t, w.Append(t.Context(), &ev2),
		"writer must remain unusable after a failed OnAfterSeal")
}

// TestAppend_OnAppendFiresBeforeSealVisibility pins the ordering the
// compaction tombstone set depends on: by the time a seal is visible
// (OnAfterSeal fires, sealed header on disk), OnAppend has already run
// for every event in the segment — including the one whose Append
// triggered the rotation. Without this, a concurrent compaction pass
// could compute a watermark covering an unobserved tombstone and
// permanently skip it.
func TestAppend_OnAppendFiresBeforeSealVisibility(t *testing.T) {
	t.Parallel()
	segDir := filepath.Join(t.TempDir(), "segments")

	var observed []uint64
	var observedAtSeal []uint64
	w, err := Open(Config{
		SegmentsDir:       segDir,
		Store:             newTestStore(t),
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:           NewMetrics(prometheus.NewRegistry()),
		MaxEventsPerBlock: 2,
		MaxSegmentBytes:   1, // first flush rotates
		OnAppend: func(ev *segment.Event) error {
			observed = append(observed, ev.Seq)
			return nil
		},
		OnAfterSeal: func(idx uint64, path string) error {
			observedAtSeal = append([]uint64(nil), observed...)
			return nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	for range 2 {
		ev := segment.Event{Kind: segment.KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "r"}
		require.NoError(t, w.Append(t.Context(), &ev))
	}

	require.Equal(t, uint64(1), w.ActiveIndex(), "the second append must have sealed segment 0")
	require.Equal(t, []uint64{1, 2}, observed)
	require.Equal(t, []uint64{1, 2}, observedAtSeal,
		"every event of the sealed segment must be observed before the seal is visible")
}

func TestAppend_OnAppendErrorFailsAppend(t *testing.T) {
	t.Parallel()
	segDir := filepath.Join(t.TempDir(), "segments")
	hookErr := errors.New("observe failed")
	w, err := Open(Config{
		SegmentsDir: segDir,
		Store:       newTestStore(t),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:     NewMetrics(prometheus.NewRegistry()),
		OnAppend:    func(*segment.Event) error { return hookErr },
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a"}
	err = w.Append(t.Context(), &ev)
	require.ErrorIs(t, err, hookErr)
}

func TestAppendBatch_AllocatesMonotonicSeq(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{MaxEventsPerBlock: 64})

	events := make([]segment.Event, 10)
	for i := range events {
		events[i] = segment.Event{
			WitnessedAt: int64(i + 1),
			Kind:        segment.KindCreate,
			DID:         "did:plc:a",
		}
	}

	require.NoError(t, w.AppendBatch(t.Context(), events))

	for i := range events {
		require.Equal(t, uint64(i+1), events[i].Seq, "event %d", i)
	}
	require.Equal(t, uint64(len(events)+1), w.NextSeq())
}

func TestAppendBatch_AsyncFlushPersistsBlocksAndSeqBeforeReturn(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{
		MaxEventsPerBlock: 2,
		AsyncFlushWorkers: 2,
	})

	events := []segment.Event{
		{WitnessedAt: 1, Kind: segment.KindCreate, DID: "did:plc:a"},
		{WitnessedAt: 2, Kind: segment.KindCreate, DID: "did:plc:b"},
		{WitnessedAt: 3, Kind: segment.KindCreate, DID: "did:plc:c"},
		{WitnessedAt: 4, Kind: segment.KindCreate, DID: "did:plc:d"},
	}
	require.NoError(t, w.AppendBatch(t.Context(), events))

	persisted, err := loadNextSeq(w.cfg.Store, w.cfg.SeqKey)
	require.NoError(t, err)
	require.Equal(t, uint64(5), persisted)

	require.NoError(t, w.Close())
	path := filepath.Join(w.cfg.SegmentsDir, SegmentFilename(0))
	var blocks [][]segment.Event
	require.NoError(t, segment.WalkActive(path, func(events []segment.Event) error {
		blocks = append(blocks, append([]segment.Event(nil), events...))
		return nil
	}))
	require.Len(t, blocks, 2)
	require.Equal(t, uint64(1), blocks[0][0].Seq)
	require.Equal(t, uint64(2), blocks[0][1].Seq)
	require.Equal(t, uint64(3), blocks[1][0].Seq)
	require.Equal(t, uint64(4), blocks[1][1].Seq)
}

func TestAppendBatch_AsyncFlushRunsDurableBatchHook(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	afterCommit := make(chan error, 1)
	w, err := Open(Config{
		SegmentsDir:       filepath.Join(dir, "segments"),
		Store:             st,
		MaxEventsPerBlock: 2,
		AsyncFlushWorkers: 2,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		OnDurableBatch: func(_ context.Context, b *pebble.Batch, nextSeq uint64, force bool, _ any) (func(), func(error), error) {
			if force {
				return nil, nil, fmt.Errorf("force = true")
			}
			if nextSeq != 3 {
				return nil, nil, fmt.Errorf("nextSeq = %d, want 3", nextSeq)
			}
			if err := b.Set([]byte("async/hook"), []byte("ok"), nil); err != nil {
				return nil, nil, fmt.Errorf("stage async/hook: %w", err)
			}
			return func() {
				got, closer, err := st.Get([]byte("async/hook"))
				if err != nil {
					afterCommit <- err
					return
				}
				if string(got) != "ok" {
					afterCommit <- fmt.Errorf("async/hook = %q, want ok", got)
					_ = closer.Close()
					return
				}
				if err := closer.Close(); err != nil {
					afterCommit <- err
					return
				}
				afterCommit <- nil
			}, nil, nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	require.NoError(t, w.AppendBatch(t.Context(), []segment.Event{
		{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r1", Rev: "1"},
		{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r2", Rev: "1"},
	}))
	select {
	case err := <-afterCommit:
		require.NoError(t, err)
	default:
		require.Fail(t, "afterCommit did not run")
	}
}

// TestAppendBatch_AsyncFlushRotatesWhenFullIncludingPendingRows pins the
// corrected async rotation contract: once the active segment crosses
// MaxSegmentBytes, AppendBatch rotates before returning, durably flushing
// any trailing sub-block remainder into the sealed segment first. The
// rotation is size-driven and does not wait for an explicit Flush or a
// downstream DrainDurability checkpoint — that checkpoint-only behavior was
// the bug that let segments grow without bound under sustained backfill.
func TestAppendBatch_AsyncFlushRotatesWhenFullIncludingPendingRows(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{
		MaxEventsPerBlock: 2,
		MaxSegmentBytes:   1,
		AsyncFlushWorkers: 2,
	})

	events := []segment.Event{
		{WitnessedAt: 1, Kind: segment.KindCreate, DID: "did:plc:a", Payload: []byte("one")},
		{WitnessedAt: 2, Kind: segment.KindCreate, DID: "did:plc:b", Payload: []byte("two")},
		{WitnessedAt: 3, Kind: segment.KindCreate, DID: "did:plc:c", Payload: []byte("three")},
	}
	require.NoError(t, w.AppendBatch(t.Context(), events))

	require.Equal(t, uint64(1), w.ActiveIndex(),
		"oversized async segment must rotate at end of AppendBatch, flushing the trailing partial block")
	persisted, err := loadNextSeq(w.cfg.Store, w.cfg.SeqKey)
	require.NoError(t, err)
	require.Equal(t, uint64(4), persisted,
		"seq/next must cover every appended event (incl. the flushed remainder) before the seal")

	// The sealed segment must contain ALL three events: the full async
	// block (2 events) plus the trailing remainder block (1 event).
	r, err := segment.Open(segment.ReaderConfig{Path: filepath.Join(w.cfg.SegmentsDir, SegmentFilename(0))})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })
	require.Len(t, r.Blocks(), 2)
	require.Equal(t, uint64(3), uint64(r.Header().EventCount),
		"no acknowledged event may be dropped across the rotation")
}

func TestAppendBatch_AsyncFlushConcurrentBatchesRemainContiguous(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{
		MaxEventsPerBlock: 3,
		AsyncFlushWorkers: 4,
	})

	const goroutines = 8
	const perBatch = 10
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			events := make([]segment.Event, perBatch)
			for i := range events {
				events[i] = segment.Event{
					WitnessedAt: int64(g*perBatch + i + 1),
					Kind:        segment.KindCreate,
					DID:         fmt.Sprintf("did:plc:test%02d%02d", g, i),
					Payload:     []byte{byte(g), byte(i)},
				}
			}
			errs <- w.AppendBatch(t.Context(), events)
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())

	path := filepath.Join(w.cfg.SegmentsDir, SegmentFilename(0))
	var seqs []uint64
	require.NoError(t, segment.WalkActive(path, func(events []segment.Event) error {
		for _, ev := range events {
			seqs = append(seqs, ev.Seq)
		}
		return nil
	}))
	require.Len(t, seqs, goroutines*perBatch)
	slices.Sort(seqs)
	for i, seq := range seqs {
		require.Equal(t, uint64(i+1), seq)
	}
}

// TestAsyncFlush_ConcurrentProducersRotateWithoutSeqCorruption stresses the
// rotation fix under the production shape: many concurrent AppendBatch
// producers against a small byte threshold, so rotateIfFull fires frequently
// and concurrently. Each producer calls rotateIfFull after releasing drainMu,
// so this exercises (a) rotateIfFull serializing on drainMu, (b) the
// under-lock activeBytes re-check making redundant rotations no-ops, and
// (c) seq integrity when a producer drains a pipeline holding other
// producers' in-flight blocks. Asserts: many rotations fired, all segments
// bounded, and every seq present exactly once with no gaps or duplicates.
func TestAsyncFlush_ConcurrentProducersRotateWithoutSeqCorruption(t *testing.T) {
	t.Parallel()

	const (
		maxSegmentBytes = 32 * 1024
		goroutines      = 16
		batchesEach     = 12
		perBatch        = 10 // not a multiple of MaxEventsPerBlock
		payloadBytes    = 256
	)
	w := newTestWriter(t, Config{
		MaxEventsPerBlock: 8,
		MaxSegmentBytes:   maxSegmentBytes,
		AsyncFlushWorkers: 4,
	})

	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			// Per-goroutine PRNG seed keeps payloads incompressible and the
			// run deterministic without shared state.
			rng := mathrand.New(mathrand.NewPCG(uint64(g)+1, 0xc0ffee))
			for b := range batchesEach {
				events := make([]segment.Event, perBatch)
				for i := range events {
					events[i] = segment.Event{
						WitnessedAt: int64(b*perBatch + i + 1),
						Kind:        segment.KindCreate,
						DID:         fmt.Sprintf("did:plc:g%02db%02di%02d", g, b, i),
						Payload:     incompressiblePayload(rng, payloadBytes),
					}
				}
				if err := w.AppendBatch(t.Context(), events); err != nil {
					errs <- err
					return
				}
			}
			errs <- nil
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	require.Greater(t, testutil.ToFloat64(w.cfg.Metrics.SegmentsRotated), 2.0,
		"concurrent oversized load must rotate repeatedly")

	require.NoError(t, w.DrainDurability(t.Context()))
	require.NoError(t, w.Close())

	require.Less(t, largestSegmentBytes(t, w.cfg.SegmentsDir), int64(4*maxSegmentBytes),
		"no segment may grow far past MaxSegmentBytes under concurrent load")

	total := goroutines * batchesEach * perBatch
	seqs := collectAllSeqs(t, w.cfg.SegmentsDir)
	require.Len(t, seqs, total, "every concurrently-appended event survives rotation exactly once")
	slices.Sort(seqs)
	for i := range seqs {
		require.Equal(t, uint64(i+1), seqs[i],
			"seqs contiguous across concurrent rotations (no gaps, no duplicates)")
	}
}

func TestWriter_AsyncCloseFlushesPendingBlockAndPersistsNextSeq(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{
		MaxEventsPerBlock: 8,
		AsyncFlushWorkers: 4,
	})

	events := []segment.Event{
		{WitnessedAt: 1, Kind: segment.KindCreate, DID: "did:plc:a"},
		{WitnessedAt: 2, Kind: segment.KindCreate, DID: "did:plc:b"},
		{WitnessedAt: 3, Kind: segment.KindCreate, DID: "did:plc:c"},
	}
	require.NoError(t, w.AppendBatch(t.Context(), events))
	require.NoError(t, w.Close())

	persisted, err := loadNextSeq(w.cfg.Store, w.cfg.SeqKey)
	require.NoError(t, err)
	require.Equal(t, uint64(4), persisted)

	path := filepath.Join(w.cfg.SegmentsDir, SegmentFilename(0))
	var got []segment.Event
	require.NoError(t, segment.WalkActive(path, func(events []segment.Event) error {
		got = append(got, events...)
		return nil
	}))
	require.Len(t, got, 3)
	for i := range got {
		require.Equal(t, uint64(i+1), got[i].Seq)
	}
}

func TestWriter_AsyncSealActiveAndCloseSealsPendingBlock(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{
		MaxEventsPerBlock: 8,
		AsyncFlushWorkers: 4,
	})

	events := []segment.Event{
		{WitnessedAt: 1, Kind: segment.KindCreate, DID: "did:plc:a"},
		{WitnessedAt: 2, Kind: segment.KindCreate, DID: "did:plc:b"},
	}
	require.NoError(t, w.AppendBatch(t.Context(), events))
	require.NoError(t, w.SealActiveAndClose())

	persisted, err := loadNextSeq(w.cfg.Store, w.cfg.SeqKey)
	require.NoError(t, err)
	require.Equal(t, uint64(3), persisted)

	path := filepath.Join(w.cfg.SegmentsDir, SegmentFilename(0))
	r, err := segment.Open(segment.ReaderConfig{Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })
	require.Len(t, r.Blocks(), 1)
	require.EqualValues(t, 2, r.Blocks()[0].EventCount)
}

func TestAppendBatch_LeavesSeqUntouchedOnClosedWriter(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{})
	require.NoError(t, w.Close())

	events := []segment.Event{
		{Seq: 0xA, Kind: segment.KindCreate, DID: "did:plc:a"},
		{Seq: 0xB, Kind: segment.KindCreate, DID: "did:plc:b"},
	}

	err := w.AppendBatch(t.Context(), events)
	require.ErrorIs(t, err, ErrClosed)
	require.Equal(t, uint64(0xA), events[0].Seq)
	require.Equal(t, uint64(0xB), events[1].Seq)
}

func TestAppendBatch_OnAppendFiresBeforeSealVisibility(t *testing.T) {
	t.Parallel()
	segDir := filepath.Join(t.TempDir(), "segments")

	var observed []uint64
	var observedAtSeal []uint64
	w, err := Open(Config{
		SegmentsDir:       segDir,
		Store:             newTestStore(t),
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:           NewMetrics(prometheus.NewRegistry()),
		MaxEventsPerBlock: 2,
		MaxSegmentBytes:   1,
		OnAppend: func(ev *segment.Event) error {
			observed = append(observed, ev.Seq)
			return nil
		},
		OnAfterSeal: func(idx uint64, path string) error {
			observedAtSeal = append([]uint64(nil), observed...)
			return nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	events := []segment.Event{
		{Kind: segment.KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "r"},
		{Kind: segment.KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "s"},
	}
	require.NoError(t, w.AppendBatch(t.Context(), events))

	require.Equal(t, uint64(1), w.ActiveIndex(), "batch append must seal segment 0")
	require.Equal(t, []uint64{1, 2}, observed)
	require.Equal(t, []uint64{1, 2}, observedAtSeal,
		"every event of the sealed segment must be observed before the seal is visible")
}

// collectAllSeqs reads every event seq from every segment file under dir,
// in segment-index then in-segment order. Sealed segments are read via the
// Reader's block index; the (single) trailing active segment is walked with
// WalkActive. This is the ground-truth view used to assert no seq is
// duplicated or skipped across rotations.
func collectAllSeqs(t *testing.T, dir string) []uint64 {
	t.Helper()
	files, err := SegmentFiles(dir)
	require.NoError(t, err)

	var seqs []uint64
	for _, f := range files {
		r, err := segment.Open(segment.ReaderConfig{Path: f.Path})
		switch {
		case err == nil:
			for i := range r.Blocks() {
				evs, derr := r.DecodeBlock(i)
				require.NoError(t, derr)
				for j := range evs {
					seqs = append(seqs, evs[j].Seq)
				}
			}
			require.NoError(t, r.Close())
		case errors.Is(err, segment.ErrActiveSegment):
			require.NoError(t, segment.WalkActive(f.Path, func(evs []segment.Event) error {
				for j := range evs {
					seqs = append(seqs, evs[j].Seq)
				}
				return nil
			}))
		default:
			require.NoError(t, err, "open segment %s", f.Path)
		}
	}
	return seqs
}

// incompressiblePayload returns n deterministic, high-entropy bytes drawn
// from a fixed-seed PRNG, so block-level zstd cannot collapse them. This
// makes the on-disk segment size (and therefore the byte-keyed rotation
// threshold) the binding constraint in rotation tests. Determinism (fixed
// seed) keeps the tests reproducible without Date.now/global rand.
func incompressiblePayload(rng *mathrand.Rand, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rng.UintN(256))
	}
	return b
}

// largestSegmentBytes returns the size of the largest seg_*.jss file under
// dir, minus the reserved header (matching the activeBytes accounting the
// rotation threshold is compared against).
func largestSegmentBytes(t *testing.T, dir string) int64 {
	t.Helper()
	files, err := SegmentFiles(dir)
	require.NoError(t, err)
	var maxBytes int64
	for _, f := range files {
		info, serr := os.Stat(f.Path)
		require.NoError(t, serr)
		if b := info.Size() - int64(segment.ReservedHeaderBytes); b > maxBytes {
			maxBytes = b
		}
	}
	return maxBytes
}

// TestAsyncFlush_RotatesUnderSustainedLoad is the headline regression test
// for the segment-rotation starvation bug. Under sustained AppendBatch load
// against an async writer, the old code only rotated when the flush pipeline
// happened to be momentarily quiescent AT a commit (asyncPrepared<=1 &&
// Pending()==0), so segments grew far past MaxSegmentBytes — unbounded under
// steady load. This test drives many batches with MaxSegmentBytes set small
// and AsyncFlushWorkers>1, and asserts that (a) many rotations actually fire
// and (b) no sealed segment exceeds the threshold by more than a tight
// bounded overshoot. It fails hard on the pre-fix code (≈1 rotation, one
// giant segment).
func TestAsyncFlush_RotatesUnderSustainedLoad(t *testing.T) {
	t.Parallel()

	const (
		blockSize = 8
		// Each event carries a ~1KiB payload; with block-level zstd on
		// incompressible random-ish bytes the on-disk segment grows
		// predictably so the byte threshold is the binding constraint.
		payloadBytes    = 1024
		maxSegmentBytes = 64 * 1024 // 64 KiB target
		batches         = 40
		// perBatch is a multiple of blockSize, but the active block is primed
		// with one event below (see prime), so every AppendBatch leaves
		// exactly one event Pending. A real pipeline is almost never
		// perfectly quiescent, and real backfill repos have arbitrary record
		// counts that never align to the block boundary — so Pending()>0
		// holds at commit time. That is precisely the shape the pre-fix
		// inline guard (`|| w.active.Pending() > 0`) refuses to rotate on, so
		// the old code starves here and never rotates regardless of size.
		perBatch = 4 * blockSize
	)
	reg := prometheus.NewRegistry()
	w := newTestWriter(t, Config{
		MaxEventsPerBlock: blockSize,
		MaxSegmentBytes:   maxSegmentBytes,
		AsyncFlushWorkers: 4,
		Metrics:           NewMetrics(reg),
	})

	rng := mathrand.New(mathrand.NewPCG(0x5eed, 0x1dea))
	mkEvent := func(n int) segment.Event {
		return segment.Event{
			WitnessedAt: int64(n + 1),
			Kind:        segment.KindCreate,
			DID:         fmt.Sprintf("did:plc:user%06d", n),
			Collection:  "app.bsky.feed.post",
			Rkey:        fmt.Sprintf("rk%06d", n),
			Rev:         "3kabc",
			Payload:     incompressiblePayload(rng, payloadBytes),
		}
	}

	// Prime the active block so the pending remainder is never zero at a
	// block boundary.
	prime := mkEvent(0)
	require.NoError(t, w.Append(t.Context(), &prime))
	total := 1
	for range batches {
		events := make([]segment.Event, perBatch)
		for i := range events {
			events[i] = mkEvent(total)
			total++
		}
		require.NoError(t, w.AppendBatch(t.Context(), events))
	}

	rotations := testutil.ToFloat64(w.cfg.Metrics.SegmentsRotated)
	require.Greater(t, rotations, 3.0,
		"sustained oversized load must trigger many rotations, not starve (got %v)", rotations)

	// Every sealed segment must be bounded near the threshold. Overshoot is
	// at most one in-flight append/batch worth of blocks; we allow a
	// generous 4x threshold ceiling, which still fails catastrophically on
	// the pre-fix code (which produced a single multi-batch segment).
	largest := largestSegmentBytes(t, w.cfg.SegmentsDir)
	require.Less(t, largest, int64(4*maxSegmentBytes),
		"no segment may grow far past MaxSegmentBytes; largest=%d threshold=%d", largest, maxSegmentBytes)

	// Correctness: drain and confirm every appended seq is present exactly
	// once across all segments, contiguous from 0.
	require.NoError(t, w.DrainDurability(t.Context()))
	require.NoError(t, w.Close())

	seqs := collectAllSeqs(t, w.cfg.SegmentsDir)
	require.Len(t, seqs, total, "every appended event must survive rotation")
	slices.Sort(seqs)
	for i := range seqs {
		require.Equal(t, uint64(i+1), seqs[i], "seqs must be contiguous with no gaps or duplicates")
	}
}

// TestAsyncFlush_SeqIntegrityAcrossRotationAndReopen drives oversized async
// load, rotating several times, then Close+Reopen (simulating a process
// restart) and continues appending. It asserts there are no duplicate or
// skipped seqs across the rotation seams and across the reopen boundary —
// the crash-recovery contract the rotation fix must preserve (Open's
// ScanMaxSeq reconciliation of seq/next).
func TestAsyncFlush_SeqIntegrityAcrossRotationAndReopen(t *testing.T) {
	t.Parallel()

	const blockSize = 8

	segDir := filepath.Join(t.TempDir(), "segments")
	st := newTestStore(t)
	mkWriter := func() *Writer {
		w, err := Open(Config{
			SegmentsDir:       segDir,
			Store:             st,
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
			Metrics:           NewMetrics(prometheus.NewRegistry()),
			MaxEventsPerBlock: blockSize,
			MaxSegmentBytes:   32 * 1024,
			AsyncFlushWorkers: 3,
		})
		require.NoError(t, err)
		return w
	}

	rng := mathrand.New(mathrand.NewPCG(0xabcd, 0x1234))
	seq := 0
	mkEvent := func() segment.Event {
		ev := segment.Event{
			WitnessedAt: int64(seq + 1),
			Kind:        segment.KindCreate,
			DID:         fmt.Sprintf("did:plc:u%06d", seq),
			Payload:     incompressiblePayload(rng, 512),
		}
		seq++
		return ev
	}
	// prime appends one event, then drives block-aligned batches so the
	// active block always has exactly one event Pending at commit time —
	// the shape the pre-fix guard (`|| Pending() > 0`) starves on.
	appendPrimedBatches := func(w *Writer, batches int) {
		prime := mkEvent()
		require.NoError(t, w.Append(t.Context(), &prime))
		for range batches {
			events := make([]segment.Event, 4*blockSize)
			for i := range events {
				events[i] = mkEvent()
			}
			require.NoError(t, w.AppendBatch(t.Context(), events))
		}
	}

	w1 := mkWriter()
	appendPrimedBatches(w1, 20)
	n1 := seq
	require.Greater(t, testutil.ToFloat64(w1.cfg.Metrics.SegmentsRotated), 1.0,
		"first run must rotate at least twice")
	// Close (not Seal): the trailing active segment stays unsealed, exactly
	// like a graceful shutdown mid-segment.
	require.NoError(t, w1.Close())

	nextAfterClose := w1.NextSeq()
	require.Equal(t, uint64(n1+1), nextAfterClose, "nextSeq covers every appended event")

	// Reopen against the same dir + store: must resume without regressing or
	// re-allocating any seq.
	w2 := mkWriter()
	require.Equal(t, uint64(n1+1), w2.NextSeq(),
		"reopen must reconcile nextSeq to exactly the prior high water mark (no gap, no rewind)")
	appendPrimedBatches(w2, 20)
	require.NoError(t, w2.DrainDurability(t.Context()))
	require.NoError(t, w2.Close())

	total := seq
	seqs := collectAllSeqs(t, segDir)
	require.Len(t, seqs, total,
		"every event across both runs must be present exactly once (no dups, no losses)")
	slices.Sort(seqs)
	for i := range seqs {
		require.Equal(t, uint64(i+1), seqs[i],
			"contiguous seqs across rotation seams and the reopen boundary")
	}
}

// TestAsyncFlush_RotationIsSizeDrivenNotPipelineDepth is the focused
// regression test pinning the exact root cause. It crosses MaxSegmentBytes
// with a single AppendBatch whose final block is a sub-MaxEventsPerBlock
// remainder (so the active block is left Pending), against multiple async
// workers. On the pre-fix code the rotation guard
// (asyncPrepared>1 || Pending()>0) vetoes rotation in this exact shape and
// the segment never seals. The fix rotates because the segment is full,
// regardless of pipeline state.
func TestAsyncFlush_RotationIsSizeDrivenNotPipelineDepth(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{
		MaxEventsPerBlock: 4,
		MaxSegmentBytes:   1, // any non-empty block crosses the threshold
		AsyncFlushWorkers: 4,
	})

	// 10 events / block-size 4 => two full blocks (8 events) submitted as
	// async jobs + a 2-event remainder left Pending in the active block.
	events := make([]segment.Event, 10)
	for i := range events {
		events[i] = segment.Event{
			WitnessedAt: int64(i + 1),
			Kind:        segment.KindCreate,
			DID:         fmt.Sprintf("did:plc:x%02d", i),
			Payload:     []byte("payload"),
		}
	}
	require.NoError(t, w.AppendBatch(t.Context(), events))

	require.Equal(t, uint64(1), w.ActiveIndex(),
		"full segment must rotate even with a pending remainder and in-flight pipeline depth")
	require.Equal(t, 1.0, testutil.ToFloat64(w.cfg.Metrics.SegmentsRotated))

	persisted, err := loadNextSeq(w.cfg.Store, w.cfg.SeqKey)
	require.NoError(t, err)
	require.Equal(t, uint64(11), persisted,
		"seq/next must be committed for every event before the seal")

	r, err := segment.Open(segment.ReaderConfig{Path: filepath.Join(w.cfg.SegmentsDir, SegmentFilename(0))})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })
	require.Equal(t, uint64(10), uint64(r.Header().EventCount),
		"the sealed segment must contain all 10 events incl. the flushed remainder")
}
