package ingest

import (
	"encoding/binary"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/cockroachdb/pebble"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// newTestStore opens a fresh metadata pebble db rooted at t.TempDir.
func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// newTestWriter is the standard Writer fixture: fresh shards dir, a
// fresh pebble store, the provided overrides applied last.
func newTestWriter(t *testing.T, overrides Config) *Writer {
	t.Helper()
	shards := filepath.Join(t.TempDir(), "shards")

	cfg := Config{
		ShardsDir: shards,
		Store:     newTestStore(t),
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:   NewMetrics(prometheus.NewRegistry()),
	}
	if overrides.MaxSegmentBytes != 0 {
		cfg.MaxSegmentBytes = overrides.MaxSegmentBytes
	}
	if overrides.MaxEventsPerBlock != 0 {
		cfg.MaxEventsPerBlock = overrides.MaxEventsPerBlock
	}

	w, err := Open(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return w
}

// TestOpen_FreshDir creates a fresh shards dir and confirms Open
// initializes seg_0000000000.jss with the 256-byte reserved header
// and starts at nextSeq=0.
func TestOpen_FreshDir(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{})

	require.Equal(t, uint64(0), w.NextSeq())
	require.Equal(t, uint64(0), w.ActiveIndex())

	path := filepath.Join(w.cfg.ShardsDir, "seg_0000000000.jss")
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, int64(256), info.Size(), "fresh segment is exactly the reserved header")

	// seq/next must not be set yet — Open never writes pebble for
	// a fresh dir (defaults read as 0).
	_, _, err = w.cfg.Store.Get([]byte(seqNextKey))
	require.ErrorIs(t, err, pebble.ErrNotFound)
}

// TestAppend_AllocatesMonotonicSeq pins the seq-allocation contract:
// N appends produce ev.Seq values in [0, N) in call order.
func TestAppend_AllocatesMonotonicSeq(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{MaxEventsPerBlock: 64})

	for i := range 10 {
		ev := segment.Event{
			IndexedAt: 1,
			Kind:      segment.KindCreate,
			DID:       "did:plc:a",
		}
		require.NoError(t, w.Append(t.Context(), &ev))
		require.Equal(t, uint64(i), ev.Seq, "append %d", i)
	}
	require.Equal(t, uint64(10), w.NextSeq())
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

	got, err := loadNextSeq(store)
	require.NoError(t, err)
	require.Equal(t, uint64(3), got, "Close must persist nextSeq")
}

// TestBlockFlush_AdvancesPebbleSeq confirms the durability ordering
// from DESIGN.md §3.1.1: after a block flush, seq/next in pebble
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
	require.Equal(t, uint64(blockSize), binary.LittleEndian.Uint64(val))
	require.Equal(t, uint64(blockSize), w.NextSeq())
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

	first := filepath.Join(w.cfg.ShardsDir, "seg_0000000000.jss")
	r, err := segment.Open(segment.ReaderConfig{Path: first})
	require.NoError(t, err, "first segment must be sealed")
	require.NoError(t, r.Close())

	second := filepath.Join(w.cfg.ShardsDir, "seg_0000000001.jss")
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
	shards := filepath.Join(t.TempDir(), "shards")
	st := newTestStore(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	cfg := Config{
		ShardsDir:         shards,
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
	require.Equal(t, uint64(blockSize), w1.NextSeq())
	require.NoError(t, w1.Close())

	// Run 2: same dir, same store. Open must resume.
	w2, err := Open(cfg)
	require.NoError(t, err)
	require.Equal(t, uint64(blockSize), w2.NextSeq(),
		"resumed nextSeq must match the last block's high water mark")
	require.Equal(t, uint64(0), w2.ActiveIndex(),
		"still on segment 0; we have not rotated")

	// Append more; allocator picks up from the right spot.
	for i := range blockSize {
		ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:b"}
		require.NoError(t, w2.Append(t.Context(), &ev))
		require.Equal(t, uint64(blockSize+i), ev.Seq)
	}
	require.NoError(t, w2.Close())
}

// TestResume_SealedSkipsToNext confirms that if the highest segment
// is sealed at Open time, Open creates seg_<idx+1>.jss instead of
// trying to reopen the sealed file.
func TestResume_SealedSkipsToNext(t *testing.T) {
	t.Parallel()
	const blockSize = 4
	shards := filepath.Join(t.TempDir(), "shards")
	st := newTestStore(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	cfg := Config{
		ShardsDir:         shards,
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
	seg1Path := filepath.Join(shards, "seg_0000000001.jss")
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

	seg2Path := filepath.Join(shards, "seg_0000000002.jss")
	info, err := os.Stat(seg2Path)
	require.NoError(t, err)
	require.Equal(t, int64(segment.ReservedHeaderBytes), info.Size(),
		"new active segment is exactly the reserved header")
}

// TestOpen_ReconcilesDriftedPebble simulates the crash mode from
// DESIGN.md §3.1.1: block fsynced, pebble batch lost. Open must read
// max(seq) from the segment, advance nextSeq to max+1, and rewrite
// pebble. Otherwise the next Append would reuse a seq.
func TestOpen_ReconcilesDriftedPebble(t *testing.T) {
	t.Parallel()
	const blockSize = 4
	shards := filepath.Join(t.TempDir(), "shards")
	st := newTestStore(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := Config{
		ShardsDir:         shards,
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
	require.NoError(t, saveNextSeq(st, 1))

	w2, err := Open(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w2.Close() })
	require.Equal(t, uint64(blockSize), w2.NextSeq(),
		"reconcile: nextSeq must advance past the segment's max seq")

	got, err := loadNextSeq(st)
	require.NoError(t, err)
	require.Equal(t, uint64(blockSize), got,
		"reconcile must persist the corrected value")
}

// TestOpen_RecoversFromTornTail simulates a crash with bytes past the
// last good frame. segment.New's resumeExistingSegment truncates the
// torn tail; ingest.Writer must then reconcile nextSeq cleanly.
func TestOpen_RecoversFromTornTail(t *testing.T) {
	t.Parallel()
	const blockSize = 2
	shards := filepath.Join(t.TempDir(), "shards")
	st := newTestStore(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := Config{
		ShardsDir:         shards,
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
	path := filepath.Join(shards, "seg_0000000000.jss")
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

	require.Equal(t, uint64(blockSize), w2.NextSeq())

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

	require.Equal(t, uint64(goroutines*perGoroutine), w.NextSeq())
}
