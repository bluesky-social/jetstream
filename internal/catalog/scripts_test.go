package catalog_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/leader"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/storagefake"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// harness is a leader session on a fresh storagefake archive. Every commit
// runs CheckInvariants; a test that expects none fails on the first.
type harness struct {
	t    *testing.T
	db   *storagefake.DB
	s    *catalog.Session
	lm   *leader.Metrics
	lock *storagefake.Lease
	// violationOK marks a test that commits an invariant violation on
	// purpose.
	violationOK bool
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	db := storagefake.New(storagefake.Config{})
	h := &harness{t: t, db: db, lm: leader.NewMetrics(prometheus.NewRegistry())}
	h.lock = db.NewLease()
	require.NoError(t, h.lock.Acquire(t.Context(), time.Hour))
	h.s = catalog.NewSession(catalog.SessionConfig{DB: db, Epoch: h.lock.Epoch(), LeaderMetrics: h.lm})
	_, err := h.s.InitNamespace(t.Context(), catalog.Main, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		if !t.Failed() && !h.violationOK {
			require.NoError(t, db.Violation(), "catalog invariants")
		}
	})
	return h
}

// newSession starts a fresh session for the current epoch, as the next
// leader session in the same process would.
func (h *harness) newSession() {
	h.s = catalog.NewSession(catalog.SessionConfig{DB: h.db, Epoch: h.lock.Epoch(), LeaderMetrics: h.lm})
}

// object uploads (in catalog terms) an object and returns a pending ref.
func (h *harness) object(data []byte) catalog.ObjectRef {
	h.t.Helper()
	req := catalog.UploadRequest{SHA256: sha256.Sum256(data), Length: int64(len(data))}
	_, _ = rand.Read(req.Key[:])
	slots, _, err := h.s.BeginUploads(h.t.Context(), []catalog.UploadRequest{req}, time.Hour)
	require.NoError(h.t, err)
	return catalog.ObjectRef{ID: slots[0].ObjectID, SHA256: req.SHA256, Pending: !slots[0].Dedup}
}

func (h *harness) hot(first, last uint64) catalog.HotBatchCommit {
	h.t.Helper()
	c, err := h.s.CommitHotBatch(h.t.Context(), catalog.HotBatch{FirstSeq: first, LastSeq: last, Frame: fmt.Appendf(nil, "%d-%d", first, last)})
	require.NoError(h.t, err)
	return c
}

// block encodes events [lo, hi] as a real block frame.
func block(t *testing.T, lo, hi uint64) ([]byte, segment.BlockInfo) {
	t.Helper()
	b, err := segment.NewBlockBuilder(int(hi - lo + 1))
	require.NoError(t, err)
	for seq := lo; seq <= hi; seq++ {
		_, err := b.Append(segment.Event{
			Seq: seq, WitnessedAt: int64(seq), Kind: segment.KindCreate,
			DID: "did:plc:test", Collection: "app.bsky.feed.post", Rkey: fmt.Sprint(seq), Rev: "r",
			Payload: []byte{0xa0},
		})
		require.NoError(t, err)
	}
	frame, info := b.Encode()
	return frame, info
}

type builtBlock struct {
	frame  []byte
	commit catalog.BlockCommit
	info   segment.BlockInfo
}

func (h *harness) fold(lo, hi uint64) builtBlock {
	h.t.Helper()
	frame, info := block(h.t, lo, hi)
	c, err := h.s.Fold(h.t.Context(), catalog.Block{Namespace: catalog.Main, Info: info, Object: h.object(frame)})
	require.NoError(h.t, err)
	return builtBlock{frame: frame, commit: c, info: info}
}

// sealOf builds the Seal for blocks the way the maintainer does.
func (h *harness) sealOf(seg uint64, blocks []builtBlock) catalog.Seal {
	h.t.Helper()
	frames := make([][]byte, len(blocks))
	sl := catalog.Seal{Namespace: catalog.Main, Segment: seg}
	for i, b := range blocks {
		frames[i] = b.frame
		sl.Blocks = append(sl.Blocks, catalog.SealBlock{ObjectID: b.commit.ObjectID, CompressedLength: int64(b.info.CompressedSize)})
	}
	hdr, footer, _, err := segment.BuildSealed(segment.SliceFrameSource(frames))
	require.NoError(h.t, err)
	sl.Header = hdr
	sl.Footer = h.object(footer)
	return sl
}

func (h *harness) meta(key string) ([]byte, bool) {
	v, err := h.db.MetaStore(nil).Get(context.Background(), []byte(key))
	if errors.Is(err, metastore.ErrNotFound) {
		return nil, false
	}
	require.NoError(h.t, err)
	return v, true
}

func requireCorruption(t *testing.T, err error, source string) {
	t.Helper()
	require.Error(t, err)
	src, ok := catalog.IsCorruption(err)
	require.True(t, ok, "want corruption, got %v", err)
	require.Equal(t, source, src)
	var sf interface{ SessionFatal() bool }
	require.ErrorAs(t, err, &sf)
	require.True(t, sf.SessionFatal(), "corruption must exit the process")
}

func TestScripts_HappyPath(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := t.Context()
	h.hot(1, 3)
	ptr := h.object([]byte("pointer batch"))
	c, err := h.s.CommitHotBatch(ctx, catalog.HotBatch{
		FirstSeq: 4, LastSeq: 5, Object: ptr,
		Meta: []metastore.Op{{Kind: metastore.OpSet, Key: []byte("relay/cursor"), Value: []byte("42")}},
	})
	require.NoError(t, err)
	require.Equal(t, ptr.ID, c.ObjectID)
	v, _ := h.meta(catalog.MainSeqKey)
	require.Equal(t, catalog.EncodeSeq(6), v)
	v, _ = h.meta("relay/cursor")
	require.Equal(t, "42", string(v))

	b1 := h.fold(1, 3)
	require.Equal(t, 0, b1.commit.Ordinal)
	h.hot(6, 8)
	b2 := h.fold(4, 8)
	require.Equal(t, 1, b2.commit.Ordinal)

	sc, err := h.s.Seal(ctx, h.sealOf(0, []builtBlock{b1, b2}))
	require.NoError(t, err)
	snap, err := h.db.Snapshot()
	require.NoError(t, err)
	require.Len(t, snap.Segments, 2)
	require.Equal(t, catalog.Sealed, snap.Segments[0].State)
	require.Equal(t, sc.GenerationID, snap.Segments[0].GenerationID)
	require.Equal(t, catalog.Active, snap.Segments[1].State)
	require.Empty(t, snap.ActiveBlocks)
	require.Empty(t, snap.HotBatches)
	require.Len(t, snap.GenerationBlocks[sc.GenerationID], 2)

	// The next segment continues from where the seal left off.
	h.hot(9, 9)
	b3 := h.fold(9, 9)
	require.Equal(t, uint64(1), b3.commit.Segment)
	require.NoError(t, h.s.Err())
}

func TestScripts_DirectBlockCommit(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := t.Context()
	_, err := h.s.InitNamespace(ctx, catalog.BootstrapLive, nil)
	require.NoError(t, err)
	frame, info := block(t, 1, 4)
	c, err := h.s.CommitBlock(ctx, catalog.Block{Namespace: catalog.BootstrapLive, Info: info, Object: h.object(frame)})
	require.NoError(t, err)
	require.Equal(t, 0, c.Ordinal)
	v, _ := h.meta(catalog.BootstrapLiveSeqKey)
	require.Equal(t, catalog.EncodeSeq(5), v)
	_, found := h.meta(catalog.MainSeqKey)
	require.False(t, found, "direct commits in bootstrap_live leave main's seq alone")

	_, err = h.s.DeleteNamespace(ctx, catalog.BootstrapLive, []metastore.Op{{Kind: metastore.OpDelete, Key: []byte(catalog.BootstrapLiveSeqKey)}})
	require.NoError(t, err)
	_, err = h.s.DeleteNamespace(ctx, catalog.Main, nil)
	require.Error(t, err, "main can never be deleted")
}

func TestScripts_Dedup(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	frame, info := block(t, 1, 2)
	h.hot(1, 2)
	first := h.object(frame)
	id, err := h.s.MarkAvailable(t.Context(), first)
	require.NoError(t, err)
	require.Equal(t, first.ID, id)

	// A second upload of the same bytes dedups onto the available row.
	again := h.object(frame)
	require.False(t, again.Pending)
	require.Equal(t, first.ID, again.ID)
	c, err := h.s.Fold(t.Context(), catalog.Block{Namespace: catalog.Main, Info: info, Object: again})
	require.NoError(t, err)
	require.Equal(t, first.ID, c.ObjectID)
}

// Two uploads of the same bytes race: both get uploading rows, and the
// transaction that makes the second one available references the winner.
func TestScripts_UploadRaceReferencesWinner(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	frame, info := block(t, 1, 2)
	h.hot(1, 2)
	a, b := h.object(frame), h.object(frame)
	require.True(t, a.Pending)
	require.True(t, b.Pending)
	require.NotEqual(t, a.ID, b.ID)
	_, err := h.s.MarkAvailable(t.Context(), a)
	require.NoError(t, err)
	c, err := h.s.Fold(t.Context(), catalog.Block{Namespace: catalog.Main, Info: info, Object: b})
	require.NoError(t, err)
	require.Equal(t, a.ID, c.ObjectID)
}

func TestScripts_FenceLost(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := t.Context()
	h.hot(1, 1)
	// A new leader takes over after the lease expires.
	require.NoError(t, h.lock.Release(ctx))
	next := h.db.NewLease()
	require.NoError(t, next.Acquire(ctx, time.Hour))
	rev := h.db.Archive().CatalogRevision

	_, err := h.s.CommitHotBatch(ctx, catalog.HotBatch{FirstSeq: 2, LastSeq: 2, Frame: []byte("f")})
	require.ErrorIs(t, err, catalog.ErrFenced)
	require.ErrorIs(t, err, catalog.ErrSessionEnded)
	require.ErrorIs(t, err, leader.ErrRestartSession)
	_, corrupt := catalog.IsCorruption(err)
	require.False(t, corrupt, "losing the fence ends the session; it is not corruption")
	require.Equal(t, rev, h.db.Archive().CatalogRevision, "a fenced-out transaction changes nothing")
	require.InDelta(t, 1, testutil.ToFloat64(h.lm.FenceFailures), 0)

	// The session stays ended, even for a script that would pass.
	_, err = h.s.CommitMeta(ctx, []metastore.Op{{Kind: metastore.OpSet, Key: []byte("k"), Value: []byte("v")}})
	require.ErrorIs(t, err, catalog.ErrSessionEnded)
	_, found := h.meta("k")
	require.False(t, found)
}

func TestScripts_SeqMismatch(t *testing.T) {
	t.Parallel()
	for name, first := range map[string]uint64{"gap": 3, "overlap": 1} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.hot(1, 1)
			_, err := h.s.CommitHotBatch(t.Context(), catalog.HotBatch{FirstSeq: first, LastSeq: first, Frame: []byte("f")})
			requireCorruption(t, err, catalog.SourceSeq)
			v, _ := h.meta(catalog.MainSeqKey)
			require.Equal(t, catalog.EncodeSeq(2), v)
		})
	}
}

func TestScripts_SeqKeyCorrupt(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	_, err := h.s.CommitMeta(t.Context(), []metastore.Op{{Kind: metastore.OpSet, Key: []byte(catalog.MainSeqKey), Value: []byte("bad")}})
	require.NoError(t, err)
	_, err = h.s.CommitHotBatch(t.Context(), catalog.HotBatch{FirstSeq: 1, LastSeq: 1, Frame: []byte("f")})
	requireCorruption(t, err, catalog.SourceMeta)
	// The garbage seq key is itself an invariant violation.
	require.Error(t, h.db.Violation())
	h.violationOK = true
}

func TestScripts_MissingReference(t *testing.T) {
	t.Parallel()
	t.Run("absent object", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		_, err := h.s.CommitHotBatch(t.Context(), catalog.HotBatch{FirstSeq: 1, LastSeq: 1, Object: catalog.ObjectRef{ID: 1 << 40}})
		requireCorruption(t, err, catalog.SourceRef)
	})
	t.Run("uploading object", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		ref := h.object([]byte("x"))
		ref.Pending = false // the caller forgot to make it available
		_, err := h.s.CommitHotBatch(t.Context(), catalog.HotBatch{FirstSeq: 1, LastSeq: 1, Object: ref})
		requireCorruption(t, err, catalog.SourceRef)
	})
	t.Run("fold block", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		h.hot(1, 2)
		_, info := block(t, 1, 2)
		_, err := h.s.Fold(t.Context(), catalog.Block{Namespace: catalog.Main, Info: info, Object: catalog.ObjectRef{ID: 1 << 40}})
		requireCorruption(t, err, catalog.SourceRef)
		snap, err := h.db.Snapshot()
		require.NoError(t, err)
		require.Len(t, snap.HotBatches, 1, "the failed fold deleted nothing")
	})
	t.Run("seal footer", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		h.hot(1, 2)
		b := h.fold(1, 2)
		sl := h.sealOf(0, []builtBlock{b})
		sl.Footer = catalog.ObjectRef{ID: 1 << 40}
		_, err := h.s.Seal(t.Context(), sl)
		requireCorruption(t, err, catalog.SourceRef)
	})
}

// An upload GC claimed while it stalled ends the session without claiming
// corruption: the next session uploads again (§7.3's accepted leak).
func TestScripts_UploadNoLongerUploading(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ref := h.object([]byte("x"))
	_, err := h.s.MarkAvailable(t.Context(), ref)
	require.NoError(t, err)
	h.newSession()
	// Same row again, now available: it is its own winner via dedup.
	id, err := h.s.MarkAvailable(t.Context(), ref)
	require.NoError(t, err)
	require.Equal(t, ref.ID, id)

	// A row that is gone entirely.
	_, err = h.s.MarkAvailable(t.Context(), catalog.ObjectRef{ID: 1 << 40, SHA256: sha256.Sum256([]byte("y")), Pending: true})
	require.ErrorIs(t, err, catalog.ErrSessionEnded)
	_, corrupt := catalog.IsCorruption(err)
	require.False(t, corrupt)
}

func TestScripts_FoldGap(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		hot    [][2]uint64
		lo, hi uint64
	}{
		"missing tail":       {hot: [][2]uint64{{1, 2}, {3, 4}}, lo: 1, hi: 5},
		"batch crosses end":  {hot: [][2]uint64{{1, 2}, {3, 5}}, lo: 1, hi: 4},
		"no batches":         {hot: [][2]uint64{{1, 2}}, lo: 3, hi: 4},
		"starts mid batch":   {hot: [][2]uint64{{1, 3}}, lo: 2, hi: 3},
		"skips first batch?": {hot: [][2]uint64{{1, 1}, {2, 3}}, lo: 2, hi: 3},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			for _, r := range tc.hot {
				h.hot(r[0], r[1])
			}
			before, err := h.db.Snapshot()
			require.NoError(t, err)
			frame, info := block(t, tc.lo, tc.hi)
			ref := h.object(frame)
			_, err = h.s.Fold(t.Context(), catalog.Block{Namespace: catalog.Main, Info: info, Object: ref})
			if name == "skips first batch?" {
				// Coverage is exact, but the block does not continue the
				// segment: [1,1] stays hot below it. The invariant check,
				// not the script, catches this one.
				require.NoError(t, err)
				require.Error(t, h.db.Violation())
				h.violationOK = true
				return
			}
			requireCorruption(t, err, catalog.SourceFold)
			after, err := h.db.Snapshot()
			require.NoError(t, err)
			require.Equal(t, before.HotBatches, after.HotBatches, "a rejected fold deletes nothing")
			require.Empty(t, after.ActiveBlocks)
		})
	}
}

// Coverage alone cannot see a block that skips seqs after the segment's
// last block; the continuity check can.
func TestScripts_BlockDiscontinuity(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.hot(1, 2)
	h.fold(1, 2)
	h.hot(3, 3)
	h.hot(4, 4)
	frame, info := block(t, 4, 4)
	_, err := h.s.Fold(t.Context(), catalog.Block{Namespace: catalog.Main, Info: info, Object: h.object(frame)})
	requireCorruption(t, err, catalog.SourceInvariant)
}

func TestScripts_SealListMismatch(t *testing.T) {
	t.Parallel()
	setup := func(t *testing.T) (*harness, []builtBlock) {
		h := newHarness(t)
		h.hot(1, 2)
		h.hot(3, 5)
		b1 := h.fold(1, 2)
		b2 := h.fold(3, 5)
		return h, []builtBlock{b1, b2}
	}
	t.Run("missing block", func(t *testing.T) {
		t.Parallel()
		h, bs := setup(t)
		_, err := h.s.Seal(t.Context(), h.sealOf(0, bs[:1]))
		requireCorruption(t, err, catalog.SourceSeal)
	})
	t.Run("reordered", func(t *testing.T) {
		t.Parallel()
		h, bs := setup(t)
		sl := h.sealOf(0, bs)
		sl.Blocks[0], sl.Blocks[1] = sl.Blocks[1], sl.Blocks[0]
		_, err := h.s.Seal(t.Context(), sl)
		requireCorruption(t, err, catalog.SourceSeal)
	})
	t.Run("wrong object", func(t *testing.T) {
		t.Parallel()
		h, bs := setup(t)
		sl := h.sealOf(0, bs)
		sl.Blocks[1].ObjectID++
		_, err := h.s.Seal(t.Context(), sl)
		requireCorruption(t, err, catalog.SourceSeal)
	})
	t.Run("header disagrees", func(t *testing.T) {
		t.Parallel()
		h, bs := setup(t)
		sl := h.sealOf(0, bs)
		sl.Header = h.sealOf(0, bs[:1]).Header
		_, err := h.s.Seal(t.Context(), sl)
		requireCorruption(t, err, catalog.SourceSeal)
	})
	t.Run("not the active segment", func(t *testing.T) {
		t.Parallel()
		h, bs := setup(t)
		_, err := h.s.Seal(t.Context(), h.sealOf(1, bs))
		requireCorruption(t, err, catalog.SourceSeal)
	})
	t.Run("active header", func(t *testing.T) {
		t.Parallel()
		h, bs := setup(t)
		sl := h.sealOf(0, bs)
		sl.Header = make([]byte, segment.ReservedHeaderBytes)
		_, err := h.s.Seal(t.Context(), sl)
		require.Error(t, err)
	})
	t.Run("rejected seal changes nothing", func(t *testing.T) {
		t.Parallel()
		h, bs := setup(t)
		before, err := h.db.Snapshot()
		require.NoError(t, err)
		_, err = h.s.Seal(t.Context(), h.sealOf(0, bs[:1]))
		require.Error(t, err)
		after, err := h.db.Snapshot()
		require.NoError(t, err)
		require.Equal(t, before.Segments, after.Segments)
		require.Equal(t, before.ActiveBlocks, after.ActiveBlocks)
	})
}

func TestScripts_ValidationEndsSession(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	_, err := h.s.CommitHotBatch(t.Context(), catalog.HotBatch{FirstSeq: 2, LastSeq: 1, Frame: []byte("f")})
	require.Error(t, err)
	_, err = h.s.CommitMeta(t.Context(), nil)
	require.Error(t, err, "a rejected script ends the session")
}

func TestScripts_PublishGenerationNotImplemented(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	require.ErrorIs(t, h.s.PublishGeneration(t.Context()), catalog.ErrNotImplemented)
}

func TestScripts_ConcurrentSessionsSerialize(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := t.Context()
	errs := make(chan error, 8)
	for i := range 8 {
		go func() {
			_, err := h.s.CommitMeta(ctx, []metastore.Op{{Kind: metastore.OpSet, Key: fmt.Appendf(nil, "k%d", i), Value: []byte("v")}})
			errs <- err
		}()
	}
	for range 8 {
		require.NoError(t, <-errs)
	}
	for i := range 8 {
		_, found := h.meta(fmt.Sprint("k", i))
		require.True(t, found)
	}
}
