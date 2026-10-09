package catalog_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/leader"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/pgstore/pgtest"
	"github.com/bluesky-social/jetstream/internal/storagefake"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// backend is a catalog.DB the scripts run on: storagefake always, and
// PostgreSQL under `just test-storage` (design §20 layer 4). open returns a
// fresh initialized archive and a lock constructor for it.
type backend struct {
	name string
	open func(t *testing.T) (catalog.DB, func() leader.Locker)
}

var (
	fakeBackend = backend{name: "storagefake", open: func(t *testing.T) (catalog.DB, func() leader.Locker) {
		db := storagefake.New(storagefake.Config{})
		return db, func() leader.Locker { return db.NewLease() }
	}}
	pgBackend = backend{name: "pg", open: func(t *testing.T) (catalog.DB, func() leader.Locker) {
		s, _ := pgtest.Open(t, nil)
		return s, func() leader.Locker { return s.NewLease() }
	}}
)

// eachBackend runs f once per backend, in parallel. The PostgreSQL run
// skips unless JETSTREAM_TEST_PG_URL is set.
func eachBackend(t *testing.T, f func(t *testing.T, be backend)) {
	t.Helper()
	for _, b := range []backend{fakeBackend, pgBackend} {
		t.Run(b.name, func(t *testing.T) {
			t.Parallel()
			if b.name == pgBackend.name {
				pgtest.URL(t)
			}
			f(t, b)
		})
	}
}

// checkedDB runs CheckInvariants on a fresh snapshot after every commit and
// records the first violation. storagefake does the same internally; doing
// it here runs the same check on PostgreSQL.
type checkedDB struct {
	catalog.DB
	mu        sync.Mutex
	violation error
}

func (d *checkedDB) Begin(ctx context.Context, kind catalog.TxKind) (catalog.Tx, error) {
	tx, err := d.DB.Begin(ctx, kind)
	if err != nil {
		return nil, err
	}
	return &checkedTx{Tx: tx, db: d}, nil
}

type checkedTx struct {
	catalog.Tx
	db *checkedDB
}

func (tx *checkedTx) Commit(ctx context.Context) error {
	if err := tx.Tx.Commit(ctx); err != nil {
		return err
	}
	tx.db.check(context.WithoutCancel(ctx))
	return nil
}

func (d *checkedDB) snapshot(ctx context.Context) (*catalog.Snapshot, error) {
	r, err := d.BeginRead(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = r.Close(context.Background()) }()
	return catalog.LoadSnapshot(ctx, r)
}

func (d *checkedDB) check(ctx context.Context) {
	snap, err := d.snapshot(ctx)
	if err != nil {
		err = fmt.Errorf("load snapshot: %w", err)
	} else if err = catalog.CheckInvariants(snap, catalog.InvariantOptions{}); err != nil {
		err = fmt.Errorf("after revision %d: %w\n%s", snap.Archive.CatalogRevision, err, snap)
	}
	if err == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.violation == nil {
		d.violation = err
	}
}

// Violation returns the first invariant violation any commit produced, or
// nil.
func (d *checkedDB) Violation() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.violation
}

// harness is a leader session on a fresh archive. Every commit runs
// CheckInvariants; a test that expects none fails on the first.
type harness struct {
	t       *testing.T
	db      *checkedDB
	newLock func() leader.Locker
	s       *catalog.Session
	lm      *leader.Metrics
	lock    leader.Locker
	// violationOK marks a test that commits an invariant violation on
	// purpose.
	violationOK bool
}

func newHarness(t *testing.T, b backend) *harness {
	t.Helper()
	db, newLock := b.open(t)
	h := &harness{t: t, db: &checkedDB{DB: db}, newLock: newLock, lm: leader.NewMetrics(prometheus.NewRegistry())}
	h.lock = newLock()
	require.NoError(t, h.lock.Acquire(t.Context(), time.Hour))
	h.s = catalog.NewSession(catalog.SessionConfig{DB: h.db, Epoch: h.lock.Epoch(), LeaderMetrics: h.lm})
	_, err := h.s.InitNamespace(t.Context(), catalog.Main, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		if !t.Failed() && !h.violationOK {
			require.NoError(t, h.db.Violation(), "catalog invariants")
		}
	})
	return h
}

// newSession starts a fresh session for the current epoch, as the next
// leader session in the same process would.
func (h *harness) newSession() {
	h.s = catalog.NewSession(catalog.SessionConfig{DB: h.db, Epoch: h.lock.Epoch(), LeaderMetrics: h.lm})
}

// snapshot loads the committed catalog.
func (h *harness) snapshot() (*catalog.Snapshot, error) {
	return h.db.snapshot(h.t.Context())
}

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
	h.t.Helper()
	r, err := h.db.BeginRead(h.t.Context())
	require.NoError(h.t, err)
	defer func() { _ = r.Close(context.Background()) }()
	got, err := r.MetaGet(h.t.Context(), [][]byte{[]byte(key)})
	require.NoError(h.t, err)
	v, ok := got[key]
	return v, ok
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

// An absent seq counter is 1; a stored 0 is corruption, not a fresh
// namespace, so a damaged seq/next cannot restart allocation at 1.
func TestDecodeSeq(t *testing.T) {
	t.Parallel()
	n, err := catalog.DecodeSeq(catalog.MainSeqKey, nil, false)
	require.NoError(t, err)
	require.Equal(t, uint64(1), n)
	n, err = catalog.DecodeSeq(catalog.MainSeqKey, catalog.EncodeSeq(7), true)
	require.NoError(t, err)
	require.Equal(t, uint64(7), n)
	for _, val := range [][]byte{catalog.EncodeSeq(0), {1, 2, 3}} {
		_, err = catalog.DecodeSeq(catalog.MainSeqKey, val, true)
		src, ok := catalog.IsCorruption(err)
		require.True(t, ok, "%x: %v", val, err)
		require.Equal(t, catalog.SourceMeta, src)
	}
}

func TestScripts_HappyPath(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		h := newHarness(t, be)
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
		snap, err := h.snapshot()
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
	})
}

func TestScripts_DirectBlockCommit(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		h := newHarness(t, be)
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
	})
}

// directBlock encodes events [lo, hi] and uploads them, as a pending ref
// the commit makes available.
func (h *harness) directBlock(ns catalog.Namespace, lo, hi uint64, meta ...metastore.Op) catalog.Block {
	h.t.Helper()
	frame, info := block(h.t, lo, hi)
	return catalog.Block{Namespace: ns, Info: info, Object: h.object(frame), Meta: meta}
}

// A direct group commit is one transaction: the blocks take consecutive
// ordinals after the active segment's last, every row shares the revision,
// the blocks' metadata applies in block order (last write wins), and the
// seq key moves once, past the last block.
func TestScripts_CommitBlocksGroup(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		h := newHarness(t, be)
		ctx := t.Context()
		first, err := h.s.CommitBlock(ctx, h.directBlock(catalog.Main, 1, 2))
		require.NoError(t, err)
		cursor := func(v string) metastore.Op {
			return metastore.Op{Kind: metastore.OpSet, Key: []byte("relay/cursor"), Value: []byte(v)}
		}
		bs := []catalog.Block{
			h.directBlock(catalog.Main, 3, 5, cursor("1")),
			h.directBlock(catalog.Main, 6, 6),
			h.directBlock(catalog.Main, 7, 9, cursor("3"), metastore.Op{Kind: metastore.OpSet, Key: []byte("other"), Value: []byte("x")}),
		}
		out, err := h.s.CommitBlocks(ctx, bs)
		require.NoError(t, err)
		require.Len(t, out, 3)
		require.Greater(t, out[0].Revision, first.Revision)
		for i, c := range out {
			require.Equal(t, out[0].Revision, c.Revision, "one transaction")
			require.Equal(t, uint64(0), c.Segment)
			require.Equal(t, i+1, c.Ordinal)
			require.Equal(t, bs[i].Object.ID, c.ObjectID)
		}
		v, _ := h.meta(catalog.MainSeqKey)
		require.Equal(t, catalog.EncodeSeq(10), v)
		v, _ = h.meta("relay/cursor")
		require.Equal(t, "3", string(v))
		_, found := h.meta("other")
		require.True(t, found)

		snap, err := h.snapshot()
		require.NoError(t, err)
		require.Len(t, snap.ActiveBlocks, 4)
		for i, r := range snap.ActiveBlocks[1:] {
			require.Equal(t, bs[i].Info.MinSeq, r.MinSeq)
			require.Equal(t, out[0].Revision, r.Revision)
		}
		require.Equal(t, map[catalog.ObjectState]int64{catalog.ObjectAvailable: 4}, h.objectStates(),
			"the group made every pending upload available")
		_, err = h.s.CommitBlock(ctx, h.directBlock(catalog.Main, 10, 10))
		require.NoError(t, err)
		require.NoError(t, h.s.Err())
	})
}

// A group that does not tile, spans namespaces, or is empty is rejected
// before any transaction and ends the session.
func TestScripts_CommitBlocksRejects(t *testing.T) {
	t.Parallel()
	for name, spans := range map[string][][2]uint64{
		"empty":   nil,
		"gap":     {{1, 2}, {4, 4}},
		"overlap": {{1, 2}, {2, 3}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			eachBackend(t, func(t *testing.T, be backend) {
				h := newHarness(t, be)
				var bs []catalog.Block
				for _, sp := range spans {
					bs = append(bs, h.directBlock(catalog.Main, sp[0], sp[1]))
				}
				_, err := h.s.CommitBlocks(t.Context(), bs)
				require.Error(t, err)
				require.Error(t, h.s.Err(), "a rejected script ends the session")
				snap, err := h.snapshot()
				require.NoError(t, err)
				require.Empty(t, snap.ActiveBlocks)
			})
		})
	}
	t.Run("namespaces", func(t *testing.T) {
		t.Parallel()
		eachBackend(t, func(t *testing.T, be backend) {
			h := newHarness(t, be)
			_, err := h.s.InitNamespace(t.Context(), catalog.BootstrapLive, nil)
			require.NoError(t, err)
			_, err = h.s.CommitBlocks(t.Context(), []catalog.Block{
				h.directBlock(catalog.Main, 1, 2), h.directBlock(catalog.BootstrapLive, 3, 3),
			})
			require.Error(t, err)
			require.Error(t, h.s.Err())
		})
	})
}

// Any block of a group failing its checks fails the whole group: nothing
// commits, not even the blocks before it.
func TestScripts_CommitBlocksAllOrNothing(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		for name, tc := range map[string]struct {
			mutate func(h *harness, bs []catalog.Block)
			source string
		}{
			"missing reference": {func(h *harness, bs []catalog.Block) { bs[2].Object = catalog.ObjectRef{ID: 1 << 40} }, catalog.SourceRef},
			"seq mismatch": {func(h *harness, bs []catalog.Block) {
				_, err := h.s.CommitBlock(h.t.Context(), h.directBlock(catalog.Main, 1, 1))
				require.NoError(h.t, err)
			}, catalog.SourceSeq},
		} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				h := newHarness(t, be)
				bs := []catalog.Block{h.directBlock(catalog.Main, 1, 2), h.directBlock(catalog.Main, 3, 4), h.directBlock(catalog.Main, 5, 5)}
				tc.mutate(h, bs)
				before, err := h.snapshot()
				require.NoError(t, err)
				_, err = h.s.CommitBlocks(t.Context(), bs)
				requireCorruption(t, err, tc.source)
				after, err := h.snapshot()
				require.NoError(t, err)
				require.Equal(t, before.ActiveBlocks, after.ActiveBlocks, "nothing in the group committed")
				require.Equal(t, before.Meta, after.Meta)
			})
		}
	})
}

// A group's first block must continue the active segment's last block,
// as a single block must.
func TestScripts_CommitBlocksDiscontinuity(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		h := newHarness(t, be)
		ctx := t.Context()
		_, err := h.s.CommitBlock(ctx, h.directBlock(catalog.Main, 1, 2))
		require.NoError(t, err)
		// The seq key agrees with the group; the active segment does not.
		_, err = h.s.CommitMeta(ctx, []metastore.Op{{Kind: metastore.OpSet, Key: []byte(catalog.MainSeqKey), Value: catalog.EncodeSeq(4)}})
		require.NoError(t, err)
		h.violationOK = true
		_, err = h.s.CommitBlocks(ctx, []catalog.Block{h.directBlock(catalog.Main, 4, 4), h.directBlock(catalog.Main, 5, 6)})
		requireCorruption(t, err, catalog.SourceInvariant)
	})
}

// InitNamespace is idempotent, and applies its metadata whether or not it
// created the segment: the orchestrator's first phase write rides on it.
func TestScripts_InitNamespaceIdempotent(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		h := newHarness(t, be)
		ctx := t.Context()
		for i, v := range []string{"first", "second"} {
			_, err := h.s.InitNamespace(ctx, catalog.BootstrapLive, []metastore.Op{{Kind: metastore.OpSet, Key: []byte("init/test"), Value: []byte(v)}})
			require.NoError(t, err, "call %d", i)
			got, found := h.meta("init/test")
			require.True(t, found)
			require.Equal(t, []byte(v), got)
		}
		snap, err := h.snapshot()
		require.NoError(t, err)
		var segs int
		for _, row := range snap.Segments {
			if row.Namespace == catalog.BootstrapLive {
				segs++
			}
		}
		require.Equal(t, 1, segs)
	})
}

func TestScripts_Dedup(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		h := newHarness(t, be)
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
	})
}

// Two uploads of the same bytes race: both get uploading rows, and the
// transaction that makes the second one available references the winner.
func TestScripts_UploadRaceReferencesWinner(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		h := newHarness(t, be)
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
	})
}

func TestScripts_FenceLost(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		h := newHarness(t, be)
		ctx := t.Context()
		h.hot(1, 1)
		// A new leader takes over after the lease expires.
		require.NoError(t, h.lock.Release(ctx))
		next := h.newLock()
		require.NoError(t, next.Acquire(ctx, time.Hour))
		before, err := h.snapshot()
		require.NoError(t, err)

		_, err = h.s.CommitHotBatch(ctx, catalog.HotBatch{FirstSeq: 2, LastSeq: 2, Frame: []byte("f")})
		require.ErrorIs(t, err, catalog.ErrFenced)
		require.ErrorIs(t, err, catalog.ErrSessionEnded)
		require.ErrorIs(t, err, leader.ErrRestartSession)
		_, corrupt := catalog.IsCorruption(err)
		require.False(t, corrupt, "losing the fence ends the session; it is not corruption")
		after, err := h.snapshot()
		require.NoError(t, err)
		require.Equal(t, before.Archive.CatalogRevision, after.Archive.CatalogRevision, "a fenced-out transaction changes nothing")
		require.InDelta(t, 1, testutil.ToFloat64(h.lm.FenceFailures), 0)

		// The session stays ended, even for a script that would pass.
		_, err = h.s.CommitMeta(ctx, []metastore.Op{{Kind: metastore.OpSet, Key: []byte("k"), Value: []byte("v")}})
		require.ErrorIs(t, err, catalog.ErrSessionEnded)
		_, found := h.meta("k")
		require.False(t, found)
	})
}

// The seq check fails the one-round-trip commit, and the ordinary one it
// falls back to reports the corruption.
func TestScripts_SeqMismatch(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		for name, first := range map[string]uint64{"gap": 3, "overlap": 1} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				h := newHarness(t, be)
				db := &fenceDB{DB: h.db}
				h.s = catalog.NewSession(catalog.SessionConfig{DB: db, Epoch: h.lock.Epoch()})
				h.hot(1, 1)
				db.counts()
				_, err := h.s.CommitHotBatch(t.Context(), catalog.HotBatch{FirstSeq: first, LastSeq: first, Frame: []byte("f")})
				requireCorruption(t, err, catalog.SourceSeq)
				bump, bumpAt := db.counts()
				require.Equal(t, [2]int{1, 1}, [2]int{bump, bumpAt})
				v, _ := h.meta(catalog.MainSeqKey)
				require.Equal(t, catalog.EncodeSeq(2), v)
			})
		}
	})
}

// A direct commit locks and checks its namespace's seq key as a hot batch
// does (§10.6).
func TestScripts_DirectSeqMismatch(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		for name, first := range map[string]uint64{"gap": 7, "overlap": 3} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				h := newHarness(t, be)
				ctx := t.Context()
				_, err := h.s.InitNamespace(ctx, catalog.BootstrapLive, nil)
				require.NoError(t, err)
				frame, info := block(t, 1, 4)
				_, err = h.s.CommitBlock(ctx, catalog.Block{Namespace: catalog.BootstrapLive, Info: info, Object: h.object(frame)})
				require.NoError(t, err)
				frame, info = block(t, first, first+1)
				_, err = h.s.CommitBlock(ctx, catalog.Block{Namespace: catalog.BootstrapLive, Info: info, Object: h.object(frame)})
				requireCorruption(t, err, catalog.SourceSeq)
				v, _ := h.meta(catalog.BootstrapLiveSeqKey)
				require.Equal(t, catalog.EncodeSeq(5), v)
			})
		}
	})
}

func TestScripts_SeqKeyCorrupt(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		h := newHarness(t, be)
		_, err := h.s.CommitMeta(t.Context(), []metastore.Op{{Kind: metastore.OpSet, Key: []byte(catalog.MainSeqKey), Value: []byte("bad")}})
		require.NoError(t, err)
		_, err = h.s.CommitHotBatch(t.Context(), catalog.HotBatch{FirstSeq: 1, LastSeq: 1, Frame: []byte("f")})
		requireCorruption(t, err, catalog.SourceMeta)
		// The garbage seq key is itself an invariant violation.
		require.Error(t, h.db.Violation())
		h.violationOK = true
	})
}

func TestScripts_MissingReference(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		t.Run("absent object", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, be)
			_, err := h.s.CommitHotBatch(t.Context(), catalog.HotBatch{FirstSeq: 1, LastSeq: 1, Object: catalog.ObjectRef{ID: 1 << 40}})
			requireCorruption(t, err, catalog.SourceRef)
		})
		t.Run("uploading object", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, be)
			ref := h.object([]byte("x"))
			ref.Pending = false // the caller forgot to make it available
			_, err := h.s.CommitHotBatch(t.Context(), catalog.HotBatch{FirstSeq: 1, LastSeq: 1, Object: ref})
			requireCorruption(t, err, catalog.SourceRef)
		})
		t.Run("fold block", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, be)
			h.hot(1, 2)
			_, info := block(t, 1, 2)
			_, err := h.s.Fold(t.Context(), catalog.Block{Namespace: catalog.Main, Info: info, Object: catalog.ObjectRef{ID: 1 << 40}})
			requireCorruption(t, err, catalog.SourceRef)
			snap, err := h.snapshot()
			require.NoError(t, err)
			require.Len(t, snap.HotBatches, 1, "the failed fold deleted nothing")
		})
		t.Run("seal footer", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, be)
			h.hot(1, 2)
			b := h.fold(1, 2)
			sl := h.sealOf(0, []builtBlock{b})
			sl.Footer = catalog.ObjectRef{ID: 1 << 40}
			_, err := h.s.Seal(t.Context(), sl)
			requireCorruption(t, err, catalog.SourceRef)
		})
	})
}

// The direct-mode block transaction reference-checks its object like the
// others, and a failed check commits nothing.
func TestScripts_DirectBlockMissingReference(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		h := newHarness(t, be)
		_, info := block(t, 1, 2)
		_, err := h.s.CommitBlock(t.Context(), catalog.Block{Namespace: catalog.Main, Info: info, Object: catalog.ObjectRef{ID: 1 << 40}})
		requireCorruption(t, err, catalog.SourceRef)
		_, found := h.meta(catalog.MainSeqKey)
		require.False(t, found, "the seq key did not move")
		snap, err := h.snapshot()
		require.NoError(t, err)
		require.Empty(t, snap.ActiveBlocks)
	})
}

// One transaction resolving several pending uploads of the same bytes
// resolves them in order, as separate transactions would: the first becomes
// available and the rest reference it, so the unique index on available
// bytes never trips. An upload of bytes already available references that
// row.
func TestScripts_SameBytesInOneCommit(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		h := newHarness(t, be)
		ctx := t.Context()
		// Two uploads of "old" race; the first to become available wins.
		old, raced := h.object([]byte("old")), h.object([]byte("old"))
		require.True(t, raced.Pending)
		_, err := h.s.MarkAvailable(ctx, old)
		require.NoError(t, err)
		deduped := h.object([]byte("old"))
		require.False(t, deduped.Pending, "the dedup lookup found the available row")
		a, b := h.object([]byte("same")), h.object([]byte("same"))
		require.True(t, a.Pending)
		require.True(t, b.Pending)
		cs, err := h.s.CommitHotBatches(ctx, []catalog.HotBatch{
			{FirstSeq: 1, LastSeq: 1, Object: a},
			{FirstSeq: 2, LastSeq: 2, Frame: []byte("inline")},
			{FirstSeq: 3, LastSeq: 3, Object: b},
			{FirstSeq: 4, LastSeq: 4, Object: raced},
			{FirstSeq: 5, LastSeq: 5, Object: a},
			{FirstSeq: 6, LastSeq: 6, Object: deduped},
		})
		require.NoError(t, err)
		got := make([]uint64, len(cs))
		for i, c := range cs {
			got[i] = c.ObjectID
		}
		require.Equal(t, []uint64{a.ID, 0, a.ID, old.ID, a.ID, old.ID}, got)
		require.Equal(t, map[catalog.ObjectState]int64{catalog.ObjectAvailable: 2, catalog.ObjectUploading: 2}, h.objectStates(),
			"b's and the racer's rows stay uploading for GC")
	})
}

// A pending upload GC claimed before the block commit that would have made
// it available ends the session, and nothing commits.
func TestScripts_DirectBlockUploadClaimed(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		h := newHarness(t, be)
		ctx := t.Context()
		frame, info := block(t, 1, 2)
		ref := h.object(frame)
		claimed, err := h.s.GCClaim(ctx, claimNow, claimNow, 100)
		require.NoError(t, err)
		require.Len(t, claimed, 1)
		_, err = h.s.CommitBlock(ctx, catalog.Block{Namespace: catalog.Main, Info: info, Object: ref})
		require.ErrorIs(t, err, catalog.ErrSessionEnded)
		_, corrupt := catalog.IsCorruption(err)
		require.False(t, corrupt, "§7.3's accepted leak: the next session uploads again")
		require.Equal(t, map[catalog.ObjectState]int64{catalog.ObjectDeleting: 1}, h.objectStates())
		_, found := h.meta(catalog.MainSeqKey)
		require.False(t, found)
	})
}

// Referencing an object clears its GC mark (§7.4), whichever script
// references it.
func TestScripts_ReferenceClearsMark(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		h := newHarness(t, be)
		ctx := t.Context()
		frame, info := block(t, 1, 2)
		ref := h.object(frame)
		_, err := h.s.MarkAvailable(ctx, ref)
		require.NoError(t, err)
		require.Equal(t, 1, h.gcMark(100))
		again := h.object(frame)
		require.False(t, again.Pending, "a fresh mark is within the dedup age")
		_, err = h.s.CommitBlock(ctx, catalog.Block{Namespace: catalog.Main, Info: info, Object: again})
		require.NoError(t, err)
		_, err = h.s.GCClaim(ctx, claimNow, claimNow, 100)
		require.NoError(t, err)
		require.Equal(t, map[catalog.ObjectState]int64{catalog.ObjectAvailable: 1}, h.objectStates(),
			"the block's reference cleared the mark, so the claim passed it over")
	})
}

// An upload GC claimed while it stalled ends the session without claiming
// corruption: the next session uploads again (§7.3's accepted leak).
func TestScripts_UploadNoLongerUploading(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		h := newHarness(t, be)
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
	})
}

func TestScripts_FoldGap(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
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
				h := newHarness(t, be)
				for _, r := range tc.hot {
					h.hot(r[0], r[1])
				}
				before, err := h.snapshot()
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
				after, err := h.snapshot()
				require.NoError(t, err)
				require.Equal(t, before.HotBatches, after.HotBatches, "a rejected fold deletes nothing")
				require.Empty(t, after.ActiveBlocks)
			})
		}
	})
}

// Coverage alone cannot see a block that skips seqs after the segment's
// last block; the continuity check can.
func TestScripts_BlockDiscontinuity(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		h := newHarness(t, be)
		h.hot(1, 2)
		h.fold(1, 2)
		h.hot(3, 3)
		h.hot(4, 4)
		frame, info := block(t, 4, 4)
		_, err := h.s.Fold(t.Context(), catalog.Block{Namespace: catalog.Main, Info: info, Object: h.object(frame)})
		requireCorruption(t, err, catalog.SourceInvariant)
	})
}

func TestScripts_SealListMismatch(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		setup := func(t *testing.T) (*harness, []builtBlock) {
			h := newHarness(t, be)
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
			before, err := h.snapshot()
			require.NoError(t, err)
			_, err = h.s.Seal(t.Context(), h.sealOf(0, bs[:1]))
			require.Error(t, err)
			after, err := h.snapshot()
			require.NoError(t, err)
			require.Equal(t, before.Segments, after.Segments)
			require.Equal(t, before.ActiveBlocks, after.ActiveBlocks)
		})
	})
}

func TestScripts_ValidationEndsSession(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		h := newHarness(t, be)
		_, err := h.s.CommitHotBatch(t.Context(), catalog.HotBatch{FirstSeq: 2, LastSeq: 1, Frame: []byte("f")})
		require.Error(t, err)
		_, err = h.s.CommitMeta(t.Context(), nil)
		require.Error(t, err, "a rejected script ends the session")
	})
}

func TestScripts_ConcurrentSessionsSerialize(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		h := newHarness(t, be)
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
	})
}

// A group commit is one transaction: every row shares its revision, the
// hooks' ops apply in batch order (last write wins), and seq/next moves once.
func TestScripts_CommitHotBatchesGroup(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		h := newHarness(t, be)
		ctx := t.Context()
		h.hot(1, 2)
		ptr := h.object([]byte("grouped pointer batch"))
		cursor := func(v string) []metastore.Op {
			return []metastore.Op{{Kind: metastore.OpSet, Key: []byte("relay/cursor"), Value: []byte(v)}}
		}
		out, err := h.s.CommitHotBatches(ctx, []catalog.HotBatch{
			{FirstSeq: 3, LastSeq: 4, Frame: []byte("a"), Meta: cursor("1")},
			{FirstSeq: 5, LastSeq: 5, Object: ptr, Meta: cursor("2")},
			{FirstSeq: 6, LastSeq: 8, Frame: []byte("c"), Meta: cursor("3")},
		})
		require.NoError(t, err)
		require.Len(t, out, 3)
		require.Zero(t, out[0].ObjectID)
		require.Equal(t, ptr.ID, out[1].ObjectID)
		for _, c := range out {
			require.Equal(t, out[0].Revision, c.Revision)
		}
		v, _ := h.meta(catalog.MainSeqKey)
		require.Equal(t, catalog.EncodeSeq(9), v)
		v, _ = h.meta("relay/cursor")
		require.Equal(t, "3", string(v))

		snap, err := h.snapshot()
		require.NoError(t, err)
		require.Len(t, snap.HotBatches, 4)
		for _, r := range snap.HotBatches[1:] {
			require.Equal(t, out[0].Revision, r.Revision)
		}
		require.Less(t, snap.HotBatches[0].Revision, out[0].Revision)
		h.hot(9, 9)
		require.NoError(t, h.s.Err())
	})
}

// fenceDB counts the fences its transactions run, by kind of fence.
type fenceDB struct {
	catalog.DB
	mu           sync.Mutex
	bump, bumpAt int
}

func (d *fenceDB) Begin(ctx context.Context, kind catalog.TxKind) (catalog.Tx, error) {
	tx, err := d.DB.Begin(ctx, kind)
	if err != nil {
		return nil, err
	}
	return &fenceTx{Tx: tx, db: d}, nil
}

// counts returns and resets the fence counts.
func (d *fenceDB) counts() (bump, bumpAt int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	bump, bumpAt = d.bump, d.bumpAt
	d.bump, d.bumpAt = 0, 0
	return bump, bumpAt
}

type fenceTx struct {
	catalog.Tx
	db *fenceDB
}

func (tx *fenceTx) FenceBump(ctx context.Context, epoch uint64, reads ...catalog.Read) (uint64, bool, error) {
	tx.db.mu.Lock()
	tx.db.bump++
	tx.db.mu.Unlock()
	return tx.Tx.FenceBump(ctx, epoch, reads...)
}

func (tx *fenceTx) FenceBumpAt(ctx context.Context, epoch, rev uint64, checks ...catalog.MetaCheck) error {
	tx.db.mu.Lock()
	tx.db.bumpAt++
	tx.db.mu.Unlock()
	return tx.Tx.FenceBumpAt(ctx, epoch, rev, checks...)
}

// An inline hot batch commit takes FenceBumpAt's one round trip once the
// session knows its revision, and falls back to FenceBump when another
// transaction took that revision. Either way it commits the same thing.
func TestScripts_CommitHotBatchesFenceAt(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		h := newHarness(t, be)
		ctx := t.Context()
		db := &fenceDB{DB: h.db}
		m := catalog.NewMetrics(prometheus.NewRegistry())
		session := func() *catalog.Session {
			return catalog.NewSession(catalog.SessionConfig{DB: db, Epoch: h.lock.Epoch(), Metrics: m})
		}
		h.s = session()
		fallbacks := func() float64 { return testutil.ToFloat64(m.Fallbacks.WithLabelValues(string(catalog.TxHotBatch))) }
		requireFences := func(bump, bumpAt int, msg string) {
			t.Helper()
			gotBump, gotAt := db.counts()
			require.Equal(t, [2]int{bump, bumpAt}, [2]int{gotBump, gotAt}, msg)
		}

		c1 := h.hot(1, 2)
		requireFences(1, 0, "a new session has not learned the revision")
		c2 := h.hot(3, 3)
		requireFences(0, 1, "then it has")
		require.Equal(t, c1.Revision+1, c2.Revision)

		// Another session with the epoch takes the next revision.
		other := session()
		_, err := other.CommitMeta(ctx, []metastore.Op{{Kind: metastore.OpSet, Key: []byte("k"), Value: []byte("v")}})
		require.NoError(t, err)
		db.counts()
		c3 := h.hot(4, 6)
		requireFences(1, 1, "the revision moved, so it falls back")
		require.Equal(t, 1.0, fallbacks())
		require.Equal(t, c2.Revision+2, c3.Revision)
		h.hot(7, 7)
		requireFences(0, 1, "and has learned the revision again")

		_, err = h.s.CommitHotBatch(ctx, catalog.HotBatch{FirstSeq: 8, LastSeq: 8, Object: h.object([]byte("pointer"))})
		require.NoError(t, err)
		_, at := db.counts()
		require.Zero(t, at, "a pointer batch resolves its object rows first")

		snap, err := h.snapshot()
		require.NoError(t, err)
		require.Len(t, snap.HotBatches, 5)
		for i, want := range []uint64{c1.Revision, c2.Revision, c3.Revision} {
			require.Equal(t, want, snap.HotBatches[i].Revision)
		}
		v, _ := h.meta(catalog.MainSeqKey)
		require.Equal(t, catalog.EncodeSeq(9), v)
		require.Equal(t, 1.0, fallbacks())
		require.NoError(t, h.s.Err())
	})
}

// A group that does not tile, or is empty, is rejected before any
// transaction and ends the session.
func TestScripts_CommitHotBatchesRejects(t *testing.T) {
	t.Parallel()
	for name, bs := range map[string][]catalog.HotBatch{
		"empty":     nil,
		"gap":       {{FirstSeq: 1, LastSeq: 1, Frame: []byte("a")}, {FirstSeq: 3, LastSeq: 3, Frame: []byte("b")}},
		"overlap":   {{FirstSeq: 1, LastSeq: 2, Frame: []byte("a")}, {FirstSeq: 2, LastSeq: 3, Frame: []byte("b")}},
		"bad batch": {{FirstSeq: 1, LastSeq: 1, Frame: []byte("a")}, {FirstSeq: 2, LastSeq: 2}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			eachBackend(t, func(t *testing.T, be backend) {
				h := newHarness(t, be)
				_, err := h.s.CommitHotBatches(t.Context(), bs)
				require.Error(t, err)
				require.Error(t, h.s.Err(), "a rejected script ends the session")
				snap, err := h.snapshot()
				require.NoError(t, err)
				require.Empty(t, snap.HotBatches)
			})
		})
	}
}

// gatedDB holds the first transaction of kind at Begin until gate closes,
// and counts the transactions of that kind.
type gatedDB struct {
	catalog.DB
	kind  catalog.TxKind
	gate  chan struct{}
	mu    sync.Mutex
	count int
}

func (d *gatedDB) Begin(ctx context.Context, kind catalog.TxKind) (catalog.Tx, error) {
	if kind == d.kind {
		d.mu.Lock()
		d.count++
		first := d.count == 1
		d.mu.Unlock()
		if first {
			<-d.gate
		}
	}
	return d.DB.Begin(ctx, kind)
}

// BeginUploads calls that arrive while a transaction is in flight share the
// next one, and each still gets its own slots.
func TestScripts_BeginUploadsGroup(t *testing.T) {
	t.Parallel()
	for _, fault := range []bool{false, true} {
		t.Run(fmt.Sprintf("fault=%v", fault), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				fake := storagefake.New(storagefake.Config{})
				lock := fake.NewLease()
				require.NoError(t, lock.Acquire(t.Context(), time.Hour))
				db := &gatedDB{DB: fake, kind: catalog.TxObjects, gate: make(chan struct{})}
				s := catalog.NewSession(catalog.SessionConfig{DB: db, Epoch: lock.Epoch()})
				if fault {
					fake.InjectFaults(&storagefake.Fault{Kind: storagefake.FaultCommitFails, TxKind: catalog.TxObjects, Ordinal: 2})
				}
				req := func(n int) []catalog.UploadRequest {
					reqs := make([]catalog.UploadRequest, n)
					for i := range reqs {
						_, _ = rand.Read(reqs[i].Key[:])
						_, _ = rand.Read(reqs[i].SHA256[:])
						reqs[i].Length = 1
					}
					return reqs
				}
				type result struct {
					slots []catalog.UploadSlot
					err   error
				}
				call := func(n int) chan result {
					ch := make(chan result, 1)
					go func() {
						slots, _, err := s.BeginUploads(t.Context(), req(n), time.Hour)
						ch <- result{slots, err}
					}()
					return ch
				}
				first := call(1)
				synctest.Wait()
				rest := []chan result{call(1), call(2), call(3)}
				synctest.Wait()
				close(db.gate)

				r := <-first
				require.NoError(t, r.err)
				require.Len(t, r.slots, 1)
				ids := map[uint64]bool{r.slots[0].ObjectID: true}
				for i, ch := range rest {
					r := <-ch
					if fault {
						require.Error(t, r.err, "the calls sharing a failed transaction all fail")
						continue
					}
					require.NoError(t, r.err)
					require.Len(t, r.slots, i+1)
					for _, sl := range r.slots {
						require.False(t, sl.Dedup)
						require.False(t, ids[sl.ObjectID], "slot IDs are distinct")
						ids[sl.ObjectID] = true
					}
				}
				require.Equal(t, 2, db.count, "the queued calls share one transaction")
				if !fault {
					require.NoError(t, s.Err())
					// The lead is free again: a later call runs at once.
					r := <-call(1)
					require.NoError(t, r.err)
					require.Equal(t, 3, db.count)
				}
			})
		})
	}
}
