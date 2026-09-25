// Package catalogtest is the catalog.Tx primitive contract suite (plan D1,
// layer 4). storagefake runs it in every `just`; pgstore runs it under
// `just test-storage`. Passing it is what lets the layer 3 oracle's verdicts
// on storagefake stand for PostgreSQL.
package catalogtest

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/leader"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/stretchr/testify/require"
)

// Backend is one fresh, initialized archive.
type Backend struct {
	DB        catalog.DB
	NewLocker func() leader.Locker
	// Listener may be nil to skip the NOTIFY test.
	Listener catalog.Listener
}

// Run runs every contract test, each against its own Backend.
func Run(t *testing.T, newBackend func(t *testing.T) Backend) {
	tests := []struct {
		name string
		fn   func(t *testing.T, b Backend)
	}{
		{"FenceRevisions", testFenceRevisions},
		{"FenceStaleEpoch", testFenceStaleEpoch},
		{"FenceSerializes", testFenceSerializes},
		{"AcquireFencesOldEpoch", testAcquireFencesOldEpoch},
		{"Meta", testMeta},
		{"Objects", testObjects},
		{"ObjectKeyUnique", testObjectKeyUnique},
		{"ObjectSHAUnique", testObjectSHAUnique},
		{"ObjectLengthCheck", testObjectLengthCheck},
		{"HotBatches", testHotBatches},
		{"HotBatchConstraints", testHotBatchConstraints},
		{"Segments", testSegments},
		{"SegmentConstraints", testSegmentConstraints},
		{"ActiveBlockConstraints", testActiveBlockConstraints},
		{"GenerationConstraints", testGenerationConstraints},
		{"DeleteNamespace", testDeleteNamespace},
		{"ReaderSnapshot", testReaderSnapshot},
		{"AbortedTransaction", testAbortedTransaction},
		{"Notify", testNotify},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newBackend(t))
		})
	}
}

func ctxT(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// acquire takes the lease and returns the new epoch.
func acquire(t *testing.T, b Backend) (leader.Locker, uint64) {
	t.Helper()
	l := b.NewLocker()
	require.NoError(t, l.Acquire(ctxT(t), time.Minute))
	return l, l.Epoch()
}

// begin opens a fenced transaction.
func begin(t *testing.T, b Backend, epoch uint64) (catalog.Tx, uint64) {
	t.Helper()
	ctx := ctxT(t)
	tx, err := b.DB.Begin(ctx, catalog.TxMetadata)
	require.NoError(t, err)
	rev, ok, err := tx.FenceBump(ctx, epoch)
	require.NoError(t, err)
	require.True(t, ok, "fence for epoch %d", epoch)
	return tx, rev
}

// write runs fn in one fenced committed transaction and returns its
// revision.
func write(t *testing.T, b Backend, epoch uint64, fn func(ctx context.Context, tx catalog.Tx, rev uint64)) uint64 {
	t.Helper()
	tx, rev := begin(t, b, epoch)
	fn(ctxT(t), tx, rev)
	require.NoError(t, tx.Commit(ctxT(t)))
	return rev
}

// failing runs fn, which must fail a statement, and rolls back.
func failing(t *testing.T, b Backend, epoch uint64, fn func(ctx context.Context, tx catalog.Tx) error) {
	t.Helper()
	tx, _ := begin(t, b, epoch)
	require.Error(t, fn(ctxT(t), tx))
	require.NoError(t, tx.Rollback(ctxT(t)))
}

func read(t *testing.T, b Backend) catalog.ReadTx {
	t.Helper()
	r, err := b.DB.BeginRead(ctxT(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	return r
}

func snapshot(t *testing.T, b Backend) *catalog.Snapshot {
	t.Helper()
	s, err := catalog.LoadSnapshot(ctxT(t), read(t, b))
	require.NoError(t, err)
	return s
}

func newObject(t *testing.T, data []byte) catalog.NewObject {
	var o catalog.NewObject
	_, err := rand.Read(o.Key[:])
	require.NoError(t, err)
	o.SHA256 = sha256.Sum256(data)
	o.Length = int64(len(data))
	return o
}

// availableObject inserts an object and makes it available.
func availableObject(t *testing.T, b Backend, epoch uint64, data []byte) uint64 {
	t.Helper()
	var id uint64
	write(t, b, epoch, func(ctx context.Context, tx catalog.Tx, _ uint64) {
		ids, err := tx.InsertObjects(ctx, []catalog.NewObject{newObject(t, data)})
		require.NoError(t, err)
		id = ids[0]
		ok, err := tx.SetObjectAvailable(ctx, id)
		require.NoError(t, err)
		require.True(t, ok)
	})
	return id
}

func header() []byte { return bytes.Repeat([]byte{0}, 256) }

func testFenceRevisions(t *testing.T, b Backend) {
	_, epoch := acquire(t, b)
	start := snapshot(t, b).Archive.CatalogRevision
	r1 := write(t, b, epoch, func(context.Context, catalog.Tx, uint64) {})
	require.Equal(t, start+1, r1)

	// A rolled-back fence does not advance the revision.
	tx, rev := begin(t, b, epoch)
	require.Equal(t, r1+1, rev)
	require.NoError(t, tx.Rollback(ctxT(t)))

	r2 := write(t, b, epoch, func(context.Context, catalog.Tx, uint64) {})
	require.Equal(t, r1+1, r2)
	a := snapshot(t, b).Archive
	require.Equal(t, r2, a.CatalogRevision)
	require.Equal(t, epoch, a.WriterEpoch)
}

func testFenceStaleEpoch(t *testing.T, b Backend) {
	_, epoch := acquire(t, b)
	ctx := ctxT(t)
	tx, err := b.DB.Begin(ctx, catalog.TxMetadata)
	require.NoError(t, err)
	_, ok, err := tx.FenceBump(ctx, epoch+1)
	require.NoError(t, err)
	require.False(t, ok)
	require.NoError(t, tx.Rollback(ctx))

	// A fence that matched nothing holds no lock.
	write(t, b, epoch, func(context.Context, catalog.Tx, uint64) {})
}

func testFenceSerializes(t *testing.T, b Backend) {
	_, epoch := acquire(t, b)
	tx1, r1 := begin(t, b, epoch)
	got := make(chan uint64, 1)
	go func() {
		ctx := context.Background()
		tx2, err := b.DB.Begin(ctx, catalog.TxMetadata)
		if err != nil {
			got <- 0
			return
		}
		rev, ok, err := tx2.FenceBump(ctx, epoch)
		if err != nil || !ok {
			_ = tx2.Rollback(ctx)
			got <- 0
			return
		}
		if err := tx2.Commit(ctx); err != nil {
			got <- 0
			return
		}
		got <- rev
	}()
	select {
	case rev := <-got:
		t.Fatalf("second fence finished (rev %d) while the first transaction held the row lock", rev)
	case <-time.After(50 * time.Millisecond):
	}
	require.NoError(t, tx1.Commit(ctxT(t)))
	select {
	case rev := <-got:
		require.Equal(t, r1+1, rev)
	case <-time.After(5 * time.Second):
		t.Fatal("second fence never finished")
	}
}

func testAcquireFencesOldEpoch(t *testing.T, b Backend) {
	old, epoch := acquire(t, b)
	require.NoError(t, old.Release(ctxT(t)))
	_, next := acquire(t, b)
	require.Greater(t, next, epoch)

	ctx := ctxT(t)
	tx, err := b.DB.Begin(ctx, catalog.TxMetadata)
	require.NoError(t, err)
	_, ok, err := tx.FenceBump(ctx, epoch)
	require.NoError(t, err)
	require.False(t, ok, "old epoch passed the fence after a new holder acquired")
	require.NoError(t, tx.Rollback(ctx))
}

func testMeta(t *testing.T, b Backend) {
	_, epoch := acquire(t, b)
	write(t, b, epoch, func(ctx context.Context, tx catalog.Tx, _ uint64) {
		require.NoError(t, tx.ApplyMeta(ctx, []metastore.Op{
			{Kind: metastore.OpSet, Key: []byte("a"), Value: []byte("1")},
			{Kind: metastore.OpSet, Key: []byte("b"), Value: []byte("2")},
			{Kind: metastore.OpSet, Key: []byte("b\x00"), Value: []byte("2z")},
			{Kind: metastore.OpSet, Key: []byte("c"), Value: []byte{}},
			{Kind: metastore.OpSet, Key: []byte("d"), Value: []byte("4")},
		}))
		// Ordered semantics: set, delete, set on one key, and a range
		// delete that only removes keys staged before it.
		require.NoError(t, tx.ApplyMeta(ctx, []metastore.Op{
			{Kind: metastore.OpSet, Key: []byte("a"), Value: []byte("x")},
			{Kind: metastore.OpDelete, Key: []byte("a")},
			{Kind: metastore.OpSet, Key: []byte("a"), Value: []byte("y")},
			{Kind: metastore.OpDeleteRange, Key: []byte("b"), End: []byte("c")},
			{Kind: metastore.OpSet, Key: []byte("b\x01"), Value: []byte("new")},
		}))
		v, found, err := tx.MetaGetForUpdate(ctx, []byte("a"))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "y", string(v))
		_, found, err = tx.MetaGetForUpdate(ctx, []byte("b"))
		require.NoError(t, err)
		require.False(t, found)
	})
	got, err := read(t, b).MetaGet(ctxT(t), [][]byte{[]byte("a"), []byte("b"), []byte("b\x00"), []byte("b\x01"), []byte("c"), []byte("d"), []byte("zz")})
	require.NoError(t, err)
	require.Equal(t, map[string][]byte{"a": []byte("y"), "b\x01": []byte("new"), "c": {}, "d": []byte("4")}, got)

	// A rolled-back write is invisible.
	tx, _ := begin(t, b, epoch)
	require.NoError(t, tx.ApplyMeta(ctxT(t), []metastore.Op{{Kind: metastore.OpSet, Key: []byte("a"), Value: []byte("z")}}))
	require.NoError(t, tx.Rollback(ctxT(t)))
	got, err = read(t, b).MetaGet(ctxT(t), [][]byte{[]byte("a")})
	require.NoError(t, err)
	require.Equal(t, "y", string(got["a"]))
}

func testObjects(t *testing.T, b Backend) {
	_, epoch := acquire(t, b)
	o1, o2 := newObject(t, []byte("one")), newObject(t, []byte("two"))
	var ids []uint64
	write(t, b, epoch, func(ctx context.Context, tx catalog.Tx, _ uint64) {
		var err error
		ids, err = tx.InsertObjects(ctx, []catalog.NewObject{o1, o2})
		require.NoError(t, err)
		require.Len(t, ids, 2)
		require.NotEqual(t, ids[0], ids[1])
		missing, err := tx.RefCheck(ctx, []uint64{ids[0], ids[1], 1 << 40})
		require.NoError(t, err)
		require.ElementsMatch(t, []uint64{ids[0], ids[1], 1 << 40}, missing, "uploading and absent objects fail the reference check")
		_, found, err := tx.FindAvailableObject(ctx, o1.SHA256, 0)
		require.NoError(t, err)
		require.False(t, found, "uploading rows are not dedup candidates")
	})
	write(t, b, epoch, func(ctx context.Context, tx catalog.Tx, _ uint64) {
		ok, err := tx.SetObjectAvailable(ctx, ids[0])
		require.NoError(t, err)
		require.True(t, ok)
		ok, err = tx.SetObjectAvailable(ctx, ids[0])
		require.NoError(t, err)
		require.False(t, ok, "only an uploading row becomes available")
		ok, err = tx.SetObjectAvailable(ctx, 1<<40)
		require.NoError(t, err)
		require.False(t, ok)
		row, found, err := tx.FindAvailableObject(ctx, o1.SHA256, time.Hour)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, ids[0], row.ID)
		require.Equal(t, o1.Key, row.Key)
		require.Equal(t, o1.Length, row.Length)
		require.Equal(t, catalog.ObjectAvailable, row.State)
		require.True(t, row.UnreferencedAt.IsZero())
		missing, err := tx.RefCheck(ctx, []uint64{ids[0], ids[0]})
		require.NoError(t, err)
		require.Empty(t, missing)
	})
	objs, err := read(t, b).Objects(ctxT(t), []uint64{ids[1], ids[0], ids[0]})
	require.NoError(t, err)
	require.Len(t, objs, 2)
	require.Equal(t, ids[0], objs[0].ID)
	require.Equal(t, catalog.ObjectAvailable, objs[0].State)
	require.Equal(t, catalog.ObjectUploading, objs[1].State)
	require.Equal(t, o2.SHA256, objs[1].SHA256)
	require.False(t, objs[1].CreatedAt.IsZero())

	// Sequences are not transactional: a rolled-back insert burns its ID.
	tx, _ := begin(t, b, epoch)
	burnt, err := tx.InsertObjects(ctxT(t), []catalog.NewObject{newObject(t, []byte("x"))})
	require.NoError(t, err)
	require.NoError(t, tx.Rollback(ctxT(t)))
	next := availableObject(t, b, epoch, []byte("y"))
	require.Greater(t, next, burnt[0])
}

func testObjectKeyUnique(t *testing.T, b Backend) {
	_, epoch := acquire(t, b)
	o := newObject(t, []byte("k"))
	write(t, b, epoch, func(ctx context.Context, tx catalog.Tx, _ uint64) {
		_, err := tx.InsertObjects(ctx, []catalog.NewObject{o})
		require.NoError(t, err)
	})
	failing(t, b, epoch, func(ctx context.Context, tx catalog.Tx) error {
		_, err := tx.InsertObjects(ctx, []catalog.NewObject{o})
		return err
	})
}

func testObjectSHAUnique(t *testing.T, b Backend) {
	_, epoch := acquire(t, b)
	first := availableObject(t, b, epoch, []byte("same"))
	var second uint64
	write(t, b, epoch, func(ctx context.Context, tx catalog.Tx, _ uint64) {
		ids, err := tx.InsertObjects(ctx, []catalog.NewObject{newObject(t, []byte("same"))})
		require.NoError(t, err)
		second = ids[0]
	})
	require.NotEqual(t, first, second)
	failing(t, b, epoch, func(ctx context.Context, tx catalog.Tx) error {
		_, err := tx.SetObjectAvailable(ctx, second)
		return err
	})
	objs, err := read(t, b).Objects(ctxT(t), []uint64{second})
	require.NoError(t, err)
	require.Equal(t, catalog.ObjectUploading, objs[0].State)
}

func testObjectLengthCheck(t *testing.T, b Backend) {
	_, epoch := acquire(t, b)
	failing(t, b, epoch, func(ctx context.Context, tx catalog.Tx) error {
		o := newObject(t, nil)
		_, err := tx.InsertObjects(ctx, []catalog.NewObject{o})
		return err
	})
}

func hot(first, last uint64, frame []byte, obj uint64) catalog.HotBatchRow {
	return catalog.HotBatchRow{
		FirstSeq: first, LastSeq: last, EventCount: uint32(last - first + 1),
		MinWitnessedUS: 10, MaxWitnessedUS: 20, Epoch: 1, Revision: 1,
		Frame: frame, ObjectID: obj,
	}
}

func testHotBatches(t *testing.T, b Backend) {
	_, epoch := acquire(t, b)
	obj := availableObject(t, b, epoch, []byte("pointer"))
	rev := write(t, b, epoch, func(ctx context.Context, tx catalog.Tx, rev uint64) {
		for _, h := range []catalog.HotBatchRow{hot(1, 3, []byte("f1"), 0), hot(4, 4, nil, obj), hot(5, 9, []byte("f3"), 0)} {
			h.Revision = rev
			require.NoError(t, tx.InsertHotBatch(ctx, h))
		}
	})
	rows, err := read(t, b).HotBatches(ctxT(t), 5)
	require.NoError(t, err)
	require.Len(t, rows, 3)
	require.Equal(t, []uint64{1, 4, 5}, []uint64{rows[0].FirstSeq, rows[1].FirstSeq, rows[2].FirstSeq})
	require.True(t, rows[0].Inline)
	require.Nil(t, rows[0].Frame, "frames below framesFrom are not loaded")
	require.False(t, rows[1].Inline)
	require.Equal(t, obj, rows[1].ObjectID)
	require.Nil(t, rows[1].Frame)
	require.True(t, rows[2].Inline)
	require.Equal(t, "f3", string(rows[2].Frame))
	require.Equal(t, rev, rows[2].Revision)
	require.Equal(t, uint32(5), rows[2].EventCount)
	require.Equal(t, int64(10), rows[2].MinWitnessedUS)
	require.Equal(t, int64(20), rows[2].MaxWitnessedUS)
	require.False(t, rows[2].CommittedAt.IsZero())

	write(t, b, epoch, func(ctx context.Context, tx catalog.Tx, _ uint64) {
		spans, err := tx.DeleteHotBatches(ctx, 1, 4)
		require.NoError(t, err)
		require.Equal(t, []catalog.HotBatchSpan{
			{FirstSeq: 1, LastSeq: 3, EventCount: 3},
			{FirstSeq: 4, LastSeq: 4, EventCount: 1, ObjectID: obj},
		}, spans)
		spans, err = tx.DeleteHotBatches(ctx, 6, 100)
		require.NoError(t, err)
		require.Empty(t, spans, "a batch is selected by first_seq")
	})
	rows, err = read(t, b).HotBatches(ctxT(t), 0)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, uint64(5), rows[0].FirstSeq)
}

func testHotBatchConstraints(t *testing.T, b Backend) {
	_, epoch := acquire(t, b)
	obj := availableObject(t, b, epoch, []byte("p"))
	write(t, b, epoch, func(ctx context.Context, tx catalog.Tx, _ uint64) {
		require.NoError(t, tx.InsertHotBatch(ctx, hot(1, 1, []byte("f"), 0)))
	})
	bad := map[string]catalog.HotBatchRow{
		"duplicate first_seq": hot(1, 2, []byte("f"), 0),
		"frame and object":    hot(10, 10, []byte("f"), obj),
		"neither":             hot(10, 10, nil, 0),
		"missing object":      hot(10, 10, nil, 1<<40),
		"count mismatch":      func() catalog.HotBatchRow { h := hot(10, 12, []byte("f"), 0); h.EventCount = 2; return h }(),
	}
	for name, h := range bad {
		t.Run(name, func(t *testing.T) {
			failing(t, b, epoch, func(ctx context.Context, tx catalog.Tx) error {
				return tx.InsertHotBatch(ctx, h)
			})
		})
	}
}

func activeBlock(ns catalog.Namespace, seg uint64, ord int, obj, lo, hi uint64) catalog.ActiveBlockRow {
	return catalog.ActiveBlockRow{
		Namespace: ns, Segment: seg, Ordinal: ord, ObjectID: obj,
		EventCount: uint32(hi - lo + 1), MinSeq: lo, MaxSeq: hi,
		MinWitnessedUS: 1, MaxWitnessedUS: 2, CompressedLength: 100, UncompressedLength: 200, Revision: 1,
	}
}

func testSegments(t *testing.T, b Backend) {
	_, epoch := acquire(t, b)
	obj := availableObject(t, b, epoch, []byte("blk"))
	footer := availableObject(t, b, epoch, []byte("footer"))
	r0 := write(t, b, epoch, func(ctx context.Context, tx catalog.Tx, rev uint64) {
		_, found, err := tx.ActiveSegment(ctx, catalog.Main)
		require.NoError(t, err)
		require.False(t, found)
		require.NoError(t, tx.InsertSegment(ctx, catalog.SegmentRow{Namespace: catalog.Main, Index: 0, State: catalog.Active, Revision: rev}))
		seg, found, err := tx.ActiveSegment(ctx, catalog.Main)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, uint64(0), seg.Index)
		_, found, err = tx.LastActiveBlock(ctx, catalog.Main, 0)
		require.NoError(t, err)
		require.False(t, found)
		for i := range 3 {
			ab := activeBlock(catalog.Main, 0, i, obj, uint64(i*10+1), uint64(i*10+10))
			ab.Revision = rev
			require.NoError(t, tx.InsertActiveBlock(ctx, ab))
		}
		last, found, err := tx.LastActiveBlock(ctx, catalog.Main, 0)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, 2, last.Ordinal)
	})
	r := read(t, b)
	blocks, err := r.ActiveBlocksSince(ctxT(t), r0-1)
	require.NoError(t, err)
	require.Len(t, blocks, 3)
	require.Equal(t, activeBlock(catalog.Main, 0, 1, obj, 11, 20).MinSeq, blocks[1].MinSeq)
	require.Equal(t, int64(100), blocks[1].CompressedLength)
	require.Equal(t, int64(200), blocks[1].UncompressedLength)
	none, err := r.ActiveBlocksSince(ctxT(t), r0)
	require.NoError(t, err)
	require.Empty(t, none)
	keys, err := r.ActiveBlockKeys(ctxT(t))
	require.NoError(t, err)
	require.Equal(t, []catalog.ActiveBlockKey{
		{Namespace: catalog.Main, Ordinal: 0}, {Namespace: catalog.Main, Ordinal: 1}, {Namespace: catalog.Main, Ordinal: 2},
	}, keys)

	var gen uint64
	r1 := write(t, b, epoch, func(ctx context.Context, tx catalog.Tx, rev uint64) {
		rows, err := tx.ActiveBlocksForUpdate(ctx, catalog.Main, 0)
		require.NoError(t, err)
		require.Len(t, rows, 3)
		gen, err = tx.InsertGeneration(ctx, catalog.GenerationRow{Namespace: catalog.Main, Segment: 0, Header: header(), FooterObjectID: footer, Revision: rev})
		require.NoError(t, err)
		require.NotZero(t, gen)
		gbs := make([]catalog.GenerationBlockRow, len(rows))
		for i, row := range rows {
			gbs[i] = catalog.GenerationBlockRow{GenerationID: gen, Ordinal: i, ObjectID: row.ObjectID, CompressedLength: row.CompressedLength}
		}
		require.NoError(t, tx.InsertGenerationBlocks(ctx, gbs))
		n, err := tx.DeleteActiveBlocks(ctx, catalog.Main, 0)
		require.NoError(t, err)
		require.Equal(t, 3, n)
		ok, err := tx.SealSegment(ctx, catalog.Main, 0, gen, rev)
		require.NoError(t, err)
		require.True(t, ok)
		ok, err = tx.SealSegment(ctx, catalog.Main, 0, gen, rev)
		require.NoError(t, err)
		require.False(t, ok, "a sealed segment cannot be sealed again")
		require.NoError(t, tx.InsertSegment(ctx, catalog.SegmentRow{Namespace: catalog.Main, Index: 1, State: catalog.Active, Revision: rev}))
	})
	r = read(t, b)
	segs, err := r.SegmentsSince(ctxT(t), 0)
	require.NoError(t, err)
	require.Equal(t, []catalog.SegmentRow{
		{Namespace: catalog.Main, Index: 0, State: catalog.Sealed, GenerationID: gen, Revision: r1},
		{Namespace: catalog.Main, Index: 1, State: catalog.Active, Revision: r1},
	}, segs)
	gens, err := r.Generations(ctxT(t), []uint64{gen, gen, 1 << 40})
	require.NoError(t, err)
	require.Len(t, gens, 1)
	require.Equal(t, footer, gens[0].FooterObjectID)
	require.Equal(t, header(), gens[0].Header)
	require.Equal(t, r1, gens[0].Revision)
	require.False(t, gens[0].CreatedAt.IsZero())
	gbs, err := r.GenerationBlocks(ctxT(t), []uint64{gen})
	require.NoError(t, err)
	require.Len(t, gbs, 3)
	for i, gb := range gbs {
		require.Equal(t, i, gb.Ordinal)
		require.Equal(t, int64(100), gb.CompressedLength)
	}
	blocks, err = r.ActiveBlocksSince(ctxT(t), 0)
	require.NoError(t, err)
	require.Empty(t, blocks)
}

func initMain(t *testing.T, b Backend, epoch uint64) {
	write(t, b, epoch, func(ctx context.Context, tx catalog.Tx, rev uint64) {
		require.NoError(t, tx.InsertSegment(ctx, catalog.SegmentRow{Namespace: catalog.Main, Index: 0, State: catalog.Active, Revision: rev}))
	})
}

func testSegmentConstraints(t *testing.T, b Backend) {
	_, epoch := acquire(t, b)
	initMain(t, b, epoch)
	bad := map[string]catalog.SegmentRow{
		"second active":        {Namespace: catalog.Main, Index: 1, State: catalog.Active},
		"duplicate index":      {Namespace: catalog.Main, Index: 0, State: catalog.Sealed, GenerationID: 1},
		"unknown namespace":    {Namespace: "other", Index: 0, State: catalog.Active},
		"sealed no generation": {Namespace: catalog.BootstrapLive, Index: 0, State: catalog.Sealed},
		"active generation":    {Namespace: catalog.BootstrapLive, Index: 0, State: catalog.Active, GenerationID: 1},
	}
	for name, row := range bad {
		t.Run(name, func(t *testing.T) {
			failing(t, b, epoch, func(ctx context.Context, tx catalog.Tx) error {
				return tx.InsertSegment(ctx, row)
			})
		})
	}
	// Each namespace has its own active segment.
	write(t, b, epoch, func(ctx context.Context, tx catalog.Tx, rev uint64) {
		require.NoError(t, tx.InsertSegment(ctx, catalog.SegmentRow{Namespace: catalog.BootstrapLive, Index: 0, State: catalog.Active, Revision: rev}))
	})
}

func testActiveBlockConstraints(t *testing.T, b Backend) {
	_, epoch := acquire(t, b)
	initMain(t, b, epoch)
	obj := availableObject(t, b, epoch, []byte("o"))
	write(t, b, epoch, func(ctx context.Context, tx catalog.Tx, _ uint64) {
		require.NoError(t, tx.InsertActiveBlock(ctx, activeBlock(catalog.Main, 0, 0, obj, 1, 5)))
	})
	bad := map[string]catalog.ActiveBlockRow{
		"duplicate":       activeBlock(catalog.Main, 0, 0, obj, 6, 9),
		"missing object":  activeBlock(catalog.Main, 0, 1, 1<<40, 6, 9),
		"missing segment": activeBlock(catalog.Main, 7, 0, obj, 6, 9),
		"zero events": func() catalog.ActiveBlockRow {
			r := activeBlock(catalog.Main, 0, 1, obj, 6, 9)
			r.EventCount = 0
			return r
		}(),
	}
	for name, row := range bad {
		t.Run(name, func(t *testing.T) {
			failing(t, b, epoch, func(ctx context.Context, tx catalog.Tx) error {
				return tx.InsertActiveBlock(ctx, row)
			})
		})
	}
}

func testGenerationConstraints(t *testing.T, b Backend) {
	_, epoch := acquire(t, b)
	initMain(t, b, epoch)
	footer := availableObject(t, b, epoch, []byte("f"))
	t.Run("short header", func(t *testing.T) {
		failing(t, b, epoch, func(ctx context.Context, tx catalog.Tx) error {
			_, err := tx.InsertGeneration(ctx, catalog.GenerationRow{Namespace: catalog.Main, Header: make([]byte, 255), FooterObjectID: footer})
			return err
		})
	})
	t.Run("missing footer", func(t *testing.T) {
		failing(t, b, epoch, func(ctx context.Context, tx catalog.Tx) error {
			_, err := tx.InsertGeneration(ctx, catalog.GenerationRow{Namespace: catalog.Main, Header: header(), FooterObjectID: 1 << 40})
			return err
		})
	})
	t.Run("missing segment", func(t *testing.T) {
		failing(t, b, epoch, func(ctx context.Context, tx catalog.Tx) error {
			_, err := tx.InsertGeneration(ctx, catalog.GenerationRow{Namespace: catalog.Main, Segment: 3, Header: header(), FooterObjectID: footer})
			return err
		})
	})
	t.Run("block of missing generation", func(t *testing.T) {
		failing(t, b, epoch, func(ctx context.Context, tx catalog.Tx) error {
			return tx.InsertGenerationBlocks(ctx, []catalog.GenerationBlockRow{{GenerationID: 1 << 40, ObjectID: footer, CompressedLength: 1}})
		})
	})
	t.Run("duplicate block", func(t *testing.T) {
		failing(t, b, epoch, func(ctx context.Context, tx catalog.Tx) error {
			gen, err := tx.InsertGeneration(ctx, catalog.GenerationRow{Namespace: catalog.Main, Header: header(), FooterObjectID: footer})
			if err != nil {
				return errors.Join(errors.New("unexpected"), err)
			}
			row := catalog.GenerationBlockRow{GenerationID: gen, ObjectID: footer, CompressedLength: 1}
			return tx.InsertGenerationBlocks(ctx, []catalog.GenerationBlockRow{row, row})
		})
	})
}

func testDeleteNamespace(t *testing.T, b Backend) {
	_, epoch := acquire(t, b)
	obj := availableObject(t, b, epoch, []byte("o"))
	initMain(t, b, epoch)
	write(t, b, epoch, func(ctx context.Context, tx catalog.Tx, rev uint64) {
		require.NoError(t, tx.InsertSegment(ctx, catalog.SegmentRow{Namespace: catalog.BootstrapLive, Index: 0, State: catalog.Active, Revision: rev}))
		gen, err := tx.InsertGeneration(ctx, catalog.GenerationRow{Namespace: catalog.BootstrapLive, Header: header(), FooterObjectID: obj, Revision: rev})
		require.NoError(t, err)
		require.NoError(t, tx.InsertGenerationBlocks(ctx, []catalog.GenerationBlockRow{{GenerationID: gen, ObjectID: obj, CompressedLength: 1}}))
		ok, err := tx.SealSegment(ctx, catalog.BootstrapLive, 0, gen, rev)
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, tx.InsertSegment(ctx, catalog.SegmentRow{Namespace: catalog.BootstrapLive, Index: 1, State: catalog.Active, Revision: rev}))
		require.NoError(t, tx.InsertActiveBlock(ctx, activeBlock(catalog.BootstrapLive, 1, 0, obj, 1, 1)))
		require.NoError(t, tx.InsertActiveBlock(ctx, activeBlock(catalog.Main, 0, 0, obj, 1, 1)))
	})
	write(t, b, epoch, func(ctx context.Context, tx catalog.Tx, _ uint64) {
		require.NoError(t, tx.DeleteNamespace(ctx, catalog.BootstrapLive))
		_, found, err := tx.ActiveSegment(ctx, catalog.BootstrapLive)
		require.NoError(t, err)
		require.False(t, found)
	})
	s := snapshot(t, b)
	require.Equal(t, []catalog.SegmentRow{{Namespace: catalog.Main, Index: 0, State: catalog.Active, Revision: s.Segments[0].Revision}}, s.Segments)
	require.Len(t, s.ActiveBlocks, 1)
	require.Equal(t, catalog.Main, s.ActiveBlocks[0].Namespace)
	require.Empty(t, s.Generations)
	// The namespace can be recreated.
	write(t, b, epoch, func(ctx context.Context, tx catalog.Tx, rev uint64) {
		require.NoError(t, tx.InsertSegment(ctx, catalog.SegmentRow{Namespace: catalog.BootstrapLive, Index: 0, State: catalog.Active, Revision: rev}))
	})
}

func testReaderSnapshot(t *testing.T, b Backend) {
	_, epoch := acquire(t, b)
	set := func(v string) uint64 {
		return write(t, b, epoch, func(ctx context.Context, tx catalog.Tx, _ uint64) {
			require.NoError(t, tx.ApplyMeta(ctx, []metastore.Op{{Kind: metastore.OpSet, Key: []byte("k"), Value: []byte(v)}}))
		})
	}
	r1 := set("1")
	r := read(t, b)
	a, err := r.Archive(ctxT(t))
	require.NoError(t, err)
	require.Equal(t, r1, a.CatalogRevision)
	set("2")
	got, err := r.MetaGet(ctxT(t), [][]byte{[]byte("k")})
	require.NoError(t, err)
	require.Equal(t, "1", string(got["k"]), "a reader keeps its snapshot")
	a, err = r.Archive(ctxT(t))
	require.NoError(t, err)
	require.Equal(t, r1, a.CatalogRevision)
	got, err = read(t, b).MetaGet(ctxT(t), [][]byte{[]byte("k")})
	require.NoError(t, err)
	require.Equal(t, "2", string(got["k"]))
}

func testAbortedTransaction(t *testing.T, b Backend) {
	_, epoch := acquire(t, b)
	tx, _ := begin(t, b, epoch)
	ctx := ctxT(t)
	require.NoError(t, tx.ApplyMeta(ctx, []metastore.Op{{Kind: metastore.OpSet, Key: []byte("k"), Value: []byte("v")}}))
	require.Error(t, tx.InsertHotBatch(ctx, hot(1, 1, nil, 0)))
	_, _, err := tx.MetaGetForUpdate(ctx, []byte("k"))
	require.Error(t, err, "statements after a failure are rejected")
	require.Error(t, tx.Commit(ctx), "committing an aborted transaction rolls back")
	require.NoError(t, tx.Rollback(ctx))
	got, err := read(t, b).MetaGet(ctx, [][]byte{[]byte("k")})
	require.NoError(t, err)
	require.Empty(t, got)
	// The aborted transaction released the row lock.
	write(t, b, epoch, func(context.Context, catalog.Tx, uint64) {})
}

func testNotify(t *testing.T, b Backend) {
	if b.Listener == nil {
		t.Skip("backend has no listener")
	}
	_, epoch := acquire(t, b)
	ctx, cancel := context.WithCancel(ctxT(t))
	defer cancel()
	ch, err := b.Listener.Listen(ctx)
	require.NoError(t, err)
	tx, rev := begin(t, b, epoch)
	require.NoError(t, tx.Notify(ctxT(t), rev))
	select {
	case got := <-ch:
		t.Fatalf("notification %d delivered before commit", got)
	case <-time.After(20 * time.Millisecond):
	}
	require.NoError(t, tx.Commit(ctxT(t)))
	for {
		select {
		case got := <-ch:
			if got == rev {
				return
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("notification %d never arrived", rev)
		}
	}
}
