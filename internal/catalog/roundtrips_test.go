package catalog_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/leader"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/pgstore"
	"github.com/bluesky-social/jetstream/internal/pgstore/pgtest"
	"github.com/stretchr/testify/require"
)

// callDB records, per transaction, the calls that wait for a round trip in
// a backend that pipelines as pgstore does: FenceBump, Read, Commit, and
// every method that returns more than an error. The queued calls (catalog.Tx)
// ride with the next of those, so they are not recorded. The archive row
// lock is held from the fence to COMMIT, so every recorded call after the
// fence is a round trip every other leader transaction waits behind.
//
// callTx implements catalog.Tx method by method rather than embedding it, so
// a new method does not compile until it is classified here.
type callDB struct {
	catalog.DB
	mu    sync.Mutex
	calls map[catalog.TxKind][][]string
}

func (d *callDB) Begin(ctx context.Context, kind catalog.TxKind) (catalog.Tx, error) {
	tx, err := d.DB.Begin(ctx, kind)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.calls == nil {
		d.calls = map[catalog.TxKind][][]string{}
	}
	d.calls[kind] = append(d.calls[kind], nil)
	return &callTx{tx: tx, db: d, kind: kind, n: len(d.calls[kind]) - 1}, nil
}

// last returns the round trips of kind's most recent transaction.
func (d *callDB) last(kind catalog.TxKind) []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	txs := d.calls[kind]
	if len(txs) == 0 {
		return nil
	}
	return txs[len(txs)-1]
}

type callTx struct {
	tx   catalog.Tx
	db   *callDB
	kind catalog.TxKind
	n    int
}

var _ catalog.Tx = (*callTx)(nil)

func (t *callTx) roundTrip(name string) {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	t.db.calls[t.kind][t.n] = append(t.db.calls[t.kind][t.n], name)
}

func (t *callTx) FenceBump(ctx context.Context, epoch uint64, reads ...catalog.Read) (uint64, bool, error) {
	t.roundTrip("FenceBump")
	return t.tx.FenceBump(ctx, epoch, reads...)
}

func (t *callTx) FenceBumpAt(ctx context.Context, epoch, rev uint64, checks ...catalog.MetaCheck) error {
	return t.tx.FenceBumpAt(ctx, epoch, rev, checks...)
}

func (t *callTx) Read(ctx context.Context, reads ...catalog.Read) error {
	if len(reads) > 0 {
		t.roundTrip("Read")
	}
	return t.tx.Read(ctx, reads...)
}

func (t *callTx) ApplyMeta(ctx context.Context, ops []metastore.Op) error {
	return t.tx.ApplyMeta(ctx, ops)
}

func (t *callTx) InsertObjects(ctx context.Context, objs []catalog.NewObject, ids []uint64) error {
	return t.tx.InsertObjects(ctx, objs, ids)
}

func (t *callTx) SetObjectsAvailable(ctx context.Context, ids []uint64) error {
	return t.tx.SetObjectsAvailable(ctx, ids)
}

func (t *callTx) ClearUnreferenced(ctx context.Context, ids []uint64) error {
	return t.tx.ClearUnreferenced(ctx, ids)
}

func (t *callTx) InsertHotBatch(ctx context.Context, row catalog.HotBatchRow) error {
	return t.tx.InsertHotBatch(ctx, row)
}

func (t *callTx) DeleteHotBatches(ctx context.Context, lo, hi uint64) ([]catalog.HotBatchSpan, error) {
	t.roundTrip("DeleteHotBatches")
	return t.tx.DeleteHotBatches(ctx, lo, hi)
}

func (t *callTx) ActiveBlocksForUpdate(ctx context.Context, ns catalog.Namespace, idx uint64) ([]catalog.ActiveBlockRow, error) {
	t.roundTrip("ActiveBlocksForUpdate")
	return t.tx.ActiveBlocksForUpdate(ctx, ns, idx)
}

func (t *callTx) InsertActiveBlock(ctx context.Context, row catalog.ActiveBlockRow) error {
	return t.tx.InsertActiveBlock(ctx, row)
}

func (t *callTx) DeleteActiveBlocks(ctx context.Context, ns catalog.Namespace, idx uint64) (int, error) {
	t.roundTrip("DeleteActiveBlocks")
	return t.tx.DeleteActiveBlocks(ctx, ns, idx)
}

func (t *callTx) InsertGeneration(ctx context.Context, row catalog.GenerationRow) (uint64, error) {
	t.roundTrip("InsertGeneration")
	return t.tx.InsertGeneration(ctx, row)
}

func (t *callTx) InsertGenerationAt(ctx context.Context, row catalog.GenerationRow) error {
	return t.tx.InsertGenerationAt(ctx, row)
}

func (t *callTx) InsertGenerationBlocks(ctx context.Context, rows []catalog.GenerationBlockRow) error {
	return t.tx.InsertGenerationBlocks(ctx, rows)
}

func (t *callTx) SealSegment(ctx context.Context, ns catalog.Namespace, idx, gen, revision uint64) (bool, error) {
	t.roundTrip("SealSegment")
	return t.tx.SealSegment(ctx, ns, idx, gen, revision)
}

func (t *callTx) InsertSegment(ctx context.Context, row catalog.SegmentRow) error {
	return t.tx.InsertSegment(ctx, row)
}

func (t *callTx) DeleteNamespace(ctx context.Context, ns catalog.Namespace) error {
	return t.tx.DeleteNamespace(ctx, ns)
}

func (t *callTx) SetSegmentGeneration(ctx context.Context, ns catalog.Namespace, idx, gen, revision uint64) error {
	return t.tx.SetSegmentGeneration(ctx, ns, idx, gen, revision)
}

func (t *callTx) DeleteGeneration(ctx context.Context, id uint64) error {
	return t.tx.DeleteGeneration(ctx, id)
}

func (t *callTx) MarkUnreferenced(ctx context.Context, after uint64, limit int) (catalog.MarkPage, error) {
	t.roundTrip("MarkUnreferenced")
	return t.tx.MarkUnreferenced(ctx, after, limit)
}

func (t *callTx) ClaimObjects(ctx context.Context, gcDelay, orphanAge time.Duration, limit int) ([]catalog.ObjectRow, error) {
	t.roundTrip("ClaimObjects")
	return t.tx.ClaimObjects(ctx, gcDelay, orphanAge, limit)
}

func (t *callTx) DeletingObjects(ctx context.Context, limit int) ([]catalog.ObjectRow, error) {
	t.roundTrip("DeletingObjects")
	return t.tx.DeletingObjects(ctx, limit)
}

func (t *callTx) ReferencedObjects(ctx context.Context, ids []uint64) ([]uint64, error) {
	t.roundTrip("ReferencedObjects")
	return t.tx.ReferencedObjects(ctx, ids)
}

func (t *callTx) ForgetObjects(ctx context.Context, ids []uint64) (int, error) {
	t.roundTrip("ForgetObjects")
	return t.tx.ForgetObjects(ctx, ids)
}

func (t *callTx) Notify(ctx context.Context, revision uint64) error {
	return t.tx.Notify(ctx, revision)
}

func (t *callTx) Commit(ctx context.Context) error {
	t.roundTrip("Commit")
	return t.tx.Commit(ctx)
}

func (t *callTx) Rollback(ctx context.Context) error { return t.tx.Rollback(ctx) }

// A compaction pass publishes thousands of generations back to back, and
// every live event's hot batch commit waits behind each one for the archive
// row lock. So a publish reads everything with its fence and queues every
// write: the lock is held for COMMIT's round trip and no other, whether
// its blocks are reused or freshly uploaded.
func TestScripts_PublishGenerationHoldsTheLockForCommitOnly(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		h := newHarness(t, be)
		db := &callDB{DB: h.db}
		h.s = catalog.NewSession(catalog.SessionConfig{DB: db, Epoch: h.lock.Epoch()})
		sg := h.sealed()
		require.Equal(t, []string{"FenceBump", "ActiveBlocksForUpdate", "InsertGeneration", "DeleteActiveBlocks", "SealSegment", "Commit"},
			db.last(catalog.TxSeal), "the seal still takes its own round trips; this test pins the difference")

		p, frames, footer := h.rewrite(sg.commit.GenerationID, sg.seal.Header, sg.footer, sg.frames, sg.seal.Blocks, 5)
		require.True(t, p.Blocks[0].Reused)
		require.False(t, p.Blocks[1].Reused)
		c, err := h.s.PublishGeneration(t.Context(), p)
		require.NoError(t, err)
		require.Equal(t, []string{"FenceBump", "Commit"}, db.last(catalog.TxCompaction))

		// And again over a generation with no reused block.
		snap, err := h.snapshot()
		require.NoError(t, err)
		gbs := snap.GenerationBlocks[c.GenerationID]
		p2, _, _ := h.rewrite(c.GenerationID, p.Header, footer, frames, []catalog.SealBlock{
			{ObjectID: c.ObjectIDs[0], CompressedLength: gbs[0].CompressedLength},
			{ObjectID: c.ObjectIDs[1], CompressedLength: gbs[1].CompressedLength},
		}, 1, 6)
		for _, b := range p2.Blocks {
			require.False(t, b.Reused)
		}
		_, err = h.s.PublishGeneration(t.Context(), p2)
		require.NoError(t, err)
		require.Equal(t, []string{"FenceBump", "Commit"}, db.last(catalog.TxCompaction))
		require.NoError(t, h.s.Err())
	})
}

// The same, counted on the wire: a publish over PostgreSQL is BEGIN, the
// fence and its reads in one round trip, then the writes, NOTIFY, and COMMIT
// in one more.
func TestScripts_PublishGenerationRoundTripsPG(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	_, u := pgtest.Open(t, nil)
	proxy, pu := pgtest.NewProxy(t, u)
	// One connection, so the warm-up publish prepares every statement on
	// the connection the measured one uses.
	s, err := pgstore.Open(ctx, pgstore.Config{URL: pu, MaxConns: 1})
	require.NoError(t, err)
	t.Cleanup(s.Close)
	h := newHarness(t, backend{name: "pg-proxy", open: func(*testing.T) (catalog.DB, func() leader.Locker) {
		return s, func() leader.Locker { return s.NewLease() }
	}})

	sg := h.sealed()
	p, frames, footer := h.rewrite(sg.commit.GenerationID, sg.seal.Header, sg.footer, sg.frames, sg.seal.Blocks, 5)
	c, err := h.s.PublishGeneration(ctx, p)
	require.NoError(t, err)

	snap, err := h.snapshot()
	require.NoError(t, err)
	gbs := snap.GenerationBlocks[c.GenerationID]
	p2, _, _ := h.rewrite(c.GenerationID, p.Header, footer, frames, []catalog.SealBlock{
		{ObjectID: c.ObjectIDs[0], CompressedLength: gbs[0].CompressedLength},
		{ObjectID: c.ObjectIDs[1], CompressedLength: gbs[1].CompressedLength},
	}, 2)
	// Straight to the store: the harness's invariant check after each
	// commit reads through the proxy too.
	sess := catalog.NewSession(catalog.SessionConfig{DB: s, Epoch: h.lock.Epoch()})
	before := proxy.RoundTrips()
	_, err = sess.PublishGeneration(ctx, p2)
	require.NoError(t, err)
	require.Equal(t, int64(2), proxy.RoundTrips()-before)
}
