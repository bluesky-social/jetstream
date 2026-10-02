package backfill

import (
	"bytes"
	"context"
	"errors"
	"sync"

	"github.com/bluesky-social/jetstream/internal/metastore"
)

// metaView is a read-your-writes view of the metadata store for one locked
// write. It serves keys prefetched together (one round trip against
// PostgreSQL instead of one per key) and keys staged earlier through its
// batch, so a run of read-modify-writes to shared rows — the counts row and
// host aggregates — costs no extra round trips and each one sees the last.
//
// A view lives only as long as the write it serves and must be used under the
// locks that write holds: it assumes nothing else changes the keys it caches.
// It is not safe for concurrent use.
type metaView struct {
	metastore.Store
	// vals maps a key to its value, or to nil when it is known absent.
	vals map[string][]byte
	// deleted holds the DeleteRange spans staged through the view; a key
	// in one reads as absent until it is staged again.
	deleted [][2][]byte
}

var _ metastore.Store = (*metaView)(nil)

func newMetaView(db metastore.Store) *metaView {
	return &metaView{Store: db, vals: make(map[string][]byte)}
}

// prefetch loads every key the view does not already know in one GetMany.
func (v *metaView) prefetch(ctx context.Context, keys [][]byte) error {
	var missing [][]byte
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if _, ok := seen[string(key)]; ok {
			continue
		}
		seen[string(key)] = struct{}{}
		if v.known(key) {
			continue
		}
		missing = append(missing, key)
	}
	if len(missing) == 0 {
		return nil
	}
	vals, err := v.Store.GetMany(ctx, missing)
	if err != nil {
		return err
	}
	for i, key := range missing {
		v.vals[string(key)] = vals[i]
	}
	return nil
}

func (v *metaView) known(key []byte) bool {
	if _, ok := v.vals[string(key)]; ok {
		return true
	}
	return v.inDeletedRange(key)
}

func (v *metaView) inDeletedRange(key []byte) bool {
	for _, r := range v.deleted {
		if bytes.Compare(key, r[0]) >= 0 && bytes.Compare(key, r[1]) < 0 {
			return true
		}
	}
	return false
}

// Get serves the key from the view, falling through to the store (and
// caching the answer) on a miss.
func (v *metaView) Get(ctx context.Context, key []byte) ([]byte, error) {
	if val, ok := v.vals[string(key)]; ok {
		if val == nil {
			return nil, metastore.ErrNotFound
		}
		return bytes.Clone(val), nil
	}
	if v.inDeletedRange(key) {
		return nil, metastore.ErrNotFound
	}
	val, err := v.Store.Get(ctx, key)
	switch {
	case err == nil:
		if val == nil {
			val = []byte{}
		}
		v.vals[string(key)] = bytes.Clone(val)
		return val, nil
	case errors.Is(err, metastore.ErrNotFound):
		v.vals[string(key)] = nil
		return nil, err
	default:
		return nil, err
	}
}

// GetMany prefetches the keys, then answers from the view.
func (v *metaView) GetMany(ctx context.Context, keys [][]byte) ([][]byte, error) {
	if err := v.prefetch(ctx, keys); err != nil {
		return nil, err
	}
	out := make([][]byte, len(keys))
	for i, key := range keys {
		if val := v.vals[string(key)]; val != nil {
			out[i] = bytes.Clone(val)
		}
	}
	return out, nil
}

// errViewMisuse rejects metaView calls that would bypass or read past the
// write it serves: iteration does not see staged ops, and direct writes would
// commit outside the batch. No caller makes them.
var errViewMisuse = errors.New("backfill: internal error: metaView supports only reads and writes through its batch")

func (v *metaView) NewIter(context.Context, []byte, []byte) (metastore.Iterator, error) {
	return nil, errViewMisuse
}

func (v *metaView) Set(context.Context, []byte, []byte) error { return errViewMisuse }

func (v *metaView) Delete(context.Context, []byte) error { return errViewMisuse }

func (v *metaView) NewBatch() metastore.Batch {
	return metastore.NewOpBatch(func(context.Context, []metastore.Op) error { return errViewMisuse })
}

// batch wraps b so every op it stages is also visible to the view's reads.
func (v *metaView) batch(b metastore.Batch) metastore.Batch {
	return &viewBatch{Batch: b, v: v}
}

type viewBatch struct {
	metastore.Batch
	v *metaView
}

func (b *viewBatch) Set(key, value []byte) {
	b.Batch.Set(key, value)
	if value == nil {
		value = []byte{}
	}
	b.v.vals[string(key)] = bytes.Clone(value)
}

func (b *viewBatch) Delete(key []byte) {
	b.Batch.Delete(key)
	b.v.vals[string(key)] = nil
}

func (b *viewBatch) DeleteRange(start, end []byte) {
	b.Batch.DeleteRange(start, end)
	for k := range b.v.vals {
		if k >= string(start) && k < string(end) {
			delete(b.v.vals, k)
		}
	}
	b.v.deleted = append(b.v.deleted, [2][]byte{bytes.Clone(start), bytes.Clone(end)})
}

// groupWrite is one caller's contribution to a group commit.
type groupWrite struct {
	// keys are prefetched, with every other member's, before any apply.
	keys [][]byte
	// apply stages the write into batch, reading through view, which sees
	// every earlier member's staged ops.
	apply func(ctx context.Context, view *metaView, batch metastore.Batch) error
	// committed runs after the shared commit succeeds.
	committed func()

	done chan error
	lead chan struct{}
}

// groupCommitter folds concurrent metadata writes into one transaction. In
// disaggregated mode every metadata commit is a fenced catalog transaction
// that serializes on the archive row, so commit latency, not row count, bounds
// write throughput; one transaction carrying many callers' writes is nearly as
// fast as one carrying a single caller's.
//
// The first caller to arrive leads: it commits every write queued so far,
// including its own, then hands the lead to the oldest write that queued
// during its commit. Callers wait on channels, never on a held sync.Mutex,
// so the layer 3 oracle's seeded scheduler can park a commit (see chanMutex).
type groupCommitter struct {
	mu      sync.Mutex
	pending []*groupWrite
	active  bool
}

// commit queues w and returns once the transaction carrying it committed or
// failed. Every member of a failed group gets the same error. commitGroup runs
// one group's transaction.
func (g *groupCommitter) commit(w *groupWrite, commitGroup func([]*groupWrite) error) error {
	w.done = make(chan error, 1)
	w.lead = make(chan struct{}, 1)
	g.mu.Lock()
	g.pending = append(g.pending, w)
	if !g.active {
		g.active = true
		w.lead <- struct{}{}
	}
	g.mu.Unlock()

	select {
	case err := <-w.done:
		return err
	case <-w.lead:
	}
	g.mu.Lock()
	group := g.pending
	g.pending = nil
	g.mu.Unlock()

	err := commitGroup(group)
	for _, member := range group {
		if err == nil && member.committed != nil {
			member.committed()
		}
		member.done <- err
	}

	g.mu.Lock()
	if len(g.pending) > 0 {
		g.pending[0].lead <- struct{}{}
	} else {
		g.active = false
	}
	g.mu.Unlock()
	return <-w.done
}
