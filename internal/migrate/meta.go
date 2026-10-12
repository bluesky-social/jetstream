package migrate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/metastore/pebblestore"
)

// dirtySet is the metadata keys written locally since they were last
// copied, fed by the Pebble commit observer (migration plan §6.6.1). It
// holds keys, never values: a flush reads each key's current value.
type dirtySet struct {
	max     int
	metrics *Metrics

	mu       sync.Mutex
	keys     map[string]struct{}
	ranges   [][2][]byte
	overflow bool
}

func newDirtySet(max int, m *Metrics) *dirtySet {
	return &dirtySet{max: max, metrics: m, keys: map[string]struct{}{}}
}

// observe is the commit observer. Keys the migration never copies are not
// kept, so the identity cache and the relay cursor, written on every batch,
// cost nothing. Past max keys the set is dropped and marked overflowed: a
// full resync is cheaper than an unbounded set.
func (d *dirtySet) observe(keys [][]byte, ranges [][2][]byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.overflow {
		return
	}
	for _, k := range keys {
		if c := classify(k); c == classDrop || c == classHandoff {
			continue
		}
		d.keys[string(k)] = struct{}{}
	}
	d.ranges = append(d.ranges, ranges...)
	if len(d.keys)+len(d.ranges) > d.max {
		d.keys, d.ranges, d.overflow = map[string]struct{}{}, nil, true
		d.metrics.overflow()
	}
	d.metrics.setDirty(len(d.keys))
}

// take empties the set and returns what it held.
func (d *dirtySet) take() (keys [][]byte, ranges [][2][]byte, overflow bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	keys = make([][]byte, 0, len(d.keys))
	for k := range d.keys {
		keys = append(keys, []byte(k))
	}
	ranges, overflow = d.ranges, d.overflow
	d.keys, d.ranges, d.overflow = map[string]struct{}{}, nil, false
	d.metrics.setDirty(0)
	return keys, ranges, overflow
}

func (d *dirtySet) len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.keys) + len(d.ranges)
}

// metaSync copies the source's metadata into the catalog and keeps it in
// step. Resync and flush hold mu, so a resync's snapshot values never land
// after a flush's newer ones.
type metaSync struct {
	local   *pebblestore.Store
	remote  metastore.Store
	dirty   *dirtySet
	batch   int
	metrics *Metrics

	mu sync.Mutex

	// statMu guards lastResync, when the last full resync finished, and
	// resyncDiffs, what it repaired. It is separate from mu so status
	// reads never wait on a handoff, which holds mu throughout.
	statMu      sync.Mutex
	lastResync  time.Time
	resyncDiffs int
}

// errUnclassified means the source holds a metadata key the migration has
// no rule for (migration plan §6.6).
var errUnclassified = errors.New("migrate: the source holds a metadata key with no migration rule")

// opWriter batches metadata ops into ImportMeta transactions.
type opWriter struct {
	ctx     context.Context
	sess    *catalog.Session
	batch   int
	ops     []metastore.Op
	written int
}

func (w *opWriter) add(op metastore.Op) error {
	w.ops = append(w.ops, op)
	if len(w.ops) >= w.batch {
		return w.flush()
	}
	return nil
}

func (w *opWriter) flush() error {
	if len(w.ops) == 0 {
		return nil
	}
	if _, err := w.sess.ImportMeta(w.ctx, w.ops); err != nil {
		return fmt.Errorf("migrate: copy metadata: %w", err)
	}
	w.written += len(w.ops)
	w.ops = w.ops[:0]
	return nil
}

// resync makes the catalog's kept keys equal a fresh snapshot of the
// source's: a merge join of both in key order that writes only the
// differences. It reads both sides in full. Keys changed after the
// snapshot are in the dirty set, which resync empties first, so a flush
// after it brings them up to date. It returns the number of differences.
//
// With verify set it writes nothing and only counts.
func (ms *metaSync) resync(ctx context.Context, sess *catalog.Session, tailing, verify bool) (int, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.resyncLocked(ctx, sess, tailing, verify)
}

func (ms *metaSync) resyncLocked(ctx context.Context, sess *catalog.Session, tailing, verify bool) (int, error) {
	if !verify {
		ms.dirty.take()
	}
	sn := ms.local.NewSnapshot()
	defer func() { _ = sn.Close() }()
	diffs, written, err := ms.diffRange(ctx, sess, sn, nil, nil, tailing, verify)
	ms.metrics.metaWritten(written)
	if err != nil {
		return diffs, err
	}
	if !verify {
		now := time.Now()
		ms.statMu.Lock()
		ms.lastResync, ms.resyncDiffs = now, diffs
		ms.statMu.Unlock()
		ms.metrics.resynced(diffs, float64(now.Unix()))
	}
	return diffs, nil
}

// iterable is a Pebble store or snapshot.
type iterable interface {
	NewIter(ctx context.Context, lower, upper []byte) (metastore.Iterator, error)
}

// keptIter steps an iterator over the keys the migration keeps in step.
// On the source it refuses unclassified keys; on the catalog it skips keys
// the migration does not own, such as the seq key and migration/state.
type keptIter struct {
	it      metastore.Iterator
	tailing bool
	source  bool
	key     []byte
	val     []byte
	ok      bool
}

func (k *keptIter) next() error {
	for k.it.Next() {
		c := classify(k.it.Key())
		if c == classUnknown && k.source {
			return fmt.Errorf("%w: %q (prefix %q)", errUnclassified, k.it.Key(), prefixOf(k.it.Key()))
		}
		if !c.kept(k.tailing) {
			continue
		}
		k.key, k.val, k.ok = bytes.Clone(k.it.Key()), bytes.Clone(k.it.Value()), true
		return nil
	}
	k.key, k.val, k.ok = nil, nil, false
	return k.it.Err()
}

// diffRange is resync over [lower, upper) of src, nil bounds open.
func (ms *metaSync) diffRange(ctx context.Context, sess *catalog.Session, src iterable, lower, upper []byte, tailing, verify bool) (diffs, written int, err error) {
	lit, err := src.NewIter(ctx, lower, upper)
	if err != nil {
		return 0, 0, fmt.Errorf("migrate: iterate local metadata: %w", err)
	}
	defer func() { _ = lit.Close() }()
	rit, err := ms.remote.NewIter(ctx, lower, upper)
	if err != nil {
		return 0, 0, fmt.Errorf("migrate: iterate catalog metadata: %w", err)
	}
	defer func() { _ = rit.Close() }()

	l := &keptIter{it: lit, tailing: tailing, source: true}
	r := &keptIter{it: rit, tailing: tailing}
	if err := l.next(); err != nil {
		return 0, 0, err
	}
	if err := r.next(); err != nil {
		return 0, 0, fmt.Errorf("migrate: iterate catalog metadata: %w", err)
	}
	w := &opWriter{ctx: ctx, sess: sess, batch: ms.batch}
	emit := func(op metastore.Op) error {
		diffs++
		if verify {
			return nil
		}
		return w.add(op)
	}
	for l.ok || r.ok {
		var cmp int
		switch {
		case !r.ok:
			cmp = -1
		case !l.ok:
			cmp = 1
		default:
			cmp = bytes.Compare(l.key, r.key)
		}
		switch {
		case cmp < 0:
			err = emit(metastore.Op{Kind: metastore.OpSet, Key: l.key, Value: l.val})
		case cmp > 0:
			err = emit(metastore.Op{Kind: metastore.OpDelete, Key: r.key})
		case !bytes.Equal(l.val, r.val):
			err = emit(metastore.Op{Kind: metastore.OpSet, Key: l.key, Value: l.val})
		}
		if err != nil {
			return diffs, w.written, err
		}
		if cmp <= 0 {
			if err := l.next(); err != nil {
				return diffs, w.written, err
			}
		}
		if cmp >= 0 {
			if err := r.next(); err != nil {
				return diffs, w.written, fmt.Errorf("migrate: iterate catalog metadata: %w", err)
			}
		}
	}
	err = w.flush()
	return diffs, w.written, err
}

// errResyncNeeded means the dirty set overflowed, so a flush cannot know
// what changed.
var errResyncNeeded = errors.New("migrate: the changed-key set overflowed")

// flush copies every dirty key's current value, and re-copies every dirty
// range. It returns errResyncNeeded when the set overflowed.
func (ms *metaSync) flush(ctx context.Context, sess *catalog.Session, tailing bool) (int, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.flushLocked(ctx, sess, tailing)
}

func (ms *metaSync) flushLocked(ctx context.Context, sess *catalog.Session, tailing bool) (int, error) {
	keys, ranges, overflow := ms.dirty.take()
	if overflow {
		return 0, errResyncNeeded
	}
	written := 0
	for _, rg := range ranges {
		_, n, err := ms.diffRange(ctx, sess, ms.local, rg[0], rg[1], tailing, false)
		written += n
		if err != nil {
			return written, err
		}
	}
	w := &opWriter{ctx: ctx, sess: sess, batch: ms.batch}
	var kept [][]byte
	for _, k := range keys {
		switch c := classify(k); {
		case c == classUnknown:
			return written, fmt.Errorf("%w: %q (prefix %q)", errUnclassified, k, prefixOf(k))
		case c.kept(tailing):
			kept = append(kept, k)
		}
	}
	for len(kept) > 0 {
		chunk := kept[:min(len(kept), ms.batch)]
		kept = kept[len(chunk):]
		vals, err := ms.local.GetMany(ctx, chunk)
		if err != nil {
			return written, fmt.Errorf("migrate: read local metadata: %w", err)
		}
		for i, k := range chunk {
			op := metastore.Op{Kind: metastore.OpDelete, Key: k}
			if vals[i] != nil {
				op = metastore.Op{Kind: metastore.OpSet, Key: k, Value: vals[i]}
			}
			if err := w.add(op); err != nil {
				return written + w.written, err
			}
		}
	}
	err := w.flush()
	written += w.written
	ms.metrics.metaWritten(written)
	return written, err
}

// lastResyncAt returns when the last full resync finished.
func (ms *metaSync) lastResyncAt() (time.Time, int) {
	ms.statMu.Lock()
	defer ms.statMu.Unlock()
	return ms.lastResync, ms.resyncDiffs
}
