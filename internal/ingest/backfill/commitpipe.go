package backfill

import (
	"bytes"
	"context"
	"errors"
	"sync"

	"github.com/bluesky-social/jetstream/internal/metastore"
)

// commitPipe lets the Store's read-modify-writes of shared rows — the counts
// row, host aggregates, the PDS roster — release countsMu before they commit.
// In disaggregated mode a commit is a fenced catalog transaction of several
// WAN round trips. Holding countsMu across one made every other writer, the
// segment writer's completion staging included, wait out the whole commit
// rather than just the staging.
//
// A write stages under countsMu as before, reading through the pipe, so it
// sees every write staged ahead of it whether or not that write has
// committed. It then takes a ticket, releases countsMu, waits for the ticket
// ahead of it, and commits. Tickets commit strictly in staging order, and a
// write whose predecessor failed does not commit: it was computed from rows
// that never landed. A finished write's rows leave the pipe, so writes staged
// after it read the store again.
//
// Writes finish outside countsMu, so a write can read a staged row and see
// its owner fail and leave the pipe before taking its own ticket, which
// would then not wait behind the failure. Each staging therefore records the
// tickets whose rows it read, and stage refuses it if any has failed.
//
// Only locked read-modify-writes read through the pipe. Other readers
// (Lookup, host cursors, status) read committed rows as before: a staged row
// may still fail, and they act on what they read with no commit to chain it
// to.
type commitPipe struct {
	mu sync.Mutex
	// staged maps a key to the newest staged, unfinished write of it.
	staged map[string]stagedRow
	// tail is the newest unfinished ticket, or nil.
	tail *pipeTicket
}

type stagedRow struct {
	val   []byte // nil when the write deletes the key
	owner *pipeTicket
}

// pipeTicket is one staged write's place in the commit order. Waiters block
// on channels, never on a held sync.Mutex, so the layer 3 oracle's seeded
// scheduler can park a commit (see chanMutex).
type pipeTicket struct {
	pipe *commitPipe
	prev *pipeTicket
	keys []string
	done chan struct{}
	// finished and err are set together under pipe.mu.
	finished bool
	err      error
}

// stagedOnFailureError is a write that cannot commit because a write it
// read, or the write staged just ahead of it, failed. Staging it again from
// the store would succeed.
type stagedOnFailureError struct{ err error }

func (e *stagedOnFailureError) Error() string {
	return "backfill: a metadata write staged ahead of this one failed: " + e.err.Error()
}

func (e *stagedOnFailureError) Unwrap() error { return e.err }

// errPipeDeleteRange rejects a range delete, which the pipe cannot overlay
// on reads. No read-modify-write stages one.
var errPipeDeleteRange = errors.New("backfill: internal error: a staged metadata write cannot carry a range delete")

// stage records ops, computed from what r read, as the newest staged write
// and returns its ticket. The caller must hold countsMu, and must finish the
// ticket.
func (p *commitPipe) stage(r *pipeReader, ops []metastore.Op) (*pipeTicket, error) {
	for _, op := range ops {
		if op.Kind == metastore.OpDeleteRange {
			return nil, errPipeDeleteRange
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	// A ticket r read from that has not finished is the tail or ahead of
	// it, so the new ticket waits for it; one that finished cleanly is in
	// the store. Only a failed one taints the write.
	for owner := range r.read {
		if owner.finished && owner.err != nil {
			return nil, &stagedOnFailureError{err: owner.err}
		}
	}
	if p.staged == nil {
		p.staged = make(map[string]stagedRow)
	}
	t := &pipeTicket{pipe: p, prev: p.tail, done: make(chan struct{})}
	seen := make(map[string]struct{}, len(ops))
	for _, op := range ops {
		row := stagedRow{owner: t}
		if op.Kind == metastore.OpSet {
			row.val = bytes.Clone(op.Value)
			if row.val == nil {
				row.val = []byte{}
			}
		}
		k := string(op.Key)
		if _, ok := seen[k]; !ok {
			seen[k] = struct{}{}
			t.keys = append(t.keys, k)
		}
		p.staged[k] = row
	}
	p.tail = t
	return t, nil
}

// wait returns once every write staged ahead of t has finished, with an
// error if the one just ahead failed.
func (t *pipeTicket) wait() error {
	if t.prev == nil {
		return nil
	}
	<-t.prev.done
	if err := t.prev.err; err != nil {
		return &stagedOnFailureError{err: err}
	}
	return nil
}

// finish records t's outcome and removes its rows from the pipe: on success
// the store has them, and on failure no one may read them again.
func (t *pipeTicket) finish(err error) {
	p := t.pipe
	p.mu.Lock()
	t.finished, t.err = true, err
	for _, k := range t.keys {
		if p.staged[k].owner == t {
			delete(p.staged, k)
		}
	}
	if p.tail == t {
		p.tail = nil
	}
	t.prev = nil
	p.mu.Unlock()
	close(t.done)
}

// commit waits for t's turn, runs fn unless a predecessor failed, and
// finishes t with the outcome.
func (t *pipeTicket) commit(fn func() error) error {
	err := t.wait()
	if err == nil {
		err = fn()
	}
	t.finish(err)
	return err
}

// lookup returns the newest staged value of key and the write that staged
// it.
func (p *commitPipe) lookup(key []byte) (val []byte, owner *pipeTicket, staged bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	row, ok := p.staged[string(key)]
	if !ok {
		return nil, nil, false
	}
	return bytes.Clone(row.val), row.owner, true
}

// pipeReader reads staged rows over the store's committed ones, recording
// which writes it read from. Locked read-modify-writes read through one per
// staging, via a metaView; it serves only reads.
type pipeReader struct {
	metastore.Store
	p    *commitPipe
	read map[*pipeTicket]struct{}
}

func (r *pipeReader) lookup(key []byte) ([]byte, bool) {
	val, owner, ok := r.p.lookup(key)
	if ok {
		if r.read == nil {
			r.read = make(map[*pipeTicket]struct{})
		}
		r.read[owner] = struct{}{}
	}
	return val, ok
}

func (r *pipeReader) Get(ctx context.Context, key []byte) ([]byte, error) {
	if val, ok := r.lookup(key); ok {
		if val == nil {
			return nil, metastore.ErrNotFound
		}
		return val, nil
	}
	return r.Store.Get(ctx, key)
}

func (r *pipeReader) GetMany(ctx context.Context, keys [][]byte) ([][]byte, error) {
	out := make([][]byte, len(keys))
	var missing [][]byte
	var at []int
	for i, key := range keys {
		if val, ok := r.lookup(key); ok {
			out[i] = val
			continue
		}
		missing = append(missing, key)
		at = append(at, i)
	}
	if len(missing) == 0 {
		return out, nil
	}
	vals, err := r.Store.GetMany(ctx, missing)
	if err != nil {
		return nil, err
	}
	for j, i := range at {
		out[i] = vals[j]
	}
	return out, nil
}

func (r *pipeReader) NewIter(context.Context, []byte, []byte) (metastore.Iterator, error) {
	return nil, errViewMisuse
}

func (r *pipeReader) Set(context.Context, []byte, []byte) error { return errViewMisuse }

func (r *pipeReader) Delete(context.Context, []byte) error { return errViewMisuse }

func (r *pipeReader) NewBatch() metastore.Batch {
	return metastore.NewOpBatch(func(context.Context, []metastore.Op) error { return errViewMisuse })
}

// applyOps commits ops to db in one batch.
func applyOps(ctx context.Context, db metastore.Store, ops []metastore.Op) error {
	b := db.NewBatch()
	for _, op := range ops {
		switch op.Kind {
		case metastore.OpSet:
			b.Set(op.Key, op.Value)
		case metastore.OpDelete:
			b.Delete(op.Key)
		case metastore.OpDeleteRange:
			b.DeleteRange(op.Key, op.End)
		}
	}
	return b.Commit(ctx)
}
