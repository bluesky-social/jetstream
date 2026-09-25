package metastore

import (
	"bytes"
	"context"
	"sync/atomic"
)

// WriteOp names the metadata-store write being attempted. The values mirror
// the jetstream_store_op_duration_seconds op labels, so a fault target reads
// the same in a test as it does on the histogram.
type WriteOp string

const (
	WriteOpSet         WriteOp = "set"
	WriteOpDelete      WriteOp = "delete"
	WriteOpBatchCommit WriteOp = "batch_commit"
)

// FaultInjector deterministically fails metadata writes in tests. Production
// never installs one.
//
// BeforeWrite runs before the underlying write. An error prevents the write
// and is returned unchanged, leaving the keyspace intact. keys holds the
// Set/Delete key, or every key staged in a batch (a DeleteRange contributes
// its start key). Implementations must be concurrency-safe.
type FaultInjector interface {
	BeforeWrite(op WriteOp, keys [][]byte) error
}

// WithFaults wraps s so every write consults f first. A nil f returns s
// unchanged, so callers can thread an optional injector unconditionally.
func WithFaults(s Store, f FaultInjector) Store {
	if f == nil {
		return s
	}
	return &faultStore{inner: s, faults: f}
}

// Unwrapper is implemented by Store wrappers so optional capabilities of the
// underlying implementation (such as DiskStats) stay reachable.
type Unwrapper interface {
	Unwrap() Store
}

type faultStore struct {
	inner  Store
	faults FaultInjector
}

func (s *faultStore) Unwrap() Store { return s.inner }

func (s *faultStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	return s.inner.Get(ctx, key)
}

func (s *faultStore) NewIter(ctx context.Context, lower, upper []byte) (Iterator, error) {
	return s.inner.NewIter(ctx, lower, upper)
}

func (s *faultStore) Set(ctx context.Context, key, value []byte) error {
	if err := s.faults.BeforeWrite(WriteOpSet, [][]byte{clone(key)}); err != nil {
		return err
	}
	return s.inner.Set(ctx, key, value)
}

func (s *faultStore) Delete(ctx context.Context, key []byte) error {
	if err := s.faults.BeforeWrite(WriteOpDelete, [][]byte{clone(key)}); err != nil {
		return err
	}
	return s.inner.Delete(ctx, key)
}

func (s *faultStore) NewBatch() Batch {
	return &faultBatch{inner: s.inner.NewBatch(), faults: s.faults}
}

// faultBatch records staged keys itself so the injector can match by name
// without decoding a backend-specific batch representation.
type faultBatch struct {
	inner  Batch
	faults FaultInjector
	keys   [][]byte
	done   bool
}

func (b *faultBatch) Set(key, value []byte) {
	b.keys = append(b.keys, clone(key))
	b.inner.Set(key, value)
}

func (b *faultBatch) Delete(key []byte) {
	b.keys = append(b.keys, clone(key))
	b.inner.Delete(key)
}

func (b *faultBatch) DeleteRange(start, end []byte) {
	b.keys = append(b.keys, clone(start))
	b.inner.DeleteRange(start, end)
}

func (b *faultBatch) Len() int { return b.inner.Len() }

// Commit consumes the batch even when the injector fails it, matching the
// concrete impls: a retried commit must not apply what the fault rejected.
func (b *faultBatch) Commit(ctx context.Context) error {
	if b.done {
		return ErrBatchCommitted
	}
	b.done = true
	if err := b.faults.BeforeWrite(WriteOpBatchCommit, b.keys); err != nil {
		return err
	}
	return b.inner.Commit(ctx)
}

// KeyPrefixFault is the canonical FaultInjector: it fails the Ordinal-th
// (1-based) write op that touches a key under Prefix, with Err. Earlier and
// later matching ops succeed, so a scenario fails exactly one targeted
// persistence op and observes how the system behaves across the boundary.
//
// "By name" is a key-prefix match (e.g. "merge/next_source_idx", "repo/",
// "seq/") and "by ordinal" is the 1-based occurrence count — both
// deterministic, so a failing run reproduces exactly. A batch commit counts
// as one occurrence when any staged key matches the prefix; Op, when set,
// further restricts matching to that write op.
type KeyPrefixFault struct {
	Prefix []byte
	// Op, when non-empty, restricts the fault to that write op (e.g. only
	// batch_commit). Empty matches any write op.
	Op      WriteOp
	Ordinal int
	Err     error

	seen atomic.Int64
}

// BeforeWrite implements FaultInjector. It is safe for concurrent use: the
// occurrence counter is atomic, so the Ordinal-th matching op fires exactly
// once even under concurrent writers.
func (f *KeyPrefixFault) BeforeWrite(op WriteOp, keys [][]byte) error {
	if f.Op != "" && f.Op != op {
		return nil
	}
	matched := false
	for _, k := range keys {
		if bytes.HasPrefix(k, f.Prefix) {
			matched = true
			break
		}
	}
	if !matched {
		return nil
	}
	if int(f.seen.Add(1)) == f.Ordinal {
		return f.Err
	}
	return nil
}
