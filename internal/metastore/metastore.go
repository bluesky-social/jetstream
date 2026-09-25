// Package metastore is the backend-neutral metadata key/value interface
// (design §14.1). It covers exactly what the ingest code uses from Pebble:
// point reads, single-key writes, ordered write batches, and bounded forward
// iteration. Snapshots and indexed batches are deliberately absent, because
// the PostgreSQL implementation cannot offer them cheaply.
//
// Implementations: metastore/pebblestore (local mode, wraps internal/store)
// and metastore/memstore (unit tests). Every implementation must pass the
// metastore/storetest contract suite.
package metastore

import (
	"context"
	"errors"
)

// ErrNotFound is returned by Get when the key is absent.
var ErrNotFound = errors.New("metastore: not found")

// ErrBatchCommitted is returned when a Batch is committed a second time.
// Batches are single-use.
var ErrBatchCommitted = errors.New("metastore: batch already committed")

// Store is a durable, ordered, bytewise-keyed metadata store.
//
// Every successful write is durable when it returns: the Pebble
// implementation syncs its WAL, and the PostgreSQL implementation commits a
// transaction.
type Store interface {
	// Get returns a copy of the value stored under key, or ErrNotFound.
	Get(ctx context.Context, key []byte) ([]byte, error)
	// NewBatch returns an empty write batch. Nothing it stages is visible
	// until Commit returns nil.
	NewBatch() Batch
	// NewIter iterates keys in [lower, upper) in ascending bytewise order.
	// A nil lower starts at the first key; a nil upper is unbounded. An
	// iterator is not a snapshot: rows committed while it is open may or
	// may not be observed.
	NewIter(ctx context.Context, lower, upper []byte) (Iterator, error)
	// Set and Delete are single-op batches.
	Set(ctx context.Context, key, value []byte) error
	Delete(ctx context.Context, key []byte) error
}

// Batch is an ordered list of writes applied atomically by Commit. Ops apply
// in the order they were staged, so Set then Delete of the same key leaves
// it absent, and a DeleteRange only removes keys staged before it.
//
// A Batch is not safe for concurrent use, and it must not be reused after
// Commit. Staging methods copy their arguments.
type Batch interface {
	Set(key, value []byte)
	Delete(key []byte)
	// DeleteRange deletes every key in [start, end). Callers must pass
	// start < end.
	DeleteRange(start, end []byte)
	// Commit applies every staged op atomically and durably. On error none
	// of them is applied.
	Commit(ctx context.Context) error
	// Len reports the number of staged ops.
	Len() int
}

// Iterator walks a key range in ascending order. Key and Value are valid
// only until the next call to Next; callers that retain them must copy.
type Iterator interface {
	Next() bool
	Key() []byte
	Value() []byte
	// Err reports the first error that ended iteration early. It is nil when
	// Next returned false because the range was exhausted.
	Err() error
	Close() error
}

// OpKind names one batch operation.
type OpKind uint8

const (
	OpSet OpKind = iota + 1
	OpDelete
	OpDeleteRange
)

func (k OpKind) String() string {
	switch k {
	case OpSet:
		return "set"
	case OpDelete:
		return "delete"
	case OpDeleteRange:
		return "delete_range"
	default:
		return "unknown"
	}
}

// Op is one staged batch operation in value form. Backends without a native
// batch (memstore, and later PostgreSQL through catalog.Tx.ApplyMeta) consume
// ordered []Op.
//
// For OpDeleteRange, Key is the inclusive start and End the exclusive end.
type Op struct {
	Kind  OpKind
	Key   []byte
	Value []byte
	End   []byte
}

// OpBatch is a Batch that records ops in order and hands them to a commit
// function. Backends without a native batch type build on it.
type OpBatch struct {
	ops    []Op
	commit func(ctx context.Context, ops []Op) error
	done   bool
}

// NewOpBatch returns an OpBatch whose Commit calls commit with the staged
// ops. commit must apply all of them or none.
func NewOpBatch(commit func(ctx context.Context, ops []Op) error) *OpBatch {
	return &OpBatch{commit: commit}
}

func (b *OpBatch) Set(key, value []byte) {
	b.ops = append(b.ops, Op{Kind: OpSet, Key: clone(key), Value: clone(value)})
}

func (b *OpBatch) Delete(key []byte) {
	b.ops = append(b.ops, Op{Kind: OpDelete, Key: clone(key)})
}

func (b *OpBatch) DeleteRange(start, end []byte) {
	b.ops = append(b.ops, Op{Kind: OpDeleteRange, Key: clone(start), End: clone(end)})
}

func (b *OpBatch) Len() int { return len(b.ops) }

// Ops returns the staged ops. The slice is owned by the batch.
func (b *OpBatch) Ops() []Op { return b.ops }

func (b *OpBatch) Commit(ctx context.Context) error {
	if b.done {
		return ErrBatchCommitted
	}
	b.done = true
	return b.commit(ctx, b.ops)
}

// clone copies b, keeping nil distinct from empty so an empty value
// round-trips as empty rather than absent.
func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
