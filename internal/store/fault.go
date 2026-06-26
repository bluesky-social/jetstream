package store

import (
	"bytes"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
)

// WriteOp names the metadata-store write being attempted. The values
// mirror the metric op labels (set / delete / batch_commit) so a fault
// target reads the same in a test as it does on the histogram.
type WriteOp string

const (
	WriteOpSet         WriteOp = "set"
	WriteOpDelete      WriteOp = "delete"
	WriteOpBatchCommit WriteOp = "batch_commit"
)

// FaultInjector is the test-only seam that lets a scenario fail selected
// metadata-store writes deterministically, modeling a Pebble persistence
// failure that production code must surface rather than swallow.
//
// Production never installs one: Open takes zero options, so the field is
// nil and BeforeWrite is never consulted (mirrors the nil *Metrics and the
// nil crashpoint.Injector idioms — the fault path adds no production cost
// and cannot be armed by accident).
//
// BeforeWrite is consulted BEFORE the underlying Pebble write. A non-nil
// return aborts the write entirely (the bytes never reach Pebble) and the
// store returns that error to the caller. This is the faithful model of a
// failed persistence op: a failed batch commit leaves the keyspace
// untouched, so a correct caller must not advance any cursor past it.
//
// keys is every key the op will touch: one for Set/Delete, all staged keys
// for a batch Commit (decoded from the batch's own repr). Implementations
// must be safe for concurrent use; *Store is.
type FaultInjector interface {
	BeforeWrite(op WriteOp, keys [][]byte) error
}

// Option configures a Store at Open time. Kept as a functional option so
// production (store.Open(dir, metrics)) stays a clean two-argument call and
// no post-construction setter can race a concurrent writer.
type Option func(*Store)

// WithFaultInjector installs a test-only write-fault seam. Passing nil is a
// no-op, so a test can thread an optional injector unconditionally.
func WithFaultInjector(f FaultInjector) Option {
	return func(s *Store) {
		if f != nil {
			s.faults = f
		}
	}
}

// faultBeforeWrite consults the injector (if any) for a single-key op. It
// returns the injected error when the write should fail, in which case the
// caller must skip the underlying Pebble write.
func (s *Store) faultBeforeWrite(op WriteOp, key []byte) error {
	if s.faults == nil {
		return nil
	}
	return s.faults.BeforeWrite(op, [][]byte{key})
}

// faultBeforeCommit consults the injector for a batch commit, passing every
// key staged in the batch so a fault can target by key name. Reading the
// batch repr is cheap (a header walk) and only happens when an injector is
// installed, so production pays nothing.
func (s *Store) faultBeforeCommit(b *pebble.Batch) error {
	if s.faults == nil {
		return nil
	}
	keys, err := batchKeys(b)
	if err != nil {
		return err
	}
	return s.faults.BeforeWrite(WriteOpBatchCommit, keys)
}

// batchKeys decodes the set of user keys staged in a pebble batch via its
// public BatchReader. Used only on the fault path; values are ignored.
func batchKeys(b *pebble.Batch) ([][]byte, error) {
	r := b.Reader()
	var keys [][]byte
	for {
		_, ukey, _, ok, err := r.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			return keys, nil
		}
		// ukey aliases the batch's internal buffer; copy so a retained
		// matcher can't observe later mutation.
		k := make([]byte, len(ukey))
		copy(k, ukey)
		keys = append(keys, k)
	}
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
