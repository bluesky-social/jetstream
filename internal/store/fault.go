package store

import (
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/cockroachdb/pebble"
)

// WriteOp, FaultInjector, and KeyPrefixFault alias the metastore types so a
// single injector fires exactly once per write while packages move from
// *Store onto metastore.Store (plan S1.2-S1.5). S1.6 removes this seam in
// favor of metastore.WithFaults.
type (
	WriteOp        = metastore.WriteOp
	FaultInjector  = metastore.FaultInjector
	KeyPrefixFault = metastore.KeyPrefixFault
)

const (
	WriteOpSet         = metastore.WriteOpSet
	WriteOpDelete      = metastore.WriteOpDelete
	WriteOpBatchCommit = metastore.WriteOpBatchCommit
)

// WithFaultInjector installs a test-only write-fault seam. Passing nil is a
// no-op, so a test can thread an optional injector unconditionally.
func WithFaultInjector(f FaultInjector) Option {
	return func(o *openOptions) {
		if f != nil {
			o.faults = f
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
