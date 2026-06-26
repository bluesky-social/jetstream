package store_test

import (
	"errors"
	"testing"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/stretchr/testify/require"
)

var errInjected = errors.New("injected store fault")

// TestKeyPrefixFault_FailsTargetedOrdinalOnly proves the canonical
// injector fails exactly the Ordinal-th write under the prefix and that
// the failed write never reaches pebble: a Set that the injector aborts
// must leave no row behind.
func TestKeyPrefixFault_FailsTargetedOrdinalOnly(t *testing.T) {
	t.Parallel()
	fault := &store.KeyPrefixFault{Prefix: []byte("merge/"), Ordinal: 2, Err: errInjected}
	s, err := store.Open(t.TempDir(), nil, store.WithFaultInjector(fault))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	// 1st matching write: succeeds.
	require.NoError(t, s.Set([]byte("merge/a"), []byte("v1"), store.SyncWrites))
	// 2nd matching write: the targeted ordinal fails.
	err = s.Set([]byte("merge/b"), []byte("v2"), store.SyncWrites)
	require.ErrorIs(t, err, errInjected)
	// 3rd matching write: succeeds again (fault is one-shot).
	require.NoError(t, s.Set([]byte("merge/c"), []byte("v3"), store.SyncWrites))

	// The aborted write must not have persisted: continuing past a failed
	// persistence op is exactly the corruption class this seam exists to
	// catch, so the seam itself must not silently write.
	_, _, getErr := s.Get([]byte("merge/b"))
	require.ErrorIs(t, getErr, store.ErrNotFound)

	// Non-matching keys are never faulted regardless of ordinal.
	require.NoError(t, s.Set([]byte("repo/x"), []byte("v"), store.SyncWrites))
}

// TestKeyPrefixFault_BatchCommitMatchesStagedKey proves a batch commit is
// faulted when any staged key matches the prefix — the path m006 rides
// (commitSourceComplete stages merge/next_source_idx + repo/<did> rows in
// one batch). A failed commit must leave the entire batch unapplied.
func TestKeyPrefixFault_BatchCommitMatchesStagedKey(t *testing.T) {
	t.Parallel()
	fault := &store.KeyPrefixFault{
		Prefix:  []byte("merge/next_source_idx"),
		Op:      store.WriteOpBatchCommit,
		Ordinal: 1,
		Err:     errInjected,
	}
	s, err := store.Open(t.TempDir(), nil, store.WithFaultInjector(fault))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	b := s.NewBatch()
	t.Cleanup(func() { _ = b.Close() })
	require.NoError(t, b.Set([]byte("merge/next_source_idx"), []byte{0x01}, nil))
	require.NoError(t, b.Set([]byte("repo/did:plc:a"), []byte("status"), nil))

	require.ErrorIs(t, s.Commit(b, store.SyncWrites), errInjected)

	// Neither staged key applied: a failed commit is atomic.
	_, _, getErr := s.Get([]byte("merge/next_source_idx"))
	require.ErrorIs(t, getErr, store.ErrNotFound)
	_, _, getErr = s.Get([]byte("repo/did:plc:a"))
	require.ErrorIs(t, getErr, store.ErrNotFound)
}

// TestKeyPrefixFault_OpFilterIgnoresNonMatchingOp confirms the Op filter:
// a batch-commit-scoped fault does not fire on a plain Set even when the
// key matches, so a scenario can target the precise write boundary.
func TestKeyPrefixFault_OpFilterIgnoresNonMatchingOp(t *testing.T) {
	t.Parallel()
	fault := &store.KeyPrefixFault{
		Prefix:  []byte("merge/"),
		Op:      store.WriteOpBatchCommit,
		Ordinal: 1,
		Err:     errInjected,
	}
	s, err := store.Open(t.TempDir(), nil, store.WithFaultInjector(fault))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	// Set matches the prefix but not the op → not faulted.
	require.NoError(t, s.Set([]byte("merge/x"), []byte("v"), store.SyncWrites))

	// A matching batch commit is faulted (ordinal still 1 — the Set above
	// did not consume it, proving op-scoped counting).
	b := s.NewBatch()
	t.Cleanup(func() { _ = b.Close() })
	require.NoError(t, b.Set([]byte("merge/y"), []byte("v"), nil))
	require.ErrorIs(t, s.Commit(b, store.SyncWrites), errInjected)
}

// TestStore_NoFaultInjectorIsClean is the production-shape guard: with no
// option installed, every write succeeds. Protects the nil-gated contract
// (production never arms the seam).
func TestStore_NoFaultInjectorIsClean(t *testing.T) {
	t.Parallel()
	s, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	require.NoError(t, s.Set([]byte("merge/a"), []byte("v"), store.SyncWrites))
	require.NoError(t, s.Delete([]byte("merge/a"), store.SyncWrites))
}
