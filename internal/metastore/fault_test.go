package metastore_test

import (
	"context"
	"errors"
	"testing"

	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/metastore/memstore"
	"github.com/bluesky-social/jetstream/internal/metastore/pebblestore"
	"github.com/bluesky-social/jetstream/internal/metastore/storetest"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

var errInjected = errors.New("injected store fault")

func openFaulty(t *testing.T, f metastore.FaultInjector) metastore.Store {
	t.Helper()
	s, err := pebblestore.Open("/data", nil, pebblestore.WithFS(vfs.NewMem()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return metastore.WithFaults(s, f)
}

// TestKeyPrefixFault_FailsTargetedOrdinalOnly proves the canonical
// injector fails exactly the Ordinal-th write under the prefix and that
// the failed write never reaches the store: a Set that the injector aborts
// must leave no row behind.
func TestKeyPrefixFault_FailsTargetedOrdinalOnly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openFaulty(t, &metastore.KeyPrefixFault{Prefix: []byte("merge/"), Ordinal: 2, Err: errInjected})

	// 1st matching write: succeeds.
	require.NoError(t, s.Set(ctx, []byte("merge/a"), []byte("v1")))
	// 2nd matching write: the targeted ordinal fails.
	require.ErrorIs(t, s.Set(ctx, []byte("merge/b"), []byte("v2")), errInjected)
	// 3rd matching write: succeeds again (fault is one-shot).
	require.NoError(t, s.Set(ctx, []byte("merge/c"), []byte("v3")))

	// The aborted write must not have persisted: continuing past a failed
	// persistence op is exactly the corruption class this seam exists to
	// catch, so the seam itself must not silently write.
	_, err := s.Get(ctx, []byte("merge/b"))
	require.ErrorIs(t, err, metastore.ErrNotFound)

	// Non-matching keys are never faulted regardless of ordinal.
	require.NoError(t, s.Set(ctx, []byte("repo/x"), []byte("v")))
}

// TestKeyPrefixFault_BatchCommitMatchesStagedKey proves a batch commit is
// faulted when any staged key matches the prefix — the path m006 rides
// (commitSourceComplete stages merge/next_source_idx + repo/<did> rows in
// one batch). A failed commit must leave the entire batch unapplied.
func TestKeyPrefixFault_BatchCommitMatchesStagedKey(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openFaulty(t, &metastore.KeyPrefixFault{
		Prefix:  []byte("merge/next_source_idx"),
		Op:      metastore.WriteOpBatchCommit,
		Ordinal: 1,
		Err:     errInjected,
	})

	b := s.NewBatch()
	b.Set([]byte("merge/next_source_idx"), []byte{0x01})
	b.Set([]byte("repo/did:plc:a"), []byte("status"))
	require.ErrorIs(t, b.Commit(ctx), errInjected)

	// Neither staged key applied: a failed commit is atomic.
	_, err := s.Get(ctx, []byte("merge/next_source_idx"))
	require.ErrorIs(t, err, metastore.ErrNotFound)
	_, err = s.Get(ctx, []byte("repo/did:plc:a"))
	require.ErrorIs(t, err, metastore.ErrNotFound)
}

// TestKeyPrefixFault_DeleteRangeMatchesStartKey keeps the pre-metastore
// behavior, where a staged range deletion contributed its start key.
func TestKeyPrefixFault_DeleteRangeMatchesStartKey(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openFaulty(t, &metastore.KeyPrefixFault{Prefix: []byte("seq/gap/"), Ordinal: 1, Err: errInjected})
	b := s.NewBatch()
	b.DeleteRange([]byte("seq/gap/"), []byte("seq/gap0"))
	require.ErrorIs(t, b.Commit(ctx), errInjected)
}

// TestKeyPrefixFault_OpFilterIgnoresNonMatchingOp confirms the Op filter:
// a batch-commit-scoped fault does not fire on a plain Set even when the
// key matches, so a scenario can target the precise write boundary.
func TestKeyPrefixFault_OpFilterIgnoresNonMatchingOp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openFaulty(t, &metastore.KeyPrefixFault{
		Prefix:  []byte("merge/"),
		Op:      metastore.WriteOpBatchCommit,
		Ordinal: 1,
		Err:     errInjected,
	})

	// Set matches the prefix but not the op → not faulted.
	require.NoError(t, s.Set(ctx, []byte("merge/x"), []byte("v")))
	require.NoError(t, s.Delete(ctx, []byte("merge/x")))

	// A matching batch commit is faulted (ordinal still 1 — the Set above
	// did not consume it, proving op-scoped counting).
	b := s.NewBatch()
	b.Set([]byte("merge/y"), []byte("v"))
	require.ErrorIs(t, b.Commit(ctx), errInjected)
}

// TestWithFaults_NilIsIdentity is the production-shape guard: a nil injector
// installs nothing.
func TestWithFaults_NilIsIdentity(t *testing.T) {
	t.Parallel()
	s, err := pebblestore.Open("/data", nil, pebblestore.WithFS(vfs.NewMem()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	require.Same(t, s, metastore.WithFaults(s, nil))
}

// TestKeyPrefixFault_FailedCommitConsumesBatch pins the single-use contract
// through the wrapper: retrying a batch the injector rejected must not apply
// it, or a fault test could pass on a retry the real stores would refuse.
func TestKeyPrefixFault_FailedCommitConsumesBatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openFaulty(t, &metastore.KeyPrefixFault{Prefix: []byte("merge/"), Ordinal: 1, Err: errInjected})

	b := s.NewBatch()
	b.Set([]byte("merge/a"), []byte("v"))
	require.ErrorIs(t, b.Commit(ctx), errInjected)
	require.ErrorIs(t, b.Commit(ctx), metastore.ErrBatchCommitted)

	_, err := s.Get(ctx, []byte("merge/a"))
	require.ErrorIs(t, err, metastore.ErrNotFound)
}

// neverFault is an installed injector that never fires, so the contract
// suite exercises the wrapper's own code paths rather than the nil identity.
type neverFault struct{}

func (neverFault) BeforeWrite(metastore.WriteOp, [][]byte) error { return nil }

func TestWithFaults_Contract(t *testing.T) {
	t.Parallel()
	storetest.Run(t, func(*testing.T) metastore.Store {
		return metastore.WithFaults(memstore.New(), neverFault{})
	})
}
