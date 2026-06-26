package syncstate

import (
	"errors"
	"testing"

	"github.com/bluesky-social/jetstream/internal/store"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/stretchr/testify/require"
)

// TestStateStore_FlushFailsLoudOnStoreFault pins the fail-loud contract for
// the syncstate commit boundary (issue #30 fault point: "syncstate commits").
// A failed verifier-state commit must surface as an error, not be swallowed —
// otherwise the in-memory promoted state would be cleared (CommitStaged) while
// nothing reached disk, silently losing sync state across a restart and
// letting the verifier run ahead of the durable archive.
//
// The fault targets the sync/ prefix batch commit; the assertion is that
// Flush propagates the injected error AND, because the commit failed, the
// promoted entry is NOT durable on a fresh reader (no silent advance).
func TestStateStore_FlushFailsLoudOnStoreFault(t *testing.T) {
	t.Parallel()

	injected := errors.New("injected: syncstate flush commit failed")
	fault := &store.KeyPrefixFault{
		Prefix:  []byte("sync/"),
		Op:      store.WriteOpBatchCommit,
		Ordinal: 1,
		Err:     injected,
	}
	raw, err := store.Open(t.TempDir(), nil, store.WithFaultInjector(fault))
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })

	s := New(raw)
	did := parseDID(t, "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")
	want := atmossync.ChainState{Rev: "3l3qo2vutsw2b", Data: fixedCID(t)}
	require.NoError(t, s.SaveChain(t.Context(), did, want))
	s.PromoteChain(did, want.Rev)

	// The flush commit fails: Flush must surface it loud.
	require.ErrorIs(t, s.Flush(), injected)

	// No silent advance: a fresh reader sees nothing durable, because the
	// failed commit applied nothing.
	fresh := New(raw)
	durable, err := fresh.LoadChain(t.Context(), did)
	require.NoError(t, err)
	require.Nil(t, durable, "syncstate must not be durable when its commit failed")
}
