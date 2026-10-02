package syncstate

import (
	"errors"
	"testing"

	"github.com/bluesky-social/jetstream/internal/metastore"
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
// the commit propagates the injected error AND, because it failed, the
// promoted entry is NOT durable on a fresh reader (no silent advance) but is
// still promoted in memory, so the next batch persists it.
func TestStateStore_FlushFailsLoudOnStoreFault(t *testing.T) {
	t.Parallel()

	injected := errors.New("injected: syncstate flush commit failed")
	fault := &metastore.KeyPrefixFault{
		Prefix:  []byte("sync/"),
		Op:      metastore.WriteOpBatchCommit,
		Ordinal: 1,
		Err:     injected,
	}
	raw := newTestStore(t)

	s := New(metastore.WithFaults(raw, fault))
	did := parseDID(t, "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")
	want := atmossync.ChainState{Rev: "3l3qo2vutsw2b", Data: fixedCID(t)}
	require.NoError(t, s.SaveChain(t.Context(), did, want))
	s.PromoteChain(did, want.Rev)

	// The flush commit fails and must surface loud.
	require.ErrorIs(t, flush(t, s), injected)

	// No silent advance: a fresh reader sees nothing durable, because the
	// failed commit applied nothing.
	fresh := New(raw)
	durable, err := fresh.LoadChain(t.Context(), did)
	require.NoError(t, err)
	require.Nil(t, durable, "syncstate must not be durable when its commit failed")

	require.NoError(t, flush(t, s))
	durable, err = New(raw).LoadChain(t.Context(), did)
	require.NoError(t, err)
	require.NotNil(t, durable, "a failed commit must leave the promoted entry for the next batch")
	require.Equal(t, want, *durable)
}
