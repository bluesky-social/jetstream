package syncstate

import (
	"context"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/jcalabro/atmos"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func parseDID(t *testing.T, s string) atmos.DID {
	t.Helper()
	d, err := atmos.ParseDID(s)
	require.NoError(t, err)
	return d
}

func TestStateStore_LoadChain_AbsentReturnsNil(t *testing.T) {
	t.Parallel()
	s := New(newTestStore(t))

	got, err := s.LoadChain(t.Context(), parseDID(t, "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"))
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestStateStore_ChainRoundTrip(t *testing.T) {
	t.Parallel()
	s := New(newTestStore(t))
	did := parseDID(t, "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")
	want := atmossync.ChainState{Rev: "3l3qo2vutsw2b", Data: fixedCID(t)}

	require.NoError(t, s.SaveChain(t.Context(), did, want))
	got, err := s.LoadChain(t.Context(), did)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, want.Rev, got.Rev)
	require.True(t, want.Data.Equal(got.Data))
}

func TestStateStore_HostingRoundTrip(t *testing.T) {
	t.Parallel()
	s := New(newTestStore(t))
	did := parseDID(t, "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb")
	want := atmossync.HostingState{
		Active: false,
		Status: "takendown",
		Seq:    99,
		Time:   "2026-05-21T00:00:00Z",
	}

	require.NoError(t, s.SaveHosting(t.Context(), did, want))
	got, err := s.LoadHosting(t.Context(), did)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, want, *got)
}

func TestStateStore_DistinctKeyspaces(t *testing.T) {
	t.Parallel()
	s := New(newTestStore(t))
	did := parseDID(t, "did:plc:cccccccccccccccccccccccc")

	require.NoError(t, s.SaveChain(t.Context(), did, atmossync.ChainState{Rev: "r", Data: fixedCID(t)}))
	got, err := s.LoadHosting(t.Context(), did)
	require.NoError(t, err)
	require.Nil(t, got, "saving chain must not produce hosting state")

	require.NoError(t, s.SaveHosting(t.Context(), did, atmossync.HostingState{Active: true}))
	cs, err := s.LoadChain(t.Context(), did)
	require.NoError(t, err)
	require.NotNil(t, cs, "saving hosting must not clobber chain state")
}

func TestStateStore_DeleteRemovesBoth(t *testing.T) {
	t.Parallel()
	s := New(newTestStore(t))
	did := parseDID(t, "did:plc:dddddddddddddddddddddddd")

	require.NoError(t, s.SaveChain(t.Context(), did, atmossync.ChainState{Rev: "r", Data: fixedCID(t)}))
	require.NoError(t, s.SaveHosting(t.Context(), did, atmossync.HostingState{Active: true}))
	require.NoError(t, s.Delete(t.Context(), did))

	cs, err := s.LoadChain(t.Context(), did)
	require.NoError(t, err)
	require.Nil(t, cs)

	hs, err := s.LoadHosting(t.Context(), did)
	require.NoError(t, err)
	require.Nil(t, hs)
}

func TestStateStore_TwoDIDs(t *testing.T) {
	t.Parallel()
	s := New(newTestStore(t))
	a := parseDID(t, "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")
	b := parseDID(t, "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb")

	require.NoError(t, s.SaveChain(t.Context(), a, atmossync.ChainState{Rev: "rev-a", Data: fixedCID(t)}))
	require.NoError(t, s.SaveChain(t.Context(), b, atmossync.ChainState{Rev: "rev-b", Data: fixedCID(t)}))

	ga, err := s.LoadChain(t.Context(), a)
	require.NoError(t, err)
	require.Equal(t, "rev-a", ga.Rev)

	gb, err := s.LoadChain(t.Context(), b)
	require.NoError(t, err)
	require.Equal(t, "rev-b", gb.Rev)
}

func TestStateStore_ImplementsInterface(t *testing.T) {
	t.Parallel()
	var _ atmossync.StateStore = (*PebbleStateStore)(nil)
}

func TestStateStore_CancelledContext(t *testing.T) {
	t.Parallel()
	s := New(newTestStore(t))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, _ = s.LoadChain(ctx, parseDID(t, "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"))
}
