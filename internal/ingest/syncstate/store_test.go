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
	s, err := store.Open(t.TempDir(), nil)
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
	raw := newTestStore(t)
	s := New(raw)
	did := parseDID(t, "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")
	want := atmossync.ChainState{Rev: "3l3qo2vutsw2b", Data: fixedCID(t)}

	require.NoError(t, s.SaveChain(t.Context(), did, want))
	got, err := s.LoadChain(t.Context(), did)
	require.NoError(t, err)
	require.NotNil(t, got, "verifier must observe its own pending writes")
	require.Equal(t, want.Rev, got.Rev)
	require.True(t, want.Data.Equal(got.Data))

	fresh := New(raw)
	absent, err := fresh.LoadChain(t.Context(), did)
	require.NoError(t, err)
	require.Nil(t, absent, "staged chain state must not be durable before promotion+Flush")

	// Pending entries never flush — their event's rows are not durable.
	require.NoError(t, s.Flush())
	stillAbsent, err := fresh.LoadChain(t.Context(), did)
	require.NoError(t, err)
	require.Nil(t, stillAbsent, "pending (unpromoted) chain state must not flush")

	s.PromoteChain(did, want.Rev)
	require.NoError(t, s.Flush())
	durable, err := fresh.LoadChain(t.Context(), did)
	require.NoError(t, err)
	require.NotNil(t, durable)
	require.Equal(t, want.Rev, durable.Rev)
}

func TestStateStore_PromoteChainRevGate(t *testing.T) {
	t.Parallel()
	raw := newTestStore(t)
	s := New(raw)
	did := parseDID(t, "did:plc:eeeeeeeeeeeeeeeeeeeeeeee")

	// A pending entry staged by a LATER pipelined event (newer rev)
	// must not be promoted by an EARLIER event's group completion.
	require.NoError(t, s.SaveChain(t.Context(), did, atmossync.ChainState{Rev: "3lrev2", Data: fixedCID(t)}))
	s.PromoteChain(did, "3lrev1")
	require.NoError(t, s.Flush())

	fresh := New(raw)
	absent, err := fresh.LoadChain(t.Context(), did)
	require.NoError(t, err)
	require.Nil(t, absent, "newer-rev pending entry must survive an older promotion")

	s.PromoteChain(did, "3lrev2")
	require.NoError(t, s.Flush())
	durable, err := fresh.LoadChain(t.Context(), did)
	require.NoError(t, err)
	require.NotNil(t, durable)
	require.Equal(t, "3lrev2", durable.Rev)
}

func TestStateStore_HostingRoundTrip(t *testing.T) {
	t.Parallel()
	raw := newTestStore(t)
	s := New(raw)
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

	fresh := New(raw)
	absent, err := fresh.LoadHosting(t.Context(), did)
	require.NoError(t, err)
	require.Nil(t, absent, "staged hosting state must not be durable before promotion+Flush")

	require.NoError(t, s.Flush())
	stillAbsent, err := fresh.LoadHosting(t.Context(), did)
	require.NoError(t, err)
	require.Nil(t, stillAbsent, "pending (unpromoted) hosting state must not flush")

	s.PromoteHosting(did, want.Seq)
	require.NoError(t, s.Flush())
	durable, err := fresh.LoadHosting(t.Context(), did)
	require.NoError(t, err)
	require.NotNil(t, durable)
	require.Equal(t, want, *durable)
}

func TestStateStore_HostingPromotionIsSeqGated(t *testing.T) {
	t.Parallel()
	raw := newTestStore(t)
	s := New(raw)
	did := parseDID(t, "did:plc:ffffffffffffffffffffffff")
	newer := atmossync.HostingState{Active: true, Seq: 11}

	require.NoError(t, s.SaveHosting(t.Context(), did, newer))

	// The verifier reads its own pending write.
	live, err := s.LoadHosting(t.Context(), did)
	require.NoError(t, err)
	require.Equal(t, newer, *live)

	// A redelivered (replay-dropped) account row carries an OLDER seq:
	// it must not promote the newer event's pending state — that
	// event's row has not been appended yet.
	s.PromoteHosting(did, 10)
	require.NoError(t, s.Flush())
	fresh := New(raw)
	durable, err := fresh.LoadHosting(t.Context(), did)
	require.NoError(t, err)
	require.Nil(t, durable, "older account row must not promote a newer event's hosting state")

	// The producing event's own row promotes it.
	s.PromoteHosting(did, 11)
	require.NoError(t, s.Flush())
	durable, err = fresh.LoadHosting(t.Context(), did)
	require.NoError(t, err)
	require.NotNil(t, durable)
	require.Equal(t, newer, *durable)
}

func TestStateStore_CommitStagedKeepsLatePromotions(t *testing.T) {
	t.Parallel()
	raw := newTestStore(t)
	s := New(raw)
	did := parseDID(t, "did:plc:gggggggggggggggggggggggg")

	require.NoError(t, s.SaveChain(t.Context(), did, atmossync.ChainState{Rev: "3lrev1", Data: fixedCID(t)}))
	s.PromoteChain(did, "3lrev1")

	// Simulate the consumer's flush: capture promoted state into a
	// batch, then — before CommitStaged — a newer promotion lands
	// (resync worker finished and the consumer appended its group).
	b := raw.NewBatch()
	defer func() { _ = b.Close() }()
	require.NoError(t, s.StageFlush(b))

	require.NoError(t, s.SaveChain(t.Context(), did, atmossync.ChainState{Rev: "3lrev2", Data: fixedCID(t)}))
	s.PromoteChain(did, "3lrev2")

	require.NoError(t, raw.Commit(b, store.SyncWrites))
	s.CommitStaged()

	// The late promotion must NOT have been discarded by CommitStaged:
	// the next flush persists it.
	require.NoError(t, s.Flush())
	fresh := New(raw)
	durable, err := fresh.LoadChain(t.Context(), did)
	require.NoError(t, err)
	require.NotNil(t, durable)
	require.Equal(t, "3lrev2", durable.Rev, "promotion landing between StageFlush and CommitStaged must survive")
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
