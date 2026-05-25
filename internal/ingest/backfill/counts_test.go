package backfill_test

import (
	"context"
	"errors"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestCountStatuses_Empty(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	got, err := backfill.CountStatuses(st)
	require.NoError(t, err)
	require.Equal(t, backfill.Counts{}, got)
}

func TestCountStatuses_MixedStates(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	bs := backfill.NewStore(st, nil)
	ctx := context.Background()

	// Three discovered.
	for i := range 3 {
		did := atmos.DID("did:plc:disc" + string(rune('a'+i)))
		require.NoError(t, bs.OnDiscover(ctx, atmossync.ListReposEntry{DID: did, Active: true}))
	}
	// Two completed.
	for i := range 2 {
		did := atmos.DID("did:plc:done" + string(rune('a'+i)))
		require.NoError(t, bs.OnDiscover(ctx, atmossync.ListReposEntry{DID: did, Active: true}))
		require.NoError(t, bs.OnComplete(ctx, did, &repo.Commit{Rev: "abcdef"}))
	}
	// One failed.
	failDID := atmos.DID("did:plc:fail")
	require.NoError(t, bs.OnDiscover(ctx, atmossync.ListReposEntry{DID: failDID, Active: true}))
	require.NoError(t, bs.OnFail(ctx, failDID, errors.New("nope"), 1))

	got, err := backfill.CountStatuses(st)
	require.NoError(t, err)
	require.Equal(t, backfill.Counts{
		Total:      6,
		Discovered: 3,
		Complete:   2,
		Failed:     1,
	}, got)
}

func TestCountStatuses_CorruptRowCountedInTotalOnly(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	bs := backfill.NewStore(st, nil)
	ctx := context.Background()

	// One valid discovered row.
	good := atmos.DID("did:plc:good")
	require.NoError(t, bs.OnDiscover(ctx, atmossync.ListReposEntry{DID: good, Active: true}))

	// One row with a non-JSON value at repo/<did>. CountStatuses must
	// tolerate the bad decode: the row contributes to Total but to no
	// bucket. Total != sum is the operator's signal that the data is
	// corrupt.
	require.NoError(t, st.Set([]byte("repo/did:plc:corrupt"), []byte("not json"), store.SyncWrites))

	got, err := backfill.CountStatuses(st)
	require.NoError(t, err)
	require.Equal(t, backfill.Counts{
		Total:      2,
		Discovered: 1,
	}, got)
}
