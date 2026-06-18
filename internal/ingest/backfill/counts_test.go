package backfill_test

import (
	"context"
	"errors"
	"testing"

	"github.com/bluesky-social/jetstream/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
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

	// One unavailable (deactivated account).
	goneDID := atmos.DID("did:plc:gone")
	require.NoError(t, bs.OnDiscover(ctx, atmossync.ListReposEntry{DID: goneDID, Active: true}))
	require.NoError(t, bs.OnFail(ctx, goneDID, &xrpc.Error{
		StatusCode: 400, Name: "RepoDeactivated", Message: "Repo has been deactivated",
	}, 1))

	got, err := backfill.CountStatuses(st)
	require.NoError(t, err)
	require.Equal(t, backfill.Counts{
		Total:       7,
		Discovered:  3,
		Complete:    2,
		Failed:      1,
		Unavailable: 1,
	}, got)
}

// TestCountStatuses_FailedToUnavailableMigration covers the self-healing
// path for rows persisted as StatusFailed before this behavior existed:
// the next Run re-dispatches them, getRepo returns RepoDeactivated again,
// and OnFail must move the row Failed -> Unavailable without leaving a
// phantom in the Failed bucket.
func TestCountStatuses_FailedToUnavailableMigration(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	bs := backfill.NewStore(st, nil)
	ctx := context.Background()

	did := atmos.DID("did:plc:legacy")
	require.NoError(t, bs.OnDiscover(ctx, atmossync.ListReposEntry{DID: did, Active: true}))
	// Legacy state: a generic failure marked it StatusFailed.
	require.NoError(t, bs.OnFail(ctx, did, errors.New("unknown xrpc 400 RepoDeactivated"), 3))

	got, err := backfill.CountStatuses(st)
	require.NoError(t, err)
	require.Equal(t, backfill.Counts{Total: 1, Failed: 1}, got)

	// Re-run now correctly classifies it as unavailable.
	require.NoError(t, bs.OnFail(ctx, did, &xrpc.Error{
		StatusCode: 400, Name: "RepoDeactivated", Message: "Repo has been deactivated",
	}, 1))

	got, err = backfill.CountStatuses(st)
	require.NoError(t, err)
	require.Equal(t, backfill.Counts{Total: 1, Unavailable: 1}, got)
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
