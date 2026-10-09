package pgstore_test

import (
	"testing"

	"github.com/bluesky-social/jetstream/internal/pgstore/pgtest"
	"github.com/stretchr/testify/require"
)

// The control tables carry requests in order, each answered once, and the
// last status document; creating them twice is harmless, and they leave the
// archive's versions alone.
func TestMigrationControl(t *testing.T) {
	t.Parallel()
	st, _ := pgtest.Open(t, nil)
	ctx := t.Context()
	for range 2 {
		require.NoError(t, st.EnsureMigrationControl(ctx))
	}
	_, err := st.CheckVersions(ctx)
	require.NoError(t, err)

	_, ok, err := st.ClaimMigrationRequest(ctx)
	require.NoError(t, err)
	require.False(t, ok)
	a, err := st.RequestMigration(ctx, "handoff")
	require.NoError(t, err)
	b, err := st.RequestMigration(ctx, "abort")
	require.NoError(t, err)
	c, err := st.RequestMigration(ctx, "handoff")
	require.NoError(t, err)

	// Claims take requests in order, each once.
	r, ok, err := st.ClaimMigrationRequest(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, a, r.ID)
	require.Equal(t, "handoff", r.Action)
	require.False(t, r.ClaimedAt.IsZero())
	withdrawn, err := st.WithdrawMigrationRequest(ctx, a, "withdrawn")
	require.NoError(t, err)
	require.False(t, withdrawn, "a claimed request cannot be withdrawn")
	require.NoError(t, st.AckMigrationRequest(ctx, a, "refused: lagging"))
	require.NoError(t, st.AckMigrationRequest(ctx, a, "ignored"), "a second answer is ignored")
	got, err := st.MigrationRequestByID(ctx, a)
	require.NoError(t, err)
	require.Equal(t, "refused: lagging", got.Result)
	require.False(t, got.AckedAt.IsZero())

	// An unclaimed request is withdrawn, and never claimed after.
	withdrawn, err = st.WithdrawMigrationRequest(ctx, b, "withdrawn")
	require.NoError(t, err)
	require.True(t, withdrawn)
	r, ok, err = st.ClaimMigrationRequest(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, c, r.ID)
	_, ok, err = st.ClaimMigrationRequest(ctx)
	require.NoError(t, err)
	require.False(t, ok, "a claimed request is not claimed again")

	// Abandoning answers claimed and unclaimed requests alike.
	d, err := st.RequestMigration(ctx, "abort")
	require.NoError(t, err)
	require.NoError(t, st.AbandonMigrationRequests(ctx, "abandoned"))
	for _, id := range []int64{c, d} {
		got, err := st.MigrationRequestByID(ctx, id)
		require.NoError(t, err)
		require.Equal(t, "abandoned", got.Result)
	}
	got, err = st.MigrationRequestByID(ctx, b)
	require.NoError(t, err)
	require.Equal(t, "withdrawn", got.Result, "an answered request keeps its answer")

	_, _, ok, err = st.MigrationStatus(ctx)
	require.NoError(t, err)
	require.False(t, ok)
	for _, doc := range []string{`{"a":1}`, `{"a":2}`} {
		require.NoError(t, st.SetMigrationStatus(ctx, []byte(doc)))
	}
	doc, at, ok, err := st.MigrationStatus(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, `{"a":2}`, string(doc))
	require.False(t, at.IsZero())
}
