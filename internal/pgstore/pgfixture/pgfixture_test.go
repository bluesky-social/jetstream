package pgfixture_test

import (
	"context"
	"crypto/rand"
	"testing"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/pgstore"
	"github.com/bluesky-social/jetstream/internal/pgstore/pgfixture"
	"github.com/bluesky-social/jetstream/internal/pgstore/pgtest"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestFixtureLifecycle(t *testing.T) {
	t.Parallel()
	admin := pgtest.URL(t)
	ctx := t.Context()

	url, drop, err := pgfixture.CreateDatabase(ctx, admin)
	require.NoError(t, err)
	dropped := false
	t.Cleanup(func() {
		if !dropped {
			_ = drop(context.Background())
		}
	})

	s, err := pgstore.Open(ctx, pgstore.Config{URL: url})
	require.NoError(t, err)
	var id [16]byte
	_, _ = rand.Read(id[:])
	require.NoError(t, s.Initialize(ctx, id))

	walBefore, err := pgfixture.WALPosition(ctx, s)
	require.NoError(t, err)
	tx, err := s.Begin(ctx, catalog.TxObjects)
	require.NoError(t, err)
	objs := make([]catalog.NewObject, 3)
	for i := range objs {
		_, _ = rand.Read(objs[i].Key[:])
		_, _ = rand.Read(objs[i].SHA256[:])
		objs[i].Length = 1
	}
	_, err = tx.InsertObjects(ctx, objs)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))
	walAfter, err := pgfixture.WALPosition(ctx, s)
	require.NoError(t, err)
	require.Greater(t, walAfter, walBefore)

	n, err := pgfixture.MarkObjectsAvailable(ctx, s)
	require.NoError(t, err)
	require.EqualValues(t, 3, n)
	n, err = pgfixture.MarkObjectsAvailable(ctx, s)
	require.NoError(t, err)
	require.Zero(t, n)

	size, err := pgfixture.DatabaseBytes(ctx, s)
	require.NoError(t, err)
	require.Positive(t, size)

	// Drop disconnects sessions still open on the database.
	require.NoError(t, drop(ctx))
	dropped = true
	s.Close()
	c, err := pgx.Connect(ctx, url)
	if err == nil {
		_ = c.Close(ctx)
	}
	require.Error(t, err)
}

func TestCreateDatabaseHidesURL(t *testing.T) {
	t.Parallel()
	for _, u := range []string{"mysql://u:s3cretpw@h/db", "postgres://u:s3cretpw@127.0.0.1:1/db?connect_timeout=1"} {
		_, _, err := pgfixture.CreateDatabase(t.Context(), u)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "s3cretpw")
	}
}
