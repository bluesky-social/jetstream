package pgstore_test

import (
	"context"
	"testing"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestGCClaimPreservesRowLock(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"force_custom_plan", "force_generic_plan", "auto"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			store, session := newGCSession(t, mode)
			ctx := t.Context()
			before := gcSessionObjects(t, store, 8)
			require.Len(t, before, 1)
			revision := gcSessionRevision(t, store)
			other, err := pgx.Connect(ctx, store.Pool().Config().ConnConfig.ConnString())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, other.Close(context.WithoutCancel(ctx))) })

			// Call the storage claim directly so the fixture's deleting row
			// does not take the session's resume-before-new path.
			tx, err := store.Begin(ctx, catalog.TxGC)
			require.NoError(t, err)
			defer func() { require.NoError(t, tx.Rollback(context.WithoutCancel(ctx))) }()
			_, ok, err := tx.FenceBump(ctx, session.Epoch())
			require.NoError(t, err)
			require.True(t, ok)
			claimed, err := tx.ClaimObjects(ctx, gcPlanGCDelay, gcPlanOrphanAge, 1)
			require.NoError(t, err)
			require.Equal(t, before, claimed, "claim returns object 8's complete pre-claim row")

			// A non-key UPDATE alone permits KEY SHARE, so this detects
			// removal of the claim's explicit FOR UPDATE lock.
			var id uint64
			err = other.QueryRow(ctx, `SELECT object_id FROM objects WHERE object_id = $1 FOR KEY SHARE NOWAIT`, int64(claimed[0].ID)).Scan(&id)
			var pgErr *pgconn.PgError
			require.ErrorAs(t, err, &pgErr)
			require.Equal(t, "55P03", pgErr.Code, "the selected row stays locked until the claim transaction ends")

			require.NoError(t, tx.Rollback(ctx))
			require.NoError(t, other.QueryRow(ctx, `SELECT object_id FROM objects WHERE object_id = $1 FOR KEY SHARE NOWAIT`, int64(claimed[0].ID)).Scan(&id))
			require.Equal(t, claimed[0].ID, id)
			require.Equal(t, before, gcSessionObjects(t, store, 8), "rollback restores all object fields")
			require.Equal(t, revision, gcSessionRevision(t, store), "rollback restores the fence revision")
		})
	}
}
