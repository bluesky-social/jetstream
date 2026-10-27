package pgstore_test

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/pgstore"
	"github.com/bluesky-social/jetstream/internal/pgstore/pgtest"
	"github.com/stretchr/testify/require"
)

func TestGCSessionPlanModes(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"force_custom_plan", "force_generic_plan", "auto"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			t.Run("mark_and_claim", func(t *testing.T) {
				store, session := newGCSession(t, mode)
				ctx := t.Context()
				before := gcSessionObjects(t, store, 1, 2, 3, 4, 7, 8, 10, 11, 12)
				beforeByID := make(map[uint64]catalog.ObjectRow, len(before))
				for _, row := range before {
					beforeByID[row.ID] = row
				}
				var after uint64
				for _, want := range []catalog.MarkPage{
					{Scanned: 3, Last: 3, Marked: 0},
					{Scanned: 3, Last: 6, Marked: 2},
					{Scanned: 1, Last: 9, Marked: 1},
					{},
				} {
					page, err := session.GCMark(ctx, after, 3)
					require.NoError(t, err)
					require.Equal(t, want, page, "committed mark page after %d", after)
					after = page.Last
				}
				require.Equal(t, uint64(4), gcSessionRevision(t, store), "each page commits its fence")
				require.Equal(t, before, gcSessionObjects(t, store, 1, 2, 3, 4, 7, 8, 10, 11, 12), "all four reference kinds and pre-existing ages survive marking")
				for _, row := range gcSessionObjects(t, store, 5, 6, 9) {
					require.False(t, row.UnreferencedAt.IsZero(), "object %d mark committed", row.ID)
				}

				rows, err := session.GCClaim(ctx, 6*time.Hour, time.Hour, 1)
				require.NoError(t, err)
				require.Equal(t, []catalog.ObjectRow{beforeByID[12]}, rows, "resume deleting rows before claiming fresh candidates")
				require.Equal(t, []catalog.ObjectRow{beforeByID[8], beforeByID[10]}, gcSessionObjects(t, store, 8, 10), "resume must not claim fresh candidates")
				n, err := session.GCForget(ctx, []uint64{12})
				require.NoError(t, err)
				require.Equal(t, 1, n)

				for _, want := range []catalog.ObjectRow{beforeByID[8], beforeByID[10]} {
					rows, err = session.GCClaim(ctx, 6*time.Hour, time.Hour, 1)
					require.NoError(t, err)
					require.Equal(t, []catalog.ObjectRow{want}, rows, "bounded claim returns complete pre-claim row")
					persisted := gcSessionObjects(t, store, want.ID)
					require.Len(t, persisted, 1)
					require.Equal(t, catalog.ObjectDeleting, persisted[0].State)
					n, err = session.GCForget(ctx, []uint64{want.ID})
					require.NoError(t, err)
					require.Equal(t, 1, n)
				}
				rows, err = session.GCClaim(ctx, 6*time.Hour, time.Hour, 100)
				require.NoError(t, err)
				require.Empty(t, rows, "recent marks and young uploading objects remain ineligible")
				require.Equal(t, []catalog.ObjectRow{beforeByID[11]}, gcSessionObjects(t, store, 11))
				require.Nil(t, session.Err())
			})

			t.Run("reference_guard", func(t *testing.T) {
				store, session := newGCSession(t, mode)
				ctx := t.Context()
				// A stale mark on a referenced row models the corruption the
				// script must catch after the SQL claim, before commit.
				_, err := store.Pool().Exec(ctx, `DELETE FROM objects WHERE object_id = 12;
					UPDATE objects SET unreferenced_at = now() - interval '8 hours' WHERE object_id = 1`)
				require.NoError(t, err)
				before := gcSessionObjects(t, store, 1, 8, 10)
				revision := gcSessionRevision(t, store)
				_, err = session.GCClaim(ctx, 6*time.Hour, time.Hour, 1)
				source, corrupt := catalog.IsCorruption(err)
				require.True(t, corrupt, "claiming a referenced object must abort the catalog script: %v", err)
				require.Equal(t, catalog.SourceGC, source)
				require.Equal(t, before, gcSessionObjects(t, store, 1, 8, 10), "claim state changes roll back on reference corruption")
				require.Equal(t, revision, gcSessionRevision(t, store), "the failed claim rolls back its fence")
				require.ErrorIs(t, session.Err(), err)
				_, later := session.GCMark(ctx, 0, 3)
				require.ErrorIs(t, later, err, "a corrupted session keeps returning its fatal error")
				require.Equal(t, revision, gcSessionRevision(t, store))
			})

			t.Run("stale_epoch", func(t *testing.T) {
				store, session := newGCSession(t, mode)
				ctx := t.Context()
				before := gcSessionObjects(t, store, 5, 8, 10, 12)
				for _, operation := range []string{"mark", "claim"} {
					stale := catalog.NewSession(catalog.SessionConfig{DB: store, Epoch: session.Epoch() + 1})
					var err error
					if operation == "mark" {
						_, err = stale.GCMark(ctx, 0, 3)
					} else {
						_, err = stale.GCClaim(ctx, 6*time.Hour, time.Hour, 1)
					}
					require.ErrorIs(t, err, catalog.ErrFenced, "%s must fence before accessing GC rows", operation)
					_, err = stale.GCMark(ctx, 0, 3)
					require.ErrorIs(t, err, catalog.ErrSessionEnded)
					require.Equal(t, before, gcSessionObjects(t, store, 5, 8, 10, 12))
					require.Zero(t, gcSessionRevision(t, store))
				}
			})
		})
	}
}

func newGCSession(t *testing.T, mode string) (*pgstore.Store, *catalog.Session) {
	t.Helper()
	u, err := url.Parse(pgtest.NewDatabase(t))
	require.NoError(t, err)
	query := u.Query()
	query.Set("plan_cache_mode", mode)
	u.RawQuery = query.Encode()
	store, err := pgstore.Open(t.Context(), pgstore.Config{URL: u.String(), MaxConns: 1})
	require.NoError(t, err)
	t.Cleanup(store.Close)
	require.NoError(t, store.Initialize(t.Context(), [16]byte{1}))
	var got string
	require.NoError(t, store.Pool().QueryRow(t.Context(), `SHOW plan_cache_mode`).Scan(&got))
	require.Equal(t, mode, got)
	lease := store.NewLease()
	require.NoError(t, lease.Acquire(t.Context(), time.Minute))
	_, err = store.Pool().Exec(t.Context(), `
		INSERT INTO objects(object_id, key, sha256, byte_length, state, created_at, unreferenced_at)
		SELECT id, md5(id::text)::uuid, decode(md5('a' || id) || md5('b' || id), 'hex'), 100 + id,
			CASE WHEN id IN (10,11) THEN 'uploading' WHEN id = 12 THEN 'deleting' ELSE 'available' END,
			now() - CASE WHEN id = 11 THEN interval '30 minutes' ELSE interval '2 days' END,
			CASE WHEN id = 7 THEN now() - interval '30 minutes' WHEN id = 8 THEN now() - interval '8 hours' END
		FROM generate_series(1,12) AS fixture(id);
		INSERT INTO segments(namespace, segment_index, state, current_generation_id, revision)
		VALUES ('main',0,'sealed',1,0), ('main',1,'active',NULL,0);
		INSERT INTO segment_generations(generation_id, namespace, segment_index, header, footer_object_id, revision)
		VALUES (1,'main',0,decode(repeat('00',256),'hex'),1,0);
		INSERT INTO generation_blocks(generation_id, ordinal, object_id, compressed_length) VALUES (1,0,2,102);
		INSERT INTO active_segment_blocks(namespace, segment_index, ordinal, object_id, event_count, min_seq, max_seq,
			min_witnessed_us, max_witnessed_us, compressed_length, uncompressed_length, revision)
		VALUES ('main',1,0,3,1,1,1,1,1,103,103,0);
		INSERT INTO hot_batches(first_seq, last_seq, event_count, min_witnessed_us, max_witnessed_us, epoch, revision, object_id)
		VALUES (2,2,1,1,1,1,0,4)`)
	require.NoError(t, err)
	return store, catalog.NewSession(catalog.SessionConfig{DB: store, Epoch: lease.Epoch()})
}

func gcSessionObjects(t *testing.T, store *pgstore.Store, ids ...uint64) []catalog.ObjectRow {
	t.Helper()
	read, err := store.BeginRead(t.Context())
	require.NoError(t, err)
	defer func() { require.NoError(t, read.Close(context.WithoutCancel(t.Context()))) }()
	rows, err := read.Objects(t.Context(), ids)
	require.NoError(t, err)
	return rows
}

func gcSessionRevision(t *testing.T, store *pgstore.Store) uint64 {
	t.Helper()
	var revision uint64
	require.NoError(t, store.Pool().QueryRow(t.Context(), `SELECT catalog_revision FROM archive WHERE id = 1`).Scan(&revision))
	return revision
}
