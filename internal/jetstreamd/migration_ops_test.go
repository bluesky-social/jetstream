package jetstreamd_test

import (
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/jetstreamd/jetstreamdtest"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/metastore/pebblestore"
	"github.com/bluesky-social/jetstream/internal/migrate"
	"github.com/bluesky-social/jetstream/internal/seqspace"
	"github.com/bluesky-social/jetstream/internal/storagefake"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// An operator request no migrator claims in time is withdrawn, so it can
// never run after the operator stopped waiting; a claimed one is reported
// as underway, and an answered one returns its answer.
func TestRequestMigration(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	fake := jetstreamdtest.New(storagefake.Config{})
	require.NoError(t, fake.InitMigration(ctx))

	_, err := fake.Backend.RequestMigration(ctx, "explode", time.Second)
	require.ErrorContains(t, err, "unknown action")

	_, err = fake.Backend.RequestMigration(ctx, migrate.ActionHandoff, 10*time.Millisecond)
	require.ErrorContains(t, err, "withdrawn")
	_, ok, err := fake.Control.Claim(ctx)
	require.NoError(t, err)
	require.False(t, ok, "a withdrawn request is never claimed")

	// A migrator that claims and does not answer in time.
	claimed := make(chan struct{})
	go func() {
		defer close(claimed)
		for {
			if _, ok, _ := fake.Control.Claim(ctx); ok {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	_, err = fake.Backend.RequestMigration(ctx, migrate.ActionHandoff, 50*time.Millisecond)
	<-claimed
	require.ErrorContains(t, err, "acting on request")

	// One that answers.
	go func() {
		for {
			if req, ok, _ := fake.Control.Claim(ctx); ok {
				_ = fake.Control.Ack(ctx, req.ID, "refused: not tailing")
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	result, err := fake.Backend.RequestMigration(ctx, migrate.ActionHandoff, 10*time.Second)
	require.NoError(t, err)
	require.Equal(t, "refused: not tailing", result)

	rep, err := fake.Backend.MigrationReport(ctx)
	require.NoError(t, err)
	require.Equal(t, catalog.MigrationSeeding, rep.State)
	require.Nil(t, rep.Migrator, "no migrator has published a status")
}

func TestReclaimLocal_InvalidMarginPreservesHandoff(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name                   string
		localNext, catalogNext uint64
	}{
		{"local frontier", 200, 100},
		{"catalog frontier", 100, 200},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			fake := jetstreamdtest.New(storagefake.Config{})
			require.NoError(t, fake.InitMigration(ctx))
			require.NoError(t, fake.SetMigration(ctx, catalog.MigrationSeeding, catalog.MigrationTailing))
			require.NoError(t, fake.SetMigration(ctx, catalog.MigrationTailing, catalog.MigrationHandingOff))
			require.NoError(t, fake.SetMigration(ctx, catalog.MigrationHandingOff, catalog.MigrationDone))
			lease := fake.DB.NewLease()
			require.NoError(t, lease.Acquire(ctx, time.Minute))
			sess := catalog.NewSession(catalog.SessionConfig{DB: fake.DB, Epoch: lease.Epoch()})
			_, err := sess.CommitMeta(ctx, []metastore.Op{{
				Kind: metastore.OpSet, Key: []byte(catalog.MainSeqKey), Value: catalog.EncodeSeq(tt.catalogNext),
			}})
			require.NoError(t, err)
			require.NoError(t, lease.Release(ctx))
			before, err := fake.DB.Snapshot()
			require.NoError(t, err)

			fsys := vfs.NewMem()
			const dataDir = "/local"
			require.NoError(t, fsys.MkdirAll(dataDir, 0o700))
			st, err := pebblestore.Open(dataDir, nil, pebblestore.WithFS(fsys))
			require.NoError(t, err)
			local := map[string][]byte{
				catalog.MainSeqKey:          catalog.EncodeSeq(tt.localNext),
				ingest.SeqReservedKey:       catalog.EncodeSeq(tt.localNext),
				migrate.HandoffGuardKey:     []byte(migrate.GuardDone),
				migrate.CompactionPausedKey: catalog.EncodeSeq(1),
			}
			for key, value := range local {
				require.NoError(t, st.Set(ctx, []byte(key), value))
			}
			require.NoError(t, st.Close())

			_, err = fake.Backend.ReclaimLocal(ctx, time.Minute, dataDir, fsys, seqspace.CursorSeqMaxThreshold-150)
			require.ErrorContains(t, err, "use a smaller --margin")
			after, err := fake.DB.Snapshot()
			require.NoError(t, err)
			require.Equal(t, before.Meta, after.Meta, "a rejected reclaim must leave the handoff committed")
			st, err = pebblestore.Open(dataDir, nil, pebblestore.WithFS(fsys))
			require.NoError(t, err)
			for key, value := range local {
				got, err := st.Get(ctx, []byte(key))
				require.NoError(t, err)
				require.Equal(t, value, got, key)
			}
			require.NoError(t, st.Close())

			res, err := fake.Backend.ReclaimLocal(ctx, time.Minute, dataDir, fsys, 1)
			require.NoError(t, err)
			require.Equal(t, tt.localNext, res.LocalNext)
			require.Equal(t, tt.catalogNext, res.CatalogNext)
			require.Equal(t, uint64(201), res.ResumeSeq)
			again, err := fake.Backend.ReclaimLocal(ctx, time.Minute, dataDir, fsys, 1)
			require.NoError(t, err)
			require.Equal(t, res, again, "reclaim can finish again from reverted state")
			after, err = fake.DB.Snapshot()
			require.NoError(t, err)
			require.Equal(t, []byte(catalog.MigrationReverted), after.Meta[catalog.MigrationStateKey])
			st, err = pebblestore.Open(dataDir, nil, pebblestore.WithFS(fsys))
			require.NoError(t, err)
			defer func() { require.NoError(t, st.Close()) }()
			reserved, err := st.Get(ctx, []byte(ingest.SeqReservedKey))
			require.NoError(t, err)
			require.Equal(t, catalog.EncodeSeq(res.ResumeSeq), reserved)
			for _, key := range []string{migrate.HandoffGuardKey, migrate.CompactionPausedKey} {
				_, err := st.Get(ctx, []byte(key))
				require.ErrorIs(t, err, metastore.ErrNotFound)
			}
		})
	}
}
