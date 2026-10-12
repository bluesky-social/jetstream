package jetstreamd_test

import (
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/jetstreamd/jetstreamdtest"
	"github.com/bluesky-social/jetstream/internal/migrate"
	"github.com/bluesky-social/jetstream/internal/storagefake"
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
