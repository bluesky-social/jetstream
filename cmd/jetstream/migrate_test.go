package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/bluesky-social/jetstream/internal/objstore/s3/s3test"
	"github.com/bluesky-social/jetstream/internal/pgstore/pgtest"
)

// TestMigrate_RealStorage drives `jetstream migrate` against the `just
// test-storage` PostgreSQL and S3 with no migrator running: status reports
// the seeding catalog, a request nobody claims is withdrawn, and reclaim
// refuses a migration that never handed off.
//
//nolint:paralleltest // withClearedEnv mutates the process environment
func TestMigrate_RealStorage(t *testing.T) {
	pgURL := pgtest.NewDatabase(t)
	s3cfg := s3test.Env(t)
	withClearedEnv(t)
	t.Setenv("AWS_ACCESS_KEY_ID", s3cfg.AccessKeyID)
	t.Setenv("AWS_SECRET_ACCESS_KEY", s3cfg.SecretAccessKey)
	t.Setenv("JETSTREAM_PG_URL", pgURL)
	storage := []string{
		"--s3-endpoint=" + s3cfg.Endpoint,
		"--s3-region=" + s3cfg.Region,
		"--s3-bucket=" + s3cfg.Bucket,
		"--s3-prefix=" + s3cfg.Prefix,
		"--s3-path-style",
	}
	run := func(args ...string) (string, error) {
		app := newTestApp()
		var out lockedBuffer
		app.Writer = &out
		err := app.Run(t.Context(), append(append([]string{"jetstream"}, args...), storage...))
		return out.String(), err
	}

	_, err := run("migrate", "status")
	require.Error(t, err, "no archive yet")
	_, err = run("storage", "init", "--migrate-from-local")
	require.NoError(t, err)

	out, err := run("migrate", "status")
	require.NoError(t, err)
	var rep jetstreamd.MigrationReport
	require.NoError(t, json.Unmarshal([]byte(out), &rep), "status prints JSON: %s", out)
	require.Equal(t, catalog.MigrationSeeding, rep.State)
	require.Nil(t, rep.Migrator)

	_, err = run("migrate", "abort", "--wait=50ms")
	require.ErrorContains(t, err, "withdrawn")
	_, err = run("migrate", "handoff", "--wait=50ms")
	require.ErrorContains(t, err, "withdrawn")

	_, err = run("migrate", "reclaim", "--data-dir="+t.TempDir())
	require.ErrorContains(t, err, `migration/state is "seeding"`)
}
