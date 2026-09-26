package main

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/objstore"
	"github.com/bluesky-social/jetstream/internal/objstore/s3/s3test"
	"github.com/bluesky-social/jetstream/internal/pgstore"
	"github.com/bluesky-social/jetstream/internal/pgstore/pgtest"
)

//nolint:paralleltest // withClearedEnv mutates the process environment
func TestStorageInit_RequiresConnections(t *testing.T) {
	withClearedEnv(t)
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"--s3-region=us-east-1", "--s3-bucket=b"}, "JETSTREAM_PG_URL is required"},
		{[]string{"--pg-url=" + secretPGURL, "--s3-bucket=b"}, "JETSTREAM_S3_REGION is required"},
		{[]string{"--pg-url=" + secretPGURL, "--s3-region=us-east-1"}, "JETSTREAM_S3_BUCKET is required"},
	} {
		app := newTestApp()
		var out, errOut lockedBuffer
		app.Writer, app.ErrWriter = &out, &errOut
		err := app.Run(t.Context(), append([]string{"jetstream", "storage", "init"}, tc.args...))
		require.ErrorContains(t, err, tc.want)
		require.NotContains(t, err.Error()+out.String()+errOut.String(), pgSecret)
	}
}

// Ground rule 7: a database that cannot be reached fails init without the
// password in the error.
//
//nolint:paralleltest // withClearedEnv mutates the process environment
func TestStorageInit_ConnectErrorOmitsPGPassword(t *testing.T) {
	withClearedEnv(t)
	app := newTestApp()
	var out, errOut lockedBuffer
	app.Writer, app.ErrWriter = &out, &errOut
	err := app.Run(t.Context(), []string{
		"jetstream", "storage", "init",
		"--pg-url=postgres://jetstream:" + pgSecret + "@127.0.0.1:1/jetstream?sslmode=disable&connect_timeout=1",
		"--s3-region=us-east-1", "--s3-bucket=b",
	})
	require.ErrorContains(t, err, "connect to PostgreSQL")
	require.NotContains(t, err.Error()+out.String()+errOut.String(), pgSecret)
}

// TestStorageInit_RealStorage runs `jetstream storage init` end to end
// against the `just test-storage` PostgreSQL and S3.
func TestStorageInit_RealStorage(t *testing.T) {
	pgURL := pgtest.NewDatabase(t)
	s3cfg := s3test.Env(t)
	withClearedEnv(t)
	// The binary reads S3 credentials from the AWS default chain.
	t.Setenv("AWS_ACCESS_KEY_ID", s3cfg.AccessKeyID)
	t.Setenv("AWS_SECRET_ACCESS_KEY", s3cfg.SecretAccessKey)
	t.Setenv("JETSTREAM_PG_URL", pgURL)
	args := []string{
		"jetstream", "storage", "init",
		"--s3-endpoint=" + s3cfg.Endpoint,
		"--s3-region=" + s3cfg.Region,
		"--s3-bucket=" + s3cfg.Bucket,
		"--s3-prefix=" + s3cfg.Prefix,
		"--s3-path-style",
	}

	// A bad credential fails the probe and leaves the database empty, so
	// init runs again once it is fixed. S3 retries a 403 until the retry
	// timeout, since a credential refresh can cure it; keep that short.
	t.Setenv("AWS_SECRET_ACCESS_KEY", "wrong-secret")
	app := newTestApp()
	require.ErrorContains(t, app.Run(t.Context(), append(args, "--s3-retry-timeout=200ms")), "object store probe")
	t.Setenv("AWS_SECRET_ACCESS_KEY", s3cfg.SecretAccessKey)

	app = newTestApp()
	var out lockedBuffer
	app.Writer = &out
	require.NoError(t, app.Run(t.Context(), args))
	m := regexp.MustCompile(`^initialized archive ([0-9a-f-]{36})\n$`).FindStringSubmatch(out.String())
	require.NotNil(t, m, "output %q", out.String())

	store := pgtest.OpenURL(t, pgURL, nil)
	archive, err := store.CheckVersions(t.Context())
	require.NoError(t, err)
	require.Equal(t, objstore.FormatUUID(archive.ArchiveID), m[1])
	require.Zero(t, archive.HolderID, "init released the writer lease")

	rtx, err := store.BeginRead(t.Context())
	require.NoError(t, err)
	snap, err := catalog.LoadSnapshot(t.Context(), rtx)
	require.NoError(t, rtx.Close(t.Context()))
	require.NoError(t, err)
	var segs []catalog.SegmentRow
	for _, s := range snap.Segments {
		s.Revision = 0
		segs = append(segs, s)
	}
	require.ElementsMatch(t, []catalog.SegmentRow{
		{Namespace: catalog.Main, Index: 0, State: catalog.Active},
		{Namespace: catalog.BootstrapLive, Index: 0, State: catalog.Active},
	}, segs)
	require.NoError(t, catalog.CheckInvariants(snap, catalog.InvariantOptions{}))

	app = newTestApp()
	app.Writer = &out
	require.ErrorIs(t, app.Run(t.Context(), args), pgstore.ErrInitialized)
}
