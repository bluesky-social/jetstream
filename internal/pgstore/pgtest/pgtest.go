// Package pgtest is the PostgreSQL test harness for `just test-storage`
// (design §20 layer 4). Tests read JETSTREAM_TEST_PG_URL; without it they
// skip, unless JETSTREAM_TEST_STORAGE_REQUIRED=1 (which `just test-storage`
// sets) makes a missing URL a failure. Plain `just` never needs PostgreSQL.
//
// Each test gets its own scratch database, so worktrees sharing one
// `just up` never collide.
package pgtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/pgstore"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

const (
	// EnvURL names the maintenance connection URL. Its role needs CREATEDB.
	EnvURL = "JETSTREAM_TEST_PG_URL"
	// EnvRequired makes a missing storage backend fail instead of skip.
	EnvRequired = "JETSTREAM_TEST_STORAGE_REQUIRED"
	// ReaderRole is the dev environment's read-only role (testing/devenv).
	// Scratch databases grant it SELECT when it exists.
	ReaderRole = "jetstream_reader"
)

// URL returns JETSTREAM_TEST_PG_URL, or skips (or fails, when storage tests
// are required) if it is unset.
func URL(t testing.TB) string {
	t.Helper()
	u := os.Getenv(EnvURL)
	if u == "" {
		if os.Getenv(EnvRequired) == "1" {
			t.Fatalf("%s is unset and %s=1", EnvURL, EnvRequired)
		}
		t.Skipf("%s is unset; run `just up` and `just test-storage`", EnvURL)
	}
	return u
}

// NewDatabase creates an empty scratch database jst_<random>, dropped when
// the test ends, and returns its URL.
func NewDatabase(t testing.TB) string {
	t.Helper()
	base := URL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var b [8]byte
	_, _ = rand.Read(b[:])
	name := "jst_" + hex.EncodeToString(b[:])

	admin, err := pgx.Connect(ctx, base)
	require.NoError(t, err, "connect to %s", EnvURL)
	defer func() { _ = admin.Close(context.Background()) }()
	_, err = admin.Exec(ctx, "CREATE DATABASE "+name)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		c, err := pgx.Connect(ctx, base)
		if err != nil {
			t.Errorf("pgtest: drop %s: %v", name, err)
			return
		}
		defer func() { _ = c.Close(context.Background()) }()
		if _, err := c.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
			t.Errorf("pgtest: drop %s: %v", name, err)
		}
	})

	var reader bool
	require.NoError(t, admin.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)", ReaderRole).Scan(&reader))
	scratch := withDatabase(t, base, name)
	if reader {
		_, err = admin.Exec(ctx, "GRANT CONNECT ON DATABASE "+name+" TO "+ReaderRole)
		require.NoError(t, err)
		c, err := pgx.Connect(ctx, scratch)
		require.NoError(t, err)
		defer func() { _ = c.Close(context.Background()) }()
		_, err = c.Exec(ctx, `GRANT USAGE ON SCHEMA public TO `+ReaderRole+`;
			ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO `+ReaderRole)
		require.NoError(t, err)
	}
	return scratch
}

// withDatabase returns base with its database name replaced.
func withDatabase(t testing.TB, base, name string) string {
	t.Helper()
	u, err := url.Parse(base)
	require.NoError(t, err, "%s must be a postgres:// URL", EnvURL)
	u.Path = "/" + name
	return u.String()
}

// WithHost returns rawURL with its host:port replaced, to route it through
// a Proxy.
func WithHost(t testing.TB, rawURL, hostport string) string {
	t.Helper()
	u, err := url.Parse(rawURL)
	require.NoError(t, err)
	u.Host = hostport
	return u.String()
}

// Open returns an initialized, version-checked archive in a fresh scratch
// database, closed when the test ends.
func Open(t testing.TB, metrics *pgstore.Metrics) (*pgstore.Store, string) {
	t.Helper()
	u := NewDatabase(t)
	s := OpenURL(t, u, metrics)
	var id [16]byte
	_, _ = rand.Read(id[:])
	require.NoError(t, s.Initialize(t.Context(), id))
	_, err := s.CheckVersions(t.Context())
	require.NoError(t, err)
	return s, u
}

// OpenURL opens a store on u, closed when the test ends.
func OpenURL(t testing.TB, u string, metrics *pgstore.Metrics) *pgstore.Store {
	t.Helper()
	s, err := pgstore.Open(t.Context(), pgstore.Config{URL: u, MaxConns: 8, Metrics: metrics})
	require.NoError(t, err)
	t.Cleanup(s.Close)
	return s
}
