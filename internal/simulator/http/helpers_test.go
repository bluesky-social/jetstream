package http_test

import (
	"context"
	"io"
	"log/slog"
	"math/rand/v2"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/simulator/fanout"
	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/jcalabro/atmos/repo"
	"github.com/stretchr/testify/require"
)

// newTestWorld constructs a populated *world.World for HTTP-layer
// tests. Used by plc_test.go (Task 12), pds_test.go (Task 13),
// relay_listrepos_test.go (Task 14), and relay_subscribe_test.go
// (Task 15).
func newTestWorld(t *testing.T, accounts, initialRecords int) *world.World {
	t.Helper()
	cfg := world.DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	cfg.Accounts = accounts
	cfg.InitialRecords = initialRecords
	w, err := world.New(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	_, err = w.EnsureSeed()
	require.NoError(t, err)
	require.NoError(t, w.Bootstrap(context.Background(), slog.Default()))
	require.NoError(t, w.AttachRuntime(rand.New(rand.NewPCG(1, 2)), fanout.New(64)))
	return w
}

// loadFromCAR is a thin shim around repo.LoadFromCAR for tests in
// this package. Tasks 13/14 use this to verify CAR contents.
func loadFromCAR(r io.Reader) (*repo.Repo, *repo.Commit, error) {
	return repo.LoadFromCAR(r)
}

var _ = loadFromCAR // used by Tasks 13/14
