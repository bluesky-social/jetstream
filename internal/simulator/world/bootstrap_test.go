package world

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBootstrap_FirstRunPopulates(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	cfg.Accounts = 50
	cfg.InitialRecords = 2
	cfg.Seed = 7

	w, err := New(context.Background(), cfg)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	wantBootstrap, err := w.EnsureSeed()
	require.NoError(t, err)
	require.True(t, wantBootstrap)

	require.NoError(t, w.Bootstrap(context.Background(), slog.Default()))

	// Every account has a state row with 2 records.
	for i := range cfg.Accounts {
		state, err := w.loadState(i)
		require.NoError(t, err)
		require.Equal(t, 2, state.RecordCount, "account %d", i)
	}
}

func TestBootstrap_DeterministicAcrossRuns(t *testing.T) {
	t.Parallel()
	cfg1 := DefaultConfig()
	cfg1.DataDir = filepath.Join(t.TempDir(), "simulator")
	cfg1.Accounts = 10
	cfg1.InitialRecords = 1

	w1, err := New(context.Background(), cfg1)
	require.NoError(t, err)
	_, err = w1.EnsureSeed()
	require.NoError(t, err)
	require.NoError(t, w1.Bootstrap(context.Background(), slog.Default()))
	a1, _ := w1.loadAccount(0)
	require.NoError(t, w1.Close())

	cfg2 := cfg1
	cfg2.DataDir = filepath.Join(t.TempDir(), "simulator")
	w2, err := New(context.Background(), cfg2)
	require.NoError(t, err)
	defer func() { _ = w2.Close() }()
	_, err = w2.EnsureSeed()
	require.NoError(t, err)
	require.NoError(t, w2.Bootstrap(context.Background(), slog.Default()))
	a2, _ := w2.loadAccount(0)

	require.Equal(t, a1.DID, a2.DID)
}
