package world

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSeedRoundTrip(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	w, err := New(context.Background(), cfg)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	// First-run: no seed persisted yet.
	_, ok, err := w.loadSeed()
	require.NoError(t, err)
	require.False(t, ok)

	require.NoError(t, w.saveSeed(123))

	seed, ok, err := w.loadSeed()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(123), seed)
}

func TestEnsureSeed_Mismatch(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	cfg.Seed = 1
	w, err := New(context.Background(), cfg)
	require.NoError(t, err)

	wantBootstrap, err := w.EnsureSeed()
	require.NoError(t, err)
	require.True(t, wantBootstrap)
	require.NoError(t, w.Close())

	cfg.Seed = 2
	w, err = New(context.Background(), cfg)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	_, err = w.EnsureSeed()
	require.True(t, errors.Is(err, ErrSeedMismatch))
}

func TestEnsureSeed_ResumeWithSameSeed(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	cfg.Seed = 17

	w, err := New(context.Background(), cfg)
	require.NoError(t, err)
	wantBootstrap, err := w.EnsureSeed()
	require.NoError(t, err)
	require.True(t, wantBootstrap, "first run should request bootstrap")
	require.NoError(t, w.Close())

	// Reopen with same seed: should resume without bootstrap.
	w, err = New(context.Background(), cfg)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()
	wantBootstrap, err = w.EnsureSeed()
	require.NoError(t, err)
	require.False(t, wantBootstrap, "second run with same seed should resume")
}

func TestSeqRoundTrip(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	w, err := New(context.Background(), cfg)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	seq, err := w.loadSeq()
	require.NoError(t, err)
	require.Equal(t, int64(0), seq)

	require.NoError(t, w.saveSeq(42))
	seq, err = w.loadSeq()
	require.NoError(t, err)
	require.Equal(t, int64(42), seq)
}
