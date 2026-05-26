package world

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNew_RejectsExactDataDir(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.DataDir = "./data"
	_, err := New(context.Background(), cfg)
	require.ErrorIs(t, err, ErrDataDirReserved)
}

func TestNew_RejectsAbsolutePathToData(t *testing.T) {
	t.Parallel()
	// Stand up a fake CWD with a `data` subdir; then try to construct
	// a World whose DataDir is the absolute path of that data subdir.
	// Even though it's spelled differently from `./data`, it should
	// resolve to the same directory and be rejected.
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	require.NoError(t, os.MkdirAll(dataDir, 0o755))

	prevWD, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(root))
	t.Cleanup(func() { _ = os.Chdir(prevWD) })

	cfg := DefaultConfig()
	cfg.DataDir = dataDir // absolute path
	_, err = New(context.Background(), cfg)
	require.ErrorIs(t, err, ErrDataDirReserved)
}

func TestNew_OpensAndCloses(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	w, err := New(context.Background(), cfg)
	require.NoError(t, err)
	require.NoError(t, w.Close())
}

func TestNew_ResetWipesDir(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "simulator")
	cfg := DefaultConfig()
	cfg.DataDir = dir
	w, err := New(context.Background(), cfg)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	cfg.Reset = true
	w, err = New(context.Background(), cfg)
	require.NoError(t, err)
	require.NoError(t, w.Close())
}
