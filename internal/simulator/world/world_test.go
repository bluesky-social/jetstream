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

// nolint:paralleltest // mutates process CWD via t.Chdir; t.Parallel would panic
func TestNew_RejectsAbsolutePathToData(t *testing.T) {
	// We construct an explicit symlink so the test exercises the
	// real validation invariant — that two different spellings of
	// the same physical directory both get rejected — on every OS,
	// instead of relying on macOS's incidental /var → /private/var
	// symlink (which would make this test pass on macOS and skip
	// the codepath entirely on Linux CI).
	//
	// Note: t.Chdir mutates process-global state, which is why
	// t.Parallel() is omitted here. Other tests in this file may
	// continue to run in parallel because Go's testing package
	// serializes non-parallel tests before parallel ones.
	root := t.TempDir()
	realDir := filepath.Join(root, "real")
	linkDir := filepath.Join(root, "link")
	require.NoError(t, os.MkdirAll(realDir, 0o755))
	require.NoError(t, os.Symlink(realDir, linkDir))

	// CWD = realDir (canonical). cfg.DataDir spells the same
	// directory through the symlink. Both should resolve to
	// realDir/data and trip the "./data" guard.
	t.Chdir(realDir)

	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(linkDir, "data")
	_, err := New(context.Background(), cfg)
	require.ErrorIs(t, err, ErrDataDirReserved)
}
