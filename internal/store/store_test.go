package store_test

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestOpen_RequiresDataDir pins the documented input contract.
func TestOpen_RequiresDataDir(t *testing.T) {
	t.Parallel()
	_, err := store.Open("", nil)
	require.ErrorContains(t, err, "data dir is required")
}

// TestOpen_CreatesPebbleSubdir is the basic happy-path test: a
// fresh data directory should result in <data-dir>/meta.pebble/
// being populated by pebble. We don't rely on pebble's specific
// filenames beyond the LOCK file, which is the most stable
// observable signal that the db opened successfully.
func TestOpen_CreatesPebbleSubdir(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()

	s, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	_, err = filepath.EvalSymlinks(filepath.Join(dataDir, store.PebbleSubdir, "LOCK"))
	require.NoError(t, err, "pebble LOCK file should exist after Open")
}

// TestStore_RoundTripViaEmbeddedDB confirms that callers can use
// the embedded *pebble.DB directly. This is the contract that lets
// keyspace-owning packages (backfill, etc.) reuse Store without
// a wrapper.
func TestStore_RoundTripViaEmbeddedDB(t *testing.T) {
	t.Parallel()
	s, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	require.NoError(t, s.Set([]byte("hello"), []byte("world"), store.SyncWrites))

	val, closer, err := s.Get([]byte("hello"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = closer.Close() })
	require.Equal(t, "world", string(val))
}

func TestOpen_StrictMemDropsUnsyncedWrites(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "darwin" {
		t.Skip("store.SyncWrites intentionally maps to pebble.NoSync on darwin")
	}

	fs := vfs.NewStrictMem()
	dataDir := "/data"
	require.NoError(t, fs.MkdirAll(dataDir, 0o755))
	syncStrictTestDir(t, fs, "/")

	s, err := store.Open(dataDir, nil, store.WithFS(fs))
	require.NoError(t, err)
	require.NoError(t, s.Set([]byte("synced"), []byte("yes"), store.SyncWrites))
	require.NoError(t, s.Set([]byte("unsynced"), []byte("no"), pebble.NoSync))

	fs.SetIgnoreSyncs(true)
	require.NoError(t, s.Close())
	fs.ResetToSyncedState()
	fs.SetIgnoreSyncs(false)

	s, err = store.Open(dataDir, nil, store.WithFS(fs))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	val, closer, err := s.Get([]byte("synced"))
	require.NoError(t, err)
	require.Equal(t, "yes", string(val))
	require.NoError(t, closer.Close())

	_, _, err = s.Get([]byte("unsynced"))
	require.ErrorIs(t, err, store.ErrNotFound)
}

func syncStrictTestDir(t *testing.T, fs *vfs.MemFS, dir string) {
	t.Helper()
	f, err := fs.OpenDir(dir)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())
}

// TestStore_CloseIsIdempotent guards the documented contract; the
// serve command's deferred close needs to be safe to call after a
// startup failure has already closed the store.
func TestStore_CloseIsIdempotent(t *testing.T) {
	t.Parallel()
	s, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	require.NoError(t, s.Close())
	require.NoError(t, s.Close())
}

// TestOpen_RecordsMetricsThroughInstrumentedMethods exercises the
// happy path of every instrumented method against a real pebble db
// and confirms each lands a sample on the matching {op,status}
// histogram series.
func TestOpen_RecordsMetricsThroughInstrumentedMethods(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := store.NewMetrics(reg)

	s, err := store.Open(t.TempDir(), m)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	require.NoError(t, s.Set([]byte("k"), []byte("v"), store.SyncWrites))

	val, closer, err := s.Get([]byte("k"))
	require.NoError(t, err)
	require.Equal(t, "v", string(val))
	require.NoError(t, closer.Close())

	_, _, err = s.Get([]byte("missing"))
	require.ErrorIs(t, err, pebble.ErrNotFound)

	require.NoError(t, s.Delete([]byte("k"), store.SyncWrites))

	mfs, err := reg.Gather()
	require.NoError(t, err)
	getCounts := map[string]uint64{}
	setCounts := map[string]uint64{}
	deleteCounts := map[string]uint64{}
	for _, mf := range mfs {
		if mf.GetName() != "jetstream_store_op_duration_seconds" {
			continue
		}
		for _, mm := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range mm.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			c := mm.GetHistogram().GetSampleCount()
			switch labels["op"] {
			case "get":
				getCounts[labels["status"]] = c
			case "set":
				setCounts[labels["status"]] = c
			case "delete":
				deleteCounts[labels["status"]] = c
			}
		}
	}
	require.Equal(t, uint64(1), getCounts["ok"])
	require.Equal(t, uint64(1), getCounts["notfound"])
	require.Equal(t, uint64(1), setCounts["ok"])
	require.Equal(t, uint64(1), deleteCounts["ok"])
}
