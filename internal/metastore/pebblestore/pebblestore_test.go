package pebblestore_test

import (
	"context"
	"testing"

	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/metastore/pebblestore"
	"github.com/bluesky-social/jetstream/internal/metastore/storetest"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func openMem(t testing.TB) *pebblestore.Store {
	t.Helper()
	s, err := pebblestore.Open("/data", nil, pebblestore.WithFS(vfs.NewMem()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

func TestContract(t *testing.T) {
	t.Parallel()
	storetest.Run(t, func(t *testing.T) metastore.Store { return openMem(t) })
}

// TestReopenDurable covers the on-disk path: committed batches and single
// writes survive a close and reopen.
func TestReopenDurable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	s, err := pebblestore.Open(dir, nil)
	require.NoError(t, err)
	require.NoError(t, s.Set(ctx, []byte("a"), []byte("1")))
	b := s.NewBatch()
	b.Set([]byte("b"), []byte("2"))
	b.DeleteRange([]byte("a"), []byte("b"))
	require.NoError(t, b.Commit(ctx))
	n, err := s.DiskBytes()
	require.NoError(t, err)
	require.Positive(t, n)
	require.NoError(t, s.Close())
	require.NoError(t, s.Close(), "Close is idempotent")

	s, err = pebblestore.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	require.Equal(t, []storetest.KV{{Key: []byte("b"), Value: []byte("2")}}, storetest.Scan(t, s, nil, nil))
}

func TestDiskStatsThroughFaultWrapper(t *testing.T) {
	t.Parallel()
	s := openMem(t)
	wrapped := metastore.WithFaults(s, &metastore.KeyPrefixFault{Prefix: []byte("x"), Ordinal: 1})
	d, ok := metastore.DiskStatsOf(wrapped)
	require.True(t, ok)
	n, err := d.DiskBytes()
	require.NoError(t, err)
	require.Zero(t, n, "alternate VFS reports no local disk")
}

// TestMetricsObserved pins that routing through metastore keeps the
// jetstream_store_op_duration_seconds series the dashboards read.
func TestMetricsObserved(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	s, err := pebblestore.Open("/data", pebblestore.NewMetrics(reg), pebblestore.WithFS(vfs.NewMem()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	require.NoError(t, s.Set(ctx, []byte("k"), []byte("v")))
	_, err = s.Get(ctx, []byte("k"))
	require.NoError(t, err)
	_, err = s.Get(ctx, []byte("missing"))
	require.ErrorIs(t, err, metastore.ErrNotFound)
	require.NoError(t, s.Delete(ctx, []byte("k")))
	b := s.NewBatch()
	b.Set([]byte("k"), []byte("v"))
	require.NoError(t, b.Commit(ctx))

	mfs, err := reg.Gather()
	require.NoError(t, err)
	got := map[string]uint64{}
	for _, mf := range mfs {
		if mf.GetName() != "jetstream_store_op_duration_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			var op, status string
			for _, l := range m.GetLabel() {
				switch l.GetName() {
				case "op":
					op = l.GetValue()
				case "status":
					status = l.GetValue()
				}
			}
			if n := m.GetHistogram().GetSampleCount(); n > 0 {
				got[op+"/"+status] = n
			}
		}
	}
	require.Equal(t, map[string]uint64{
		"get/ok": 1, "get/notfound": 1, "set/ok": 1, "delete/ok": 1, "batch_commit/ok": 1,
	}, got)
}
