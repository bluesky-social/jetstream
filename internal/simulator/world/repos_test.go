package world

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/require"
)

func TestRepoRoundTrip(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	w, err := New(context.Background(), cfg)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	a, err := deriveAccount(42, 0)
	require.NoError(t, err)
	b := w.db.NewBatch()
	require.NoError(t, w.saveAccount(b, a))
	require.NoError(t, b.Commit(nil))

	// Build an empty repo and add one record.
	rp, err := newEmptyRepo(a)
	require.NoError(t, err)
	require.NoError(t, rp.Create("app.bsky.feed.post", "3kabc123de4fg", map[string]any{
		"$type":     "app.bsky.feed.post",
		"text":      "hello",
		"createdAt": "2024-01-01T00:00:00Z",
	}))

	state, err := w.commitAndPersist(a, rp)
	require.NoError(t, err)
	require.NotEqual(t, cbor.CID{}, state.DataCID)
	require.NotEmpty(t, state.Rev)
	require.Equal(t, 1, state.RecordCount)

	// Reload from disk.
	state2, err := w.loadState(a.Index)
	require.NoError(t, err)
	require.Equal(t, state, state2)

	rp2, err := w.loadRepo(a)
	require.NoError(t, err)
	cid, data, err := rp2.Get("app.bsky.feed.post", "3kabc123de4fg")
	require.NoError(t, err)
	require.NotEqual(t, cbor.CID{}, cid)
	require.NotEmpty(t, data)
}
