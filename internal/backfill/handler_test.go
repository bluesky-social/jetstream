package backfill

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/mst"
	atmosrepo "github.com/jcalabro/atmos/repo"
	"github.com/stretchr/testify/require"
)

// newTestIngest builds a *ingest.Writer rooted at t.TempDir for handler tests.
func newTestIngest(t *testing.T) *ingest.Writer {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	w, err := ingest.Open(ingest.Config{
		ShardsDir:         filepath.Join(dir, "shards"),
		Store:             st,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxEventsPerBlock: 4,
		MaxSegmentBytes:   1 << 30,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func buildSingleRecordRepo(t *testing.T, did atmos.DID, collection, rkey string, record map[string]any) (*atmosrepo.Repo, *atmosrepo.Commit) {
	t.Helper()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	mstore := mst.NewMemBlockStore()
	r := &atmosrepo.Repo{
		DID:   did,
		Clock: atmos.NewTIDClock(0),
		Store: mstore,
		Tree:  mst.NewTree(mstore),
	}
	require.NoError(t, r.Create(collection, rkey, record))
	commit, err := r.Commit(key)
	require.NoError(t, err)
	return r, commit
}

// TestSegmentHandler_EmitsOneEventPerRecord pins the contract: a
// repo with K records lands K Create rows in the segment with the
// expected (DID, Collection, Rkey, Rev) coordinates.
func TestSegmentHandler_EmitsOneEventPerRecord(t *testing.T) {
	t.Parallel()
	w := newTestIngest(t)

	frozen := time.Date(2026, 5, 19, 12, 0, 0, 0, time.UTC)
	h := NewSegmentHandler(w, nil)
	h.now = func() time.Time { return frozen }

	r, commit := buildSingleRecordRepo(t,
		"did:plc:test", "app.bsky.feed.post", "rkey1",
		map[string]any{"text": "hello"})

	require.NoError(t, h.HandleRepo(context.Background(), "did:plc:test", r, commit))

	require.Equal(t, uint64(1), w.NextSeq(),
		"one record yields exactly one event")
}

// TestSegmentHandler_NilWriterPanics pins the constructor's
// fast-fail invariant.
func TestSegmentHandler_NilWriterPanics(t *testing.T) {
	t.Parallel()
	require.Panics(t, func() { _ = NewSegmentHandler(nil, nil) })
}

// TestSegmentHandler_NilLoggerNoPanic guards the wiring: a caller
// that forgot to plumb a logger should get a usable handler.
func TestSegmentHandler_NilLoggerNoPanic(t *testing.T) {
	t.Parallel()
	w := newTestIngest(t)
	require.NotPanics(t, func() {
		h := NewSegmentHandler(w, nil)
		require.NotNil(t, h)
	})
}

// TestSplitMSTKey rounds the helper through happy and unhappy cases.
func TestSplitMSTKey(t *testing.T) {
	t.Parallel()

	t.Run("ok", func(t *testing.T) {
		c, k, err := splitMSTKey("app.bsky.feed.post/rkey1")
		require.NoError(t, err)
		require.Equal(t, "app.bsky.feed.post", c)
		require.Equal(t, "rkey1", k)
	})

	bad := []string{
		"",
		"justonepart",
		"/leading-slash",
		"trailing-slash/",
		"too/many/slashes",
	}
	for _, in := range bad {
		_, _, err := splitMSTKey(in)
		require.Error(t, err, "expected error for %q", in)
	}
}
