package backfill

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/mst"
	atmosrepo "github.com/jcalabro/atmos/repo"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// newTestIngest builds a *ingest.Writer rooted at t.TempDir for handler tests.
func newTestIngest(t *testing.T) *ingest.Writer {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       filepath.Join(dir, "segments"),
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

func collectActiveEvents(t *testing.T, path string) []segment.Event {
	t.Helper()
	var events []segment.Event
	require.NoError(t, segment.WalkActive(path, func(block []segment.Event) error {
		events = append(events, block...)
		return nil
	}))
	return events
}

// TestSegmentHandler_EmitsOneEventPerRecord pins the contract: a
// repo with K records lands K Create rows in the segment with the
// expected (DID, Collection, Rkey, Rev) coordinates.
func TestSegmentHandler_EmitsOneEventPerRecord(t *testing.T) {
	t.Parallel()
	w := newTestIngest(t)

	frozen := time.Date(2026, 5, 19, 12, 0, 0, 0, time.UTC)
	h := NewSegmentHandler(w, nil, nil)
	h.now = func() time.Time { return frozen }

	r, commit := buildSingleRecordRepo(t,
		"did:plc:test", "app.bsky.feed.post", "rkey1",
		map[string]any{"text": "hello"})

	require.NoError(t, h.HandleRepo(context.Background(), "did:plc:test", r, commit))

	require.Equal(t, uint64(1), w.NextSeq(),
		"one record yields exactly one event")
}

func TestSegmentHandler_HandleRepoFlushesBeforeReturning(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	segmentsDir := filepath.Join(dir, "segments")
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       segmentsDir,
		Store:             st,
		MaxEventsPerBlock: 4096,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	did := atmos.DID("did:plc:flush-before-complete")
	r, commit := buildSingleRecordRepo(t,
		did, "app.bsky.feed.post", "rkey1",
		map[string]any{"text": "flush before complete"})
	h := NewSegmentHandler(w, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)

	require.NoError(t, h.HandleRepo(t.Context(), did, r, commit))

	events := collectActiveEvents(t, filepath.Join(segmentsDir, ingest.SegmentFilename(0)))
	require.Len(t, events, 1, "HandleRepo must flush appended rows before returning to the engine")
}

func TestSegmentHandler_DropsRecordThatExceedsSegmentColumnWidth(t *testing.T) {
	t.Parallel()

	w := newTestIngest(t)
	metrics := NewMetrics(prometheus.NewRegistry())
	h := NewSegmentHandler(w, slog.New(slog.NewTextHandler(io.Discard, nil)), metrics)

	var writerErr error
	h.onWriterError = func(err error) { writerErr = err }

	longRkey := strings.Repeat("x", 256)
	r, commit := buildSingleRecordRepo(t,
		"did:plc:widefield", "app.bsky.feed.post", longRkey,
		map[string]any{"text": "this rkey cannot fit in the segment column"})

	require.NoError(t, h.HandleRepo(t.Context(), "did:plc:widefield", r, commit))
	require.NoError(t, writerErr, "invalid upstream record data must not abort the local writer")
	require.Equal(t, uint64(0), w.NextSeq(), "skipped records must not allocate seqs")
	require.InDelta(t, 1.0, testutil.ToFloat64(metrics.DroppedRecords), 0,
		"the skipped record must be visible in dropped_records_total")
}

// TestSegmentHandler_NilWriterPanics pins the constructor's
// fast-fail invariant.
func TestSegmentHandler_NilWriterPanics(t *testing.T) {
	t.Parallel()
	require.Panics(t, func() { _ = NewSegmentHandler(nil, nil, nil) })
}

// TestSegmentHandler_NilLoggerNoPanic guards the wiring: a caller
// that forgot to plumb a logger should get a usable handler.
func TestSegmentHandler_NilLoggerNoPanic(t *testing.T) {
	t.Parallel()
	w := newTestIngest(t)
	require.NotPanics(t, func() {
		h := NewSegmentHandler(w, nil, nil)
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
