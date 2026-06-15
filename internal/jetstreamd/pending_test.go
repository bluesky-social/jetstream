package jetstreamd

import (
	"io"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/stretchr/testify/require"
)

func TestPendingEventsForDID_NilWriterReturnsNil(t *testing.T) {
	t.Parallel()

	var ref atomic.Pointer[ingest.Writer]
	get := pendingEventsForDID(&ref)
	require.Nil(t, get("did:plc:anything"))
}

func TestPendingEventsForDID_ReturnsUnflushedEventsFilteredByDID(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       dataDir + "/segments",
		Store:             st,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxEventsPerBlock: 4096,
		MaxSegmentBytes:   1 << 30,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	const wantDID = "did:plc:wanted"
	events := []segment.Event{
		{Kind: segment.KindCreate, DID: wantDID, Collection: "app.bsky.feed.like", Rkey: "r1", Rev: "rev1"},
		{Kind: segment.KindCreate, DID: "did:plc:other", Collection: "app.bsky.feed.post", Rkey: "x1", Rev: "rev2"},
		{Kind: segment.KindCreate, DID: wantDID, Collection: "app.bsky.feed.post", Rkey: "r2", Rev: "rev3"},
	}
	for i := range events {
		ev := events[i]
		require.NoError(t, w.Append(t.Context(), &ev))
	}

	// Sanity: nothing has been flushed, so the events live only in the
	// in-memory pending block — exactly the visibility gap we are bridging.
	var ref atomic.Pointer[ingest.Writer]
	ref.Store(w)
	get := pendingEventsForDID(&ref)

	got := get(wantDID)
	require.Len(t, got, 2)
	for _, ev := range got {
		require.Equal(t, wantDID, ev.DID)
	}
}
