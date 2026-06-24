package client

import (
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// TestSelectorFiltersAndSuppressesDuringDownload wires a Matcher + Suppressor
// into the downloader and asserts that filtered and suppressed rows are never
// emitted, while survivors come through in order.
func TestSelectorFiltersAndSuppressesDuringDownload(t *testing.T) {
	t.Parallel()
	as := newArchiveServer(t)
	// Two DIDs, two collections, seqs 1..6.
	events := []segment.Event{
		makeCreate(t, 1, "did:plc:a", "app.bsky.feed.post", "r1"),
		makeCreate(t, 2, "did:plc:b", "app.bsky.feed.post", "r2"), // wrong DID
		makeCreate(t, 3, "did:plc:a", "app.bsky.feed.like", "r3"), // wrong collection
		makeCreate(t, 4, "did:plc:a", "app.bsky.feed.post", "r4"), // suppressed below
		makeCreate(t, 5, "did:plc:a", "app.bsky.feed.post", "r5"), // kept
		makeCreate(t, 6, "did:plc:a", "app.bsky.feed.post", "r6"), // kept
	}
	as.addSegment(t, "seg_0000000000.jss", events)

	// Filter: only did:plc:a + app.bsky.feed.post.
	matcher := NewMatcher(PlanRequest{
		DIDs:        []string{"did:plc:a"},
		Collections: []string{"app.bsky.feed.post"},
	})
	// Suppress r4 (deleted at seq 50, above its create seq 4).
	snap := recordTombstoneSnapshot("did:plc:a", "app.bsky.feed.post", "r4", 50)
	sup := NewSuppressor()
	sup.base.Store(&snap) // seed directly for the test (copy-on-write store)

	sel := newRowSelector(matcher, sup)
	d := as.downloaderWith(2, sel)

	entries := []PlanEntry{{SegmentName: "seg_0000000000.jss", Index: 0, Mode: ModeWholeSegment}}
	got := collectOrdered(t, d, entries)

	// Survivors: r1 (seq1), r5 (seq5), r6 (seq6). r2 wrong DID, r3 wrong
	// collection, r4 suppressed.
	require.Equal(t, []uint64{1, 5, 6}, seqs(got))
	for _, ev := range got {
		require.Equal(t, "did:plc:a", ev.DID)
		require.Equal(t, "app.bsky.feed.post", ev.Commit.Collection)
	}
}
