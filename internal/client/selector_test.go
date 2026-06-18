package client

import (
	"strconv"
	"testing"

	"github.com/bluesky-social/jetstream/internal/tombstone"
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
	sup.snap = snap // seed directly for the test

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

// TestReconstructionMatchesGroundTruth mirrors the oracle's
// CheckOverlayReconstruction contract at the client filter+suppress layer:
// given a set of materialization rows and the combined tombstone set, the rows
// the client emits equal the independently-derived live set.
func TestReconstructionMatchesGroundTruth(t *testing.T) {
	t.Parallel()

	// Build a row stream: creates and a few deletes/updates across keys.
	type row struct {
		seq       uint64
		kind      segment.Kind
		did, rkey string
	}
	rows := []row{
		{1, segment.KindCreate, "did:plc:a", "r1"},
		{2, segment.KindCreate, "did:plc:a", "r2"},
		{3, segment.KindCreate, "did:plc:b", "r1"},
		{4, segment.KindUpdate, "did:plc:a", "r1"}, // supersedes seq1
		{5, segment.KindDelete, "did:plc:a", "r2"}, // kills r2
		{6, segment.KindCreate, "did:plc:b", "r2"},
	}

	const coll = "app.bsky.feed.post"
	toEvent := func(r row) segment.Event {
		return segment.Event{Seq: r.seq, Kind: r.kind, DID: r.did, Collection: coll, Rkey: r.rkey}
	}

	// Combined tombstone set folded from the full stream (overlay + live
	// union, as the client would hold it).
	all := make([]segment.Event, 0, len(rows))
	for _, r := range rows {
		all = append(all, toEvent(r))
	}
	snap, err := tombstone.FoldRange(all, 0, ^uint64(0))
	require.NoError(t, err)

	sup := NewSuppressor()
	sup.snap = ensureSnapshotMaps(snap)

	// Emit: every materialization row not suppressed, keeping the highest
	// surviving seq per key.
	emitted := map[string]uint64{}
	for _, r := range rows {
		ev := toEvent(r)
		if !ev.Kind.IsMaterialization() {
			continue
		}
		if drop, _ := sup.ShouldDrop(&ev); drop {
			continue
		}
		key := r.did + "/" + r.rkey
		if r.seq > emitted[key] {
			emitted[key] = r.seq
		}
	}

	// Ground truth: highest create/update seq per key not killed by a later
	// delete.
	want := map[string]uint64{
		"did:plc:a/r1": 4, // create@1 then update@4
		"did:plc:b/r1": 3,
		"did:plc:b/r2": 6,
		// did:plc:a/r2 created@2 then deleted@5 -> absent
	}
	require.Equal(t, want, emitted)
}

// makeManyCreates is a helper for larger reconstruction sweeps.
func makeManyCreates(n int) []segment.Event {
	out := make([]segment.Event, 0, n)
	for i := 1; i <= n; i++ {
		out = append(out, segment.Event{
			Seq: uint64(i), Kind: segment.KindCreate,
			DID: "did:plc:" + strconv.Itoa(i%5), Collection: "app.bsky.feed.post", Rkey: "r" + strconv.Itoa(i),
		})
	}
	return out
}

func TestReconstructionEmptyOverlayKeepsAll(t *testing.T) {
	t.Parallel()
	rows := makeManyCreates(20)
	sup := NewSuppressor()
	sup.snap = emptySnapshot()
	kept := 0
	for i := range rows {
		if drop, _ := sup.ShouldDrop(&rows[i]); !drop {
			kept++
		}
	}
	require.Equal(t, 20, kept, "empty overlay suppresses nothing")
}
