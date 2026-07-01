package segment

import (
	"path/filepath"
	"slices"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/stretchr/testify/require"
)

// TestSentinelCollectionsAreInvalidNSIDs locks the collision-proof
// guarantee the whole DID-marker-sentinel scheme rests on: a sentinel
// collection name must never be a parseable NSID. planBackfill validates
// every requested collection (exact) and every wildcard authority through
// atmos.ParseNSID, so if a sentinel cannot parse as an NSID, no client
// request can name it or prefix-match it, and the sentinel can never
// collide with real collection traffic. If a future NSID grammar ever
// accepted a '$'-leading label, this test fails loudly rather than
// silently opening a collision.
func TestSentinelCollectionsAreInvalidNSIDs(t *testing.T) {
	t.Parallel()
	for _, name := range []string{
		SentinelCollectionAccount,
		SentinelCollectionIdentity,
		SentinelCollectionSync,
	} {
		_, err := atmos.ParseNSID(name)
		require.Errorf(t, err, "sentinel %q must NOT be a valid NSID", name)
		require.True(t, IsDIDMarkerSentinelCollection(name))
	}
	// A real NSID and an arbitrary non-sentinel string are not sentinels.
	require.False(t, IsDIDMarkerSentinelCollection("app.bsky.feed.post"))
	require.False(t, IsDIDMarkerSentinelCollection(""))
}

// TestDIDMarkerSentinelMapping pins the kind→sentinel mapping: the three
// DID-level marker kinds map to their reserved names, and every other kind
// (including commit kinds that carry a real collection) maps to "".
func TestDIDMarkerSentinelMapping(t *testing.T) {
	t.Parallel()
	require.Equal(t, SentinelCollectionAccount, didMarkerSentinel(KindAccount))
	require.Equal(t, SentinelCollectionIdentity, didMarkerSentinel(KindIdentity))
	require.Equal(t, SentinelCollectionSync, didMarkerSentinel(KindSync))
	for _, k := range []Kind{KindCreate, KindUpdate, KindDelete, KindCreateResync} {
		require.Empty(t, didMarkerSentinel(k), "commit kind %d must not map to a sentinel", k)
	}
}

// collectionIDByNameFromReader inverts a reader's collection string table.
func collectionIDByNameFromReader(t *testing.T, r *Reader) map[string]uint32 {
	t.Helper()
	out := map[string]uint32{}
	for id, name := range r.Collections() {
		out[name] = uint32(id)
	}
	return out
}

// blockHasCollectionName reports whether the named collection's id is
// listed in the given block's collection set.
func blockHasCollectionName(t *testing.T, r *Reader, blockIdx int, name string) bool {
	t.Helper()
	byName := collectionIDByNameFromReader(t, r)
	id, ok := byName[name]
	if !ok {
		return false
	}
	ids, err := r.BlockCollections(blockIdx)
	require.NoError(t, err)
	return slices.Contains(ids, id)
}

// TestSealIndexesDIDMarkerSentinels is the core seal-path assertion: a
// block containing a DID-level marker (account/identity/sync) gets the
// corresponding sentinel collection indexed into that block, while the
// marker's own (empty) collection is NOT interned and the sentinels do not
// inflate per-collection event counts. This is what makes the markers
// selectable by a collection-filtered planner.
func TestSealIndexesDIDMarkerSentinels(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	// One marker per block (MaxEventsPerBlock=1) so each sentinel lands in a
	// known, isolated block alongside no real collection.
	w, err := New(Config{Path: path, MaxEventsPerBlock: 1})
	require.NoError(t, err)

	events := []Event{
		{Seq: 1, WitnessedAt: 100, Kind: KindCreate, DID: "did:plc:a",
			Collection: "app.bsky.feed.post", Rkey: "k1", Rev: "v1", Payload: []byte("p1")},
		{Seq: 2, WitnessedAt: 200, Kind: KindAccount, DID: "did:plc:a"},
		{Seq: 3, WitnessedAt: 300, Kind: KindIdentity, DID: "did:plc:a"},
		{Seq: 4, WitnessedAt: 400, Kind: KindSync, DID: "did:plc:a"},
	}
	for _, ev := range events {
		full, err := w.Append(ev)
		require.NoError(t, err)
		if full {
			require.NoError(t, w.Flush())
		}
	}
	res, err := w.Seal()
	require.NoError(t, err)
	require.EqualValues(t, 4, res.BlockCount)

	r, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	// Block 0: the real collection, no sentinel.
	require.True(t, blockHasCollectionName(t, r, 0, "app.bsky.feed.post"))
	require.False(t, blockHasCollectionName(t, r, 0, SentinelCollectionAccount))

	// Blocks 1-3: each marker block carries exactly its sentinel.
	require.True(t, blockHasCollectionName(t, r, 1, SentinelCollectionAccount))
	require.True(t, blockHasCollectionName(t, r, 2, SentinelCollectionIdentity))
	require.True(t, blockHasCollectionName(t, r, 3, SentinelCollectionSync))

	// The sentinels are present in the string table but carry a zero event
	// count: they are a selection hint, not real per-collection traffic.
	byName := collectionIDByNameFromReader(t, r)
	counts := r.CollectionEventCounts()
	for _, name := range []string{
		SentinelCollectionAccount, SentinelCollectionIdentity, SentinelCollectionSync,
	} {
		id, ok := byName[name]
		require.Truef(t, ok, "sentinel %q must be in the string table", name)
		require.Zerof(t, counts[id], "sentinel %q must not inflate event counts", name)
	}
	// The one real collection counts its single create.
	require.EqualValues(t, 1, counts[byName["app.bsky.feed.post"]])
}

// TestSealCoalescesMarkerWithRealCollectionInBlock proves a block holding
// both a real-collection commit and a DID-level marker carries both the
// real collection id and the sentinel, so neither selection path is lost
// when they share a block.
func TestSealCoalescesMarkerWithRealCollectionInBlock(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	// Large block so both events land together.
	w, err := New(Config{Path: path, MaxEventsPerBlock: 16})
	require.NoError(t, err)
	for _, ev := range []Event{
		{Seq: 1, WitnessedAt: 100, Kind: KindCreate, DID: "did:plc:a",
			Collection: "app.bsky.feed.post", Rkey: "k1", Rev: "v1", Payload: []byte("p1")},
		{Seq: 2, WitnessedAt: 200, Kind: KindAccount, DID: "did:plc:a"},
	} {
		_, err := w.Append(ev)
		require.NoError(t, err)
	}
	require.NoError(t, w.Flush())
	_, err = w.Seal()
	require.NoError(t, err)

	r, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	require.True(t, blockHasCollectionName(t, r, 0, "app.bsky.feed.post"))
	require.True(t, blockHasCollectionName(t, r, 0, SentinelCollectionAccount))
}

// TestRewriteReindexesDIDMarkerSentinels proves the compaction rewrite path
// re-derives the sentinel index from scratch: a DID-level marker that
// survives a rewrite keeps its sentinel collection indexed, so a
// collection-filtered backfill can still select it from a compacted
// segment. (Compaction only drops superseded materialization rows; the
// markers themselves are retained.)
func TestRewriteReindexesDIDMarkerSentinels(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	// One event per block so the create and the marker occupy distinct
	// blocks; the create's block goes empty after the drop while the
	// marker's block (block 1) must keep its sentinel.
	w, err := New(Config{Path: path, MaxEventsPerBlock: 1})
	require.NoError(t, err)
	for _, ev := range []Event{
		// A create that the rewrite will drop (its account was deleted).
		{Seq: 1, WitnessedAt: 10, Kind: KindCreate, DID: "did:plc:a",
			Collection: "app.bsky.feed.post", Rkey: "r1", Payload: []byte("p1")},
		// The account-delete marker (empty collection) — must be retained
		// and keep its sentinel after rewrite.
		{Seq: 2, WitnessedAt: 20, Kind: KindAccount, DID: "did:plc:a"},
	} {
		full, err := w.Append(ev)
		require.NoError(t, err)
		if full {
			require.NoError(t, w.Flush())
		}
	}
	_, err = w.Seal()
	require.NoError(t, err)

	// Drop the create, keep the marker — the shape compaction produces.
	res, err := Rewrite(path, func(ev *Event) RowDecision {
		if ev.Kind == KindCreate {
			return RowDrop
		}
		return RowKeep
	}, RewriteOptions{})
	require.NoError(t, err)
	require.True(t, res.Rewritten)

	r, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	// The account marker survived in block 1 and is still selectable by
	// the sentinel; its zero event count is preserved.
	require.True(t, blockHasCollectionName(t, r, 1, SentinelCollectionAccount))
	byName := collectionIDByNameFromReader(t, r)
	id, ok := byName[SentinelCollectionAccount]
	require.True(t, ok)
	require.Zero(t, r.CollectionEventCounts()[id])
}
