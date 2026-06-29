package client

import (
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func TestDIDTombstoneSnapshotSuppresses(t *testing.T) {
	t.Parallel()
	snap := newDIDTombstoneSnapshot([]DIDTombstone{{DID: "did:plc:dead", Seq: 100}})

	// A create for the dead DID below the tombstone seq is suppressed.
	below := segment.Event{Seq: 50, Kind: segment.KindCreate, DID: "did:plc:dead", Collection: "c", Rkey: "r"}
	require.True(t, snap.suppresses(&below), "create below the tombstone seq must be suppressed")

	// A re-create strictly above the tombstone seq survives (reactivation, §R4).
	above := segment.Event{Seq: 150, Kind: segment.KindCreate, DID: "did:plc:dead", Collection: "c", Rkey: "r2"}
	require.False(t, snap.suppresses(&above), "re-create above the tombstone seq must survive")

	// A create for a DID with no tombstone is never suppressed.
	other := segment.Event{Seq: 10, Kind: segment.KindCreate, DID: "did:plc:live", Collection: "c", Rkey: "r"}
	require.False(t, snap.suppresses(&other))

	// Non-materialization rows (delete) are never suppressed by the snapshot —
	// they ride inline and the consumer folds them.
	del := segment.Event{Seq: 40, Kind: segment.KindDelete, DID: "did:plc:dead", Collection: "c", Rkey: "r"}
	require.False(t, snap.suppresses(&del), "the snapshot suppresses only materialization rows")

	// An update below the tombstone is a materialization row and IS suppressed.
	upd := segment.Event{Seq: 60, Kind: segment.KindUpdate, DID: "did:plc:dead", Collection: "c", Rkey: "r"}
	require.True(t, snap.suppresses(&upd))
}

func TestDIDTombstoneSnapshotEmptyNeverSuppresses(t *testing.T) {
	t.Parallel()
	var snap didTombstoneSnapshot // nil
	ev := segment.Event{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r"}
	require.False(t, snap.suppresses(&ev))
	require.False(t, newDIDTombstoneSnapshot(nil).suppresses(&ev))
}

func TestDIDTombstoneSnapshotKeepsMaxPerDID(t *testing.T) {
	t.Parallel()
	// Duplicate DID entries: the higher seq must win so suppression is not
	// silently lowered.
	snap := newDIDTombstoneSnapshot([]DIDTombstone{
		{DID: "did:plc:a", Seq: 50},
		{DID: "did:plc:a", Seq: 200},
	})
	require.EqualValues(t, 200, snap["did:plc:a"])
	mid := segment.Event{Seq: 100, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r"}
	require.True(t, snap.suppresses(&mid), "the max tombstone seq must govern suppression")
}

// TestSnapshotSelectorComposesMatcherAndSuppression proves the backfill
// RowSelector applies BOTH the exact matcher filter AND the DID-only
// suppression: a filtered-out row is dropped regardless, an in-scope row from a
// dead DID below the tombstone is suppressed, and an in-scope live row passes.
func TestSnapshotSelectorComposesMatcherAndSuppression(t *testing.T) {
	t.Parallel()
	m := NewMatcher(PlanRequest{Collections: []string{"app.bsky.feed.post"}})
	snap := newDIDTombstoneSnapshot([]DIDTombstone{{DID: "did:plc:dead", Seq: 100}})
	sel := newSnapshotSelector(m, snap)

	// Out of collection scope: dropped by the matcher.
	off := segment.Event{Seq: 10, Kind: segment.KindCreate, DID: "did:plc:dead", Collection: "app.bsky.feed.like", Rkey: "r"}
	keep, reason := sel.Keep(&off)
	require.False(t, keep)
	require.Equal(t, "filtered", reason)

	// In scope but from a dead DID below the tombstone: suppressed.
	dead := segment.Event{Seq: 50, Kind: segment.KindCreate, DID: "did:plc:dead", Collection: "app.bsky.feed.post", Rkey: "r"}
	keep, reason = sel.Keep(&dead)
	require.False(t, keep)
	require.Equal(t, "did_tombstone", reason)

	// In scope, live DID: kept.
	live := segment.Event{Seq: 60, Kind: segment.KindCreate, DID: "did:plc:live", Collection: "app.bsky.feed.post", Rkey: "r"}
	keep, _ = sel.Keep(&live)
	require.True(t, keep)
}

// TestNewSnapshotSelectorEmptyReturnsMatcher guards the no-deletions fast path:
// an empty snapshot yields the bare matcher (no wrapping cost) so the common
// backfill is unchanged.
func TestNewSnapshotSelectorEmptyReturnsMatcher(t *testing.T) {
	t.Parallel()
	m := NewMatcher(PlanRequest{})
	sel := newSnapshotSelector(m, nil)
	got, ok := sel.(*Matcher)
	require.True(t, ok, "empty snapshot must return the bare matcher, not a wrapper")
	require.Same(t, m, got)
}
