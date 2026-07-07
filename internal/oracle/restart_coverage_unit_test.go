package oracle

import (
	"testing"

	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/stretchr/testify/require"
)

func rowKey(kind, did, coll, rkey, rev string) EventLogRow {
	return EventLogRow{Kind: kind, DID: did, Collection: coll, Rkey: rkey, Rev: rev}
}

// TestCompareEventLogCoverage_PassesWhenAllExpectedPresent pins the happy
// path: every expected row present (in any order, with extra rows) passes.
func TestCompareEventLogCoverage_PassesWhenAllExpectedPresent(t *testing.T) {
	t.Parallel()
	want := []EventLogRow{
		rowKey("create", "did:a", "c", "k1", "r1"),
		rowKey("delete", "did:a", "c", "k1", "r2"),
	}
	got := []EventLogRow{
		rowKey("delete", "did:a", "c", "k1", "r2"),
		rowKey("create", "did:a", "c", "k9", "r9"), // extra, tolerated
		rowKey("create", "did:a", "c", "k1", "r1"),
	}
	require.NoError(t, CompareEventLogCoverage(want, got))
}

// TestCompareEventLogCoverage_ToleratesDuplicates pins the at-least-once
// contract: a duplicated durable row (e.g. a merge re-run across a crash)
// does not fail coverage.
func TestCompareEventLogCoverage_ToleratesDuplicates(t *testing.T) {
	t.Parallel()
	want := []EventLogRow{rowKey("create", "did:a", "c", "k1", "r1")}
	got := []EventLogRow{
		rowKey("create", "did:a", "c", "k1", "r1"),
		rowKey("create", "did:a", "c", "k1", "r1"),
	}
	require.NoError(t, CompareEventLogCoverage(want, got))
}

// TestCompareEventLogCoverage_FailsOnLoss is the red-first power check: a
// dropped durable row (the lost-intermediate failure mode) must fail
// coverage with a descriptive error.
func TestCompareEventLogCoverage_FailsOnLoss(t *testing.T) {
	t.Parallel()
	want := []EventLogRow{
		rowKey("create", "did:a", "c", "k1", "r1"),
		rowKey("delete", "did:a", "c", "k1", "r2"),
	}
	got := []EventLogRow{
		rowKey("delete", "did:a", "c", "k1", "r2"), // create row lost
	}
	err := CompareEventLogCoverage(want, got)
	require.Error(t, err)
	require.Contains(t, err.Error(), "coverage gap")
	require.Contains(t, err.Error(), "k1")
}

// TestExpectedChainRows_MatchesNormalizedObserved pins that model-derived
// chain rows are key-comparable to what NormalizeEventLog produces from
// the equivalent on-disk events: same kind/did/key/rev and, for
// create/update, the same payload hash (identical record bytes). This is
// what lets the coverage check join the two sides.
func TestExpectedChainRows_MatchesNormalizedObserved(t *testing.T) {
	t.Parallel()
	const did = "did:plc:host"
	payload := []byte("record-block-bytes")
	ops := []world.GeneratedChainOp{
		{Action: "create", Collection: "app.bsky.feed.post", Rkey: "k1", Rev: "rev1", Payload: payload},
		{Action: "delete", Collection: "app.bsky.feed.post", Rkey: "k1", Rev: "rev2"},
	}

	want := expectedChainRows(did, ops)
	require.Len(t, want, 2)

	// Build the analogous observed rows the way the disk path would.
	observed := []ObservedEvent{
		{Kind: chainActionKind("create"), DID: did, Collection: "app.bsky.feed.post", Rkey: "k1", Rev: "rev1", Payload: payload, Seq: 50},
		{Kind: chainActionKind("delete"), DID: did, Collection: "app.bsky.feed.post", Rkey: "k1", Rev: "rev2", Seq: 51},
	}
	got := zeroRowSeqs(NormalizeEventLog(observed))

	require.NoError(t, CompareEventLogCoverage(want, got))

	// Red-first: a differing payload (wrong record bytes) breaks coverage.
	tampered := []ObservedEvent{
		{Kind: chainActionKind("create"), DID: did, Collection: "app.bsky.feed.post", Rkey: "k1", Rev: "rev1", Payload: []byte("other"), Seq: 50},
		{Kind: chainActionKind("delete"), DID: did, Collection: "app.bsky.feed.post", Rkey: "k1", Rev: "rev2", Seq: 51},
	}
	require.Error(t, CompareEventLogCoverage(want, zeroRowSeqs(NormalizeEventLog(tampered))))
}

// TestExpectedChainRows_CompactionFilterDropsSupersededCreate pins the
// integration with filterCompactedExpectedRows: once seqs are assigned, a
// create superseded by a delete at/below the watermark is correctly
// dropped from the expected set (so coverage does not demand a row
// compaction legitimately removed), while the surviving tombstone stays.
func TestExpectedChainRows_CompactionFilterDropsSupersededCreate(t *testing.T) {
	t.Parallel()
	// create@10, delete@11, both <= W: the create is superseded and must
	// be dropped; the delete tombstone is retained.
	rows := []EventLogRow{
		{Seq: 10, Kind: "create", DID: "did:a", Collection: "c", Rkey: "k1", Rev: "r1"},
		{Seq: 11, Kind: "delete", DID: "did:a", Collection: "c", Rkey: "k1", Rev: "r2"},
	}
	filtered := filterCompactedExpectedRows(rows, 11)
	require.Len(t, filtered, 1)
	require.Equal(t, "delete", filtered[0].Kind)

	// With W below the delete, the create is NOT yet compactable: both
	// survive (straddles-watermark shape).
	straddle := filterCompactedExpectedRows(rows, 10)
	require.Len(t, straddle, 2)
}

func TestExpectedChainRows_ObservedSyncTombstoneDropsSupersededMaterialization(t *testing.T) {
	t.Parallel()

	want := []EventLogRow{
		{Seq: 0, Kind: "update", DID: "did:a", Collection: "c", Rkey: "k1", Rev: "r2"},
	}
	observed := []EventLogRow{
		{Seq: 20, Kind: "sync", DID: "did:a", Rev: "r3"},
		{Seq: 21, Kind: "create_resync", DID: "did:a", Collection: "c", Rkey: "k1", Rev: "r3"},
	}

	filtered := filterCompactedExpectedRowsWithObservedTombstones(want, observed, 20)
	require.Empty(t, filtered)
}
