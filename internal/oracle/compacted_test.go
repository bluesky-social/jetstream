package oracle

import (
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

func TestCheckCompactedRejectsSurvivingSupersededRecord(t *testing.T) {
	t.Parallel()

	events := []ObservedEvent{
		{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Payload: []byte("old")},
		{Seq: 2, Kind: segment.KindUpdate, DID: "did:plc:a", Collection: "c", Rkey: "r", Payload: []byte("new")},
	}

	err := CheckCompacted(events, 2)
	require.ErrorContains(t, err, "superseded record row survived")
	require.ErrorContains(t, err, "seq=1")
	require.ErrorContains(t, err, "tombstone_seq=2")
}

func TestCheckCompactedAcceptsRowsAboveWatermark(t *testing.T) {
	t.Parallel()

	events := []ObservedEvent{
		{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Payload: []byte("old")},
		{Seq: 2, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "r"},
	}

	require.NoError(t, CheckCompacted(events, 1))
}

func TestCheckCompactedRejectsSurvivingAccountDeletedRow(t *testing.T) {
	t.Parallel()

	events := []ObservedEvent{
		{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Payload: []byte("old")},
		{Seq: 2, Kind: segment.KindAccount, DID: "did:plc:a", Payload: oracleAccountPayload(t, false, "deleted")},
	}

	err := CheckCompacted(events, 2)
	require.ErrorContains(t, err, "superseded account row survived")
	require.ErrorContains(t, err, "seq=1")
	require.ErrorContains(t, err, "tombstone_seq=2")
}

func TestCheckCompactedRejectsSurvivingSyncSupersededRow(t *testing.T) {
	t.Parallel()

	events := []ObservedEvent{
		{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Payload: []byte("old")},
		{Seq: 2, Kind: segment.KindSync, DID: "did:plc:a", Rev: "r2"},
	}

	err := CheckCompacted(events, 2)
	require.ErrorContains(t, err, "superseded sync row survived")
}

// TestCheckCompactedFailureIncludesOffendingDIDTimeline locks the
// observability contract (#186): a superseded-survivor failure must carry the
// offending DID's full on-disk row timeline — every row's seq/kind, the
// watermark, and the killer tombstone — so a rare nightly-sweep occurrence is
// diagnosable from the artifact alone instead of just "seq=2 tombstone_seq=13".
func TestCheckCompactedFailureIncludesOffendingDIDTimeline(t *testing.T) {
	t.Parallel()

	events := []ObservedEvent{
		{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:victim", Collection: "app.bsky.feed.post", Rkey: "rk1", Payload: []byte("v1")},
		{Seq: 2, Kind: segment.KindCreate, DID: "did:plc:other", Collection: "app.bsky.feed.like", Rkey: "lk1", Payload: []byte("keep")},
		{Seq: 5, Kind: segment.KindAccount, DID: "did:plc:victim", Payload: oracleAccountPayload(t, false, "deleted")},
	}

	err := CheckCompacted(events, 5)
	require.Error(t, err)
	msg := err.Error()

	// Core verdict (unchanged contract).
	require.Contains(t, msg, "superseded account row survived")
	require.Contains(t, msg, "did:plc:victim")
	require.Contains(t, msg, "watermark=5")

	// Enriched timeline: every on-disk row for the offending DID, with the
	// surviving row and the killer both identifiable.
	require.Contains(t, msg, "timeline for did:plc:victim")
	require.Contains(t, msg, "seq=1 kind=create")      // the survivor
	require.Contains(t, msg, "seq=5 kind=account")     // the killer
	require.Contains(t, msg, "app.bsky.feed.post/rk1") // survivor coordinates

	// The timeline is DID-scoped: an unrelated DID's row must not be dragged in.
	require.NotContains(t, msg, "did:plc:other")
}

func TestCheckCompactedIgnoresNonDeletedAccountStatuses(t *testing.T) {
	t.Parallel()

	for _, status := range []string{"takendown", "suspended", "deactivated", "desynchronized", "throttled", "future"} {
		events := []ObservedEvent{
			{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Payload: []byte("old")},
			{Seq: 2, Kind: segment.KindAccount, DID: "did:plc:a", Payload: oracleAccountPayload(t, false, status)},
		}
		require.NoError(t, CheckCompacted(events, 2), status)
	}
}

func oracleAccountPayload(t *testing.T, active bool, status string) []byte {
	t.Helper()
	acc := &comatproto.SyncSubscribeRepos_Account{
		DID:    "did:plc:a",
		Active: active,
		Status: gt.Some(status),
	}
	payload, err := acc.MarshalCBOR()
	require.NoError(t, err)
	return payload
}
