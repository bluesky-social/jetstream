package tombstone

import (
	"testing"

	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

func TestSnapshotShouldDropRecordChains(t *testing.T) {
	t.Parallel()
	snap := Snapshot{
		Records: map[RecordKey]uint64{{DID: "did:plc:a", Collection: "c", Rkey: "r"}: 10},
		DIDs:    map[string]DIDTombstone{},
	}
	drop, reason := snap.ShouldDrop(&segment.Event{Seq: 9, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r"})
	require.True(t, drop)
	require.Equal(t, "record", reason)

	drop, _ = snap.ShouldDrop(&segment.Event{Seq: 11, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r"})
	require.False(t, drop)

	drop, _ = snap.ShouldDrop(&segment.Event{Seq: 9, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "r"})
	require.False(t, drop, "delete markers are retained forever")
}

func TestObserveAccountDeletedOnlyPurgesLiteralDeleted(t *testing.T) {
	t.Parallel()
	for _, status := range []string{"takendown", "suspended", "deactivated", "desynchronized", "throttled", "future"} {
		set := New()
		payload := accountPayload(t, false, status)
		require.NoError(t, set.Observe(&segment.Event{Seq: 5, Kind: segment.KindAccount, DID: "did:plc:a", Payload: payload}))
		require.Empty(t, set.Snapshot(10).DIDs)
	}

	set := New()
	require.NoError(t, set.Observe(&segment.Event{Seq: 5, Kind: segment.KindAccount, DID: "did:plc:a", Payload: accountPayload(t, false, "deleted")}))
	require.Equal(t, DIDTombstone{Seq: 5, Reason: "account"}, set.Snapshot(10).DIDs["did:plc:a"])
}

func TestSnapshotShouldDropDIDChainsWithSpecificReason(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		tombstone DIDTombstone
		reason    string
	}{
		{name: "account delete", tombstone: DIDTombstone{Seq: 10, Reason: "account"}, reason: "account"},
		{name: "sync replacement", tombstone: DIDTombstone{Seq: 10, Reason: "sync"}, reason: "sync"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			snap := Snapshot{
				Records: map[RecordKey]uint64{},
				DIDs:    map[string]DIDTombstone{"did:plc:a": tc.tombstone},
			}
			drop, reason := snap.ShouldDrop(&segment.Event{Seq: 9, Kind: segment.KindUpdate, DID: "did:plc:a"})
			require.True(t, drop)
			require.Equal(t, tc.reason, reason)

			drop, _ = snap.ShouldDrop(&segment.Event{Seq: 10, Kind: segment.KindCreate, DID: "did:plc:a"})
			require.False(t, drop, "tombstone marker seq itself must be retained")
		})
	}
}

func TestSnapshotRangeFiltersLowAndHighBounds(t *testing.T) {
	t.Parallel()

	set := New()
	require.NoError(t, set.Observe(&segment.Event{Seq: 3, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "old"}))
	require.NoError(t, set.Observe(&segment.Event{Seq: 5, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "in"}))
	require.NoError(t, set.Observe(&segment.Event{Seq: 7, Kind: segment.KindSync, DID: "did:plc:a"}))
	require.NoError(t, set.Observe(&segment.Event{Seq: 9, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "future"}))

	snap := set.SnapshotRange(3, 7)
	require.NotContains(t, snap.Records, RecordKey{DID: "did:plc:a", Collection: "c", Rkey: "old"})
	require.Contains(t, snap.Records, RecordKey{DID: "did:plc:a", Collection: "c", Rkey: "in"})
	require.NotContains(t, snap.Records, RecordKey{DID: "did:plc:a", Collection: "c", Rkey: "future"})
	require.Equal(t, DIDTombstone{Seq: 7, Reason: "sync"}, snap.DIDs["did:plc:a"])
}

func accountPayload(t *testing.T, active bool, status string) []byte {
	t.Helper()
	acc := &comatproto.SyncSubscribeRepos_Account{DID: "did:plc:a", Active: active, Status: gt.Some(status)}
	payload, err := acc.MarshalCBOR()
	require.NoError(t, err)
	return payload
}
