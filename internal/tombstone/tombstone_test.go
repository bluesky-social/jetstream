package tombstone

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/bluesky-social/jetstream/segment"
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

func TestSetDirtyChangesOnMutation(t *testing.T) {
	t.Parallel()
	s := New()
	d0 := s.Dirty()
	ev := segment.Event{Seq: 1, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "r"}
	require.NoError(t, s.Observe(&ev))
	require.NotEqual(t, d0, s.Dirty())
	d1 := s.Dirty()
	s.Evict(1)
	require.NotEqual(t, d1, s.Dirty())
}

func accountPayload(t *testing.T, active bool, status string) []byte {
	t.Helper()
	acc := &comatproto.SyncSubscribeRepos_Account{DID: "did:plc:a", Active: active, Status: gt.Some(status)}
	payload, err := acc.MarshalCBOR()
	require.NoError(t, err)
	return payload
}

func TestObserveAccountStatusMatrixRetains(t *testing.T) {
	t.Parallel()

	// Active accounts must retain even when a (stale/buggy) status
	// string says "deleted" — only Active==false && status=="deleted"
	// purges.
	set := New()
	require.NoError(t, set.Observe(&segment.Event{Seq: 5, Kind: segment.KindAccount, DID: "did:plc:a", Payload: accountPayload(t, true, "deleted")}))
	require.Empty(t, set.Snapshot(10).DIDs, "Active==true must retain regardless of status")

	// Inactive with ABSENT status must retain (unknown reason).
	set = New()
	acc := &comatproto.SyncSubscribeRepos_Account{DID: "did:plc:a", Active: false}
	payload, err := acc.MarshalCBOR()
	require.NoError(t, err)
	require.NoError(t, set.Observe(&segment.Event{Seq: 5, Kind: segment.KindAccount, DID: "did:plc:a", Payload: payload}))
	require.Empty(t, set.Snapshot(10).DIDs, "Active==false with absent status must retain")
}

func TestObserveMalformedAccountPayloadErrorsWithRowIdentity(t *testing.T) {
	t.Parallel()
	set := New()
	err := set.Observe(&segment.Event{Seq: 77, Kind: segment.KindAccount, DID: "did:plc:poison", Payload: []byte{0xff, 0x00, 0x01}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "did:plc:poison")
	require.Contains(t, err.Error(), "77")
}

func TestEvictDropsOnlyAtOrBelowWatermark(t *testing.T) {
	t.Parallel()
	set := New()
	require.NoError(t, set.Observe(&segment.Event{Seq: 3, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "r3"}))
	require.NoError(t, set.Observe(&segment.Event{Seq: 5, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "r5"}))
	require.NoError(t, set.Observe(&segment.Event{Seq: 4, Kind: segment.KindSync, DID: "did:plc:b"}))
	require.NoError(t, set.Observe(&segment.Event{Seq: 6, Kind: segment.KindSync, DID: "did:plc:c"}))

	set.Evict(4)
	require.Equal(t, 2, set.Len())
	snap := set.Snapshot(^uint64(0))
	require.NotContains(t, snap.Records, RecordKey{DID: "did:plc:a", Collection: "c", Rkey: "r3"})
	require.Contains(t, snap.Records, RecordKey{DID: "did:plc:a", Collection: "c", Rkey: "r5"})
	require.NotContains(t, snap.DIDs, "did:plc:b")
	require.Contains(t, snap.DIDs, "did:plc:c")

	// Boundary: exactly-equal seq is evicted (<= watermark is applied).
	set.Evict(5)
	require.Equal(t, 1, set.Len(), "only the seq-6 sync tombstone survives Evict(5)")
	set.Evict(6)
	require.Zero(t, set.Len())
}

func TestApproxBytesTracksInsertEvictReplace(t *testing.T) {
	t.Parallel()
	set := New()
	require.Zero(t, set.ApproxBytes())

	require.NoError(t, set.Observe(&segment.Event{Seq: 1, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "coll", Rkey: "rkey"}))
	one := set.ApproxBytes()
	require.Positive(t, one)

	// Re-observing the same key (latest-wins) must not grow bytes.
	require.NoError(t, set.Observe(&segment.Event{Seq: 2, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "coll", Rkey: "rkey"}))
	require.Equal(t, one, set.ApproxBytes())

	require.NoError(t, set.Observe(&segment.Event{Seq: 3, Kind: segment.KindSync, DID: "did:plc:b"}))
	require.Greater(t, set.ApproxBytes(), one)

	set.Evict(3)
	require.Zero(t, set.ApproxBytes())

	set.Replace(Snapshot{
		Records: map[RecordKey]uint64{{DID: "did:plc:a", Collection: "c", Rkey: "r"}: 1},
		DIDs:    map[string]DIDTombstone{"did:plc:b": {Seq: 2, Reason: "sync"}},
	})
	require.Positive(t, set.ApproxBytes())
	set.Replace(Snapshot{})
	require.Zero(t, set.ApproxBytes())
}

// TestRebuildEqualsIncremental is the spec §3.4/§11 property test: a
// set rebuilt by scan-fold over the same events must equal one built
// incrementally by per-event Observe.
func TestRebuildEqualsIncremental(t *testing.T) {
	t.Parallel()
	for seed := range int64(25) {
		rng := rand.New(rand.NewSource(seed))
		n := 50 + rng.Intn(200)
		events := make([]segment.Event, 0, n)
		dids := []string{"did:plc:a", "did:plc:b", "did:plc:c"}
		for i := range n {
			seq := uint64(i + 1)
			did := dids[rng.Intn(len(dids))]
			switch rng.Intn(7) {
			case 0:
				events = append(events, segment.Event{Seq: seq, Kind: segment.KindCreate, DID: did, Collection: "c", Rkey: fmt.Sprintf("r%d", rng.Intn(10))})
			case 1:
				events = append(events, segment.Event{Seq: seq, Kind: segment.KindUpdate, DID: did, Collection: "c", Rkey: fmt.Sprintf("r%d", rng.Intn(10))})
			case 2:
				events = append(events, segment.Event{Seq: seq, Kind: segment.KindDelete, DID: did, Collection: "c", Rkey: fmt.Sprintf("r%d", rng.Intn(10))})
			case 3:
				events = append(events, segment.Event{Seq: seq, Kind: segment.KindCreateResync, DID: did, Collection: "c", Rkey: fmt.Sprintf("r%d", rng.Intn(10))})
			case 4:
				events = append(events, segment.Event{Seq: seq, Kind: segment.KindSync, DID: did})
			case 5:
				status := []string{"deleted", "takendown", "suspended"}[rng.Intn(3)]
				acc := &comatproto.SyncSubscribeRepos_Account{DID: did, Active: false, Status: gt.Some(status)}
				payload, err := acc.MarshalCBOR()
				require.NoError(t, err)
				events = append(events, segment.Event{Seq: seq, Kind: segment.KindAccount, DID: did, Payload: payload})
			case 6:
				events = append(events, segment.Event{Seq: seq, Kind: segment.KindIdentity, DID: did})
			}
		}
		watermark := uint64(rng.Intn(n / 2))

		incremental := New()
		for i := range events {
			if events[i].Seq <= watermark {
				continue
			}
			require.NoError(t, incremental.Observe(&events[i]))
		}

		folded, err := Fold(events, watermark)
		require.NoError(t, err)
		rebuilt := New()
		rebuilt.Replace(folded)

		maxSeq := ^uint64(0)
		require.Equal(t, incremental.SnapshotRange(0, maxSeq).Records, rebuilt.SnapshotRange(0, maxSeq).Records, "seed %d", seed)
		require.Equal(t, incremental.SnapshotRange(0, maxSeq).DIDs, rebuilt.SnapshotRange(0, maxSeq).DIDs, "seed %d", seed)
		require.Equal(t, incremental.Len(), rebuilt.Len(), "seed %d", seed)
	}
}
