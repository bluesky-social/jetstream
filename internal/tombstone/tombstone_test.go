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

	// Boundary: a materialization at EXACTLY the record-tombstone seq must be
	// retained. The record tombstone for an update is produced BY that same
	// update (observeLocked stores records[key]=ev.Seq for KindUpdate), so the
	// update sits at its own tombstone seq and is the current value — it must
	// survive. This is the strict-`>` boundary; the `seq > ev.Seq -> seq >= ev.Seq`
	// mutant drops this live update (data loss). Asserting it here kills that
	// mutant at the unit tier, mirroring the DID-overlay boundary assertion.
	drop, _ = snap.ShouldDrop(&segment.Event{Seq: 10, Kind: segment.KindUpdate, DID: "did:plc:a", Collection: "c", Rkey: "r"})
	require.False(t, drop, "the update materialization at its own record-tombstone seq must be kept")

	drop, _ = snap.ShouldDrop(&segment.Event{Seq: 9, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "r"})
	require.False(t, drop, "delete markers are retained forever")
}

func TestObserveAccountDeletedOnlyPurgesLiteralDeleted(t *testing.T) {
	t.Parallel()
	for _, status := range []string{"takendown", "suspended", "deactivated", "desynchronized", "throttled", "future"} {
		set := New()
		payload := accountPayload(t, false, status)
		require.NoError(t, set.Observe(&segment.Event{Seq: 5, Kind: segment.KindAccount, DID: "did:plc:a", Payload: payload}))
		require.Empty(t, set.Snapshot().DIDs)
	}

	set := New()
	require.NoError(t, set.Observe(&segment.Event{Seq: 5, Kind: segment.KindAccount, DID: "did:plc:a", Payload: accountPayload(t, false, "deleted")}))
	require.Equal(t, DIDTombstone{Seq: 5, Reason: "account"}, set.Snapshot().DIDs["did:plc:a"])
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
			// A materialization BELOW the tombstone seq is superseded → dropped.
			drop, reason := snap.ShouldDrop(&segment.Event{Seq: 9, Kind: segment.KindUpdate, DID: "did:plc:a"})
			require.True(t, drop, "a row below the DID tombstone seq must be dropped")
			require.Equal(t, tc.reason, reason)

			// The tombstone marker seq itself is retained (boundary: not strictly less).
			drop, _ = snap.ShouldDrop(&segment.Event{Seq: 10, Kind: segment.KindCreate, DID: "did:plc:a"})
			require.False(t, drop, "tombstone marker seq itself must be retained")

			// A materialization ABOVE the tombstone seq is a post-delete
			// reactivation and MUST survive. This is the data-loss direction the
			// m022 mutant inverts (ts.Seq > ev.Seq -> <): under the inversion this
			// live row would be dropped while the superseded seq-9 row above would
			// be kept. Asserting both directions is what makes the tombstone unit
			// suite kill m022 (see the mutation `tombstone` tier).
			drop, _ = snap.ShouldDrop(&segment.Event{Seq: 11, Kind: segment.KindCreate, DID: "did:plc:a"})
			require.False(t, drop, "a row above the DID tombstone seq (reactivation) must survive")
		})
	}
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
	require.Empty(t, set.Snapshot().DIDs, "Active==true must retain regardless of status")

	// Inactive with ABSENT status must retain (unknown reason).
	set = New()
	acc := &comatproto.SyncSubscribeRepos_Account{DID: "did:plc:a", Active: false}
	payload, err := acc.MarshalCBOR()
	require.NoError(t, err)
	require.NoError(t, set.Observe(&segment.Event{Seq: 5, Kind: segment.KindAccount, DID: "did:plc:a", Payload: payload}))
	require.Empty(t, set.Snapshot().DIDs, "Active==false with absent status must retain")
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
	snap := set.Snapshot()
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

		require.Equal(t, incremental.Snapshot().Records, rebuilt.Snapshot().Records, "seed %d", seed)
		require.Equal(t, incremental.Snapshot().DIDs, rebuilt.Snapshot().DIDs, "seed %d", seed)
		require.Equal(t, incremental.Len(), rebuilt.Len(), "seed %d", seed)
	}
}
