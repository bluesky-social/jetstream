package oracle

import (
	"testing"

	"github.com/bluesky-social/jetstream-v2/segment"
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

func TestCheckCompactedChunksRejectsBoundarySurvivorDroppedByChunkSnapshot(t *testing.T) {
	t.Parallel()

	events := []ObservedEvent{
		{Seq: 10, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Payload: []byte("old")},
	}
	chunks := []CompactionChunkObservation{{
		StartWatermark:  5,
		TargetWatermark: 12,
		ChunkEnd:        10,
		RecordTombstones: []CompactionRecordTombstone{{
			DID:        "did:plc:a",
			Collection: "c",
			Rkey:       "r",
			Seq:        12,
		}},
	}}

	require.NoError(t, CheckCompacted(events, 10), "final-watermark semantics alone cannot see chunk-local tombstones above the watermark")
	err := CheckCompactionChunks(events, chunks)
	require.ErrorContains(t, err, "compaction chunk")
	require.ErrorContains(t, err, "superseded record row survived")
	require.ErrorContains(t, err, "seq=10")
	require.ErrorContains(t, err, "tombstone_seq=12")
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
