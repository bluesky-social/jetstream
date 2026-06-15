package oracle

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/stretchr/testify/require"
)

func TestReconstructAppliesCreateUpdateDelete(t *testing.T) {
	t.Parallel()

	events := []ObservedEvent{
		{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r1", Rev: "r1", Payload: []byte("one")},
		{Seq: 2, Kind: segment.KindUpdate, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r1", Rev: "r2", Payload: []byte("two")},
		{Seq: 3, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r1", Rev: "r3"},
	}

	got, err := Reconstruct(events)
	require.NoError(t, err)
	require.Empty(t, got.Accounts["did:plc:a"].Records)
}

func TestReconstructPurgesDeletedAccount(t *testing.T) {
	t.Parallel()

	events := []ObservedEvent{
		{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Payload: []byte("old")},
		{Seq: 2, Kind: segment.KindAccount, DID: "did:plc:a", Payload: oracleAccountPayload(t, false, "deleted")},
	}

	got, err := Reconstruct(events)
	require.NoError(t, err)
	require.Empty(t, got.Accounts["did:plc:a"].Records)
}

func TestReconstructPurgesSyncedRepo(t *testing.T) {
	t.Parallel()

	events := []ObservedEvent{
		{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "old", Payload: []byte("old")},
		{Seq: 2, Kind: segment.KindSync, DID: "did:plc:a", Rev: "r2"},
		{Seq: 3, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "new", Rev: "r2", Payload: []byte("new")},
	}

	got, err := Reconstruct(events)
	require.NoError(t, err)
	require.NotContains(t, got.Accounts["did:plc:a"].Records, RecordKey{DID: "did:plc:a", Collection: "c", Rkey: "old"})
	require.Equal(t, RecordValue{Rev: "r2", Payload: []byte("new")}, got.Accounts["did:plc:a"].Records[RecordKey{DID: "did:plc:a", Collection: "c", Rkey: "new"}])
}

func TestReconstructRetainsNonDeletedAccountStatuses(t *testing.T) {
	t.Parallel()

	for _, status := range []string{"takendown", "suspended", "deactivated", "desynchronized", "throttled", "future"} {
		events := []ObservedEvent{
			{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Payload: []byte("old")},
			{Seq: 2, Kind: segment.KindAccount, DID: "did:plc:a", Payload: oracleAccountPayload(t, false, status)},
		}
		got, err := Reconstruct(events)
		require.NoError(t, err)
		require.NotEmpty(t, got.Accounts["did:plc:a"].Records, status)
	}
}

func TestCheckInvariantsRejectsSeqRegression(t *testing.T) {
	t.Parallel()

	err := CheckInvariants([]ObservedEvent{
		{Seq: 2, Kind: segment.KindCreate, DID: "did:plc:a", Rev: "r1"},
		{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:a", Rev: "r2"},
	})
	require.ErrorContains(t, err, "seq")
}

func TestCheckInvariantsRejectsEmptyRevOnCommitKind(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		kind segment.Kind
	}{
		{name: "create", kind: segment.KindCreate},
		{name: "update", kind: segment.KindUpdate},
		{name: "delete", kind: segment.KindDelete},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckInvariants([]ObservedEvent{{
				Seq:        1,
				Kind:       tt.kind,
				DID:        "did:plc:a",
				Collection: "app.bsky.feed.post",
				Rkey:       "r1",
			}})
			require.ErrorContains(t, err, "empty rev")
		})
	}
}

func TestObserveSegmentsPreservesPhysicalOrderForInvariants(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	segmentsDir := filepath.Join(dataDir, "segments")
	require.NoError(t, os.MkdirAll(segmentsDir, 0o755))
	w, err := segment.New(segment.Config{
		Path:              filepath.Join(segmentsDir, ingest.SegmentFilename(0)),
		MaxEventsPerBlock: 2,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	_, err = w.Append(segment.Event{Seq: 2, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r2", Rev: "r2"})
	require.NoError(t, err)
	_, err = w.Append(segment.Event{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r1", Rev: "r1"})
	require.NoError(t, err)
	require.NoError(t, w.Flush())

	events, err := ObserveSegments(dataDir)
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, uint64(2), events[0].Seq)
	require.Equal(t, uint64(1), events[1].Seq)
	require.ErrorContains(t, CheckInvariants(events), "seq")

	sorted := EventsSortedBySeq(events)
	require.Equal(t, uint64(1), sorted[0].Seq)
	require.Equal(t, uint64(2), sorted[1].Seq)
	require.Equal(t, uint64(2), events[0].Seq, "sorted helper must not mutate observed order")
}
