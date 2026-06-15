package oracle

import (
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/tombstone"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/stretchr/testify/require"
)

// overlayFor builds the snapshot the server would serve for (w, m].
func overlayFor(t *testing.T, events []ObservedEvent, w, m uint64) tombstone.Snapshot {
	t.Helper()
	segEvents := make([]segment.Event, 0, len(events))
	for _, ev := range events {
		segEvents = append(segEvents, toSegmentEvent(ev))
	}
	snap, err := tombstone.FoldRange(segEvents, w, m)
	require.NoError(t, err)
	return snap
}

func TestOverlayReconstruction_SuppressesDeletedRecord(t *testing.T) {
	t.Parallel()
	events := []ObservedEvent{
		{Seq: 50, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r"},
		{Seq: 120, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "r"},
	}
	require.NoError(t, CheckOverlayReconstruction(events, 100, 150, overlayFor(t, events, 100, 150)))
}

func TestOverlayReconstruction_NoPermanentTombstone(t *testing.T) {
	t.Parallel()
	events := []ObservedEvent{
		{Seq: 60, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "old"},
		{Seq: 100, Kind: segment.KindAccount, DID: "did:plc:a", Payload: oracleAccountPayload(t, false, "deleted")},
		{Seq: 200, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "new"},
	}
	require.NoError(t, CheckOverlayReconstruction(events, 150, 250, overlayFor(t, events, 150, 250)),
		"reactivated account's newer record must be emitted")
}

func TestOverlayReconstruction_SeamBoundaries(t *testing.T) {
	t.Parallel()
	events := []ObservedEvent{
		{Seq: 10, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r1"},
		{Seq: 150, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "r1"},
		{Seq: 80, Kind: segment.KindCreate, DID: "did:plc:b", Collection: "c", Rkey: "r2"},
		{Seq: 151, Kind: segment.KindDelete, DID: "did:plc:b", Collection: "c", Rkey: "r2"},
	}
	// W=100, M=150: delete@150 in overlay; delete@151 over live tail.
	require.NoError(t, CheckOverlayReconstruction(events, 100, 150, overlayFor(t, events, 100, 150)))
}
