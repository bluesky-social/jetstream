package oracle

import (
	"testing"

	"github.com/bluesky-social/jetstream/internal/tombstone"
	"github.com/bluesky-social/jetstream/segment"
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
	// Account deleted at seq 100, then the DID posts a new record at 200.
	// W=50 keeps every row inside the overlay window (W, M], so nothing
	// here is a <=W row superseded by a <=W tombstone (which compaction
	// would have physically removed). The DID tombstone is exercised
	// directly: the pre-deletion "old" record is suppressed, but the
	// post-deletion "new" record (seq 200 > the account-delete's 100)
	// survives — the tombstone is a half-open seq window, not a permanent
	// mask on the DID.
	events := []ObservedEvent{
		{Seq: 60, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "old"},
		{Seq: 100, Kind: segment.KindAccount, DID: "did:plc:a", Payload: oracleAccountPayload(t, false, "deleted")},
		{Seq: 200, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "new"},
	}
	require.NoError(t, CheckOverlayReconstruction(events, 50, 250, overlayFor(t, events, 50, 250)),
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
