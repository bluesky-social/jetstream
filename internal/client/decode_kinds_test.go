package client

import (
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

func mustRecord(t *testing.T, collection string) []byte {
	t.Helper()
	payload, err := cbor.Marshal(map[string]any{"$type": collection, "text": "hi"})
	require.NoError(t, err)
	return payload
}

// These exercise the backfill decode path for non-commit kinds, which the live
// path tests cover for JSON frames but not for segment rows.

func TestDecodeSegmentIdentity(t *testing.T) {
	t.Parallel()
	id := comatproto.SyncSubscribeRepos_Identity{
		DID: "did:plc:a", Handle: gt.Some("alice.test"), Seq: 7, Time: "t",
	}
	payload, err := id.MarshalCBOR()
	require.NoError(t, err)

	ev, err := decodeSegmentEvent(&segment.Event{Seq: 7, Kind: segment.KindIdentity, DID: "did:plc:a", Payload: payload})
	require.NoError(t, err)
	require.Equal(t, KindIdentity, ev.Kind)
	require.Equal(t, "alice.test", ev.Identity.Handle)
	require.EqualValues(t, 7, ev.Identity.Seq)
}

func TestDecodeSegmentAccount(t *testing.T) {
	t.Parallel()
	acct := comatproto.SyncSubscribeRepos_Account{
		DID: "did:plc:a", Active: false, Status: gt.Some("deleted"), Seq: 9, Time: "t",
	}
	payload, err := acct.MarshalCBOR()
	require.NoError(t, err)

	ev, err := decodeSegmentEvent(&segment.Event{Seq: 9, Kind: segment.KindAccount, DID: "did:plc:a", Payload: payload})
	require.NoError(t, err)
	require.Equal(t, KindAccount, ev.Kind)
	require.False(t, ev.Account.Active)
	require.Equal(t, "deleted", ev.Account.Status)
}

func TestDecodeSegmentSync(t *testing.T) {
	t.Parallel()
	sync := comatproto.SyncSubscribeRepos_Sync{
		DID: "did:plc:a", Rev: "rev1", Seq: 11, Time: "t",
	}
	payload, err := sync.MarshalCBOR()
	require.NoError(t, err)

	ev, err := decodeSegmentEvent(&segment.Event{Seq: 11, Kind: segment.KindSync, DID: "did:plc:a", Payload: payload})
	require.NoError(t, err)
	require.Equal(t, KindSync, ev.Kind)
	require.Equal(t, "rev1", ev.Sync.Rev)
}

func TestDecodeSegmentCreateResyncIsCommit(t *testing.T) {
	t.Parallel()
	payload := mustRecord(t, "app.bsky.feed.post")
	ev, err := decodeSegmentEvent(&segment.Event{
		Seq: 3, Kind: segment.KindCreateResync, DID: "did:plc:a",
		Collection: "app.bsky.feed.post", Rkey: "r1", Payload: payload,
	})
	require.NoError(t, err)
	require.Equal(t, KindCommit, ev.Kind)
	require.Equal(t, OpCreate, ev.Commit.Operation, "resync replacement presents as a create")
}

// TestDecodeSegmentTimeUSResolvesDisplayValue pins the backfill decode path to
// the same time_us contract as the live wire: an operator-imported indexed_at
// wins; otherwise witnessed_at. A backfill/live divergence here would show the
// same event with two different timestamps depending on how the client saw it.
func TestDecodeSegmentTimeUSResolvesDisplayValue(t *testing.T) {
	t.Parallel()
	payload := mustRecord(t, "app.bsky.feed.post")

	unimported, err := decodeSegmentEvent(&segment.Event{
		Seq: 1, Kind: segment.KindCreate, DID: "did:plc:a",
		Collection: "app.bsky.feed.post", Rkey: "r1", Payload: payload,
		WitnessedAt: 5_000,
	})
	require.NoError(t, err)
	require.EqualValues(t, 5_000, unimported.TimeUS, "unimported row falls back to witnessed_at")

	imported, err := decodeSegmentEvent(&segment.Event{
		Seq: 2, Kind: segment.KindCreate, DID: "did:plc:a",
		Collection: "app.bsky.feed.post", Rkey: "r2", Payload: payload,
		WitnessedAt: 5_000, IndexedAt: 1_600,
	})
	require.NoError(t, err)
	require.EqualValues(t, 1_600, imported.TimeUS, "imported indexed_at wins over witnessed_at")
}

func TestDecodeSegmentUnknownKindFails(t *testing.T) {
	t.Parallel()
	_, err := decodeSegmentEvent(&segment.Event{Seq: 1, Kind: segment.Kind(99), DID: "did:plc:a"})
	require.Error(t, err)
}
