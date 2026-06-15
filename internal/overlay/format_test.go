package overlay

import (
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/tombstone"
	"github.com/stretchr/testify/require"
)

func sampleSnapshot() tombstone.Snapshot {
	return tombstone.Snapshot{
		Records: map[tombstone.RecordKey]uint64{
			{DID: "did:plc:aaa", Collection: "app.bsky.feed.post", Rkey: "r1"}: 110,
			{DID: "did:plc:aaa", Collection: "app.bsky.feed.like", Rkey: "r2"}: 130,
			{DID: "did:plc:bbb", Collection: "app.bsky.feed.post", Rkey: "r3"}: 150,
		},
		DIDs: map[string]tombstone.DIDTombstone{
			"did:plc:ccc": {Seq: 120, Reason: "account"},
			"did:plc:ddd": {Seq: 140, Reason: "sync"},
		},
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	t.Parallel()
	const W, M = uint64(100), uint64(150)
	blob := Encode(sampleSnapshot(), W, M)

	gotW, gotM, gotSnap, err := decodeForTest(blob)
	require.NoError(t, err)
	require.Equal(t, W, gotW)
	require.Equal(t, M, gotM)
	require.Equal(t, sampleSnapshot(), gotSnap)
}

func TestEncodeDeterministic(t *testing.T) {
	t.Parallel()
	a := Encode(sampleSnapshot(), 100, 150)
	b := Encode(sampleSnapshot(), 100, 150)
	require.Equal(t, a, b, "same snapshot must produce byte-identical blobs")
}

func TestEncodePanicsOnUnknownReason(t *testing.T) {
	t.Parallel()
	require.Panics(t, func() {
		Encode(tombstone.Snapshot{
			Records: map[tombstone.RecordKey]uint64{},
			DIDs:    map[string]tombstone.DIDTombstone{"did:plc:a": {Seq: 110, Reason: "bogus"}},
		}, 100, 110)
	})
}

func TestEncodePanicsOnSeqBelowWatermark(t *testing.T) {
	t.Parallel()
	require.Panics(t, func() {
		Encode(tombstone.Snapshot{
			Records: map[tombstone.RecordKey]uint64{
				{DID: "did:plc:a", Collection: "c", Rkey: "r"}: 50, // <= W
			},
			DIDs: map[string]tombstone.DIDTombstone{},
		}, 100, 100)
	})
}

func TestEncodeEmpty(t *testing.T) {
	t.Parallel()
	blob := Encode(tombstone.Snapshot{
		Records: map[tombstone.RecordKey]uint64{},
		DIDs:    map[string]tombstone.DIDTombstone{},
	}, 200, 200)
	w, m, snap, err := decodeForTest(blob)
	require.NoError(t, err)
	require.Equal(t, uint64(200), w)
	require.Equal(t, uint64(200), m)
	require.True(t, snap.Empty())
}
