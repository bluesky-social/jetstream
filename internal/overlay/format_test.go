package overlay

import (
	"testing"
	"testing/quick"

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

	gotW, gotM, gotSnap, err := Decode(blob)
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
	w, m, snap, err := Decode(blob)
	require.NoError(t, err)
	require.Equal(t, uint64(200), w)
	require.Equal(t, uint64(200), m)
	require.True(t, snap.Empty())
}

func TestEncodeRoundTripProperty(t *testing.T) {
	t.Parallel()
	f := func(seed uint32) bool {
		snap, w, m := randomSnapshot(seed)
		blob := Encode(snap, w, m)
		gw, gm, gs, err := Decode(blob)
		if err != nil || gw != w || gm != m {
			return false
		}
		return snapshotsEqual(snap, gs)
	}
	require.NoError(t, quick.Check(f, &quick.Config{MaxCount: 500}))
}

// randomSnapshot builds a deterministic-from-seed snapshot covering
// adversarial shapes: empty/long/non-UTF8 rkeys, a DID present in both
// Records and DIDs, and seqs at the W+1 and M edges. Every seq is > w so
// it never trips Encode's seq>watermark guard.
func randomSnapshot(seed uint32) (tombstone.Snapshot, uint64, uint64) {
	r := uint64(seed)*2862933555777941757 + 3037000493
	next := func() uint64 { r ^= r >> 12; r ^= r << 25; r ^= r >> 27; return r * 2685821657736338717 }

	w := next() % 1000
	snap := tombstone.Snapshot{
		Records: map[tombstone.RecordKey]uint64{},
		DIDs:    map[string]tombstone.DIDTombstone{},
	}
	maxSeq := w
	nDID := int(next()%5) + 1
	rkeys := []string{"", "rk", string([]byte{0x00, 0xff, 0x80}), "aaaaaaaaaaaaaaaaaaaa"}
	colls := []string{"app.bsky.feed.post", "app.bsky.feed.like", "x"}
	for i := 0; i < nDID; i++ {
		did := "did:plc:" + string(rune('a'+i))
		nRec := int(next() % 4)
		for j := 0; j < nRec; j++ {
			seq := w + 1 + next()%500
			if seq > maxSeq {
				maxSeq = seq
			}
			snap.Records[tombstone.RecordKey{
				DID: did, Collection: colls[next()%uint64(len(colls))], Rkey: rkeys[next()%uint64(len(rkeys))],
			}] = seq
		}
		if next()%2 == 0 {
			seq := w + 1 + next()%500
			if seq > maxSeq {
				maxSeq = seq
			}
			reason := "account"
			if next()%2 == 0 {
				reason = "sync"
			}
			snap.DIDs[did] = tombstone.DIDTombstone{Seq: seq, Reason: reason}
		}
	}
	return snap, w, maxSeq
}

func snapshotsEqual(a, b tombstone.Snapshot) bool {
	if len(a.Records) != len(b.Records) || len(a.DIDs) != len(b.DIDs) {
		return false
	}
	for k, v := range a.Records {
		if b.Records[k] != v {
			return false
		}
	}
	for k, v := range a.DIDs {
		if b.DIDs[k] != v {
			return false
		}
	}
	return true
}

func FuzzDecodeForTest(f *testing.F) {
	f.Add(Encode(sampleSnapshot(), 100, 150))
	f.Add([]byte("jsto"))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, blob []byte) {
		// Must never panic; any structurally invalid blob returns an error.
		_, _, _, _ = Decode(blob)
	})
}
