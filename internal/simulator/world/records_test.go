package world

import (
	"testing"

	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/require"
)

func TestGenerateRecord_RoundTrips(t *testing.T) {
	t.Parallel()
	r := newTestRand()
	for _, coll := range []string{
		"app.bsky.feed.post",
		"app.bsky.feed.like",
		"app.bsky.graph.follow",
		"app.bsky.feed.repost",
		"app.bsky.actor.profile",
	} {
		rec := generateRecord(r, coll, "did:plc:targetabcdefghijklmnopqr")
		_, err := cbor.Marshal(rec)
		require.NoError(t, err, coll)
	}
}
