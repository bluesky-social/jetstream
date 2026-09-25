package catalog_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// FuzzDecodeHotBatch feeds DecodeHotBatch a descriptor and frame as they
// come from storage (design §20): a hot_batches row and its inline frame or
// pointer object. It must never panic, every failure must be a
// CorruptionError, and a success must hold exactly the row's seqs.
func FuzzDecodeHotBatch(f *testing.F) {
	good := hotFrame(f, 5, 7)
	f.Add(uint64(5), uint64(7), uint32(3), good)
	f.Add(uint64(4), uint64(6), uint32(3), good) // seqs shifted by one
	f.Add(uint64(5), uint64(8), uint32(4), good) // one event short
	f.Add(uint64(7), uint64(5), uint32(3), good) // inverted range
	f.Add(uint64(0), uint64(2), uint32(3), good) // seq 0
	f.Add(uint64(1), uint64(math.MaxUint64), uint32(0), good)
	f.Add(uint64(0), uint64(math.MaxUint64), uint32(0), good) // count wraps to 0
	f.Add(uint64(5), uint64(7), uint32(3), good[:len(good)/2])
	f.Add(uint64(5), uint64(7), uint32(3), []byte{})
	f.Add(uint64(5), uint64(7), uint32(3), []byte{0x28, 0xB5, 0x2F, 0xFD})

	f.Fuzz(func(t *testing.T, first, last uint64, count uint32, frame []byte) {
		row := catalog.HotBatchRow{FirstSeq: first, LastSeq: last, EventCount: count, Frame: frame, Inline: true}
		evs, err := catalog.DecodeHotBatch(row, frame)
		if err != nil {
			_, ok := catalog.IsCorruption(err)
			require.True(t, ok, "not a corruption error: %v", err)
			return
		}
		require.Len(t, evs, int(count))
		for i := range evs {
			require.Equal(t, first+uint64(i), evs[i].Seq)
		}
	})
}

func hotFrame(tb testing.TB, lo, hi uint64) []byte {
	tb.Helper()
	b, err := segment.NewBlockBuilder(int(hi - lo + 1))
	require.NoError(tb, err)
	for seq := lo; seq <= hi; seq++ {
		_, err := b.Append(segment.Event{
			Seq: seq, WitnessedAt: int64(seq), Kind: segment.KindCreate,
			DID: "did:plc:test", Collection: "app.bsky.feed.post", Rkey: fmt.Sprint(seq), Rev: "r",
			Payload: []byte{0xa0},
		})
		require.NoError(tb, err)
	}
	frame, _ := b.Encode()
	return frame
}
