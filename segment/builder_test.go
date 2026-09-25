package segment

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBlockBuilder(t *testing.T) {
	t.Parallel()

	for _, n := range []int{-1, maxBlockEventsLimit + 1} {
		_, err := NewBlockBuilder(n)
		require.ErrorIsf(t, err, ErrInvalidConfig, "max %d", n)
	}
	def, err := NewBlockBuilder(0)
	require.NoError(t, err)
	require.Equal(t, DefaultMaxEventsPerBlock, def.Cap())

	b, err := NewBlockBuilder(3)
	require.NoError(t, err)
	_, ok := b.PendingBounds()
	require.False(t, ok)
	frame, info := b.Encode()
	require.Nil(t, frame)
	require.Zero(t, info)

	_, err = b.Append(Event{Seq: 1, Kind: KindCreate, DID: strings.Repeat("x", 1<<17)})
	require.ErrorIs(t, err, ErrFieldTooLong)
	_, err = b.Append(Event{Seq: 1, Kind: 0})
	require.ErrorIs(t, err, ErrInvalidKind)
	require.Equal(t, 0, b.Len(), "a rejected event leaves no trace")

	events := []Event{
		{Seq: 7, WitnessedAt: 30, Kind: KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r1", Payload: []byte{1, 2}},
		{Seq: 5, WitnessedAt: 10, Kind: KindUpdate, DID: "did:plc:b", Collection: "c", Rkey: "r2"},
		{Seq: 9, WitnessedAt: 20, Kind: KindDelete, DID: "did:plc:a", Collection: "d", Rkey: "r3"},
	}
	for i, ev := range events {
		full, err := b.Append(ev)
		require.NoError(t, err)
		require.Equal(t, i == len(events)-1, full)
	}
	require.Equal(t, 3, b.Len())
	_, err = b.Append(events[0])
	require.ErrorIs(t, err, ErrBufferFull)

	bounds, ok := b.PendingBounds()
	require.True(t, ok)
	require.Equal(t, BlockInfo{
		EventCount: 3, MinSeq: 5, MaxSeq: 9, MinWitnessedAt: 10, MaxWitnessedAt: 30,
	}, bounds)

	snap := b.Snapshot()
	require.Len(t, snap, 3)
	snap[0].Payload[0] = 0xff
	again := b.Snapshot()
	require.Equal(t, byte(1), again[0].Payload[0], "Snapshot copies")

	frame, info = b.Encode()
	require.NotNil(t, frame)
	require.Equal(t, 0, b.Len(), "Encode resets the builder")
	_, ok = b.PendingBounds()
	require.False(t, ok)
	require.EqualValues(t, len(frame), info.CompressedSize)
	require.Zero(t, info.Offset, "the caller places the frame")
	bounds.CompressedSize = info.CompressedSize
	bounds.UncompressedSize = info.UncompressedSize
	require.Equal(t, bounds, info)

	got, uncompressed, err := decodeBlockCompressedSized(frame)
	require.NoError(t, err)
	require.EqualValues(t, uncompressed, info.UncompressedSize)
	require.Len(t, got, len(events))
	for i := range events {
		require.True(t, eventsEqual(events[i], got[i]), "event %d", i)
	}

	// The builder is reusable after Encode.
	full, err := b.Append(events[1])
	require.NoError(t, err)
	require.False(t, full)
	require.Equal(t, 1, b.Len())
}
