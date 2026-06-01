package subscribe

import (
	"testing"

	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/stretchr/testify/require"
)

func ringEntry(seq uint64, payloadLen int) *Entry {
	return newEntry(&segment.Event{
		Seq: seq, Kind: segment.KindCreate, DID: "did:plc:r",
		Payload: make([]byte, payloadLen),
	})
}

func TestHotRing_AppendAndLookupHot(t *testing.T) {
	t.Parallel()
	r := newHotRing(1 << 20) // 1 MiB
	for seq := uint64(1); seq <= 5; seq++ {
		r.append(ringEntry(seq, 10))
	}
	got, ok := r.lookup(3)
	require.True(t, ok)
	seqs := make([]uint64, len(got))
	for i, e := range got {
		seqs[i] = e.Event.Seq
	}
	require.Equal(t, []uint64{3, 4, 5}, seqs)
}

func TestHotRing_CursorBelowBaseIsCold(t *testing.T) {
	t.Parallel()
	// Tiny budget so older entries are evicted.
	r := newHotRing(300)
	for seq := uint64(1); seq <= 10; seq++ {
		r.append(ringEntry(seq, 10))
	}
	// Oldest resident seq is > 1; asking for seq 1 is not serviceable.
	_, ok := r.lookup(1)
	require.False(t, ok, "evicted seq must not be serviceable by the ring")

	// The most recent seq is still resident.
	got, ok := r.lookup(10)
	require.True(t, ok)
	require.Len(t, got, 1)
	require.Equal(t, uint64(10), got[0].Event.Seq)
}

func TestHotRing_CursorAtTipIsHotEmpty(t *testing.T) {
	t.Parallel()
	r := newHotRing(1 << 20)
	for seq := uint64(1); seq <= 3; seq++ {
		r.append(ringEntry(seq, 10))
	}
	// tip is 4 (next unwritten seq); a cursor at/beyond the resident tip is
	// NOT serviceable by the ring (Tail decides block vs cold).
	_, ok := r.lookup(4)
	require.False(t, ok)
}

func TestHotRing_EmptyReturnsNotOk(t *testing.T) {
	t.Parallel()
	r := newHotRing(1 << 20)
	got, ok := r.lookup(0)
	require.False(t, ok)
	require.Nil(t, got)
}

func TestHotRing_ByteBudgetBoundsResidency(t *testing.T) {
	t.Parallel()
	r := newHotRing(2000)
	for seq := uint64(1); seq <= 1000; seq++ {
		r.append(ringEntry(seq, 10))
	}
	require.LessOrEqual(t, r.bytes(), 2000, "ring must never exceed byte budget")
	require.Greater(t, r.len(), 0)
}
