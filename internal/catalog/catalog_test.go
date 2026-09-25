package catalog

import (
	"math/rand/v2"
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// randomSegments builds a namespace of segments with strictly increasing,
// gappy block envelopes, including segments with no blocks.
func randomSegments(rng *rand.Rand) ([]SegmentView, []segment.BlockInfo) {
	var (
		segs []SegmentView
		all  []segment.BlockInfo
		seq  = uint64(1 + rng.IntN(5))
	)
	for idx := range uint64(rng.IntN(8)) {
		v := SegmentView{Namespace: Main, Index: idx * uint64(1+rng.IntN(2)), State: Sealed}
		if len(segs) > 0 && v.Index <= segs[len(segs)-1].Index {
			v.Index = segs[len(segs)-1].Index + 1
		}
		for range rng.IntN(5) {
			width := uint64(rng.IntN(4))
			b := segment.BlockInfo{MinSeq: seq, MaxSeq: seq + width, EventCount: uint32(width + 1)}
			v.Blocks = append(v.Blocks, b)
			all = append(all, b)
			seq += width + 1 + uint64(rng.IntN(3)) // leave vacancies between blocks
		}
		segs = append(segs, v)
	}
	if len(segs) > 0 && rng.IntN(2) == 0 {
		segs[len(segs)-1].State = Active
	}
	return segs, all
}

// TestView_RefsFromCoversSuffixExactlyOnce is the RefsFrom property: for any
// seq, the refs are exactly the blocks whose envelope reaches seq or later,
// each once, in seq order, and the block before the first ref ends before
// seq. TipSeq is one past the last envelope.
func TestView_RefsFromCoversSuffixExactlyOnce(t *testing.T) {
	t.Parallel()

	for seed := range uint64(500) {
		rng := rand.New(rand.NewPCG(seed, 0x5eed))
		segs, all := randomSegments(rng)
		v, err := NewView(seed, map[Namespace][]SegmentView{Main: segs}, func(s SegmentView, i int) Locator {
			return ObjectBlock{ObjectID: s.Index<<16 | uint64(i)}
		})
		require.NoError(t, err, "seed %d", seed)

		var tip uint64
		if len(all) > 0 {
			tip = all[len(all)-1].MaxSeq + 1
		}
		require.Equal(t, tip, v.TipSeq(Main), "seed %d", seed)
		require.Zero(t, v.TipSeq(BootstrapLive))

		for seq := uint64(0); seq <= tip+2; seq++ {
			var want []segment.BlockInfo
			for _, b := range all {
				if b.MaxSeq >= seq {
					want = append(want, b)
				}
			}
			var got []segment.BlockInfo
			for ref := range v.RefsFrom(Main, seq) {
				loc, ok := ref.Loc.(ObjectBlock)
				require.True(t, ok, "seed %d: locator type %T", seed, ref.Loc)
				require.Equal(t, ref.Segment<<16|uint64(ref.Block), loc.ObjectID, "seed %d: locator for the wrong block", seed)
				got = append(got, segment.BlockInfo{MinSeq: ref.MinSeq, MaxSeq: ref.MaxSeq, EventCount: uint32(ref.MaxSeq - ref.MinSeq + 1)})
			}
			require.Equal(t, want, got, "seed %d seq %d", seed, seq)
		}

		// Stopping early must not yield past the break.
		n := 0
		for range v.RefsFrom(Main, 0) {
			n++
			if n == 2 {
				break
			}
		}
		require.LessOrEqual(t, n, 2)
	}
}

func TestNewView_RejectsOverlap(t *testing.T) {
	t.Parallel()

	blk := func(lo, hi uint64) segment.BlockInfo { return segment.BlockInfo{MinSeq: lo, MaxSeq: hi} }
	cases := map[string][]SegmentView{
		"blocks overlap within a segment": {{Index: 0, Blocks: []segment.BlockInfo{blk(1, 5), blk(5, 6)}}},
		"segments overlap":                {{Index: 0, Blocks: []segment.BlockInfo{blk(1, 5)}}, {Index: 1, Blocks: []segment.BlockInfo{blk(3, 9)}}},
		"segments out of index order":     {{Index: 1, Blocks: []segment.BlockInfo{blk(1, 5)}}, {Index: 0, Blocks: []segment.BlockInfo{blk(6, 9)}}},
		"inverted envelope":               {{Index: 0, Blocks: []segment.BlockInfo{blk(5, 1)}}},
		"overlap across an empty segment": {{Index: 0, Blocks: []segment.BlockInfo{blk(1, 5)}}, {Index: 1}, {Index: 2, Blocks: []segment.BlockInfo{blk(5, 9)}}},
	}
	for name, segs := range cases {
		_, err := NewView(1, map[Namespace][]SegmentView{Main: segs}, func(SegmentView, int) Locator { return InlineBlock{} })
		require.ErrorIs(t, err, ErrOverlap, name)
	}
}

func TestLogEntry_CopiesPayloadAndMemoizes(t *testing.T) {
	t.Parallel()

	payload := []byte{1, 2, 3}
	e := NewLogEntry(&segment.Event{Seq: 7, DID: "did:plc:a", Payload: payload})
	payload[0] = 9
	require.Equal(t, []byte{1, 2, 3}, e.Event().Payload)
	require.Positive(t, e.ApproxBytes())

	require.Nil(t, e.LoadMemo())
	first := e.LoadOrStoreMemo("a")
	require.Equal(t, "a", first)
	require.Equal(t, "a", e.LoadOrStoreMemo("b"), "the first stored memo wins")
}
