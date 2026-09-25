package catalog

import (
	"math/rand/v2"
	"slices"
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
		// Half the seeds put the last segment in the view as a tail, the way
		// backends add their active segment.
		lists, tails := map[Namespace][]SegmentView{Main: segs}, map[Namespace]SegmentView{}
		if n := len(segs); n > 0 && seed%2 == 0 {
			lists[Main], tails[Main] = segs[:n-1], segs[n-1]
		}
		list, err := NewSegmentList(lists[Main])
		require.NoError(t, err, "seed %d", seed)
		v, err := NewView(seed, map[Namespace]SegmentList{Main: list}, tails, func(s SegmentView, i int) Locator {
			return ObjectBlock{ObjectID: s.Index<<16 | uint64(i)}
		})
		require.NoError(t, err, "seed %d", seed)
		require.Equal(t, len(segs), len(v.Segments(Main)), "seed %d", seed)
		if len(segs) > 0 {
			require.Equal(t, segs, v.Segments(Main), "seed %d", seed)
		}

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
				require.Equal(t, Main, ref.Namespace)
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
		_, err := NewSegmentList(segs)
		require.ErrorIs(t, err, ErrOverlap, name)

		// The same overlaps are caught when the last segment arrives through
		// Put, or as a view's tail, which check only around the new segment.
		n := len(segs)
		list, err := NewSegmentList(segs[:n-1])
		if err != nil {
			continue // the overlap is inside the prefix
		}
		_, err = list.Put(segs[n-1])
		require.ErrorIs(t, err, ErrOverlap, name+" (put)")
		_, err = NewView(1, map[Namespace]SegmentList{Main: list}, map[Namespace]SegmentView{Main: segs[n-1]},
			func(SegmentView, int) Locator { return InlineBlock{} })
		require.ErrorIs(t, err, ErrOverlap, name+" (tail)")
	}
}

// TestSegmentList_PutDeleteLeaveOriginal checks copy-on-write: views keep
// the list they were built from while the backend moves on.
func TestSegmentList_PutDeleteLeaveOriginal(t *testing.T) {
	t.Parallel()

	blk := func(lo, hi uint64) segment.BlockInfo { return segment.BlockInfo{MinSeq: lo, MaxSeq: hi} }
	orig, err := NewSegmentList([]SegmentView{
		{Index: 0, Generation: 1, Blocks: []segment.BlockInfo{blk(1, 5)}},
		{Index: 2, Generation: 1, Blocks: []segment.BlockInfo{blk(10, 12)}},
	})
	require.NoError(t, err)
	snapshot := slices.Clone(orig.Segments())

	put, err := orig.Put(SegmentView{Index: 1, Generation: 1, Blocks: []segment.BlockInfo{blk(6, 9)}})
	require.NoError(t, err)
	replaced, err := put.Put(SegmentView{Index: 0, Generation: 2, Blocks: []segment.BlockInfo{blk(1, 5)}})
	require.NoError(t, err)
	deleted := replaced.Delete(2)

	require.Equal(t, snapshot, orig.Segments())
	require.Equal(t, 3, put.Len())
	got, ok := replaced.Get(0)
	require.True(t, ok)
	require.Equal(t, uint64(2), got.Generation)
	last, ok := deleted.Last()
	require.True(t, ok)
	require.Equal(t, uint64(1), last.Index)
	require.Equal(t, deleted, deleted.Delete(7), "deleting a missing index is a no-op")
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
