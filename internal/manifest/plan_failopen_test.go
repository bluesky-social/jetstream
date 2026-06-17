package manifest

import (
	"testing"

	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/jcalabro/gloom"
	"github.com/stretchr/testify/require"
)

// These tests guard the one-sided contract directly at the selection helpers:
// whenever a bloom or collection index is missing/short, the planner MUST
// fail OPEN (include the block) rather than risk a false negative. They live in
// package manifest because they construct SegmentMetadata with deliberately
// degenerate resident metadata that the public API never produces.

func twoBlockSeg() *SegmentMetadata {
	return &SegmentMetadata{
		Blocks: []segment.BlockInfo{
			{EventCount: 1, MinSeq: 1, MaxSeq: 1},
			{EventCount: 1, MinSeq: 2, MaxSeq: 2},
		},
	}
}

func TestSegmentBloomMayContainAny_NilBloomFailsOpen(t *testing.T) {
	t.Parallel()

	seg := twoBlockSeg() // SegmentBloom is nil.
	require.True(t, segmentBloomMayContainAny(seg, []string{"did:plc:anything"}),
		"a nil segment bloom must not prune the segment")
}

func TestBlockBloomMayContainAny_NilEntryAndShortSliceFailOpen(t *testing.T) {
	t.Parallel()

	seg := twoBlockSeg()
	// BlockBlooms shorter than Blocks: block index 0 has a nil entry, block 1
	// is past the end of the slice. Both must fail open.
	seg.BlockBlooms = []*gloom.Filter{nil}

	require.True(t, blockBloomMayContainAny(seg, 0, []string{"did:plc:x"}),
		"a nil per-block bloom must fail open")
	require.True(t, blockBloomMayContainAny(seg, 1, []string{"did:plc:x"}),
		"a block index past the bloom slice must fail open")
}

func TestBlockHasAnyCollection_ShortSliceFailsOpen(t *testing.T) {
	t.Parallel()

	seg := twoBlockSeg()
	// BlockCollections shorter than Blocks: block 1 has no resident summary and
	// must fail open rather than be pruned.
	seg.BlockCollections = [][]uint32{{0}}
	ids := map[uint32]struct{}{7: {}}

	require.False(t, blockHasAnyCollection(seg, 0, ids),
		"block 0 has a resident summary that does not contain id 7")
	require.True(t, blockHasAnyCollection(seg, 1, ids),
		"block 1 has no resident summary and must fail open")
}

// TestSelectPlanBlocks_DegradedMetadataNeverDropsBlocks ties the guards
// together at selectPlanBlocks: with nil/short resident metadata, every
// in-window block must still be selected for a DID filter (no false negative),
// even though exact filtering would later reject some.
func TestSelectPlanBlocks_DegradedMetadataNeverDropsBlocks(t *testing.T) {
	t.Parallel()

	seg := &SegmentMetadata{
		Blocks: []segment.BlockInfo{
			{EventCount: 1, MinSeq: 1, MaxSeq: 1},
			{EventCount: 1, MinSeq: 2, MaxSeq: 2},
			{EventCount: 1, MinSeq: 3, MaxSeq: 3},
		},
		// SegmentBloom nil, BlockBlooms shorter than Blocks with a nil entry.
		BlockBlooms: []*gloom.Filter{nil},
	}

	got := selectPlanBlocks(seg, PlanBackfillRequest{DIDs: []string{"did:plc:needle"}}, nil)
	require.Equal(t, []int{0, 1, 2}, got,
		"degraded DID metadata must fail open and select every in-window block")
}

// TestSelectPlanBlocks_BloomFalsePositiveStillIncludesRealBlock is the
// false-positive property: a per-block bloom that collides on a DID it does not
// contain causes over-inclusion (allowed), and never causes the real matching
// block to be dropped (forbidden).
func TestSelectPlanBlocks_BloomFalsePositiveStillIncludesRealBlock(t *testing.T) {
	t.Parallel()

	const want = "did:plc:target"
	const decoy = "did:plc:decoy"

	// Build two real per-block blooms. Block 0 actually contains `want`. Block
	// 1 contains only `decoy`; we additionally force a collision by inserting a
	// crafted string until block 1's bloom reports `want` as present, emulating
	// a natural false positive.
	segBloom := gloom.New(8, 0.001)
	segBloom.AddString(want)
	segBloom.AddString(decoy)

	b0 := gloom.New(4, 0.001)
	b0.AddString(want)

	b1 := gloom.New(64, 0.5) // high FP rate to make a collision easy to force
	b1.AddString(decoy)
	if !b1.TestString(want) {
		// Force a positive for `want` in b1 without it being a real member by
		// adding members until the bloom collides. Bounded loop; high FP rate
		// makes this terminate quickly.
		for i := 0; i < 100000 && !b1.TestString(want); i++ {
			b1.AddString("filler:" + itoa(i))
		}
	}
	require.True(t, b1.TestString(want), "test setup must produce a block-1 false positive")

	seg := &SegmentMetadata{
		Blocks: []segment.BlockInfo{
			{EventCount: 1, MinSeq: 1, MaxSeq: 1},
			{EventCount: 1, MinSeq: 2, MaxSeq: 2},
		},
		SegmentBloom: segBloom,
		BlockBlooms:  []*gloom.Filter{b0, b1},
	}

	got := selectPlanBlocks(seg, PlanBackfillRequest{DIDs: []string{want}}, nil)
	require.Contains(t, got, 0, "the block that truly contains the DID must never be dropped")
	require.Contains(t, got, 1, "a false-positive block is over-included, which is allowed")
}

// itoa is a tiny dependency-free int->string for the filler loop above.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
