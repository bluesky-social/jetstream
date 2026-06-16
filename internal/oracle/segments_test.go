package oracle

import (
	"testing"

	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/stretchr/testify/require"
)

func TestCheckSegmentStructureRejectsNonIncreasingOffsets(t *testing.T) {
	t.Parallel()

	err := checkSegmentStructure("seg_00000000000000000000.jss", segment.Header{BlockCount: 2}, []segment.BlockInfo{
		{Offset: 200, EventCount: 1, MinSeq: 1, MaxSeq: 1},
		{Offset: 199, EventCount: 1, MinSeq: 2, MaxSeq: 2},
	})
	require.ErrorContains(t, err, "non-increasing block offset")
}

func TestCheckSegmentStructureRejectsSeqRegressionAcrossBlocks(t *testing.T) {
	t.Parallel()

	err := checkSegmentStructure("seg_00000000000000000000.jss", segment.Header{BlockCount: 2}, []segment.BlockInfo{
		{Offset: 200, EventCount: 1, MinSeq: 10, MaxSeq: 20},
		{Offset: 300, EventCount: 1, MinSeq: 20, MaxSeq: 30},
	})
	require.ErrorContains(t, err, "block seq overlap")
}
