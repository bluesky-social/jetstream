package seqspace_test

import (
	"testing"

	"github.com/bluesky-social/jetstream/internal/seqspace"
	"github.com/stretchr/testify/require"
)

func TestGapsNormalizeAndLookup(t *testing.T) {
	t.Parallel()
	gaps, err := seqspace.NewGaps([]seqspace.Gap{{Start: 20, End: 30}, {Start: 10, End: 20}, {Start: 12, End: 15}, {Start: 40, End: 50}})
	require.NoError(t, err)
	require.Equal(t, []seqspace.Gap{{Start: 10, End: 30}, {Start: 40, End: 50}}, gaps.Ranges())

	for _, seq := range []uint64{10, 19, 29, 40, 49} {
		_, ok := gaps.EndContaining(seq)
		require.Truef(t, ok, "seq %d", seq)
	}
	for _, seq := range []uint64{9, 30, 39, 50} {
		_, ok := gaps.EndContaining(seq)
		require.Falsef(t, ok, "seq %d", seq)
	}
}

func TestGapsValidateVacantAgainstBlocks(t *testing.T) {
	t.Parallel()
	gaps, err := seqspace.NewGaps([]seqspace.Gap{{Start: 5, End: 10}})
	require.NoError(t, err)
	require.NoError(t, gaps.ValidateVacant([]seqspace.BlockRange{{Min: 10, Max: 12}, {Min: 1, Max: 4}}), "validation accepts unsorted block input")
	require.Error(t, gaps.ValidateVacant([]seqspace.BlockRange{{Min: 9, Max: 11}}))
}

func TestReserveEndChecksCeiling(t *testing.T) {
	t.Parallel()
	end, err := seqspace.ReserveEnd(10, 4)
	require.NoError(t, err)
	require.Equal(t, uint64(14), end)
	_, err = seqspace.ReserveEnd(seqspace.CursorSeqMaxThreshold-1, 2)
	require.Error(t, err)
	_, err = seqspace.ReserveEnd(0, 1)
	require.Error(t, err)
	_, err = seqspace.ReserveEnd(1, 0)
	require.Error(t, err)
}

func TestGapsRejectInvalidRangesAndReportWidth(t *testing.T) {
	t.Parallel()
	for _, gap := range []seqspace.Gap{
		{Start: 0, End: 1},
		{Start: 5, End: 5},
		{Start: 6, End: 5},
		{Start: 1, End: seqspace.CursorSeqMaxThreshold + 1},
	} {
		_, err := seqspace.NewGaps([]seqspace.Gap{gap})
		require.Error(t, err)
	}
	gaps, err := seqspace.NewGaps([]seqspace.Gap{{Start: 1, End: 5}, {Start: 10, End: 13}})
	require.NoError(t, err)
	require.Equal(t, 2, gaps.Count())
	require.Equal(t, uint64(7), gaps.Width())
}
