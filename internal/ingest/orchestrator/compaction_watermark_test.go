package orchestrator

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// initCompactionWatermarkFloor's first-init contract: the floor sits at
// nextSeq-1 — the last seq the merge destination has already allocated — so
// the FIRST event the merge appends (at nextSeq) stays strictly above the
// watermark and remains compactable. The off-by-one (floor = nextSeq) claims
// that first seq as already-compacted: a superseding tombstone landing there
// makes the pass no-op (targetWatermark <= floor) and its victim survives
// permanently (mutant m002). TestMerge_FirstInitWatermarkFloor_BoundarySeqCompacts
// proves the data-loss consequence end-to-end; these pin the contract exactly.
func TestInitCompactionWatermarkFloor_FloorIsNextSeqMinusOne(t *testing.T) {
	t.Parallel()
	st := newOrchestratorTestStore(t)

	require.NoError(t, initCompactionWatermarkFloor(st, 5))

	w, ok, err := loadCompactionWatermark(st)
	require.NoError(t, err)
	require.True(t, ok, "first init must persist a watermark")
	require.Equal(t, uint64(4), w,
		"first-init floor must be nextSeq-1 (the last already-allocated seq), not nextSeq")
}

func TestInitCompactionWatermarkFloor_ZeroNextSeq(t *testing.T) {
	t.Parallel()
	st := newOrchestratorTestStore(t)

	require.NoError(t, initCompactionWatermarkFloor(st, 0))

	w, ok, err := loadCompactionWatermark(st)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(0), w, "empty archive floors at 0")
}

func TestInitCompactionWatermarkFloor_DoesNotOverwriteExisting(t *testing.T) {
	t.Parallel()
	st := newOrchestratorTestStore(t)

	require.NoError(t, saveCompactionWatermark(st, 7))
	require.NoError(t, initCompactionWatermarkFloor(st, 100))

	w, ok, err := loadCompactionWatermark(st)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(7), w, "an already-persisted watermark is never re-floored")
}
