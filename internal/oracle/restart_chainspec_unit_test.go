package oracle

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDeriveChainSpec_Deterministic pins reproducibility: the same
// (seed, accounts) yields a byte-identical spec, so a CI failure replays
// exactly.
func TestDeriveChainSpec_Deterministic(t *testing.T) {
	t.Parallel()
	a := deriveChainSpec(12345, 4)
	b := deriveChainSpec(12345, 4)
	require.True(t, reflect.DeepEqual(a, b), "same seed must produce identical spec")
}

// TestDeriveChainSpec_VariesAcrossSeeds pins that successive seeds
// explore the state space rather than re-running identical logic: across
// a window of seeds the concrete specifics (account/collection/rkey)
// must not all collapse to one value.
func TestDeriveChainSpec_VariesAcrossSeeds(t *testing.T) {
	t.Parallel()
	accounts := 8
	rkeys := map[string]struct{}{}
	accts := map[int]struct{}{}
	colls := map[string]struct{}{}
	for seed := uint64(101); seed < 141; seed++ {
		spec := deriveChainSpec(seed, accounts)
		accts[spec.chainAccountIdx()] = struct{}{}
		for _, rc := range spec.records {
			rkeys[rc.rkey] = struct{}{}
			colls[rc.collection] = struct{}{}
		}
	}
	require.Greater(t, len(rkeys), 1, "rkeys must vary across seeds")
	require.Greater(t, len(accts), 1, "chain account must vary across seeds")
	require.Greater(t, len(colls), 1, "collections must vary across seeds")
}

// TestDeriveChainSpec_AllPinnedShapesPresent pins the no-vacuous-run
// guarantee: every pinned shape appears on every seed, each with a
// well-formed op sequence.
func TestDeriveChainSpec_AllPinnedShapesPresent(t *testing.T) {
	t.Parallel()
	for seed := range uint64(25) {
		spec := deriveChainSpec(seed, 4)
		got := map[chainShape]recordChain{}
		for _, rc := range spec.records {
			got[rc.shape] = rc
		}
		for _, want := range pinnedShapes {
			rc, ok := got[want]
			require.Truef(t, ok, "seed %d missing pinned shape %q", seed, want)
			require.NotEmpty(t, rc.ops, "shape %q must have ops", want)
			require.Equal(t, "create", rc.ops[0], "every chain starts with a create")
			require.NotEmpty(t, rc.collection)
			require.NotEmpty(t, rc.rkey)
		}

		// The delete-recreate shape must reuse its rkey across the final
		// create (the no-permanent-tombstone fixture).
		dr := got[shapeLiveDeleteRecreate]
		require.Equal(t, []string{"create", "delete", "create"}, dr.ops)
	}
}

// TestDeriveChainSpec_SingleChainDID pins that all chains share one host
// DID, leaving the other accounts as pure-backfill regression witnesses.
func TestDeriveChainSpec_SingleChainDID(t *testing.T) {
	t.Parallel()
	spec := deriveChainSpec(777, 5)
	for _, rc := range spec.records {
		require.Equal(t, spec.chainAccountIdx(), rc.accountIdx)
	}
}
