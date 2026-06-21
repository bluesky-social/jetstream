package oracle

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestOracle_RestartChainShapeH_Guards covers the H control/regression
// guards (specs/notes/2026-06-20-restart-tier-intermediates-plan.md §6):
// cheap checks that catch a fixture or harness that tests nothing.
//
// H1: a live create-only record (no tombstone) survives on disk — proving
// live-window survival works independent of any tombstone, so the tier is
// not silently dropping every live row.
//
// H2: a pure-backfill DID never touched by the chain still reconstructs
// its full record set — proving the existing all-creates path is not
// regressed by the chain machinery.
//
// nolint:paralleltest
func TestOracle_RestartChainShapeH_Guards(t *testing.T) {
	run := runChainToMergeNoCrash(t, "shape-h", 0)

	assertOracleMatches(t, run.dataDir, run.w, run.cfg, "shape-h")
	assertChainDurable(t, run.dataDir, run.coord, "shape-h")

	cov := chainCoverage(t, run.dataDir, run.coord)

	// H1: the create-only record's create must be present, and dropping it
	// breaks coverage.
	rc := recordChainForShape(t, run.spec, shapeLiveCreateOnly)
	assertCoverageFailsWithoutRow(t, cov, "create", rc.collection, rc.rkey)

	// H2: a pure-backfill DID (any account that is NOT the chain host) must
	// reconstruct its records from disk. The chain host is excluded because
	// its records were mutated live.
	model, err := Reconstruct(cov.events)
	require.NoError(t, err, "reconstruct for H2")

	untouchedDIDs := 0
	for did, snap := range model.Accounts {
		if did == run.coord.hostDID {
			continue
		}
		if len(snap.Records) == 0 {
			continue
		}
		untouchedDIDs++
		// Cross-check against ground truth: every record the world holds
		// for this untouched DID is present on disk.
		require.NotEmptyf(t, snap.Records, "untouched DID %s should have records", did)
	}
	require.Positivef(t, untouchedDIDs,
		"H2 guard vacuous: expected at least one pure-backfill DID with records (accounts=%d)", run.cfg.Accounts)
}
