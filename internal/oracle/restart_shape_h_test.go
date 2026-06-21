package oracle

import (
	"bytes"
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
	// reconstruct its records from disk byte-for-byte. The untouched-DID set is
	// derived from ground truth — NOT from the reconstructed model — so a DID
	// the disk reconstruction wrongly dropped entirely is still cross-checked
	// (it would surface as a missing key here) rather than silently skipped.
	model, err := Reconstruct(cov.events)
	require.NoError(t, err, "reconstruct for H2")
	ground, err := GroundTruthFromWorld(run.w)
	require.NoError(t, err, "ground truth for H2")

	untouchedDIDs := 0
	for did, wantSnap := range ground.Accounts {
		if did == run.coord.hostDID {
			continue // mutated live; covered by the chain assertions above
		}
		if len(wantSnap.Records) == 0 {
			continue // nothing to cross-check for an empty account
		}
		untouchedDIDs++
		gotSnap, ok := model.Accounts[did]
		require.Truef(t, ok, "H2: pure-backfill DID %s missing entirely from reconstructed disk model", did)
		require.Lenf(t, gotSnap.Records, len(wantSnap.Records),
			"H2: pure-backfill DID %s reconstructed record count must equal ground truth", did)
		for key, wantVal := range wantSnap.Records {
			gotVal, ok := gotSnap.Records[key]
			require.Truef(t, ok, "H2: pure-backfill DID %s missing record %s/%s on disk", did, key.Collection, key.Rkey)
			require.Truef(t, bytes.Equal(wantVal.Payload, gotVal.Payload),
				"H2: pure-backfill DID %s record %s/%s payload mismatch vs ground truth", did, key.Collection, key.Rkey)
		}
	}
	require.Positivef(t, untouchedDIDs,
		"H2 guard vacuous: expected at least one pure-backfill DID with records (accounts=%d)", run.cfg.Accounts)
}
