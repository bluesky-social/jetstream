package oracle

import "testing"

// TestOracle_RestartChainShapeC_LiveCreateUpdateDelete covers shape C
// (specs/notes/2026-06-20-restart-tier-intermediates-plan.md §6): a record
// born ENTIRELY in the live window (no backfill create) with the full
// create→update→delete lifecycle. The seam it proves is the live-only
// durable path — none of these rows have a backfill counterpart, so their
// survival depends solely on the bootstrap-live → merge path, independent
// of the backfill snapshot.
//
// Red-first power test: the delete tombstone is the row guaranteed to
// survive compaction (the create/update are superseded ≤W). Dropping it
// must break coverage. (The §180-182 lost-middle-row power for the
// create/update, which compaction legitimately removes in a no-crash run,
// is exercised by B-crash's straddle.)
//
// nolint:paralleltest
func TestOracle_RestartChainShapeC_LiveCreateUpdateDelete(t *testing.T) {
	run := runChainToMergeNoCrash(t, "shape-c", 0)

	assertOracleMatches(t, run.dataDir, run.w, run.cfg, "shape-c")
	assertChainDurable(t, run.dataDir, run.coord, "shape-c")

	rc := recordChainForShape(t, run.spec, shapeLiveCUD)
	cov := chainCoverage(t, run.dataDir, run.coord)
	assertCoverageFailsWithoutRow(t, cov, "delete", rc.collection, rc.rkey)
}
