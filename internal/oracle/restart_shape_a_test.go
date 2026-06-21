package oracle

import "testing"

// TestOracle_RestartChainShapeA_BfCreateUpdate covers shape A
// (specs/notes/2026-06-20-restart-tier-intermediates-plan.md §6): an R_bf
// record whose create lands at backfill (at the repo head rev) is
// superseded by a LIVE update at a higher rev. The seam it proves is the
// merge rev-filter survival boundary — the live update must survive
// (rev > BackfillRev) and become a durable intermediate on disk, then
// supersede the backfilled create at compaction.
//
// Red-first power check: dropping the durable update row from the observed
// disk set must make at-least-once coverage fail (the lost-intermediate
// failure mode), while the fixture itself confirms the update actually
// landed (anti-vacuity).
//
// nolint:paralleltest
func TestOracle_RestartChainShapeA_BfCreateUpdate(t *testing.T) {
	run := runChainToMergeNoCrash(t, "shape-a", 0)

	// Final-state + the three durable-intermediate assertions hold.
	assertOracleMatches(t, run.dataDir, run.w, run.cfg, "shape-a")
	assertChainDurable(t, run.dataDir, run.coord, "shape-a")

	// Power test: the R_bf live update is the signature durable row of
	// this shape. Losing it must break coverage.
	rc := recordChainForShape(t, run.spec, shapeBfCreateUpdate)
	cov := chainCoverage(t, run.dataDir, run.coord)
	assertCoverageFailsWithoutRow(t, cov, "update", rc.collection, rc.rkey)
}
