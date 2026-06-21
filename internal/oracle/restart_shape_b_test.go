package oracle

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestOracle_RestartChainShapeB_BfCreateDelete covers shape B
// (specs/notes/2026-06-20-restart-tier-intermediates-plan.md §6): an R_bf
// record whose create lands at backfill is then DELETED live. The seam it
// proves is distinct from shape A: a live delete tombstone (rev >
// BackfillRev) survives the merge, supersedes the backfilled create at
// compaction (the create is physically removed), and the record's final
// state is ABSENT.
//
// NOTE on §180-182: the convergence-hiding LOST-CREATE signature (final
// state identical with/without the create, so only the event log catches
// a lost create) requires the create to survive UNCOMPACTED — the straddle
// create≤W / delete>W. In a no-crash merge the watermark covers the whole
// merged chain, so the create is legitimately compacted away and is not in
// the coverage want-set. That convergence-hiding power is therefore
// delivered by B-crash (shape B at AfterCompactionRewriteBeforeWatermark),
// not here. This test proves the tombstone-supersedes-backfilled-create
// seam and the absent final state; its power check is a lost tombstone.
//
// nolint:paralleltest
func TestOracle_RestartChainShapeB_BfCreateDelete(t *testing.T) {
	run := runChainToMergeNoCrash(t, "shape-b", 0)

	assertOracleMatches(t, run.dataDir, run.w, run.cfg, "shape-b")
	assertChainDurable(t, run.dataDir, run.coord, "shape-b")

	rc := recordChainForShape(t, run.spec, shapeBfCreateDelete)
	cov := chainCoverage(t, run.dataDir, run.coord)

	// Final state: the deleted record is ABSENT (the lost-intermediate
	// setup — final state converges to absent regardless of the create).
	model, err := Reconstruct(cov.events)
	require.NoError(t, err, "reconstruct for absence check")
	if snap, ok := model.Accounts[run.coord.hostDID]; ok {
		_, present := snap.Records[RecordKey{DID: run.coord.hostDID, Collection: rc.collection, Rkey: rc.rkey}]
		require.Falsef(t, present, "shape-b record %s/%s must be absent in final state", rc.collection, rc.rkey)
	}

	// The backfilled create must NOT survive on disk (superseded by the
	// delete tombstone at/below W).
	for _, r := range cov.got {
		if r.Kind == "create" && r.Collection == rc.collection && r.Rkey == rc.rkey {
			t.Fatalf("shape-b backfilled create %s/%s must be compacted away, but survived on disk", rc.collection, rc.rkey)
		}
	}

	// Power test: the live delete tombstone is the signature durable row.
	// Losing it must break coverage.
	assertCoverageFailsWithoutRow(t, cov, "delete", rc.collection, rc.rkey)
}
