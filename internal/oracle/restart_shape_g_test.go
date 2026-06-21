package oracle

import (
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// TestOracle_RestartChainShapeG_SyncDivergence covers shape G
// (specs/notes/2026-06-20-restart-tier-intermediates-plan.md §6), the
// highest-complexity shape, landed last. On a dedicated, early-served DID
// the fixture performs a silent repo mutation (a create whose #commit is
// suppressed) then a #sync. Jetstream's verifier detects the chain break
// and repairs via an async getRepo resync, archiving a KindSync DID
// tombstone plus KindCreateResync rows for the full current repo.
//
// The seam, across a restart: the resync rows must (a) survive the merge,
// (b) reconstruct the FULL repo — including the silently-created record
// jetstream never saw a #commit for — and (c) sit above the sync tombstone
// so the DID is visible again (the sync-flavored DID-level
// no-permanent-tombstone contract). Final-state Compare converging is the
// proof the verifier-getRepo repair completed correctly across the
// process boundary.
//
// Timing (spike-verified): the DID is served EARLY (spec allocates host+2)
// so the async resync — which runs on a verifier worker and re-fetches via
// getRepo — lands its rows before cutover truncates the live window. A
// late-served DID flakes (resync rows truncated mid-flight). This mirrors
// the Group 0 Q2 early-DID + baseline-gated discipline.
//
// nolint:paralleltest
func TestOracle_RestartChainShapeG_SyncDivergence(t *testing.T) {
	run := runChainToMergeNoCrash(t, "shape-g", 0)
	require.NotNil(t, run.spec.syncDiverge, "shape G fixture requires >= 3 accounts")

	// Shape G must have actually generated.
	run.coord.syncDivResult()

	// Final-state + record-chain assertions still hold (shape G is on a
	// dedicated DID; its resync repair must not perturb them, and the
	// silently-diverged DID must converge via the resync).
	assertOracleMatches(t, run.dataDir, run.w, run.cfg, "shape-g")
	assertChainDurable(t, run.dataDir, run.coord, "shape-g")

	g := run.spec.syncDiverge
	gacct, err := run.w.LoadAccount(g.accountIdx)
	require.NoError(t, err)
	gDID := string(gacct.DID)

	cov := chainCoverage(t, run.dataDir, run.coord)

	// A durable KindSync tombstone for the shape-G DID landed on disk — the
	// DID-level sync intermediate.
	syncRows, resyncRows := 0, 0
	for _, ev := range cov.events {
		if ev.DID != gDID {
			continue
		}
		switch ev.Kind {
		case segment.KindSync:
			syncRows++
		case segment.KindCreateResync:
			resyncRows++
		}
	}
	require.Positivef(t, syncRows, "expected a durable KindSync tombstone for shape-G DID %s", gDID)
	require.Positivef(t, resyncRows, "expected durable KindCreateResync rows for shape-G DID %s", gDID)

	// No-permanent-tombstone (sync flavor): the resync rows sit above the
	// sync tombstone, so the DID reconstructs with its full record set —
	// including the silently-created record jetstream never saw a commit
	// for. assertOracleMatches above already proved the full model matches
	// ground truth; here we pin that the G DID specifically is non-empty
	// and matches, so a regression that dropped the resync rows (leaving
	// the DID permanently tombstoned/empty) is caught here explicitly.
	model, err := Reconstruct(cov.events)
	require.NoError(t, err)
	gotG, ok := model.Accounts[gDID]
	require.Truef(t, ok, "shape-G DID %s must appear in reconstructed model", gDID)
	require.NotEmptyf(t, gotG.Records,
		"shape-G DID %s must reconstruct with records (resync above the sync tombstone — no permanent tombstone)", gDID)

	want, err := GroundTruthFromWorld(run.w)
	require.NoError(t, err)
	require.Lenf(t, gotG.Records, len(want.Accounts[gDID].Records),
		"shape-G DID %s reconstructed record count must equal ground truth (resync rebuilt the full repo)", gDID)
}
