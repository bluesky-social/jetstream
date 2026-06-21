package oracle

import (
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// TestOracle_RestartChainShapeF_AccountDeleteReactivate covers shape F
// (specs/notes/2026-06-20-restart-tier-intermediates-plan.md §6): the
// DID-level no-permanent-tombstone path. On a dedicated account the chain
// is account-delete → reactivate → post, all in the live window. The DID
// tombstone (a durable, non-rev-filtered account row) resets the repo;
// the post created AFTER reactivation, at a higher seq, must be visible
// again, while the records that existed at backfill must be GONE.
//
// nolint:paralleltest
func TestOracle_RestartChainShapeF_AccountDeleteReactivate(t *testing.T) {
	run := runChainToMergeNoCrash(t, "shape-f", 0)
	require.NotNil(t, run.spec.didReact, "shape F fixture requires >= 2 accounts")

	// shape F must have actually generated.
	run.coord.didReactResult()

	// Final-state + record-chain assertions still hold (shape F is on a
	// dedicated DID, so it must not perturb them).
	assertOracleMatches(t, run.dataDir, run.w, run.cfg, "shape-f")
	assertChainDurable(t, run.dataDir, run.coord, "shape-f")

	f := run.spec.didReact
	dacct, err := run.w.LoadAccount(f.accountIdx)
	require.NoError(t, err)
	didReactDID := string(dacct.DID)

	cov := chainCoverage(t, run.dataDir, run.coord)

	// A durable account tombstone (KindAccount, deleted) for the shape-F
	// DID landed on disk — the DID-level intermediate.
	tombstone := false
	for _, ev := range cov.events {
		if ev.Kind == segment.KindAccount && ev.DID == didReactDID {
			if deleted, derr := oracleAccountDeleted(ev.Payload); derr == nil && deleted {
				tombstone = true
				break
			}
		}
	}
	require.Truef(t, tombstone, "expected a durable account-delete tombstone for shape-F DID %s", didReactDID)

	// Reconstruct: the post-reactivation record is visible; nothing else
	// for this DID survives the reset.
	model, err := Reconstruct(cov.events)
	require.NoError(t, err)
	snap, ok := model.Accounts[didReactDID]
	require.Truef(t, ok, "shape-F DID %s must appear in reconstructed model", didReactDID)
	postKey := RecordKey{DID: didReactDID, Collection: f.collection, Rkey: f.rkey}
	_, present := snap.Records[postKey]
	require.Truef(t, present, "post-reactivation record %s/%s must be visible (no permanent DID tombstone)", f.collection, f.rkey)
	require.Lenf(t, snap.Records, 1,
		"only the post-reactivation record survives the DID reset; got %d records", len(snap.Records))
}
