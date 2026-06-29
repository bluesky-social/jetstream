package oracle

import (
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// TestOracle_RestartChainShapeD_LiveDeleteRecreate covers shape D
// (specs/notes/2026-06-20-restart-tier-intermediates-plan.md §6): a record
// created, deleted, then RECREATED on the same rkey at a higher seq. The
// seam it proves is record-level no-permanent-tombstone (docs/README.md:358):
// the compaction tombstone mask is half-open (seq < tombstone.seq), so the
// recreate ABOVE the tombstone is not masked and the record reconstructs as
// present.
//
// Red-first power test: with the recreate row removed, reconstruction must
// show the record ABSENT — i.e. the recreate is the only thing making it
// visible past the delete tombstone. This is the counterfactual of the
// permanent-tombstone bug (a mask that incorrectly hid the recreate).
//
// nolint:paralleltest
func TestOracle_RestartChainShapeD_LiveDeleteRecreate(t *testing.T) {
	run := runChainToMergeNoCrash(t, "shape-d", 0)

	assertOracleMatches(t, run.dataDir, run.w, run.cfg, "shape-d")
	// assertChainDurable already checks the recreate reconstructs present.
	assertChainDurable(t, run.dataDir, run.coord, "shape-d")

	// The positive direction (recreate reconstructs as present) is already
	// asserted by assertChainDurable above via assertRecreatedRecordsVisible;
	// re-checking it here would be redundant. What follows is the shape-D
	// counterfactual that block does NOT cover.
	rc := recordChainForShape(t, run.spec, shapeLiveDeleteRecreate)
	cov := chainCoverage(t, run.dataDir, run.coord)
	key := RecordKey{DID: run.coord.hostDID, Collection: rc.collection, Rkey: rc.rkey}

	// Red: drop the recreate (the create row ABOVE the delete tombstone)
	// and confirm the record then reconstructs ABSENT — proving the
	// recreate is what defeats the tombstone, and that a mask incorrectly
	// hiding it (a permanent tombstone) would be caught.
	withoutRecreate := dropHighestCreateForKey(t, cov.events, run.coord.hostDID, rc.collection, rc.rkey)
	masked, err := Reconstruct(withoutRecreate)
	require.NoError(t, err)
	// The host account must still exist after dropping a single record's
	// recreate (only the record dies, not the repo); guarding the assertion
	// behind `ok` here would let it pass vacuously if it ever didn't.
	snap, ok := masked.Accounts[run.coord.hostDID]
	require.Truef(t, ok, "host DID %s must remain present after dropping the recreate", run.coord.hostDID)
	_, stillThere := snap.Records[key]
	require.Falsef(t, stillThere,
		"counterfactual: without the recreate, %s/%s must be absent (delete tombstone wins)", rc.collection, rc.rkey)
}

// dropHighestCreateForKey removes the create/create_resync row with the
// HIGHEST seq for (did, coll, rkey) — i.e. the recreate above any delete
// tombstone — leaving everything else intact. Models a mask that
// incorrectly hides the recreate. Fails the test if no such create exists:
// a fixture with no recreate to drop means the counterfactual is vacuous,
// which must be loud, not silent.
func dropHighestCreateForKey(t *testing.T, events []ObservedEvent, did, coll, rkey string) []ObservedEvent {
	t.Helper()
	bestIdx := -1
	var bestSeq uint64
	for i, e := range events {
		if e.DID != did || e.Collection != coll || e.Rkey != rkey {
			continue
		}
		if e.Kind != segment.KindCreate && e.Kind != segment.KindCreateResync {
			continue
		}
		if bestIdx == -1 || e.Seq > bestSeq {
			bestIdx, bestSeq = i, e.Seq
		}
	}
	if bestIdx == -1 {
		t.Fatalf("test fixture must contain a create/create_resync for %s/%s to drop", coll, rkey)
	}
	out := make([]ObservedEvent, 0, len(events)-1)
	out = append(out, events[:bestIdx]...)
	out = append(out, events[bestIdx+1:]...)
	return out
}
