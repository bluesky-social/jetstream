package orchestrator

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// TestMerge_StoreFaultOnCursorCommit_FailsLoudNoSilentAdvance is the
// store-fault tier's primary kill for mutation m006
// (merge_commit_error_swallowed). The mutant inverts the error check on
// the source-cursor commit in merge_runner.go so a FAILED commit is
// silently swallowed and the merge proceeds — the classic swallowed-
// persistence-error that can advance past unarchived data.
//
// We force commitSourceComplete's batch commit to fail by injecting a
// fault on the merge/next_source_idx batch (the only batch that key rides)
// and assert the phase-specific contract: runMerge fails LOUD, and the
// durable state is left untouched for a clean restart — the cursor is not
// advanced and the backfill source tree is NOT cleaned up. Under the
// mutant the commit error is swallowed, so runMerge runs to completion:
// it returns nil and removes data/backfill. Either divergence fails this
// test, killing the mutant.
//
// Contract reference: issue #30 — "fail loud where continuing risks
// corruption ... never silently advance cursors past unarchived data."
func TestMerge_StoreFaultOnCursorCommit_FailsLoudNoSilentAdvance(t *testing.T) {
	t.Parallel()
	injected := errors.New("injected: merge cursor commit failed")
	fault := &store.KeyPrefixFault{
		Prefix:  []byte(mergeNextSourceIdxKey),
		Op:      store.WriteOpBatchCommit,
		Ordinal: 1,
		Err:     injected,
	}

	srcEvs := []segment.Event{ev("did:plc:a", "3l6", segment.KindCreate, 1000)}
	fix := newMergeFixture(t, [][]segment.Event{srcEvs},
		map[string]string{"did:plc:a": "3l5"}, store.WithFaultInjector(fault))

	require.NoError(t, lifecycle.WritePhase(fix.store, lifecycle.PhaseMerging, time.Now().UTC()))
	o, err := New(fix.cfg)
	require.NoError(t, err)

	// Correct code propagates the commit failure; the mutant swallows it
	// and returns nil. This assertion is the kill.
	err = o.runMerge(t.Context())
	require.Error(t, err, "merge must fail loud when the source-cursor commit fails")
	require.ErrorIs(t, err, injected)

	// No silent advance: the cursor key must remain absent (the failed
	// commit never applied), so a restart re-drains the source.
	cur, err := loadMergeCursor(fix.store)
	require.NoError(t, err)
	require.Equal(t, uint64(0), cur, "failed cursor commit must not advance the merge cursor")

	// Durable source preserved: the backfill tree must NOT be cleaned up,
	// because cleanup past an un-archived source is the data-loss class
	// m006 models. Under the mutant runMerge reaches terminal cleanup and
	// removes this directory.
	_, statErr := os.Stat(filepath.Join(fix.dataDir, "backfill", "live_segments"))
	require.NoError(t, statErr, "backfill source tree must survive a failed merge for restart")
}

// TestMergeNextSourceIdxKeyMatchesOracleStoreFault pins the oracle
// store-fault tier's duplicated key constant to the orchestrator's source of
// truth. The oracle test (internal/oracle) cannot import this package-private
// const, so it hardcodes the string; if mergeNextSourceIdxKey ever changes,
// this guard fails and forces both to move together — otherwise the oracle
// fault would stop matching the merge cursor commit and the m006 kill would
// silently go vacuous.
func TestMergeNextSourceIdxKeyMatchesOracleStoreFault(t *testing.T) {
	t.Parallel()
	require.Equal(t, "merge/next_source_idx", mergeNextSourceIdxKey,
		"oracle store-fault tier hardcodes this key (internal/oracle/restart_storefault_test.go "+
			"mergeNextSourceIdxStoreKey); keep them in sync")
}

// TestMerge_MultiSourceDrainsAllSources is the complementary kill for
// m006's other inverted branch. With err==nil the mutant returns early
// after the FIRST source commit, so sources 2..N are never drained and
// their survivors never reach the destination. The existing
// TestMerge_MultiSourceContiguousCommit only checks post-cleanup state
// (cursor absent, backfill removed), which holds under the early return
// too, so it does not catch this. Here we assert every source's survivor
// lands in the destination.
func TestMerge_MultiSourceDrainsAllSources(t *testing.T) {
	t.Parallel()
	// Three distinct DIDs across three source segments. Each is a fresh
	// DID with no backfill row, so every event is a keep (no rev-filter
	// drop), making the survivor count unambiguous.
	src1 := []segment.Event{ev("did:plc:a", "3l6", segment.KindCreate, 1000)}
	src2 := []segment.Event{ev("did:plc:b", "3l7", segment.KindCreate, 1001)}
	src3 := []segment.Event{ev("did:plc:c", "3l8", segment.KindCreate, 1002)}
	fix := newMergeFixture(t, [][]segment.Event{src1, src2, src3}, nil)

	require.NoError(t, lifecycle.WritePhase(fix.store, lifecycle.PhaseMerging, time.Now().UTC()))
	o, err := New(fix.cfg)
	require.NoError(t, err)
	require.NoError(t, o.runMerge(t.Context()))

	got := readDestEvents(t, fix.dataDir)
	revs := map[string]bool{}
	for _, e := range got {
		revs[e.Rev] = true
	}
	require.True(t, revs["3l6"], "source 1 survivor must reach destination")
	require.True(t, revs["3l7"], "source 2 survivor must reach destination")
	require.True(t, revs["3l8"], "source 3 survivor must reach destination (mutant early-returns after source 1)")
	require.Len(t, got, 3, "exactly one survivor per source segment")
}
