package oracle

import (
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// mergeNextSourceIdxStoreKey mirrors the orchestrator's unexported
// mergeNextSourceIdxKey ("merge/next_source_idx", merge_cursor.go). It is the
// pebble key the source-cursor batch commit rides — the boundary m006
// swallows. Duplicated here (not imported) because it is package-private to
// orchestrator; if that key ever changes, this test's fault stops matching and
// the kill goes vacuous, so the two must stay in sync. A guard test in the
// orchestrator package (TestMergeNextSourceIdxKeyMatchesOracleStoreFault)
// pins them together.
const mergeNextSourceIdxStoreKey = "merge/next_source_idx"

// TestOracle_RestartStoreFaultOnMergeCursor_FailsLoudThenRecovers is the
// oracle-level store-fault tier for issue #30, and the gate-recognized kill
// for mutation m006 (merge_commit_error_swallowed).
//
// The first child runs a real jetstreamd through backfill into the merge with
// a deterministic metadata-store fault armed on the merge/next_source_idx
// batch commit — the exact persistence op the m006 mutant swallows. The
// contract under test (issue #30): the runtime must FAIL LOUD when that commit
// fails, never silently advance past unarchived data. The child asserts the
// runtime surfaced the injected sentinel (errors.Is up through
// commitSourceComplete -> mergeRunner.run -> Orchestrator.Run) and writes the
// observed-marker; the parent requires that marker.
//
//   - Correct code: commit error propagates, runMerge fails, rt.Run returns the
//     sentinel, observed-marker written. PASS.
//   - m006 mutant: inverted check swallows the error, merge completes, the
//     after-merge barrier fires, rt.Run returns context.Canceled — the sentinel
//     is NEVER observed, so the observed-marker is absent. The parent's
//     require.FileExists fails. KILL.
//
// A second, fault-free child then re-runs the merge idempotently to a clean
// after-merge exit, and the full chain-durability bundle asserts the data the
// faulted merge could not commit is not lost — it converges on recovery. This
// is the "transient persistence failure must be survivable, not silently
// dropped" half of the contract.
//
// nolint:paralleltest
func TestOracle_RestartStoreFaultOnMergeCursor_FailsLoudThenRecovers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping restart oracle under -short")
	}

	cfg := Config{
		Mode:                "restart",
		Seed:                restartSeed(0),
		Accounts:            4,
		MinInitialRecords:   1,
		MaxInitialRecords:   4,
		LiveEventsBootstrap: 4,
		LiveEventsSteady:    4,
	}
	trace, _, closeTrace := newOracleTrace(t, "restart-storefault-merge-cursor.jsonl")
	t.Cleanup(closeTrace)

	spec := deriveChainSpec(cfg.Seed, cfg.Accounts)
	recordTraceOrError(t, trace, "run_start", map[string]any{
		"mode":          cfg.Mode,
		"seed":          cfg.Seed,
		"go_version":    runtime.Version(),
		"gomaxprocs":    runtime.GOMAXPROCS(0),
		"accounts":      cfg.Accounts,
		"case":          "storefault-merge-cursor",
		"store_fault":   mergeNextSourceIdxStoreKey,
		"chain_did_idx": spec.chainAccountIdx(),
		"chain_records": len(spec.records),
	})

	w := newRestartWorld(t, cfg)
	t.Cleanup(func() { require.NoError(t, w.Close()) })

	coord := newChainCoordinator(t, w, spec)
	srv := newRestartServer(t, w, coord.onGetRepoServed)
	t.Cleanup(srv.Close)

	dataDir := t.TempDir()
	markersDir := t.TempDir()
	observedPath := filepath.Join(markersDir, "store-fault-observed")
	firstMergeDonePath := filepath.Join(markersDir, "first-after-merge")
	mergeDonePath := filepath.Join(markersDir, "after-merge")

	// First child: backfill serves the chain, the merge drains it, and the
	// FIRST source-cursor batch commit fails. Correct code surfaces it loud
	// (sentinel observed via errors.Is); the m006 mutant swallows it and
	// reaches the after-merge barrier instead, leaving the observed-marker
	// absent. The after-merge barrier is armed so the mutant path cancels and
	// the child exits promptly (a clean fast kill) rather than running on in
	// steady state until the timeout; correct code never reaches that barrier.
	first := runRestartChild(t, restartChildArgs{
		dataDir:                dataDir,
		relayURL:               srv.URL,
		mergeDonePath:          firstMergeDonePath,
		storeFaultPrefix:       mergeNextSourceIdxStoreKey,
		storeFaultOrdinal:      1,
		storeFaultObservedPath: observedPath,
		timeout:                30 * time.Second,
		trace:                  trace,
		runLabel:               "first-storefault",
	})
	recordTraceOrError(t, trace, "restart_child_result", traceRestartChildResult("first", first))
	require.NoErrorf(t, first.err, "first store-fault child should exit cleanly (fail-loud is in-process, not a crash)\n%s", first.output)
	require.FileExistsf(t, observedPath,
		"runtime must FAIL LOUD on the merge-cursor commit failure (m006 swallows it and reaches after-merge instead)\n%s",
		first.output)
	// Anti-vacuity: under correct code the merge must NOT have completed (the
	// fault aborts it), so the first child's after-merge barrier never fired.
	// If this marker exists, the merge completed despite the armed fault — the
	// fault did not bite, and the kill would be vacuous.
	require.NoFileExistsf(t, firstMergeDonePath,
		"merge completed despite the armed cursor-commit fault — fault did not bite, kill is vacuous\n%s",
		first.output)

	// Second child: no fault armed. The merge re-runs idempotently from the
	// un-advanced cursor (the failed commit never persisted), drains the same
	// sources, and exits cleanly at the after-merge barrier.
	second := runRestartChild(t, restartChildArgs{
		dataDir:       dataDir,
		relayURL:      srv.URL,
		mergeDonePath: mergeDonePath,
		timeout:       30 * time.Second,
		trace:         trace,
		runLabel:      "second-storefault",
	})
	recordTraceOrError(t, trace, "restart_child_result", traceRestartChildResult("second", second))
	require.NoErrorf(t, second.err, "recovery child should exit cleanly\n%s", second.output)
	require.FileExistsf(t, mergeDonePath, "recovery child must reach after-merge barrier before exiting")

	// Convergence: the data the faulted merge could not commit is NOT lost.
	// Final state matches the simulator (replay-aware: re-running the merge can
	// re-emit survivors at fresh seqs), and the durable chain survived intact.
	assertOracleMatchesAfterReplay(t, dataDir, w, cfg, "storefault-merge-cursor")
	assertChainDurable(t, dataDir, coord, "storefault-merge-cursor")
}
