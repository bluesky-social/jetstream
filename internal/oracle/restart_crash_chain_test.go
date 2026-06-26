package oracle

import (
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// runChainThroughCrash drives the seed-derived durable-intermediate chain
// through a SIGKILL crash at the given enumerated crashpoint, then recovers
// it with a second merge child that exits cleanly at the after-merge
// barrier. It is the crash-tier analogue of runChainToMergeNoCrash: the
// chainCoordinator (installed as the simulator's OnGetRepoServed hook) and
// the simulator server are owned by the PARENT, so they persist across both
// children — generation happens exactly once on the first child's getRepo
// (sync.Once) and the recovered live_segments carry the chain into the
// second child's re-run merge.
//
// This closes the gap #114 names: TestOracle_RestartCrashPointsDoNotLoseRecords
// wires a nil coordinator, so no durable update/delete/tombstone was ever
// subjected to crash + recovery. Here the chain's tombstones/updates straddle
// the crash boundary.
//
// nolint:paralleltest
func runChainThroughCrash(t *testing.T, label string, seedIdx int, point crashpoint.Point) recoveredChainRun {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping restart oracle under -short")
	}

	cfg := Config{
		Mode:                "restart",
		Seed:                restartSeed(seedIdx),
		Accounts:            4,
		MinInitialRecords:   1,
		MaxInitialRecords:   4,
		LiveEventsBootstrap: 4,
		LiveEventsSteady:    4,
	}
	trace, _, closeTrace := newOracleTrace(t, "restart-chain-crash-"+label+".jsonl")
	t.Cleanup(closeTrace)

	spec := deriveChainSpec(cfg.Seed, cfg.Accounts)
	recordTraceOrError(t, trace, "run_start", map[string]any{
		"mode":          cfg.Mode,
		"seed":          cfg.Seed,
		"go_version":    runtime.Version(),
		"gomaxprocs":    runtime.GOMAXPROCS(0),
		"accounts":      cfg.Accounts,
		"case":          label,
		"crash_point":   point.String(),
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
	markerPath := filepath.Join(markersDir, point.String())
	mergeDonePath := filepath.Join(markersDir, "after-merge")

	// First child: backfill serves the chain DID's getRepo (coordinator
	// generates the chain over the live firehose), the merge drains it
	// durably, then we SIGKILL mid-recovery at the crashpoint.
	first := runRestartChild(t, restartChildArgs{
		dataDir:         dataDir,
		relayURL:        srv.URL,
		markerPath:      markerPath,
		crashPoint:      point,
		killAfterMarker: true,
		timeout:         30 * time.Second,
		trace:           trace,
		runLabel:        "first-" + label,
	})
	recordTraceOrError(t, trace, "restart_child_result", traceRestartChildResult("first", first))
	require.Truef(t, wasSIGKILL(first.err), "first child should be killed at %s: err=%v\n%s", point, first.err, first.output)

	// Second child: re-run the merge idempotently to a clean after-merge
	// exit. The coordinator does NOT regenerate (sync.Once already fired on
	// the first child's getRepo, and the chain ops are durable in the world).
	second := runRestartChild(t, restartChildArgs{
		dataDir:       dataDir,
		relayURL:      srv.URL,
		mergeDonePath: mergeDonePath,
		timeout:       30 * time.Second,
		trace:         trace,
		runLabel:      "second-" + label,
	})
	recordTraceOrError(t, trace, "restart_child_result", traceRestartChildResult("second", second))
	require.NoErrorf(t, second.err, "restart child should exit cleanly\n%s", second.output)
	require.FileExistsf(t, mergeDonePath, "restart child must reach after-merge barrier before exiting")

	return recoveredChainRun{cfg: cfg, spec: spec, w: w, coord: coord, dataDir: dataDir}
}

// maxDurableSeq returns the highest jetstream seq among observed on-disk
// events, or 0 if there are none.
func maxDurableSeq(events []ObservedEvent) uint64 {
	var maxSeq uint64
	for _, ev := range events {
		if ev.Seq > maxSeq {
			maxSeq = ev.Seq
		}
	}
	return maxSeq
}

// TestOracle_RestartChainShapeB_NoStraddleAfterMergeTailCrash makes the
// RESULTS.md "reachability correction" (2026-06-20, commit 8ca4df1) an
// enforced invariant rather than prose.
//
// #114's original premise was a convergence-hiding over-drop on the
// restart/merge-tail crash path: a create <= W independently superseded by a
// delete > W (the "straddle"), where a compaction over-drop is invisible to
// final-state Compare but caught by event-log coverage. That straddle CANNOT
// form in merge-tail: the pass runs at a quiescent point after the merge has
// sealed every segment, so its targetWatermark spans the whole durable stream
// and every durable row sits at or below W. The convergence-hiding property
// is real but lives in the STEADY tier (mutant m025, KILLED@stress), where a
// delete can land in the fresh active segment above the force-rotate
// watermark.
//
// This test crashes shape B (R_bf create@backfill -> live delete) at
// AfterCompactionRewriteBeforeWatermark — the exact crashpoint #114 named —
// recovers, and asserts the no-straddle invariant: every durable row is <= W,
// so no create<=W/delete>W straddle exists on disk. If a future merge-tail
// refactor ever let a durable row outrun the watermark, this goes red and the
// "B-crash is infeasible" claim must be revisited.
//
// nolint:paralleltest
func TestOracle_RestartChainShapeB_NoStraddleAfterMergeTailCrash(t *testing.T) {
	run := runChainThroughCrash(t, "shape-b-nostraddle", 0, crashpoint.AfterCompactionRewriteBeforeWatermark)

	// Final state still converges after the crash.
	assertOracleMatches(t, run.dataDir, run.w, run.cfg, "shape-b-nostraddle")

	// cov.events carries the real on-disk jetstream seqs (cov.got/.want are
	// seq-zeroed for key-based coverage comparison, so seq checks MUST read
	// cov.events, not cov.got).
	cov := chainCoverage(t, run.dataDir, run.coord)
	rc := recordChainForShape(t, run.spec, shapeBfCreateDelete)

	// Anti-vacuity: a merge-tail compaction pass committed a real watermark.
	require.Greater(t, cov.watermark, uint64(0),
		"no-straddle test is vacuous unless a merge-tail watermark was committed")

	// Anti-vacuity: the shape-B live delete tombstone is durable at/below W
	// (the fixture actually landed and the seam under test is exercised), and
	// the backfilled create is compacted away by it (matching no-crash shape B).
	tombstoneDurable := false
	for _, ev := range cov.events {
		if ev.Collection != rc.collection || ev.Rkey != rc.rkey {
			continue
		}
		switch ev.Kind {
		case segment.KindDelete:
			require.LessOrEqualf(t, ev.Seq, cov.watermark,
				"shape-b delete tombstone seq=%d must be <= W=%d", ev.Seq, cov.watermark)
			tombstoneDurable = true
		case segment.KindCreate:
			t.Fatalf("shape-b backfilled create %s/%s must be compacted away, but survived on disk at seq=%d",
				rc.collection, rc.rkey, ev.Seq)
		}
	}
	require.True(t, tombstoneDurable,
		"shape-b live delete tombstone must be durable on disk after crash recovery")

	// The invariant: every durable row is at or below the watermark. No
	// create<=W/delete>W straddle can exist, so the convergence-hiding
	// over-drop #114 contemplated is structurally unreachable here.
	maxSeq := maxDurableSeq(cov.events)
	t.Logf("no-straddle: watermark W=%d maxDurableSeq=%d events=%d", cov.watermark, maxSeq, len(cov.events))
	require.LessOrEqualf(t, maxSeq, cov.watermark,
		"merge-tail no-straddle invariant violated: max durable seq=%d exceeds watermark W=%d "+
			"(a create<=W/delete>W straddle would be reachable — revisit the B-crash infeasibility finding)",
		maxSeq, cov.watermark)
}

// TestOracle_RestartChainCrashConsistency is the real value #114 delivers:
// the durable-intermediate chain (creates, updates, delete tombstones, a
// delete->recreate, the R_bf survival boundary) is subjected to a SIGKILL
// mid-recovery and must survive crash + re-merge intact. The pre-existing
// crash tier (TestOracle_RestartCrashPointsDoNotLoseRecords) wires a nil
// coordinator, so it only ever lands surviving creates — no update/delete/
// tombstone is exposed to a crash. This test closes that gap by running the
// full assertChainDurable bundle (at-least-once coverage, compaction
// contract, no-permanent-tombstone) over the recovered segments.
//
// Crash timing is wall-clock nondeterministic; the assertions are
// seq-agnostic and the coverage comparator is at-least-once (>=), which
// tolerates the benign re-merge duplicate a crash between dst-flush and
// source-commit can produce (plan §6 run-level edge cases).
//
// nolint:paralleltest
func TestOracle_RestartChainCrashConsistency(t *testing.T) {
	run := runChainThroughCrash(t, "crash-consistency", 0, crashpoint.AfterCompactionRewriteBeforeWatermark)

	// Final state converges after crash + recovery.
	assertOracleMatches(t, run.dataDir, run.w, run.cfg, "crash-consistency")

	// The chain's durable intermediates survived the crash: every expected
	// durable row is present at least once, the compaction contract holds at
	// the recovered watermark, and the delete->recreate record is visible.
	assertChainDurable(t, run.dataDir, run.coord, "crash-consistency")

	// Red-first power check: losing the shape-B live delete tombstone across
	// the crash boundary must break coverage. Ties the crash test's power to
	// the actual recovered fixture (mirrors the no-crash shape-B power test).
	rc := recordChainForShape(t, run.spec, shapeBfCreateDelete)
	cov := chainCoverage(t, run.dataDir, run.coord)
	assertCoverageFailsWithoutRow(t, cov, "delete", rc.collection, rc.rkey)
}
