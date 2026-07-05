package oracle

import (
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/ingest"
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
	return runChainThroughCrashAt(t, label, seedIdx, point, 1)
}

// runChainThroughCrashAt is runChainThroughCrash with an explicit 1-based kill
// ordinal: the first child is SIGKILLed on the ordinal-th hit of point rather
// than the first. ordinal=1 is the original behavior. This is the mechanism
// behind the predicate-driven kill tier (#29): a seeded predicate chooses
// (point, ordinal) so a crash can land "between" named crashpoints by
// occurrence count (e.g. after the 3rd repo completes).
//
// nolint:paralleltest
func runChainThroughCrashAt(t *testing.T, label string, seedIdx int, point crashpoint.Point, ordinal int) recoveredChainRun {
	t.Helper()
	return runChainThroughCrashWithOptions(t, restartChainCrashOptions{
		label:               label,
		seedIdx:             seedIdx,
		point:               point,
		ordinal:             ordinal,
		accounts:            4,
		minInitialRecords:   1,
		maxInitialRecords:   4,
		liveEventsBootstrap: 4,
		liveEventsSteady:    4,
	})
}

type restartChainCrashOptions struct {
	label   string
	seedIdx int
	point   crashpoint.Point
	ordinal int

	accounts            int
	minInitialRecords   int
	maxInitialRecords   int
	liveEventsBootstrap int
	liveEventsSteady    int

	bootstrapLiveMaxSegmentBytes   int64
	bootstrapLiveMaxEventsPerBlock int
	minMergeSourceSegments         int
	captureCommittedSourceRows     bool
}

func (o restartChainCrashOptions) resolved() restartChainCrashOptions {
	if o.accounts == 0 {
		o.accounts = 4
	}
	if o.minInitialRecords == 0 {
		o.minInitialRecords = 1
	}
	if o.maxInitialRecords == 0 {
		o.maxInitialRecords = 4
	}
	if o.liveEventsBootstrap == 0 {
		o.liveEventsBootstrap = 4
	}
	if o.liveEventsSteady == 0 {
		o.liveEventsSteady = 4
	}
	if o.ordinal == 0 {
		o.ordinal = 1
	}
	return o
}

// runChainThroughCrashWithOptions is the configurable implementation behind
// the restart-chain crash helpers. Most callers use runChainThroughCrashAt's
// defaults; the multi-source m003 tier overrides the bootstrap-live writer
// sizing so every accepted live event rotates into its own merge source.
//
// nolint:paralleltest
func runChainThroughCrashWithOptions(t *testing.T, opts restartChainCrashOptions) recoveredChainRun {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping restart oracle under -short")
	}
	opts = opts.resolved()

	cfg := Config{
		Mode:                "restart",
		Seed:                restartSeed(opts.seedIdx),
		Accounts:            opts.accounts,
		MinInitialRecords:   opts.minInitialRecords,
		MaxInitialRecords:   opts.maxInitialRecords,
		LiveEventsBootstrap: opts.liveEventsBootstrap,
		LiveEventsSteady:    opts.liveEventsSteady,
	}
	trace, _, closeTrace := newOracleTrace(t, "restart-chain-crash-"+opts.label+".jsonl")
	t.Cleanup(closeTrace)

	spec := deriveChainSpec(cfg.Seed, cfg.Accounts)
	recordTraceOrError(t, trace, "run_start", map[string]any{
		"mode":                                cfg.Mode,
		"seed":                                cfg.Seed,
		"go_version":                          runtime.Version(),
		"gomaxprocs":                          runtime.GOMAXPROCS(0),
		"accounts":                            cfg.Accounts,
		"case":                                opts.label,
		"crash_point":                         opts.point.String(),
		"crash_ordinal":                       opts.ordinal,
		"chain_did_idx":                       spec.chainAccountIdx(),
		"chain_records":                       len(spec.records),
		"bootstrap_live_max_segment_bytes":    opts.bootstrapLiveMaxSegmentBytes,
		"bootstrap_live_max_events_per_block": opts.bootstrapLiveMaxEventsPerBlock,
		"min_merge_source_segments":           opts.minMergeSourceSegments,
	})

	w := newRestartWorld(t, cfg)
	t.Cleanup(func() { require.NoError(t, w.Close()) })

	coord := newChainCoordinator(t, w, spec)
	srv := newRestartServer(t, w, coord.onGetRepoServed)
	t.Cleanup(srv.Close)

	dataDir := t.TempDir()
	markersDir := t.TempDir()
	markerPath := filepath.Join(markersDir, opts.point.String())
	mergeDonePath := filepath.Join(markersDir, "after-merge")

	// First child: backfill serves the chain DID's getRepo (coordinator
	// generates the chain over the live firehose), the merge drains it
	// durably, then we SIGKILL mid-recovery at the crashpoint.
	first := runRestartChild(t, restartChildArgs{
		dataDir:                        dataDir,
		relayURL:                       srv.URL,
		markerPath:                     markerPath,
		crashPoint:                     opts.point,
		crashOrdinal:                   opts.ordinal,
		killAfterMarker:                true,
		timeout:                        30 * time.Second,
		trace:                          trace,
		runLabel:                       "first-" + opts.label,
		bootstrapLiveMaxSegmentBytes:   opts.bootstrapLiveMaxSegmentBytes,
		bootstrapLiveMaxEventsPerBlock: opts.bootstrapLiveMaxEventsPerBlock,
	})
	recordTraceOrError(t, trace, "restart_child_result", traceRestartChildResult("first", first))
	require.Truef(t, wasSIGKILL(first.err), "first child should be killed at %s ordinal=%d: err=%v\n%s", opts.point, opts.ordinal, first.err, first.output)

	var committedSourceRows []EventLogRow
	if opts.minMergeSourceSegments > 0 || opts.captureCommittedSourceRows {
		files := requireMergeSourceSegments(t, dataDir, opts.minMergeSourceSegments)
		recordTraceOrError(t, trace, "restart_source_segments_after_kill", map[string]any{
			"count": len(files),
		})
		if opts.captureCommittedSourceRows {
			committedIdx := opts.ordinal - 2
			require.GreaterOrEqualf(t, committedIdx, 0,
				"cannot capture committed source rows for ordinal %d: no source committed before crash", opts.ordinal)
			require.Lessf(t, committedIdx, len(files),
				"committed source index %d out of range for %d source segments", committedIdx, len(files))
			committedSourceRows = readSourceRowsForNoReprocessCheck(t, files[committedIdx].Path)
			recordTraceOrError(t, trace, "restart_committed_source_rows_after_kill", map[string]any{
				"source_idx": committedIdx,
				"rows":       len(committedSourceRows),
			})
		}
	}

	// Second child: re-run the merge idempotently to a clean after-merge
	// exit. The coordinator does NOT regenerate (sync.Once already fired on
	// the first child's getRepo, and the chain ops are durable in the world).
	second := runRestartChild(t, restartChildArgs{
		dataDir:                        dataDir,
		relayURL:                       srv.URL,
		mergeDonePath:                  mergeDonePath,
		timeout:                        30 * time.Second,
		trace:                          trace,
		runLabel:                       "second-" + opts.label,
		bootstrapLiveMaxSegmentBytes:   opts.bootstrapLiveMaxSegmentBytes,
		bootstrapLiveMaxEventsPerBlock: opts.bootstrapLiveMaxEventsPerBlock,
	})
	recordTraceOrError(t, trace, "restart_child_result", traceRestartChildResult("second", second))
	require.NoErrorf(t, second.err, "restart child should exit cleanly\n%s", second.output)
	require.FileExistsf(t, mergeDonePath, "restart child must reach after-merge barrier before exiting")

	return recoveredChainRun{cfg: cfg, spec: spec, w: w, coord: coord, dataDir: dataDir, committedSourceRows: committedSourceRows}
}

func requireMergeSourceSegments(t *testing.T, dataDir string, minCount int) []ingest.SegmentFile {
	t.Helper()

	files, err := ingest.SegmentFiles(filepath.Join(dataDir, "backfill", "live_segments"))
	require.NoError(t, err, "list merge source segments after restart child kill")
	require.GreaterOrEqualf(t, len(files), minCount,
		"restart fixture must produce at least %d merge source segments", minCount)
	return files
}

func readSourceRowsForNoReprocessCheck(t *testing.T, path string) []EventLogRow {
	t.Helper()

	events, err := observeSealedSegment(path)
	require.NoErrorf(t, err, "read committed merge source %s", path)
	require.NotEmptyf(t, events, "committed merge source %s must contain rows", path)
	return zeroRowSeqs(NormalizeEventLog(events))
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
// is real but lives in the STEADY tier, where a delete can land in the fresh
// active segment above the force-rotate watermark; it is NOT reachable from
// this merge-tail crash. That steady-tier over-drop was historically modeled by
// mutant m025 (KILLED@stress), but m025 was RETIRED in #178 when its
// Set.SnapshotRange mechanism was deleted — the on-disk windowed fold can no
// longer reach the above-watermark over-drop — so the property currently has no
// live mutant; re-deriving a dedicated #100 over-drop recorder is an open gap
// tracked by #183 (see specs/oracle.md "Crash And Restart Tier" and
// testing/mutation/RESULTS.md).
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

	// Final state still converges after the crash (replay-aware: a recovered
	// merge boundary may re-emit survivors out of per-DID rev order).
	assertOracleMatchesAfterReplay(t, run.dataDir, run.w, run.cfg, "shape-b-nostraddle")

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
// It crashes the chain at EVERY enumerated crashpoint (#113 "cover all crash
// points"), so a durable update/delete/tombstone is exposed to a crash at each
// merge/backfill/compaction seam, not just one. The crashpoint only affects
// the FIRST child; the second child always re-runs the merge idempotently to a
// clean after-merge exit, and the chain ops live in the parent-owned world, so
// recovery converges regardless of where the first child died.
//
// Crash timing is wall-clock nondeterministic; the assertions are
// seq-agnostic and the coverage comparator is at-least-once (>=), which
// tolerates the benign re-merge duplicate a crash between dst-flush and
// source-commit can produce (plan §6 run-level edge cases).
//
// nolint:paralleltest
func TestOracle_RestartChainCrashConsistency(t *testing.T) {
	// Every crashpoint the restart child can be killed at, spanning the
	// backfill-complete, merge (flush/seal/discovery), bootstrap-live close,
	// and mid-compaction-rewrite seams. Mirrors the crashpoint set the nil-
	// coordinator tier (TestOracle_RestartCrashPointsDoNotLoseRecords) uses,
	// plus the compaction-rewrite seam, now with the chain wired in.
	crashPoints := []crashpoint.Point{
		crashpoint.AfterRepoComplete,
		crashpoint.AfterMergeDstFlushBeforeSourceCommit,
		crashpoint.AfterMergeDstSealBeforeDiscovery,
		crashpoint.AfterBootstrapLiveCloseBeforeSeal,
		crashpoint.AfterCompactionRewriteBeforeWatermark,
	}

	for i, point := range crashPoints {
		t.Run(point.String(), func(t *testing.T) {
			label := "crash-consistency-" + point.String()
			run := runChainThroughCrash(t, label, i, point)

			// Final state converges after crash + recovery (replay-aware: a
			// crash at a merge boundary may re-emit already-merged survivors
			// at fresh seqs in non-rev order — benign per the at-least-once
			// contract; Compare still converges).
			assertOracleMatchesAfterReplay(t, run.dataDir, run.w, run.cfg, label)

			// The chain's durable intermediates survived the crash: every
			// expected durable row is present at least once, the compaction
			// contract holds at the recovered watermark, and the
			// delete->recreate record is visible.
			assertChainDurable(t, run.dataDir, run.coord, label)

			// Red-first power check: losing the shape-B live delete tombstone
			// across the crash boundary must break coverage. Ties the crash
			// test's power to the actual recovered fixture at THIS crashpoint
			// (mirrors the no-crash shape-B power test).
			rc := recordChainForShape(t, run.spec, shapeBfCreateDelete)
			cov := chainCoverage(t, run.dataDir, run.coord)
			assertCoverageFailsWithoutRow(t, cov, "delete", rc.collection, rc.rkey)
		})
	}
}
