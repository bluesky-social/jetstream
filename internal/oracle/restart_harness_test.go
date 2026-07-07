package oracle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/bluesky-social/jetstream/internal/simulator/fanout"
	simhttp "github.com/bluesky-social/jetstream/internal/simulator/http"
	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/internal/xrpcapi"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

const (
	envRestartChild      = "JETSTREAM_ORACLE_RESTART_CHILD"
	envRestartDataDir    = "JETSTREAM_ORACLE_RESTART_DATA_DIR"
	envRestartRelayURL   = "JETSTREAM_ORACLE_RESTART_RELAY_URL"
	envRestartMarker     = "JETSTREAM_ORACLE_RESTART_MARKER"
	envRestartMergeDone  = "JETSTREAM_ORACLE_RESTART_MERGE_DONE"
	envRestartCrashPoint = "JETSTREAM_ORACLE_RESTART_CRASH_POINT"
	// envRestartCrashOrdinal selects the 1-based occurrence of the crashpoint
	// to kill on (default 1). Lets a predicate kill "between" named crashpoints
	// by occurrence count, e.g. the 3rd AfterRepoComplete.
	envRestartCrashOrdinal                   = "JETSTREAM_ORACLE_RESTART_CRASH_ORDINAL"
	envRestartBootstrapLiveMaxSegmentBytes   = "JETSTREAM_ORACLE_RESTART_BOOTSTRAP_LIVE_MAX_SEGMENT_BYTES"
	envRestartBootstrapLiveMaxEventsPerBlock = "JETSTREAM_ORACLE_RESTART_BOOTSTRAP_LIVE_MAX_EVENTS_PER_BLOCK"

	// Store-fault tier (#30): the child installs a deterministic metadata-store
	// write fault that fails the Ordinal-th batch_commit touching a key under
	// Prefix. This models a Pebble persistence failure (e.g. the merge
	// source-cursor commit) that the orchestrator must surface rather than
	// swallow. When the prefix is set the child runs to natural completion (no
	// SIGKILL): a correct runtime fails the merge LOUD, which the child
	// recognizes via the injected sentinel and records in the observed-marker
	// file; a runtime that swallows the error instead completes the merge and
	// reaches the after-merge barrier, leaving the observed-marker absent.
	envRestartStoreFaultPrefix   = "JETSTREAM_ORACLE_RESTART_STORE_FAULT_PREFIX"
	envRestartStoreFaultOrdinal  = "JETSTREAM_ORACLE_RESTART_STORE_FAULT_ORDINAL"
	envRestartStoreFaultObserved = "JETSTREAM_ORACLE_RESTART_STORE_FAULT_OBSERVED"

	// Segment-fault tier (#200): the child installs a deterministic segment
	// I/O fault (segment.IOFaultInjector) that fails the Ordinal-th
	// occurrence of one I/O op kind (write/sync/rename) across every segment
	// writer plus the compaction-rewrite and import-patch paths. Same
	// fail-loud protocol as the store-fault tier: the child runs to natural
	// completion, and writes the observed-marker IFF the runtime surfaced
	// the injected sentinel through rt.Run; marker absence is the kill.
	envRestartSegmentFaultOp       = "JETSTREAM_ORACLE_RESTART_SEGMENT_FAULT_OP"
	envRestartSegmentFaultOrdinal  = "JETSTREAM_ORACLE_RESTART_SEGMENT_FAULT_ORDINAL"
	envRestartSegmentFaultKind     = "JETSTREAM_ORACLE_RESTART_SEGMENT_FAULT_KIND"
	envRestartSegmentFaultObserved = "JETSTREAM_ORACLE_RESTART_SEGMENT_FAULT_OBSERVED"
)

// errStoreFaultInjected is the sentinel the store-fault tier injects into the
// metadata store. The child matches it with errors.Is to distinguish the
// expected fail-loud merge error (which propagates wrapped up through
// commitSourceComplete -> mergeRunner.run -> Orchestrator.Run) from any other
// runtime error, so the kill signal is unambiguous.
var errStoreFaultInjected = errors.New("oracle: injected store fault (merge cursor commit)")

// errSegmentFaultInjected is the segment-fault tier's sentinel. The injected
// error always wraps it (so the child can recognize the fault via errors.Is
// on rt.Run's error) and, depending on the armed kind, also wraps the
// syscall errno the scenario models — notably syscall.ENOSPC, so the
// disk-full operator-message contract (#201) triggers on the real path.
var errSegmentFaultInjected = errors.New("oracle: injected segment I/O fault")

// nolint:paralleltest
func TestOracle_RestartCrashPointsDoNotLoseRecords(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping restart oracle under -short")
	}

	cases := []struct {
		name          string
		point         crashpoint.Point
		preLiveEvents int
	}{
		{
			name:  "after-repo-complete",
			point: crashpoint.AfterRepoComplete,
		},
		{
			name:          "after-merge-dst-flush-before-source-commit",
			point:         crashpoint.AfterMergeDstFlushBeforeSourceCommit,
			preLiveEvents: 4,
		},
		{
			name:          "after-merge-dst-seal-before-discovery",
			point:         crashpoint.AfterMergeDstSealBeforeDiscovery,
			preLiveEvents: 4,
		},
		{
			name:          "after-bootstrap-live-close-before-seal",
			point:         crashpoint.AfterBootstrapLiveCloseBeforeSeal,
			preLiveEvents: 4,
		},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{
				Mode:                "restart",
				Seed:                restartSeed(i),
				Accounts:            4,
				MinInitialRecords:   1,
				MaxInitialRecords:   4,
				LiveEventsBootstrap: 4,
				LiveEventsSteady:    4,
			}
			trace, _, closeTrace := newOracleTrace(t, "restart-"+tc.name+".jsonl")
			defer closeTrace()
			recordTraceOrError(t, trace, "run_start", map[string]any{
				"mode":                  cfg.Mode,
				"seed":                  cfg.Seed,
				"go_version":            runtime.Version(),
				"gomaxprocs":            runtime.GOMAXPROCS(0),
				"accounts":              cfg.Accounts,
				"min_initial_records":   cfg.MinInitialRecords,
				"max_initial_records":   cfg.MaxInitialRecords,
				"live_events_bootstrap": cfg.LiveEventsBootstrap,
				"live_events_steady":    cfg.LiveEventsSteady,
				"case":                  tc.name,
				"crash_point":           tc.point.String(),
				"pre_live_events":       tc.preLiveEvents,
			})

			w := newRestartWorld(t, cfg)
			defer func() { require.NoError(t, w.Close()) }()
			if tc.preLiveEvents > 0 {
				generateN(t, w, tc.preLiveEvents)
			}
			recordTraceOrError(t, trace, "simulator_config", map[string]any{
				"seed":                cfg.Seed,
				"accounts":            cfg.Accounts,
				"initial_records_min": cfg.MinInitialRecords,
				"initial_records_max": cfg.MaxInitialRecords,
				"firehose_history":    max(10_000, cfg.LiveEventsBootstrap+cfg.LiveEventsSteady+1024),
				"pre_live_events":     tc.preLiveEvents,
				"current_seq":         w.CurrentSeq(),
			})

			srv := newRestartServer(t, w, nil)
			defer srv.Close()

			dataDir := t.TempDir()
			markersDir := t.TempDir()
			markerPath := filepath.Join(markersDir, tc.point.String())
			mergeDonePath := filepath.Join(markersDir, "after-merge")

			first := runRestartChild(t, restartChildArgs{
				dataDir:         dataDir,
				relayURL:        srv.URL,
				markerPath:      markerPath,
				crashPoint:      tc.point,
				killAfterMarker: true,
				timeout:         30 * time.Second,
				trace:           trace,
				runLabel:        "first-" + tc.name,
			})
			recordTraceOrError(t, trace, "restart_child_result", traceRestartChildResult("first", first))
			require.True(t, wasSIGKILL(first.err), "first child should be killed at %s: err=%v\n%s", tc.point, first.err, first.output)

			second := runRestartChild(t, restartChildArgs{
				dataDir:       dataDir,
				relayURL:      srv.URL,
				mergeDonePath: mergeDonePath,
				timeout:       30 * time.Second,
				trace:         trace,
				runLabel:      "second-" + tc.name,
			})
			recordTraceOrError(t, trace, "restart_child_result", traceRestartChildResult("second", second))
			require.NoError(t, second.err, "restart child should exit cleanly\n%s", second.output)
			require.FileExists(t, mergeDonePath, "restart child must reach after-merge barrier before exiting")

			recordTraceOrError(t, trace, "phase", map[string]any{"phase": "restart-final-assertions", "marker": "begin"})
			assertOracleMatches(t, dataDir, w, cfg, tc.name)
			recordTraceOrError(t, trace, "phase", map[string]any{"phase": "restart-final-assertions", "marker": "done"})
		})
	}
}

// restartSeed returns the seed for restart case i. It honors
// JETSTREAM_ORACLE_SEED (so the nightly sweep can vary it), defaulting to
// the historical 101+i for push CI so that remains fixed and
// reproducible. When the env seed is set, cases are offset from it so
// they don't all collide on one seed.
func restartSeed(i int) uint64 {
	if v, ok := os.LookupEnv(envOracleSeed); ok && v != "" {
		var base uint64
		if err := parseUint64Env(func(string) (string, bool) { return v, true }, envOracleSeed, &base); err == nil {
			return base + uint64(i)
		}
	}
	return uint64(101 + i)
}

// TestOracle_RestartChainDurableIntermediates_Baseline is the Group 0
// no-crash baseline (plan §7 step 0e): it proves the getRepo-served timing
// signal lands a seed-derived create/update/delete chain durably on disk
// THROUGH the merge, and that the three post-restart assertions
// (coverage, compaction contract, no-permanent-tombstone) pass when
// nothing is lost. It runs the child to a clean exit at the after-merge
// barrier — NO crash — so any assertion failure is a real mechanism
// defect, not crash-recovery noise. Every later per-shape crash test
// depends on this baseline holding.
//
// recoveredChainRun is the result of driving the chain through one
// no-crash restart child to a clean after-merge exit.
type recoveredChainRun struct {
	cfg     Config
	spec    chainSpec
	w       *world.World
	coord   *chainCoordinator
	dataDir string
	// committedSourceRows carries the normalized rows from a source segment
	// whose merge cursor commit completed before the first child was killed.
	// Multi-source restart tests use it to prove recovery does not reprocess
	// an already-committed source segment.
	committedSourceRows []EventLogRow
}

// runChainToMergeNoCrash builds a restart world, installs the chain
// coordinator, runs ONE child to a clean exit at the after-merge barrier
// (no crash), and returns the recovered run for assertions. The caller
// owns nothing to clean up beyond what t.Cleanup/defers here handle. label
// names the trace + child log.
//
// nolint:paralleltest
func runChainToMergeNoCrash(t *testing.T, label string, seedIdx int) recoveredChainRun {
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
	trace, _, closeTrace := newOracleTrace(t, "restart-chain-"+label+".jsonl")
	t.Cleanup(closeTrace)

	spec := deriveChainSpec(cfg.Seed, cfg.Accounts)
	recordTraceOrError(t, trace, "run_start", map[string]any{
		"mode":          cfg.Mode,
		"seed":          cfg.Seed,
		"go_version":    runtime.Version(),
		"gomaxprocs":    runtime.GOMAXPROCS(0),
		"accounts":      cfg.Accounts,
		"case":          label,
		"chain_did_idx": spec.chainAccountIdx(),
		"chain_records": len(spec.records),
	})

	w := newRestartWorld(t, cfg)
	t.Cleanup(func() { require.NoError(t, w.Close()) })

	coord := newChainCoordinator(t, w, spec)
	srv := newRestartServer(t, w, coord.onGetRepoServed)
	t.Cleanup(srv.Close)

	dataDir := t.TempDir()
	mergeDonePath := filepath.Join(t.TempDir(), "after-merge")

	child := runRestartChild(t, restartChildArgs{
		dataDir:       dataDir,
		relayURL:      srv.URL,
		mergeDonePath: mergeDonePath,
		timeout:       30 * time.Second,
		trace:         trace,
		runLabel:      label,
	})
	recordTraceOrError(t, trace, "restart_child_result", traceRestartChildResult(label, child))
	require.NoErrorf(t, child.err, "%s child should exit cleanly\n%s", label, child.output)
	require.FileExistsf(t, mergeDonePath, "%s child must reach after-merge barrier before exiting", label)

	return recoveredChainRun{cfg: cfg, spec: spec, w: w, coord: coord, dataDir: dataDir}
}

// nolint:paralleltest
func TestOracle_RestartChainDurableIntermediates_Baseline(t *testing.T) {
	run := runChainToMergeNoCrash(t, "baseline", 0)

	// Existing final-state check still holds.
	assertOracleMatches(t, run.dataDir, run.w, run.cfg, "chain-baseline")
	// The chain landed durably and all three new assertions pass.
	assertChainDurable(t, run.dataDir, run.coord, "chain-baseline")
}

// nolint:paralleltest
func TestOracleRestartChild(t *testing.T) {
	if os.Getenv(envRestartChild) != "1" {
		t.Skip("restart child helper only runs under parent harness")
	}

	dataDir := os.Getenv(envRestartDataDir)
	relayURL := os.Getenv(envRestartRelayURL)
	markerPath := os.Getenv(envRestartMarker)
	require.NotEmpty(t, dataDir, "%s is required", envRestartDataDir)
	require.NotEmpty(t, relayURL, "%s is required", envRestartRelayURL)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	crashInjector := newOracleCrashInjectorFromEnv(t, markerPath)
	storeFault := newOracleStoreFaultFromEnv(t)
	segmentFault := newOracleSegmentIOFaultFromEnv(t)
	bootstrapLiveMaxSegmentBytes, bootstrapLiveMaxEventsPerBlock := restartBootstrapLiveLimitsFromEnv(t)
	var afterMerge jetstreamd.PhaseBarrier
	if mergeDonePath := os.Getenv(envRestartMergeDone); mergeDonePath != "" {
		afterMerge = func(context.Context) error {
			if err := os.WriteFile(mergeDonePath, []byte("ok"), 0o644); err != nil {
				return err
			}
			cancel()
			return nil
		}
	}

	// Cross-process cutover gate (#114 flake fix). The parent injects the
	// durable-intermediate chain on the live firehose during this child's
	// backfill (chainCoordinator.onGetRepoServed). Those frames must be
	// durably archived into live_segments BEFORE cutover cancels the
	// bootstrap-live consumer — otherwise an undrained tail is lost and a
	// chain record vanishes from disk while staying in ground truth
	// ("oracle: missing ..."). Production re-fetches such in-flight events
	// from the persisted cursor in steady-state, but the restart child
	// exits at the after-merge barrier and never runs steady-state, so the
	// in-process BarrierBeforeCutover the main harness uses
	// (bootstrapTraffic.WaitDelivered) has no cross-process analogue here
	// today. This gate is that analogue: at cutover it samples the relay's
	// firehose tip and blocks until the bootstrap-live consumer has
	// contiguously archived every frame up to it. The plan authorized this
	// escalation once the no-crash baseline proved flaky (specs/notes/
	// 2026-06-20-restart-tier-intermediates-plan.md §3.1 Q2(b)).
	cutoverGate := newCutoverDeliveryGate(relayURL, 30*time.Second)

	rt, err := jetstreamd.Build(ctx, jetstreamd.Options{
		PublicAddr:                     "127.0.0.1:0",
		DebugAddr:                      "127.0.0.1:0",
		DataDir:                        dataDir,
		RelayURL:                       relayURL,
		PLCURL:                         relayURL,
		OTelServiceName:                "jetstream-oracle-restart",
		LogLevel:                       "warn",
		LogFormat:                      "text",
		LogOutput:                      testWriter{t: t},
		ShutdownTimeout:                5 * time.Second,
		ClientDrainTimeout:             time.Second,
		CursorLookback:                 36 * time.Hour,
		SegmentCacheMaxAge:             0,
		PlanMaxDIDs:                    xrpcapi.DefaultPlanMaxDIDs,
		PlanMaxCollections:             xrpcapi.DefaultPlanMaxCollections,
		PlanMaxEntries:                 xrpcapi.DefaultPlanMaxEntries,
		PlanWholeSegmentThreshold:      xrpcapi.DefaultPlanWholeSegmentThreshold,
		SubscribeReadLogRetentionBytes: 16 << 20,
		SubscribeBlockCacheBytes:       16 << 20,
		SubscribeReadBatch:             1024,
		SubscribeSlowWindow:            time.Second,
		SubscribeSlowMinRate:           1,
		CursorBlockIndexCacheSize:      32,
		CompactionInterval:             time.Hour,
		BootstrapLiveMaxSegmentBytes:   bootstrapLiveMaxSegmentBytes,
		BootstrapLiveMaxEventsPerBlock: bootstrapLiveMaxEventsPerBlock,
		BarrierBeforeCutover:           cutoverGate.waitDelivered,
		BarrierAfterMerge:              afterMerge,
		CrashInjector:                  crashInjector,
		StoreFaultInjector:             storeFault,
		SegmentIOFaultInjector:         segmentFault,
		OnBootstrapLiveEvent:           cutoverGate.observe,
	})
	require.NoError(t, err)

	runErr := rt.Run(ctx)
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, rt.Close(closeCtx))

	// Store-fault tier: when a fault is armed, the contract is that the runtime
	// surfaces it LOUD. The merge error propagates wrapped up through
	// Orchestrator.Run, so errors.Is finds the sentinel. The child does NOT
	// assert here — it RECORDS the outcome and lets the parent judge: it writes
	// the observed-marker IFF it saw the sentinel. Under the m006 mutant the
	// error is swallowed, the merge completes, the after-merge barrier cancels
	// the run, and rt.Run returns context.Canceled (not the sentinel) — so the
	// marker is never written and the parent's require.FileExists fails. Either
	// way the child exits 0, so the kill signal is the marker's absence, not a
	// child crash or a timeout.
	if observedPath := os.Getenv(envRestartStoreFaultObserved); observedPath != "" {
		if errors.Is(runErr, errStoreFaultInjected) {
			require.NoError(t, os.WriteFile(observedPath, []byte(runErr.Error()), 0o644))
		} else {
			t.Logf("store fault armed but runtime did not surface the sentinel (got %v); "+
				"observed-marker withheld — parent will treat this as a swallowed-error kill", runErr)
		}
		return
	}

	// Segment-fault tier (#200): same record-don't-assert protocol as the
	// store-fault tier. The marker carries rt.Run's full error text so the
	// parent can additionally assert message contracts (e.g. the ENOSPC
	// disk-full operator message) without re-plumbing the error.
	if observedPath := os.Getenv(envRestartSegmentFaultObserved); observedPath != "" {
		if runErr != nil && errors.Is(runErr, errSegmentFaultInjected) {
			require.NoError(t, os.WriteFile(observedPath, []byte(runErr.Error()), 0o644))
		} else {
			t.Logf("segment fault armed but runtime did not surface the sentinel (got %v); "+
				"observed-marker withheld — parent will treat this as a swallowed-error kill", runErr)
		}
		return
	}

	require.True(t,
		runErr == nil || errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded),
		"runtime error: %v", runErr)
}

func restartBootstrapLiveLimitsFromEnv(t *testing.T) (int64, int) {
	t.Helper()

	var maxSegmentBytes int64
	if raw := os.Getenv(envRestartBootstrapLiveMaxSegmentBytes); raw != "" {
		require.NoError(t, parseInt64Env(os.LookupEnv, envRestartBootstrapLiveMaxSegmentBytes, &maxSegmentBytes))
		require.Greaterf(t, maxSegmentBytes, int64(0), "%s must be positive", envRestartBootstrapLiveMaxSegmentBytes)
	}

	var maxEventsPerBlock int
	if raw := os.Getenv(envRestartBootstrapLiveMaxEventsPerBlock); raw != "" {
		require.NoError(t, parseIntEnv(os.LookupEnv, envRestartBootstrapLiveMaxEventsPerBlock, &maxEventsPerBlock))
		require.Greaterf(t, maxEventsPerBlock, 0, "%s must be positive", envRestartBootstrapLiveMaxEventsPerBlock)
	}

	return maxSegmentBytes, maxEventsPerBlock
}

// newOracleStoreFaultFromEnv builds the store-fault injector for the child
// from env, or returns nil when no fault is armed (the common case for the
// crash/predicate tiers). The fault fails the Ordinal-th batch_commit that
// touches a key under the configured prefix — the merge source-cursor commit
// rides merge/next_source_idx, the boundary m006 swallows.
func newOracleStoreFaultFromEnv(t *testing.T) store.FaultInjector {
	t.Helper()

	prefix := os.Getenv(envRestartStoreFaultPrefix)
	if prefix == "" {
		return nil
	}
	ordinal := 1
	if raw := os.Getenv(envRestartStoreFaultOrdinal); raw != "" {
		require.NoError(t, parseIntEnv(os.LookupEnv, envRestartStoreFaultOrdinal, &ordinal))
		require.Greaterf(t, ordinal, 0, "%s must be >= 1", envRestartStoreFaultOrdinal)
	}
	return &store.KeyPrefixFault{
		Prefix:  []byte(prefix),
		Op:      store.WriteOpBatchCommit,
		Ordinal: ordinal,
		Err:     errStoreFaultInjected,
	}
}

// oracleSegmentIOFault fails the ordinal-th occurrence of one segment I/O op
// kind across the whole child process (every writer plus Patch/Rewrite),
// mirroring opOrdinalIOFault in segment/writer_test.go. The atomic counter
// makes the ordinal race-safe across backfill worker goroutines; which
// concrete file operation lands on the ordinal may vary run-to-run for
// write/sync (concurrent writers), but the fail-loud contract under test is
// op-agnostic. IOOpRename is deterministic: only Patch/Rewrite rename.
type oracleSegmentIOFault struct {
	op      segment.IOOp
	ordinal int
	err     error
	seen    atomic.Int64
}

func (f *oracleSegmentIOFault) BeforeSegmentIO(_ string, op segment.IOOp) error {
	if op != f.op {
		return nil
	}
	if int(f.seen.Add(1)) == f.ordinal {
		return f.err
	}
	return nil
}

// newOracleSegmentIOFaultFromEnv builds the segment-fault injector for the
// child from env, or returns nil when no fault is armed. The op env value is
// parsed against the segment package's own IOOp constants (the parent sets it
// from the same constants), so there is no cross-package string duplication
// to drift — an unknown op fails the child loudly here instead of arming a
// fault that can never fire (a vacuous kill).
func newOracleSegmentIOFaultFromEnv(t *testing.T) segment.IOFaultInjector {
	t.Helper()

	rawOp := os.Getenv(envRestartSegmentFaultOp)
	if rawOp == "" {
		return nil
	}
	op := segment.IOOp(rawOp)
	switch op {
	case segment.IOOpWrite, segment.IOOpSync, segment.IOOpRename:
	default:
		t.Fatalf("%s: unknown segment I/O op %q", envRestartSegmentFaultOp, rawOp)
	}

	ordinal := 1
	if raw := os.Getenv(envRestartSegmentFaultOrdinal); raw != "" {
		require.NoError(t, parseIntEnv(os.LookupEnv, envRestartSegmentFaultOrdinal, &ordinal))
		require.Greaterf(t, ordinal, 0, "%s must be >= 1", envRestartSegmentFaultOrdinal)
	}

	// The injected error always wraps the tier sentinel (child-side
	// errors.Is recognition) and the errno modelling the scenario, so the
	// production ENOSPC contract (#201 operator message) triggers exactly as
	// it would on a real full disk.
	var errno error
	switch kind := os.Getenv(envRestartSegmentFaultKind); kind {
	case "", "eio":
		errno = syscall.EIO
	case "enospc":
		errno = syscall.ENOSPC
	case "shortwrite":
		errno = io.ErrShortWrite
	default:
		t.Fatalf("%s: unknown fault kind %q", envRestartSegmentFaultKind, kind)
	}

	return &oracleSegmentIOFault{
		op:      op,
		ordinal: ordinal,
		err:     fmt.Errorf("%w: %w", errSegmentFaultInjected, errno),
	}
}

func newOracleCrashInjectorFromEnv(t *testing.T, markerPath string) crashpoint.Injector {
	t.Helper()

	rawPoint := os.Getenv(envRestartCrashPoint)
	if rawPoint == "" {
		return nil
	}
	require.NotEmpty(t, markerPath, "%s is required when %s is set", envRestartMarker, envRestartCrashPoint)

	point, err := crashpoint.Parse(rawPoint)
	require.NoError(t, err)

	// Optional 1-based ordinal: kill on the Nth hit of the target crashpoint
	// rather than the first. Absent/empty => 1 (the historical behavior). This
	// is what lets a seeded predicate kill "between" named crashpoints — e.g.
	// after the 3rd repo completes, not just the 1st — without adding new
	// firing sites (Notes: prefer trace-event-count kill points).
	ordinal := 1
	if raw := os.Getenv(envRestartCrashOrdinal); raw != "" {
		require.NoError(t, parseIntEnv(os.LookupEnv, envRestartCrashOrdinal, &ordinal))
		require.Greaterf(t, ordinal, 0, "%s must be >= 1", envRestartCrashOrdinal)
	}

	return &oracleCrashInjector{
		target:     point,
		ordinal:    ordinal,
		markerPath: markerPath,
	}
}

// oracleCrashInjector fires the kill-marker when the target crashpoint is
// reached for the ordinal-th time: it writes the marker file (the parent polls
// for it, then SIGKILLs this child) and blocks until the process is killed.
// The atomic hit counter + sync.Once makes the marker write exactly-once even
// though backfill invokes SimulateCrash from multiple per-DID worker goroutines
// concurrently; only the goroutine that observes the ordinal-th hit writes the
// marker, and every hit at/after the ordinal blocks so the process is reliably
// killed at the chosen point.
type oracleCrashInjector struct {
	target     crashpoint.Point
	ordinal    int
	markerPath string
	hits       atomic.Int64
	once       sync.Once
	writeErr   error
}

func (i *oracleCrashInjector) SimulateCrash(ctx context.Context, point crashpoint.Point) error {
	if point != i.target {
		return nil
	}

	// Hits before the target ordinal pass through (the lifecycle proceeds);
	// the ordinal-th and any later hit arm the kill and block until SIGKILL.
	if i.hits.Add(1) < int64(i.ordinal) {
		return nil
	}

	i.once.Do(func() {
		i.writeErr = os.WriteFile(i.markerPath, []byte(point.String()), 0o644)
	})
	if i.writeErr != nil {
		return i.writeErr
	}
	<-ctx.Done()
	return ctx.Err()
}

// cutoverDeliveryGate is the cross-process analogue of the main harness's
// bootstrapTraffic.WaitDelivered (#114 flake fix). The parent injects the
// durable-intermediate chain on the live firehose during the child's
// backfill; those frames must be durably archived into live_segments
// before cutover cancels the bootstrap-live consumer, or an undrained tail
// is lost (production re-fetches such in-flight events from the persisted
// cursor in steady-state, but the restart child exits at the after-merge
// barrier and never runs steady-state).
//
// waitDelivered (wired to BarrierBeforeCutover) samples the relay's
// firehose tip once at cutover and blocks until the bootstrap-live consumer
// has contiguously archived every frame up to it. observe (wired to
// OnBootstrapLiveEvent) records each archived frame's upstream seq.
//
// Why contiguity-from-lowest-observed is sound: every world seq.Add stages
// exactly one firehose frame (shape G's silent mutation bumps no seq, so
// there are no gaps), and every generated frame yields at least one archived
// event carrying UpstreamRelayCursor == seq. bootstrap-live runs BatchSize=1
// and fires OnEvent after each durable Append, so observed seqs arrive in
// archive order. A fresh child resumes at cursor 0 and observes 1..tip; a
// child recovering in PhaseBootstrap (e.g. an AfterRepoComplete crash before
// the merging-phase write) resumes at its persisted cursor C and observes
// C+1..tip — frames 1..C are already durable from the first child, so
// flooring contiguity at the lowest observed seq is correct in both cases.
type cutoverDeliveryGate struct {
	relayURL string
	timeout  time.Duration

	mu   sync.Mutex
	seen map[int64]struct{}
}

func newCutoverDeliveryGate(relayURL string, timeout time.Duration) *cutoverDeliveryGate {
	return &cutoverDeliveryGate{
		relayURL: relayURL,
		timeout:  timeout,
		seen:     make(map[int64]struct{}),
	}
}

// observe records one durably-archived bootstrap-live frame's upstream seq.
func (g *cutoverDeliveryGate) observe(ev *segment.Event) {
	if ev == nil || ev.UpstreamRelayCursor <= 0 {
		return
	}
	g.mu.Lock()
	g.seen[ev.UpstreamRelayCursor] = struct{}{}
	g.mu.Unlock()
}

// waitDelivered samples the relay firehose tip and blocks until every frame
// up to it has been contiguously archived by the bootstrap-live consumer, or
// ctx is cancelled, or the timeout elapses. A timeout is a genuine delivery
// failure (the chain never landed durably before cutover) and fails loud
// rather than silently dropping the unmet tail.
func (g *cutoverDeliveryGate) waitDelivered(ctx context.Context) error {
	tip, err := g.fetchTip(ctx)
	if err != nil {
		return fmt.Errorf("cutover gate: fetch firehose tip: %w", err)
	}
	if tip <= 0 {
		// No live ops were generated this run (e.g. the nil-coordinator
		// after-repo-complete case): nothing to wait for.
		return nil
	}

	deadline := time.NewTimer(g.timeout)
	defer deadline.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()

	for {
		if g.contiguousToTip(tip) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			lo, hi := g.contiguousSpan()
			return fmt.Errorf("cutover gate: timeout after %s waiting for bootstrap-live to archive firehose tip=%d "+
				"(lowest_observed=%d highest_contiguous=%d observed=%d): the injected chain did not land durably before cutover",
				g.timeout, tip, lo, hi, g.observedCount())
		case <-tick.C:
		}
	}
}

func (g *cutoverDeliveryGate) fetchTip(ctx context.Context) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, g.relayURL+"/_oracle/firehose-tip", nil)
	if err != nil {
		return 0, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	var out struct {
		Seq int64 `json:"seq"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, err
	}
	return out.Seq, nil
}

// contiguousToTip reports whether every seq from the lowest observed up to
// tip has been archived. With zero observations it is false (keep waiting):
// a fresh consumer always replays from seq 1, so an empty set means the
// consumer simply hasn't delivered yet, not that delivery is complete.
func (g *cutoverDeliveryGate) contiguousToTip(tip int64) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.seen) == 0 {
		return false
	}
	lo := g.lowestLocked()
	for s := lo; s <= tip; s++ {
		if _, ok := g.seen[s]; !ok {
			return false
		}
	}
	return true
}

func (g *cutoverDeliveryGate) contiguousSpan() (lo, hi int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.seen) == 0 {
		return 0, 0
	}
	lo = g.lowestLocked()
	hi = lo - 1
	for {
		if _, ok := g.seen[hi+1]; !ok {
			return lo, hi
		}
		hi++
	}
}

func (g *cutoverDeliveryGate) lowestLocked() int64 {
	lo := int64(-1)
	for s := range g.seen {
		if lo == -1 || s < lo {
			lo = s
		}
	}
	return lo
}

func (g *cutoverDeliveryGate) observedCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.seen)
}

func newRestartWorld(t *testing.T, cfg Config) *world.World {
	t.Helper()

	simCfg := world.DefaultConfig()
	simCfg.DataDir = t.TempDir()
	simCfg.Seed = cfg.Seed
	simCfg.Accounts = cfg.Accounts
	simCfg.InitialRecords = 0
	simCfg.InitialRecordsMin = cfg.MinInitialRecords
	simCfg.InitialRecordsMax = cfg.MaxInitialRecords
	simCfg.FirehoseHistory = max(10_000, cfg.LiveEventsBootstrap+cfg.LiveEventsSteady+1024)

	w, err := world.New(t.Context(), simCfg)
	require.NoError(t, err)
	_, err = w.EnsureSeed()
	require.NoError(t, err)
	require.NoError(t, w.Bootstrap(t.Context(), slog.Default()))
	require.NoError(t, w.AttachRuntime(
		rand.New(rand.NewPCG(cfg.Seed^0xfeedf00d, cfg.Seed^0xc0ffee)),
		fanout.New(4096),
	))
	return w
}

func newRestartServer(t *testing.T, w *world.World, onGetRepoServed func(did string)) *httptest.Server {
	t.Helper()

	ln, err := new(net.ListenConfig).Listen(t.Context(), "tcp4", "127.0.0.1:0")
	require.NoError(t, err)

	srv := httptest.NewUnstartedServer(nil)
	srv.Listener = ln
	srv.Config.Handler = simhttp.NewHandlerWithOptions(w, "http://"+ln.Addr().String(), simhttp.HandlerOptions{
		OnGetRepoServed: onGetRepoServed,
		// The cutover gate (TestOracleRestartChild's BarrierBeforeCutover)
		// queries the firehose tip so a child can hold cutover until its
		// bootstrap-live consumer has durably archived every generated frame.
		EnableFirehoseTip: true,
	})
	srv.Start()
	return srv
}

type restartChildArgs struct {
	dataDir                        string
	relayURL                       string
	markerPath                     string
	mergeDonePath                  string
	crashPoint                     crashpoint.Point
	crashOrdinal                   int // 1-based kill ordinal; 0 == default (1st hit)
	killAfterMarker                bool
	bootstrapLiveMaxSegmentBytes   int64
	bootstrapLiveMaxEventsPerBlock int
	// Store-fault tier (#30): when storeFaultPrefix is set the child arms a
	// metadata-store write fault on the Ordinal-th batch_commit touching the
	// prefix, and writes storeFaultObservedPath if (and only if) the runtime
	// failed loud with the injected sentinel.
	storeFaultPrefix       string
	storeFaultOrdinal      int
	storeFaultObservedPath string
	// Segment-fault tier (#200): when segmentFaultOp is set the child arms a
	// segment I/O fault on the Ordinal-th occurrence of that op kind, and
	// writes segmentFaultObservedPath IFF the runtime failed loud with the
	// injected sentinel. segmentFaultKind selects the wrapped errno:
	// "eio" (default), "enospc", or "shortwrite".
	segmentFaultOp           segment.IOOp
	segmentFaultOrdinal      int
	segmentFaultKind         string
	segmentFaultObservedPath string
	timeout                  time.Duration
	trace                    *Trace
	runLabel                 string
}

type restartChildResult struct {
	output  string
	err     error
	logPath string
}

func runRestartChild(t *testing.T, args restartChildArgs) restartChildResult {
	t.Helper()

	logPath, logFile, closeLog := newOracleArtifactFile(t, args.runLabel+"-restart-child.log")
	defer closeLog()
	// 0 means "unset"; the child treats a missing ordinal as 1.
	ordinal := args.crashOrdinal
	recordTraceOrError(t, args.trace, "restart_child_start", map[string]any{
		"label":                               args.runLabel,
		"log_path":                            logPath,
		"crash_point":                         args.crashPoint.String(),
		"crash_ordinal":                       ordinal,
		"kill_after_marker":                   args.killAfterMarker,
		"marker_path":                         args.markerPath,
		"merge_done_path":                     args.mergeDonePath,
		"bootstrap_live_max_segment_bytes":    args.bootstrapLiveMaxSegmentBytes,
		"bootstrap_live_max_events_per_block": args.bootstrapLiveMaxEventsPerBlock,
	})

	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestOracleRestartChild$", "-test.v")
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Env = append(os.Environ(),
		envRestartChild+"=1",
		envRestartDataDir+"="+args.dataDir,
		envRestartRelayURL+"="+args.relayURL,
		envRestartMarker+"="+args.markerPath,
		envRestartMergeDone+"="+args.mergeDonePath,
		envRestartCrashPoint+"="+args.crashPoint.String(),
	)
	if ordinal > 0 {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%d", envRestartCrashOrdinal, ordinal))
	}
	if args.bootstrapLiveMaxSegmentBytes > 0 {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%d", envRestartBootstrapLiveMaxSegmentBytes, args.bootstrapLiveMaxSegmentBytes))
	}
	if args.bootstrapLiveMaxEventsPerBlock > 0 {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%d", envRestartBootstrapLiveMaxEventsPerBlock, args.bootstrapLiveMaxEventsPerBlock))
	}
	if args.storeFaultPrefix != "" {
		cmd.Env = append(cmd.Env,
			envRestartStoreFaultPrefix+"="+args.storeFaultPrefix,
			envRestartStoreFaultObserved+"="+args.storeFaultObservedPath,
		)
		if args.storeFaultOrdinal > 0 {
			cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%d", envRestartStoreFaultOrdinal, args.storeFaultOrdinal))
		}
	}
	if args.segmentFaultOp != "" {
		cmd.Env = append(cmd.Env,
			envRestartSegmentFaultOp+"="+string(args.segmentFaultOp),
			envRestartSegmentFaultObserved+"="+args.segmentFaultObservedPath,
		)
		if args.segmentFaultOrdinal > 0 {
			cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%d", envRestartSegmentFaultOrdinal, args.segmentFaultOrdinal))
		}
		if args.segmentFaultKind != "" {
			cmd.Env = append(cmd.Env, envRestartSegmentFaultKind+"="+args.segmentFaultKind)
		}
	}
	require.NoError(t, cmd.Start())
	recordTraceOrError(t, args.trace, "restart_child_process", map[string]any{
		"label": args.runLabel,
		"pid":   cmd.Process.Pid,
	})

	waitDone := make(chan error, 1)
	reaped := false
	defer func() {
		if reaped {
			return
		}
		_ = cmd.Process.Kill()
		select {
		case <-waitDone:
		case <-time.After(5 * time.Second):
		}
	}()
	go func() {
		waitDone <- cmd.Wait()
	}()

	if args.killAfterMarker {
		recordTraceOrError(t, args.trace, "restart_marker_wait_start", map[string]any{
			"label":       args.runLabel,
			"marker_path": args.markerPath,
		})
		markerErr := waitForMarker(args.markerPath, waitDone, args.timeout, logPath)
		recordTraceOrError(t, args.trace, "restart_marker_wait_done", map[string]any{
			"label": args.runLabel,
			"err":   traceErr(markerErr),
		})
		require.NoError(t, markerErr)
		signalErr := cmd.Process.Signal(syscall.SIGKILL)
		recordTraceOrError(t, args.trace, "restart_child_signal", map[string]any{
			"label":  args.runLabel,
			"signal": syscall.SIGKILL.String(),
			"err":    traceErr(signalErr),
		})
		require.NoError(t, signalErr)
	}

	var waitErr error
	select {
	case waitErr = <-waitDone:
		reaped = true
	case <-time.After(args.timeout):
		_ = cmd.Process.Kill()
		waitErr = fmt.Errorf("restart child did not exit within %s", args.timeout)
		select {
		case <-waitDone:
			reaped = true
		case <-time.After(5 * time.Second):
		}
	}

	output, readErr := os.ReadFile(logPath)
	require.NoError(t, readErr)
	result := restartChildResult{output: string(output), err: waitErr, logPath: logPath}
	recordTraceOrError(t, args.trace, "restart_child_exit", traceRestartChildResult(args.runLabel, result))
	return result
}

func traceRestartChildResult(label string, result restartChildResult) map[string]any {
	return map[string]any{
		"label":        label,
		"log_path":     result.logPath,
		"output_bytes": len(result.output),
		"err":          traceErr(result.err),
		"was_sigkill":  wasSIGKILL(result.err),
	}
}

func waitForMarker(markerPath string, waitDone <-chan error, timeout time.Duration, logPath string) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()

	for {
		if _, err := os.Stat(markerPath); err == nil {
			return nil
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("stat restart marker %s: %w", markerPath, err)
		}

		select {
		case err := <-waitDone:
			output, readErr := os.ReadFile(logPath)
			if readErr != nil {
				if err != nil {
					return fmt.Errorf("restart child exited before marker: %w; read log: %w", err, readErr)
				}
				return fmt.Errorf("restart child exited before marker without error; read log: %w", readErr)
			}
			if err != nil {
				return fmt.Errorf("restart child exited before marker: %w\n%s", err, output)
			}
			return fmt.Errorf("restart child exited before marker without error\n%s", output)
		case <-deadline.C:
			output, readErr := os.ReadFile(logPath)
			if readErr != nil {
				return fmt.Errorf("restart marker %s not created within %s; read log: %w", markerPath, timeout, readErr)
			}
			return fmt.Errorf("restart marker %s not created within %s\n%s", markerPath, timeout, output)
		case <-tick.C:
		}
	}
}

func wasSIGKILL(err error) bool {
	if err == nil {
		return false
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return false
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	return ok && status.Signaled() && status.Signal() == syscall.SIGKILL
}
