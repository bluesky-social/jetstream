package oracle

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/bluesky-social/jetstream/internal/xrpcapi"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

var errPowerLossInjected = errors.New("oracle: injected strict-FS power loss")

// TestOracle_PowerLossCrashPointsStrictMem runs the real runtime against one
// shared strict in-memory filesystem, simulates a power cut at selected
// enumerated crashpoints by discarding unsynced bytes and dirents, then
// reopens the runtime and asserts convergence through the FS-aware oracle
// observers. This is the in-process sibling of the SIGKILL restart tier:
// SIGKILL preserves the OS page cache, while strict FS reset proves the
// written-vs-synced durability boundary.
//
// nolint:paralleltest
func TestOracle_PowerLossCrashPointsStrictMem(t *testing.T) {
	for i, tc := range strictPowerLossRuntimeCases() {
		t.Run(tc.name, func(t *testing.T) {
			runStrictPowerLossRuntimeCase(t, tc, 50+i)
		})
	}
}

// TestOracle_PowerLossSeededRandomCrashPointsStrictMem adds seeded random
// selection over the runtime strict-FS matrix. The exhaustive test above is
// the main contract; this catches accidental dependence on fixed case order
// and, for repeated crashpoints, covers non-first crash ordinals.
//
// nolint:paralleltest
func TestOracle_PowerLossSeededRandomCrashPointsStrictMem(t *testing.T) {
	rng := rand.New(rand.NewPCG(264, 1))
	cases := strictPowerLossRuntimeCases()
	t.Run("seeded-ordinal-after-repo-complete", func(t *testing.T) {
		tc := cases[0]
		tc.name = "seeded-ordinal-after-repo-complete"
		tc.crashOrdinal = 2 + rng.IntN(2)
		runStrictPowerLossRuntimeCase(t, tc, 200)
	})
	for i := range 4 {
		tc := cases[rng.IntN(len(cases))]
		tc.name = fmt.Sprintf("seeded-%02d-%s", i, tc.name)
		if tc.point == crashpoint.AfterRepoComplete {
			tc.crashOrdinal = 1 + rng.IntN(3)
		}
		t.Run(tc.name, func(t *testing.T) {
			runStrictPowerLossRuntimeCase(t, tc, 201+i)
		})
	}
}

type strictPowerLossRuntimeCase struct {
	name                 string
	point                crashpoint.Point
	crashOrdinal         int
	preLiveEvents        int
	recoverySteadyEvents int
}

func strictPowerLossRuntimeCases() []strictPowerLossRuntimeCase {
	return []strictPowerLossRuntimeCase{
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
			name:          "after-merge-discovery-before-cleanup",
			point:         crashpoint.AfterMergeDiscoveryBeforeCleanup,
			preLiveEvents: 4,
		},
		{
			// End-to-end recovery coverage for a strict-FS power cut at the tail
			// of merge cleanup (after the backfill removal and the durable
			// SyncWrites cursor deletes, before phase=steady_state). Whether this
			// seed keeps any merge survivors depends on the simulator, so the
			// duplicate-on-re-drain regression this window guards is pinned
			// deterministically by
			// orchestrator.TestRunMerge_StrictMemPowerLossCleanupComplete, which
			// forces a survivor. This case verifies the full runtime still
			// converges through the crashpoint.
			name:          "after-merge-cleanup-complete",
			point:         crashpoint.AfterMergeCleanupComplete,
			preLiveEvents: 4,
		},
		{
			name:          "after-bootstrap-live-close-before-seal",
			point:         crashpoint.AfterBootstrapLiveCloseBeforeSeal,
			preLiveEvents: 4,
		},
		{
			name:                 "after-steady-phase-before-steady-run",
			point:                crashpoint.AfterSteadyPhaseBeforeSteadyRun,
			preLiveEvents:        4,
			recoverySteadyEvents: 1,
		},
	}
}

func runStrictPowerLossRuntimeCase(t *testing.T, tc strictPowerLossRuntimeCase, seedIdx int) {
	t.Helper()
	if tc.crashOrdinal == 0 {
		tc.crashOrdinal = 1
	}
	cfg := Config{
		Mode:                "powerloss",
		Seed:                restartSeed(seedIdx),
		Accounts:            4,
		MinInitialRecords:   1,
		MaxInitialRecords:   4,
		LiveEventsBootstrap: 4,
		LiveEventsSteady:    4,
	}
	trace, _, closeTrace := newOracleTrace(t, "powerloss-"+tc.name+".jsonl")
	defer closeTrace()
	recordTraceOrError(t, trace, "run_start", map[string]any{
		"mode":                 cfg.Mode,
		"seed":                 cfg.Seed,
		"go_version":           runtime.Version(),
		"gomaxprocs":           runtime.GOMAXPROCS(0),
		"accounts":             cfg.Accounts,
		"case":                 tc.name,
		"crash_point":          tc.point.String(),
		"crash_ordinal":        tc.crashOrdinal,
		"pre_live_events":      tc.preLiveEvents,
		"strict_storage_power": true,
	})

	w := newRestartWorld(t, cfg)
	defer func() { require.NoError(t, w.Close()) }()
	if tc.preLiveEvents > 0 {
		generateN(t, w, tc.preLiveEvents)
	}
	srv := newRestartServer(t, w, nil)
	defer srv.Close()

	fs := vfs.NewStrictMem()
	syncStrictPowerLossDir(t, fs, "/")
	const dataDir = "/data"

	first := runStrictPowerLossRuntime(t, strictPowerLossRunOptions{
		cfg:          cfg,
		fs:           fs,
		dataDir:      dataDir,
		relayURL:     srv.URL,
		crashPoint:   tc.point,
		crashOrdinal: tc.crashOrdinal,
		trace:        trace,
		label:        "first-" + tc.name,
	})
	require.Truef(t, first.crashed, "first run did not fire crashpoint %s ordinal=%d: err=%v",
		tc.point, tc.crashOrdinal, first.err)
	require.Truef(t,
		first.err == nil || errors.Is(first.err, errPowerLossInjected) || errors.Is(first.err, context.Canceled),
		"first run returned unexpected error at %s ordinal=%d: %v", tc.point, tc.crashOrdinal, first.err)

	fs.ResetToSyncedState()
	fs.SetIgnoreSyncs(false)
	if tc.recoverySteadyEvents > 0 {
		generateN(t, w, tc.recoverySteadyEvents)
	}

	second := runStrictPowerLossRuntime(t, strictPowerLossRunOptions{
		cfg:                      cfg,
		fs:                       fs,
		dataDir:                  dataDir,
		relayURL:                 srv.URL,
		stopAfterMerge:           tc.recoverySteadyEvents == 0,
		stopAfterSteadyEvents:    tc.recoverySteadyEvents,
		trace:                    trace,
		label:                    "second-" + tc.name,
		requireAfterMerge:        tc.recoverySteadyEvents == 0,
		requireAfterSteadyEvents: tc.recoverySteadyEvents,
	})
	require.False(t, second.crashed, "second run must not inject another power loss")
	require.NoErrorf(t, second.err, "second run should exit cleanly after recovery")

	assertOracleMatchesFS(t, fs, dataDir, w, cfg, "powerloss-"+tc.name)
}

type strictPowerLossRunOptions struct {
	cfg      Config
	fs       *vfs.MemFS
	dataDir  string
	relayURL string
	label    string
	trace    *Trace

	crashPoint   crashpoint.Point
	crashOrdinal int

	stopAfterMerge    bool
	requireAfterMerge bool

	stopAfterSteadyEvents    int
	requireAfterSteadyEvents int
}

type strictPowerLossRunResult struct {
	err               error
	crashed           bool
	reachedAfterMerge bool
	steadyEvents      int64
}

func runStrictPowerLossRuntime(t *testing.T, opts strictPowerLossRunOptions) strictPowerLossRunResult {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	gate := newCutoverDeliveryGate(opts.relayURL, 30*time.Second)
	var inj *strictPowerLossInjector
	var crashInjector crashpoint.Injector
	if opts.crashPoint != "" {
		crashOrdinal := opts.crashOrdinal
		if crashOrdinal == 0 {
			crashOrdinal = 1
		}
		inj = &strictPowerLossInjector{
			target:  opts.crashPoint,
			ordinal: crashOrdinal,
			fs:      opts.fs,
			cancel:  cancel,
		}
		crashInjector = inj
	}

	var reachedAfterMerge atomic.Bool
	var steadyEvents atomic.Int64
	var afterMerge jetstreamd.PhaseBarrier
	if opts.stopAfterMerge {
		afterMerge = func(context.Context) error {
			reachedAfterMerge.Store(true)
			cancel()
			return nil
		}
	}

	rt, err := jetstreamd.Build(ctx, jetstreamd.Options{
		Headless:                       true,
		DataDir:                        opts.dataDir,
		StorageFS:                      opts.fs,
		RelayURL:                       opts.relayURL,
		PLCURL:                         opts.relayURL,
		OTelServiceName:                "jetstream-oracle-powerloss",
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
		BarrierBeforeCutover:           gate.waitDelivered,
		BarrierAfterMerge:              afterMerge,
		CrashInjector:                  crashInjector,
		OnBootstrapLiveEvent:           gate.observe,
		OnSteadyStateEvent: func(*segment.Event) {
			if opts.stopAfterSteadyEvents <= 0 {
				return
			}
			if int(steadyEvents.Add(1)) >= opts.stopAfterSteadyEvents {
				cancel()
			}
		},
	})
	require.NoError(t, err)

	runErr := rt.Run(ctx)
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	closeErr := rt.Close(closeCtx)
	closeCancel()
	if closeErr != nil {
		runErr = errors.Join(runErr, closeErr)
	}
	if opts.requireAfterMerge {
		require.True(t, reachedAfterMerge.Load(), "runtime did not reach after-merge barrier")
	}
	if opts.requireAfterSteadyEvents > 0 {
		require.GreaterOrEqual(t, int(steadyEvents.Load()), opts.requireAfterSteadyEvents,
			"runtime did not observe enough steady-state events")
	}

	recordTraceOrError(t, opts.trace, "powerloss_runtime_result", map[string]any{
		"label":               opts.label,
		"err":                 traceErr(runErr),
		"crashed":             inj != nil && inj.fired.Load(),
		"reached_after_merge": reachedAfterMerge.Load(),
		"steady_events":       steadyEvents.Load(),
	})
	return strictPowerLossRunResult{
		err:               runErr,
		crashed:           inj != nil && inj.fired.Load(),
		reachedAfterMerge: reachedAfterMerge.Load(),
		steadyEvents:      steadyEvents.Load(),
	}
}

type strictPowerLossInjector struct {
	target  crashpoint.Point
	ordinal int
	fs      *vfs.MemFS
	cancel  context.CancelFunc

	hits  atomic.Int64
	once  sync.Once
	fired atomic.Bool
}

func (i *strictPowerLossInjector) SimulateCrash(_ context.Context, point crashpoint.Point) error {
	if point != i.target {
		return nil
	}
	if int(i.hits.Add(1)) != i.ordinal {
		return nil
	}
	i.once.Do(func() {
		i.fired.Store(true)
		i.fs.SetIgnoreSyncs(true)
		i.cancel()
	})
	return fmt.Errorf("%w: %s", errPowerLossInjected, point)
}

func assertOracleMatchesFS(t *testing.T, fs vfs.FS, dataDir string, w *world.World, cfg Config, phase string) {
	t.Helper()

	want, err := GroundTruthFromWorld(w)
	require.NoErrorf(t, err, "%s mode=%s seed=%d: build ground truth", phase, cfg.Mode, cfg.Seed)
	events, err := ObserveSegmentsFS(fs, dataDir)
	require.NoErrorf(t, err, "%s mode=%s seed=%d: observe segments", phase, cfg.Mode, cfg.Seed)
	require.NoErrorf(t, CheckInvariants(events), "%s mode=%s seed=%d: check invariants", phase, cfg.Mode, cfg.Seed)
	got, err := Reconstruct(EventsSortedBySeq(events))
	require.NoErrorf(t, err, "%s mode=%s seed=%d: reconstruct observed events", phase, cfg.Mode, cfg.Seed)
	require.NoErrorf(t, Compare(want, got), "%s mode=%s seed=%d: compare oracle model", phase, cfg.Mode, cfg.Seed)

	t.Logf("%s: oracle matched %d observed events in mode=%s seed=%d", phase, len(events), cfg.Mode, cfg.Seed)
}

func syncStrictPowerLossDir(t *testing.T, fs *vfs.MemFS, dir string) {
	t.Helper()
	f, err := fs.OpenDir(dir)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())
}
