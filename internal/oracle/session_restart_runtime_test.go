package oracle

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/bluesky-social/jetstream/internal/leader"
	"github.com/bluesky-social/jetstream/internal/xrpcapi"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// TestOracle_SessionRestartInProcess ends a writer session with an error
// that wraps leader.ErrRestartSession and asserts the same Runtime.Run starts
// a new session in-process, which recovers from durable state alone and
// converges on the oracle model. This is the local-mode proof of the
// per-session runtime split (design §6.3): nothing a session built may leak
// into the next one, and the per-process readers must follow the new
// session's writer.
//
// nolint:paralleltest
func TestOracle_SessionRestartInProcess(t *testing.T) {
	for i, tc := range sessionRestartCases() {
		t.Run(tc.name, func(t *testing.T) {
			runSessionRestartCase(t, tc, 300+i)
		})
	}
}

type sessionRestartCase struct {
	name string

	// Exactly one of point and steadySyncFault ends the first session.
	point crashpoint.Point
	// steadySyncFault fails the first segment fsync after the first
	// session's steady writer has appended an event, so the session ends
	// with a published writer and a non-durable tail.
	steadySyncFault bool

	preLiveEvents int
	// firstSteadyEvents are generated as the first session enters steady
	// state.
	firstSteadyEvents int
	// restartSteadyEvents are generated once the second session starts; the
	// run stops when that session has archived them. Zero stops the second
	// session at the after-merge barrier instead.
	restartSteadyEvents int
}

func sessionRestartCases() []sessionRestartCase {
	return []sessionRestartCase{
		{
			name:          "after-bootstrap-live-close-before-seal",
			point:         crashpoint.AfterBootstrapLiveCloseBeforeSeal,
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
			name:                "after-steady-phase-before-steady-run",
			point:               crashpoint.AfterSteadyPhaseBeforeSteadyRun,
			preLiveEvents:       4,
			restartSteadyEvents: 3,
		},
		{
			name:                "steady-segment-sync-fault",
			steadySyncFault:     true,
			preLiveEvents:       4,
			firstSteadyEvents:   4,
			restartSteadyEvents: 3,
		},
	}
}

func runSessionRestartCase(t *testing.T, tc sessionRestartCase, seedIdx int) {
	t.Helper()
	cfg := Config{
		Mode:                "session-restart",
		Seed:                restartSeed(seedIdx),
		Accounts:            4,
		MinInitialRecords:   1,
		MaxInitialRecords:   4,
		LiveEventsBootstrap: 4,
		LiveEventsSteady:    4,
	}

	w := newRestartWorld(t, cfg)
	defer func() { require.NoError(t, w.Close()) }()
	if tc.preLiveEvents > 0 {
		generateN(t, w, tc.preLiveEvents)
	}
	srv := newRestartServer(t, w, nil)
	defer srv.Close()

	fs := vfs.NewMem()
	const dataDir = "/data"

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// Delivery gates are per session: a recovering session only observes
	// frames after its durable cursor, and the gates check contiguity from
	// the lowest seq they saw.
	var cutoverGate, steadyGate atomic.Pointer[cutoverDeliveryGate]
	var sessions atomic.Int64
	restarted := make(chan struct{})

	crash := &sessionRestartInjector{target: tc.point}
	syncFault := &steadySyncFault{}
	var crashInjector crashpoint.Injector
	var segmentFaults segment.IOFaultInjector
	if tc.steadySyncFault {
		segmentFaults = syncFault
	} else {
		crashInjector = crash
	}

	var reachedAfterMerge atomic.Bool
	afterMerge := func(context.Context) error {
		if sessions.Load() == 1 {
			// Generated here, the frames reach the first session's steady
			// consumer rather than its bootstrap-live consumer.
			for range tc.firstSteadyEvents {
				if _, err := w.GenerateOneForTest(ctx); err != nil {
					return err
				}
			}
			return nil
		}
		if tc.restartSteadyEvents == 0 {
			reachedAfterMerge.Store(true)
			cancel()
		}
		return nil
	}

	rt, err := jetstreamd.Build(ctx, jetstreamd.Options{
		Headless:                       true,
		DataDir:                        dataDir,
		StorageFS:                      fs,
		RelayURL:                       srv.URL,
		PLCURL:                         srv.URL,
		OTelServiceName:                "jetstream-oracle-session-restart",
		LogLevel:                       "warn",
		LogFormat:                      "text",
		LogOutput:                      testWriter{t: t},
		ShutdownTimeout:                5 * time.Second,
		ClientDrainTimeout:             time.Second,
		CursorLookback:                 36 * time.Hour,
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
		// One event per block, so the steady writer fsyncs per event and the
		// sync fault lands mid-session.
		SteadyMaxEventsPerBlock: 1,
		SessionRestartDelay:     time.Millisecond,
		BarrierBeforeCutover: func(ctx context.Context) error {
			return cutoverGate.Load().waitDelivered(ctx)
		},
		BarrierAfterMerge:      afterMerge,
		CrashInjector:          crashInjector,
		SegmentIOFaultInjector: segmentFaults,
		OnBootstrapLiveEvent: func(ev *segment.Event) {
			cutoverGate.Load().observe(ev)
		},
		OnSteadyStateEvent: func(ev *segment.Event) {
			steadyGate.Load().observe(ev)
			if sessions.Load() == 1 {
				syncFault.armed.Store(true)
			}
		},
		OnSessionStart: func(uint64) {
			// The first session can leave a frame above its durable
			// cursor archived for good; the second session never
			// sees it again.
			prevCutover, prevSteady := cutoverGate.Load(), steadyGate.Load()
			cutover := newCutoverDeliveryGate(srv.URL, 30*time.Second)
			cutover.inherit(prevCutover, prevSteady)
			steady := newCutoverDeliveryGate(srv.URL, 30*time.Second)
			steady.inherit(prevCutover, prevSteady)
			cutoverGate.Store(cutover)
			steadyGate.Store(steady)
			if sessions.Add(1) == 2 {
				close(restarted)
			}
		},
	})
	require.NoError(t, err)

	runDone := make(chan error, 1)
	go func() { runDone <- rt.Run(ctx) }()

	const runTimeout = 60 * time.Second
	if tc.restartSteadyEvents > 0 {
		select {
		case <-restarted:
		case err := <-runDone:
			t.Fatalf("runtime exited before starting a second session: %v", err)
		case <-time.After(runTimeout):
			t.Fatal("no second session started")
		}
		generateN(t, w, tc.restartSteadyEvents)
		require.NoError(t, steadyGate.Load().waitDelivered(ctx))
		cancel()
	}

	var runErr error
	select {
	case runErr = <-runDone:
	case <-time.After(runTimeout):
		cancel()
		t.Fatal("runtime did not stop")
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	closeErr := rt.Close(closeCtx)
	closeCancel()
	require.NoError(t, errors.Join(runErr, closeErr))

	require.True(t, crash.fired.Load() || syncFault.fired.Load(), "the first session was never ended")
	require.EqualValues(t, 2, sessions.Load(), "expected exactly one in-process session restart")
	if tc.restartSteadyEvents == 0 {
		require.True(t, reachedAfterMerge.Load(), "second session did not reach the after-merge barrier")
	}

	assertOracleMatchesFS(t, fs, dataDir, w, cfg, "session-restart-"+tc.name)
}

// sessionRestartInjector ends the session at the first hit of target with a
// restartable error. It does not cancel the process context.
type sessionRestartInjector struct {
	target crashpoint.Point
	fired  atomic.Bool
}

func (i *sessionRestartInjector) SimulateCrash(_ context.Context, point crashpoint.Point) error {
	if point != i.target || !i.fired.CompareAndSwap(false, true) {
		return nil
	}
	return fmt.Errorf("%w: injected at %s", leader.ErrRestartSession, point)
}

// steadySyncFault fails one segment fsync, once armed, with a restartable
// error. The fault fires before the I/O, so nothing reaches disk.
type steadySyncFault struct {
	armed atomic.Bool
	fired atomic.Bool
}

func (f *steadySyncFault) BeforeSegmentIO(_ string, op segment.IOOp) error {
	if op != segment.IOOpSync || !f.armed.Load() || !f.fired.CompareAndSwap(false, true) {
		return nil
	}
	return fmt.Errorf("%w: injected steady segment sync failure", leader.ErrRestartSession)
}
