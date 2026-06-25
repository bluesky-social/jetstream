package oracle

import (
	"context"
	"io"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/bluesky-social/jetstream/internal/simulator/fanout"
	simhttp "github.com/bluesky-social/jetstream/internal/simulator/http"
	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/bluesky-social/jetstream/internal/xrpcapi"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/streaming"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

// synctestBubbleUsed guards against a second synctest bubble in the same
// process; see TestOracle_Synctest for why one bubble per process is required.
var synctestBubbleUsed atomic.Bool

// TestOracle_Synctest runs the ingest+compaction storage path inside a
// testing/synctest bubble with NO sockets: the upstream firehose is fed by
// an in-memory firehoseConn (LiveDial) and all unary HTTP (getRepo/listRepos/
// PLC) is served in-process by handlerTransport. The runtime runs headless
// (no public server). Time is the bubble's fake clock, so the run is free of
// wall-clock skew and completes in microseconds.
//
// This is the deterministic-tier reproducer for the storage-path compaction
// failures (superseded-row-survived / over-drop). It asserts the durable
// on-disk invariants the direct segment observers check.
//
// One bubble per process: the production zstd encoders (overlay/segment/
// subscribe) are package globals whose worker goroutines + channels bind to
// whichever synctest bubble first uses them. A second bubble in the same
// process (go test -count=N>1) then receives on the first bubble's channels
// and the runtime aborts with "receive on synctest channel from outside
// bubble". Re-runs must be separate `go test` invocations; the guard below
// turns the confusing fatal into a clear skip.
//
// nolint:paralleltest // synctest.Test forbids t.Parallel inside the bubble.
func TestOracle_Synctest(t *testing.T) {
	if synctestBubbleUsed.Swap(true) {
		t.Skip("oracle synctest tier must run one bubble per process; " +
			"re-run as a separate `go test` invocation, not -count>1")
	}
	synctest.Test(t, func(t *testing.T) {
		// The synctest fake clock starts at 2000-01-01 UTC, but the
		// simulator stamps commit revs at its logical-clock epoch
		// (~2023-11-14). atmos's verifier rejects a rev whose time is more
		// than 5m in the future, so without advancing the bubble clock past
		// the simulator epoch every event fails verification. Sleep the
		// bubble forward to just after the epoch; all later revs then read as
		// recent-past and verify cleanly.
		advanceClockToSimulatorEpoch()

		const seed = uint64(987654321)
		cfg := Config{
			Mode:                "fast",
			Seed:                seed,
			Accounts:            4,
			MinInitialRecords:   0,
			MaxInitialRecords:   10,
			LiveEventsBootstrap: 12,
			LiveEventsSteady:    12,
			FaultMode:           FaultModeNone,
		}

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
		defer func() { require.NoError(t, w.Close()) }()
		_, err = w.EnsureSeed()
		require.NoError(t, err)
		require.NoError(t, w.Bootstrap(t.Context(), slog.New(slog.NewTextHandler(io.Discard, nil))))
		require.NoError(t, w.AttachRuntime(
			rand.New(rand.NewPCG(cfg.Seed^0xfeedf00d, cfg.Seed^0xc0ffee)),
			fanout.New(4096),
		))

		// In-process simulator HTTP (no socket). The base URL is synthetic;
		// handlerTransport routes by request, ignoring the host.
		const simURL = "http://sim.invalid"
		handler := simhttp.NewHandlerWithOptions(w, simURL, simhttp.HandlerOptions{})

		dataDir := t.TempDir()
		afterBootstrap := newPhaseGate()
		afterMerge := newPhaseGate()
		bootstrapAck := newSeqAck()
		steadyAck := newSeqAck()
		compaction := newCompactionPassRecorder()
		ctx, cancel := context.WithCancel(t.Context())

		bootstrapTraffic := newBootstrapTrafficGenerator(cfg.Accounts, cfg.LiveEventsBootstrap, func(ctx context.Context) (int64, error) {
			if _, err := w.GenerateOneForTest(ctx); err != nil {
				return 0, err
			}
			return w.CurrentSeq(), nil
		})
		bootstrapTraffic.afterBatch = func(ctx context.Context, targetSeq int64) error {
			return bootstrapAck.WaitContiguousFrom(ctx, 1, targetSeq, time.Minute)
		}

		noBackoff := &streaming.BackoffPolicy{
			InitialDelay: gt.Some(time.Millisecond),
			MaxDelay:     gt.Some(time.Millisecond),
			Multiplier:   gt.Some(1.0),
			Jitter:       gt.Some(false),
		}

		rt, err := jetstreamd.Build(ctx, jetstreamd.Options{
			DataDir:                   dataDir,
			RelayURL:                  simURL,
			PLCURL:                    simURL,
			LogLevel:                  "warn",
			LogFormat:                 "text",
			LogOutput:                 testWriter{t: t},
			ShutdownTimeout:           5 * time.Second,
			ClientDrainTimeout:        time.Second,
			BackfillRetryBaseDelay:    time.Millisecond,
			LiveReconnectBackoff:      noBackoff,
			CursorLookback:            36 * time.Hour,
			PlanMaxDIDs:               xrpcapi.DefaultPlanMaxDIDs,
			PlanMaxCollections:        xrpcapi.DefaultPlanMaxCollections,
			PlanMaxEntries:            xrpcapi.DefaultPlanMaxEntries,
			PlanWholeSegmentThreshold: xrpcapi.DefaultPlanWholeSegmentThreshold,
			SubscribeHotTailBytes:     16 << 20,
			SubscribeBlockCacheBytes:  16 << 20,
			SubscribeReadBatch:        1024,
			SubscribeSlowWindow:       time.Second,
			SubscribeSlowMinRate:      1,
			CursorBlockIndexCacheSize: 32,
			CompactionInterval:        time.Hour,
			CompactionTombstoneCap:    1,
			OverlayRebuildInterval:    10 * time.Millisecond,
			Headless:                  true,
			LiveDial:                  inProcessDial(w),
			HTTPTransport:             handlerTransport{handler: handler},
			BarrierAfterBootstrap:     afterBootstrap.Barrier,
			BarrierAfterMerge:         afterMerge.Barrier,
			OnCompactionPass: func(result jetstreamd.CompactionPassResult) {
				compaction.Observe(result)
			},
			OnBootstrapLiveEvent: func(ev *segment.Event) { bootstrapAck.Observe(ev) },
			OnSteadyStateEvent:   func(ev *segment.Event) { steadyAck.Observe(ev) },
			AfterRepoComplete: func(ctx context.Context, did atmos.DID) error {
				return bootstrapTraffic.AfterRepoComplete(ctx, did)
			},
		})
		require.NoError(t, err)

		run := &runtimeRun{exited: make(chan struct{})}
		go func() {
			run.err = rt.Run(ctx)
			close(run.exited)
		}()

		// Always drain the runtime before the bubble function returns. A
		// require failure mid-test calls runtime.Goexit; without this defer
		// the runtime goroutine would still be live and synctest would panic
		// "blocked goroutines remain", masking the real assertion failure.
		var shutdownOnce sync.Once
		shutdown := func() {
			shutdownOnce.Do(func() {
				cancel()
				<-run.exited
				// Close releases the verifier worker pool (whose goroutines
				// exit only on Close, not on Run returning). Every bubble
				// goroutine must exit before the bubble function returns.
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer closeCancel()
				_ = rt.Close(closeCtx)
			})
		}
		defer shutdown()

		// Phase 1: bootstrap. Release the barrier once backfill + bootstrap
		// live traffic have drained (afterBatch waits on contiguous acks).
		waitForBarrierSynctest(t, "after-bootstrap", afterBootstrap, run)
		afterBootstrap.Release()

		// Phase 2: merge cutover.
		waitForBarrierSynctest(t, "after-merge", afterMerge, run)
		afterMerge.Release()

		// Phase 3: steady-state traffic, then a silent-mutation+sync to
		// exercise the resync/tombstone compaction seam.
		steadyStart := w.CurrentSeq()
		for range cfg.LiveEventsSteady {
			_, err := w.GenerateOneForTest(ctx)
			require.NoError(t, err)
		}
		syncIdx := 0
		_, err = w.GenerateSilentMutationThenSyncForTest(ctx, syncIdx)
		require.NoError(t, err)
		target := w.CurrentSeq()
		require.Greater(t, target, steadyStart)

		// Wait until every steady cursor is durably appended. Under the fake
		// clock this resolves as soon as the runtime quiesces.
		require.NoError(t, steadyAck.WaitContiguousFrom(ctx, steadyStart+1, target, time.Minute))

		steadyWatermark := compaction.lastWatermark()

		// Shutdown and wait for the runtime goroutine to exit before reading
		// the durable segments (defer'd shutdown is a no-op after this).
		shutdown()

		// Durable on-disk assertions — the storage tier the triaged failures
		// live in.
		events, err := ObserveSegments(dataDir)
		require.NoError(t, err)
		require.NoError(t, CheckInvariants(events))
		if steadyWatermark > 0 {
			require.NoError(t, CheckCompacted(EventsSortedBySeq(events), steadyWatermark))
		}

		want, err := GroundTruthFromWorld(w)
		require.NoError(t, err)
		got, err := Reconstruct(EventsSortedBySeq(events))
		require.NoError(t, err)
		require.NoError(t, Compare(want, got))
	})
}

// simulatorEpochMicros mirrors the simulator's logical-clock base
// (internal/simulator/world/logical_clock.go). Commit revs are stamped at or
// after this instant.
const simulatorEpochMicros int64 = 1_700_000_000_000_000

// advanceClockToSimulatorEpoch sleeps the synctest bubble clock from its
// 2000-01-01 start to just past the simulator's rev epoch, so verifier
// future-rev checks pass. Sleeping is the synctest-sanctioned way to move the
// fake clock.
func advanceClockToSimulatorEpoch() {
	target := time.UnixMicro(simulatorEpochMicros).Add(time.Hour)
	if d := time.Until(target); d > 0 {
		time.Sleep(d)
	}
}

// waitForBarrierSynctest blocks until the runtime reaches the named phase
// barrier, or fails if the runtime exits first. Both cases are channel
// receives — durably blocking — so the bubble settles and the fake clock
// advances the runtime to the barrier. No spin (a runnable goroutine would
// stall the clock and deadlock the bubble).
func waitForBarrierSynctest(t *testing.T, name string, gate *phaseGate, run *runtimeRun) {
	t.Helper()
	select {
	case <-gate.entered:
	case <-run.exited:
		t.Fatalf("runtime exited before reaching %s barrier: err=%v", name, run.err)
	}
}
