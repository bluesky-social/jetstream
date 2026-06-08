package oracle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/crashpoint"
	"github.com/bluesky-social/jetstream-v2/internal/jetstreamd"
	"github.com/bluesky-social/jetstream-v2/internal/simulator/fanout"
	simhttp "github.com/bluesky-social/jetstream-v2/internal/simulator/http"
	"github.com/bluesky-social/jetstream-v2/internal/simulator/world"
	"github.com/stretchr/testify/require"
)

const (
	envRestartChild      = "JETSTREAM_ORACLE_RESTART_CHILD"
	envRestartDataDir    = "JETSTREAM_ORACLE_RESTART_DATA_DIR"
	envRestartRelayURL   = "JETSTREAM_ORACLE_RESTART_RELAY_URL"
	envRestartMarker     = "JETSTREAM_ORACLE_RESTART_MARKER"
	envRestartMergeDone  = "JETSTREAM_ORACLE_RESTART_MERGE_DONE"
	envRestartCrashPoint = "JETSTREAM_ORACLE_RESTART_CRASH_POINT"
)

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
				Seed:                uint64(101 + i),
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

			srv := newRestartServer(t, w)
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
			})
			require.True(t, wasSIGKILL(first.err), "first child should be killed at %s: err=%v\n%s", tc.point, first.err, first.output)

			second := runRestartChild(t, restartChildArgs{
				dataDir:       dataDir,
				relayURL:      srv.URL,
				mergeDonePath: mergeDonePath,
				timeout:       30 * time.Second,
			})
			require.NoError(t, second.err, "restart child should exit cleanly\n%s", second.output)
			require.FileExists(t, mergeDonePath, "restart child must reach after-merge barrier before exiting")

			assertOracleMatches(t, dataDir, w, cfg, tc.name)
		})
	}
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

	rt, err := jetstreamd.Build(ctx, jetstreamd.Options{
		PublicAddr:                "127.0.0.1:0",
		DebugAddr:                 "127.0.0.1:0",
		DataDir:                   dataDir,
		RelayURL:                  relayURL,
		PLCURL:                    relayURL,
		OTelServiceName:           "jetstream-oracle-restart",
		LogLevel:                  "warn",
		LogFormat:                 "text",
		LogOutput:                 testWriter{t: t},
		ShutdownTimeout:           5 * time.Second,
		ClientDrainTimeout:        time.Second,
		CursorLookback:            36 * time.Hour,
		SegmentCacheMaxAge:        0,
		SubscribeHotTailBytes:     16 << 20,
		SubscribeBlockCacheBytes:  16 << 20,
		SubscribeReadBatch:        1024,
		SubscribeSlowWindow:       time.Second,
		SubscribeSlowMinRate:      1,
		CursorBlockIndexCacheSize: 32,
		BarrierAfterMerge:         afterMerge,
		CrashInjector:             crashInjector,
	})
	require.NoError(t, err)

	runErr := rt.Run(ctx)
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, rt.Close(closeCtx))
	require.True(t,
		runErr == nil || errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded),
		"runtime error: %v", runErr)
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

	return &oracleCrashInjector{
		target:     point,
		markerPath: markerPath,
	}
}

// oracleCrashInjector fires on the FIRST time the target crashpoint is
// reached: it writes the marker file (the parent polls for it, then
// SIGKILLs this child) and blocks until the process is killed. The
// sync.Once makes the marker write exactly-once even though backfill
// invokes SimulateCrash from multiple per-DID worker goroutines
// concurrently.
type oracleCrashInjector struct {
	target     crashpoint.Point
	markerPath string
	once       sync.Once
	writeErr   error
}

func (i *oracleCrashInjector) SimulateCrash(ctx context.Context, point crashpoint.Point) error {
	if point != i.target {
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

func newRestartServer(t *testing.T, w *world.World) *httptest.Server {
	t.Helper()

	ln, err := new(net.ListenConfig).Listen(t.Context(), "tcp4", "127.0.0.1:0")
	require.NoError(t, err)

	srv := httptest.NewUnstartedServer(nil)
	srv.Listener = ln
	srv.Config.Handler = simhttp.NewHandler(w, "http://"+ln.Addr().String())
	srv.Start()
	return srv
}

type restartChildArgs struct {
	dataDir         string
	relayURL        string
	markerPath      string
	mergeDonePath   string
	crashPoint      crashpoint.Point
	killAfterMarker bool
	timeout         time.Duration
}

type restartChildResult struct {
	output string
	err    error
}

func runRestartChild(t *testing.T, args restartChildArgs) restartChildResult {
	t.Helper()

	logPath := filepath.Join(t.TempDir(), "restart-child.log")
	logFile, err := os.Create(logPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, logFile.Close()) }()

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
	require.NoError(t, cmd.Start())

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
		require.NoError(t, waitForMarker(args.markerPath, waitDone, args.timeout, logPath))
		require.NoError(t, cmd.Process.Signal(syscall.SIGKILL))
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
	return restartChildResult{output: string(output), err: waitErr}
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
