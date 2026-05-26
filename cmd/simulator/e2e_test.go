package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"log/slog"
	"math/rand/v2"

	"github.com/bluesky-social/jetstream-v2/internal/simulator/fanout"
	simhttp "github.com/bluesky-social/jetstream-v2/internal/simulator/http"
	"github.com/bluesky-social/jetstream-v2/internal/simulator/world"
	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

// TestEndToEnd_JetstreamConsumesSimulator boots the simulator as an
// httptest.Server, spawns jetstream as a subprocess pointed at it,
// then connects a websocket client to jetstream's /subscribe.
//
// Heavy test (subprocess + backfill drain): skipped under -short.
func TestEndToEnd_JetstreamConsumesSimulator(t *testing.T) {
	if testing.Short() {
		t.Skip("end-to-end test: subprocess + backfill drain (~10s); run via just test-long")
	}
	t.Parallel()

	// Build the simulator world directly. Spawning cmd/simulator as
	// a second subprocess wouldn't add useful coverage and would
	// double startup time.
	cfg := world.DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	cfg.Accounts = 25
	cfg.InitialRecords = 1
	cfg.CommitsPerSec = 200 // crank up so events arrive fast
	w, err := world.New(context.Background(), cfg)
	require.NoError(t, err)
	_, err = w.EnsureSeed()
	require.NoError(t, err)
	require.NoError(t, w.Bootstrap(context.Background(), slog.Default()))
	require.NoError(t, w.AttachRuntime(rand.New(rand.NewPCG(99, 100)), fanout.New(64)))

	simSrv := httptest.NewServer(nil)
	simSrv.Config.Handler = simhttp.NewHandler(w, simSrv.URL)
	defer simSrv.Close()

	// Run live traffic concurrently with the test. Cancel traffic
	// before closing the world to avoid "pebble: closed" panics.
	trafficCtx, trafficCancel := context.WithCancel(context.Background())
	trafficDone := make(chan struct{})
	defer func() {
		trafficCancel()
		<-trafficDone // wait for traffic goroutine to exit
		_ = w.Close()
	}()
	go func() {
		_ = w.RunTraffic(trafficCtx, slog.Default())
		close(trafficDone)
	}()

	// Spawn jetstream pointed at the simulator. Build first so the
	// startup latency is bounded (go run cold-starts the compiler
	// every invocation).
	binPath := buildJetstreamForTest(t)

	jetDir := filepath.Join(t.TempDir(), "jetstream-data")

	jetCtx, jetCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer jetCancel()

	jetAddr := freePortAddr(t)
	jetDebug := freePortAddr(t)

	cmd := newJetstreamCmd(jetCtx, binPath, []string{
		"serve",
		"--addr", jetAddr,
		"--debug-addr", jetDebug,
		"--data-dir", jetDir,
		"--relay-url", simSrv.URL,
		"--plc-url", simSrv.URL,
		"--shutdown-timeout=5s",
	})
	stderr := &lockedBuffer{}
	cmd.Stdout = stderr
	cmd.Stderr = stderr
	require.NoError(t, cmd.Start())
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	// Wait for jetstream's /subscribe to start serving (backfill
	// drained → steady-state). The handler returns 503 while not
	// in steady-state; a successful websocket dial means we're in.
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		conn, resp, err := websocket.Dial(ctx, "ws://"+jetAddr+"/subscribe", nil)
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		if err != nil {
			return false
		}
		_ = conn.Close(websocket.StatusNormalClosure, "probe")
		return true
	}, 45*time.Second, 250*time.Millisecond,
		"jetstream did not become ready; logs:\n%s", stderr.String())

	// Now consume one live event.
	dialCtx, dialCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer dialCancel()
	conn, resp, err := websocket.Dial(dialCtx, "ws://"+jetAddr+"/subscribe", nil)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	require.NoError(t, err)
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	_, msg, err := conn.Read(dialCtx)
	require.NoError(t, err)
	// Jetstream's /subscribe emits JSON envelopes; just confirm we
	// got something parseable, and that it carries a DID. Detailed
	// shape coverage lives in the internal/subscribe package's
	// own tests.
	require.True(t, json.Valid(msg), "unexpected non-JSON frame: %q", string(msg))
	require.Contains(t, string(msg), `"did":"did:plc:`,
		"expected DID in payload, got: %s", string(msg))
}
