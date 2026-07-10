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

	"github.com/bluesky-social/jetstream/internal/simulator/fanout"
	simhttp "github.com/bluesky-social/jetstream/internal/simulator/http"
	"github.com/bluesky-social/jetstream/internal/simulator/world"
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
		t.Skip("heavy test: spawns jetstream subprocess and waits for backfill")
	}
	t.Parallel()

	// Build the simulator world directly. Spawning cmd/simulator as
	// a second subprocess wouldn't add useful coverage and would
	// double startup time.
	cfg := world.DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	cfg.Accounts = 25
	cfg.InitialRecords = 1
	// 50 cps × 25 accounts ≈ 2 cps/DID, well under atmos's per-DID FIFO
	// queue capacity (parallelism*2 = 64 by default). Higher rates have
	// been observed to overflow the per-DID queue during the bootstrap
	// warm-up window — first-touch PLC resolution + CAR fetches for each
	// new DID briefly stalls the verifier worker, the queue fills, and a
	// drop triggers a downstream chain-break/resync that we don't want
	// to fight here. We only read one event below; we don't need volume.
	cfg.CommitsPerSec = 50
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

	stderr := &lockedBuffer{}
	proc := startJetstreamForTest(t, jetCtx, binPath, []string{
		"serve",
		"--addr", jetAddr,
		"--debug-addr", jetDebug,
		"--data-dir", jetDir,
		"--relay-url", simSrv.URL,
		"--plc-url", simSrv.URL,
		"--shutdown-timeout=5s",
	}, stderr)
	defer proc.stop()

	// Wait for jetstream's /subscribe to start serving (backfill
	// drained → steady-state). The handler returns 503 while not
	// in steady-state; a successful websocket dial means we're in.
	waitForJetstreamSubscribeReady(t, proc, jetAddr, stderr, 45*time.Second)

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

	// Steady-state warning sentinels. The simulator is supposed to
	// produce wire-valid #commit frames; jetstream is supposed to
	// recognize every event variant atmos surfaces. Either of these
	// log lines means something upstream is wrong. Pinning them in an
	// E2E test catches regressions that unit tests miss because both
	// involve cross-component behaviour (simulator emits a malformed
	// commit / atmos's verifier resync emits a synthetic event with
	// no public envelope).
	//
	// Caveat: a drop in atmos's per-DID FIFO scheduler is *expected* to
	// surface a downstream chain break on the next event for that DID —
	// the previous rev's data hash no longer matches the new event's
	// prev_data because the linking event was dropped. The verifier's
	// PolicyResync recovers asynchronously, but it logs a "verification
	// failure" for visibility. That's not a regression in jetstream;
	// it's atmos doing exactly what it's designed to do under upstream
	// loss. We tune the simulator above to avoid drops, but under heavy
	// CI contention they can still happen, so we relax the chain-break
	// sentinel iff a drop preceded it. The unknown-event sentinel stays
	// strict — no upstream condition causes it.
	//
	// The drop signal is jetstream's own log message, NOT atmos's raw
	// DropError text ("event dropped: ..."): the live consumer catches
	// the *streaming.DropError and re-logs it under its own message with
	// structured fields (see internal/ingest/live/consumer.go's
	// "verify queue overflow dropped event"). atmos's string never
	// reaches this buffer. Keying off the structured slog `msg` field
	// keeps this explicit — issue #283 was a silent false pass caused by
	// substring-matching a message text that jetstream never emits.
	logs := stderr.String()
	msgs := logMsgSet(logs)
	_, dropOccurred := msgs["verify queue overflow dropped event"]
	if !dropOccurred {
		_, verificationFailed := msgs["verification failure"]
		require.Falsef(t, verificationFailed,
			"jetstream emitted verification failure during E2E run; logs:\n%s", logs)
	}
	_, unknownEventKind := msgs["unknown event kind"]
	require.Falsef(t, unknownEventKind,
		"jetstream emitted unknown event kind during E2E run; logs:\n%s", logs)
}
