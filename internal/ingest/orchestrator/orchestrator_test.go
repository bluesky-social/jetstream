package orchestrator

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/stretchr/testify/require"
)

// TestRun_EndToEnd_BootstrapToSteadyState walks the whole
// state machine in one Run. With a fake relay that returns zero
// DIDs, backfill drains immediately and the orchestrator transitions
// bootstrap → merging → steady_state, then runs the steady-state
// consumer until ctx is cancelled.
//
// Asserts:
//   - Phase progresses bootstrap → merging → steady_state.
//   - data/backfill/ is cleaned up by merge phase.
//   - data/segments/ contains at least one active file (the
//     steady-state writer rolled forward from backfill's writer).
//   - Run returns ctx.Err() on cancel.
func TestRun_EndToEnd_BootstrapToSteadyState(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	relay := newFakeRelay(t, nil)
	verifier := newTestVerifier(t, relay.URL())

	o, err := New(Config{
		DataDir:    dataDir,
		Store:      st,
		RelayURL:   relay.URL(),
		HTTPClient: &http.Client{Timeout: 5 * time.Second},
		Directory:  testIdentityDirectory(),
		Verifier:   verifier,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	done := make(chan error, 1)
	go func() { done <- o.Run(ctx) }()

	// Wait for the transition to steady_state.
	require.Eventually(t, func() bool {
		got, err := lifecycle.ReadPhase(st)
		return err == nil && got == lifecycle.PhaseSteadyState
	}, 10*time.Second, 50*time.Millisecond, "phase did not reach steady_state")

	// data/backfill should be cleaned up by merge phase.
	_, err = os.Stat(filepath.Join(dataDir, "backfill"))
	require.True(t, os.IsNotExist(err), "backfill dir should be removed")

	// data/segments should have at least the active file the
	// steady-state writer opened (whether or not events have been
	// appended). Backfill produced no events because the relay
	// returned zero DIDs, so there's exactly one fresh active file.
	mainSegs, err := readSegFiles(filepath.Join(dataDir, "segments"))
	require.NoError(t, err)
	require.NotEmpty(t, mainSegs, "expected at least one main segments file")

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestRun_BarrierAfterBootstrapBlocksBeforeMerge(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	relay := newFakeRelay(t, nil)
	entered := make(chan struct{})
	release := make(chan struct{})

	o, err := New(Config{
		DataDir:    dataDir,
		Store:      st,
		RelayURL:   relay.URL(),
		HTTPClient: &http.Client{Timeout: 5 * time.Second},
		Directory:  testIdentityDirectory(),
		Verifier:   newTestVerifier(t, relay.URL()),
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		BarrierAfterBootstrap: func(ctx context.Context) error {
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- o.Run(ctx) }()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("barrier not reached")
	}
	phase, err := lifecycle.ReadPhase(st)
	require.NoError(t, err)
	require.Equal(t, lifecycle.PhaseMerging, phase)

	close(release)
	require.Eventually(t, func() bool {
		phase, err := lifecycle.ReadPhase(st)
		return err == nil && phase == lifecycle.PhaseSteadyState
	}, 5*time.Second, 20*time.Millisecond)
	cancel()
	<-done
}
