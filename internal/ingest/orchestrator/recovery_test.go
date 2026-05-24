package orchestrator

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/lifecycle"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/stretchr/testify/require"
)

// requireNoBootstrapArtifacts asserts that the bootstrap-phase
// live_segments tree was never created during this Run. If runBootstrap
// had been (incorrectly) invoked in a non-bootstrap recovery path, it
// would create <dataDir>/backfill/live_segments and seal at least one
// segment file in it. Absence of that path proves bootstrap was skipped.
func requireNoBootstrapArtifacts(t *testing.T, dataDir string) {
	t.Helper()
	_, err := os.Stat(filepath.Join(dataDir, "backfill", "live_segments"))
	require.True(t, errors.Is(err, os.ErrNotExist),
		"backfill/live_segments must not exist (bootstrap should have been skipped); stat err=%v", err)
}

// TestRun_ResumeFromMerging_AdvancesToSteadyState verifies the
// crash-recovery path where a process died after writing
// phase=merging but before writing phase=steady_state. On restart,
// Run skips bootstrap entirely, runs the merge stub (no-op), writes
// phase=steady_state, and starts the steady-state consumer.
func TestRun_ResumeFromMerging_AdvancesToSteadyState(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	require.NoError(t, lifecycle.WritePhase(st, lifecycle.PhaseMerging))

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

	// Wait until phase has been advanced to steady_state. The
	// steady-state consumer then runs forever; cancel after we
	// observe the transition.
	require.Eventually(t, func() bool {
		got, err := lifecycle.ReadPhase(st)
		return err == nil && got == lifecycle.PhaseSteadyState
	}, 5*time.Second, 20*time.Millisecond, "phase did not advance to steady_state")

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}

	// Bootstrap was skipped: the live_segments tree was never built.
	requireNoBootstrapArtifacts(t, dataDir)
}

// TestRun_StartsCleanInSteadyState verifies that a process started
// against a data dir already at PhaseSteadyState skips bootstrap and
// merging entirely and runs the steady-state consumer until ctx is
// cancelled.
func TestRun_StartsCleanInSteadyState(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	require.NoError(t, lifecycle.WritePhase(st, lifecycle.PhaseSteadyState))

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

	// Cancel as soon as the steady-state consumer has actually
	// reached the relay — that proves Run dispatched into the
	// steady-state path. Bounded fallback timeout in case the
	// consumer never gets there.
	select {
	case <-relay.Subscribed:
	case err := <-done:
		t.Fatalf("Run exited before steady-state consumer subscribed: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("steady-state consumer never subscribed")
	}
	cancel()

	select {
	case err := <-done:
		require.True(t, err == nil || errors.Is(err, context.Canceled),
			"unexpected error: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}

	// Phase remains steady_state.
	got, err := lifecycle.ReadPhase(st)
	require.NoError(t, err)
	require.Equal(t, lifecycle.PhaseSteadyState, got)

	// Bootstrap and merging were skipped: the live_segments tree was
	// never built.
	requireNoBootstrapArtifacts(t, dataDir)
}
