package orchestrator

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/lifecycle"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/stretchr/testify/require"
)

// TestRunBootstrap_DrainsAndAdvancesPhase verifies the happy path:
// backfill drains because the relay returns zero DIDs, the
// orchestrator writes phase=merging, cancels the bootstrap-live
// consumer, and seals + closes both writers. After return, phase
// is durably PhaseMerging.
func TestRunBootstrap_DrainsAndAdvancesPhase(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	st, err := store.Open(dataDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	require.NoError(t, lifecycle.WritePhase(st, lifecycle.PhaseBootstrap))

	relay := newFakeRelay(t, nil) // empty repo list => backfill drains immediately
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

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, o.runBootstrap(ctx))

	// Phase must have advanced to merging.
	got, err := lifecycle.ReadPhase(st)
	require.NoError(t, err)
	require.Equal(t, lifecycle.PhaseMerging, got)

	// The bootstrap-live segment file must exist and be sealed
	// (non-zero checksum at offset 4).
	liveDir := filepath.Join(dataDir, "backfill", "live_segments")
	entries, err := readSegFiles(liveDir)
	require.NoError(t, err)
	require.NotEmpty(t, entries, "expected at least one bootstrap-live segment")
	for _, p := range entries {
		require.True(t, isSealed(t, p), "%s should be sealed", p)
	}
}

// TestRunBootstrap_BackfillErrorPropagates verifies that a backfill
// engine error (e.g. unreachable relay) tears down the orchestrator
// cleanly and the phase remains PhaseBootstrap.
func TestRunBootstrap_BackfillErrorPropagates(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	st, err := store.Open(dataDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	require.NoError(t, lifecycle.WritePhase(st, lifecycle.PhaseBootstrap))

	// Point at a closed listener so listRepos fails fast.
	const unreachable = "http://127.0.0.1:1" // port 1 is reserved/unused
	verifier := newTestVerifier(t, unreachable)

	o, err := New(Config{
		DataDir:    dataDir,
		Store:      st,
		RelayURL:   unreachable,
		HTTPClient: &http.Client{Timeout: 500 * time.Millisecond},
		Directory:  testIdentityDirectory(),
		Verifier:   verifier,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	err = o.runBootstrap(ctx)
	require.Error(t, err, "unreachable relay should surface as runBootstrap error")

	// Phase must still be PhaseBootstrap — no cutover happened.
	got, err := lifecycle.ReadPhase(st)
	require.NoError(t, err)
	require.Equal(t, lifecycle.PhaseBootstrap, got)
}
