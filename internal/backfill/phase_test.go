package backfill

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestBootstrapState_FreshDirReturnsUnset pins the implicit
// PhaseUnset contract: a brand-new data directory has no row at
// bootstrap/state, and that surfaces as PhaseUnset (not an error).
func TestBootstrapState_FreshDirReturnsUnset(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	st, err := GetBootstrapState(s)
	require.NoError(t, err)
	require.Equal(t, PhaseUnset, st.Phase)
	require.False(t, st.IsComplete())
	require.True(t, st.StartedAt.IsZero())
	require.True(t, st.UpdatedAt.IsZero())
	require.True(t, st.CompletedAt.IsZero())
}

// TestBootstrapState_RoundTrip exercises every field so a JSON-tag
// drift on this on-disk format surfaces as a test failure.
func TestBootstrapState_RoundTrip(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	now := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	want := BootstrapState{
		Phase:       PhaseComplete,
		StartedAt:   now.Add(-time.Hour),
		UpdatedAt:   now,
		CompletedAt: now,
	}

	require.NoError(t, PutBootstrapState(s, want))

	got, err := GetBootstrapState(s)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.True(t, got.IsComplete())
}

// TestBootstrapState_PhaseTransitions guards the explicit states
// the orchestrator walks through. Each transition is a distinct
// on-disk write and we want to make sure each persists faithfully
// across re-reads.
func TestBootstrapState_PhaseTransitions(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	for _, phase := range []Phase{PhaseSeed, PhaseDownload, PhaseMerge, PhaseComplete} {
		require.NoError(t, PutBootstrapState(s, BootstrapState{Phase: phase}))
		got, err := GetBootstrapState(s)
		require.NoError(t, err)
		require.Equal(t, phase, got.Phase, "phase did not round-trip")
	}
}
