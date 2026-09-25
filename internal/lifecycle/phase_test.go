package lifecycle

import (
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/metastore/memstore"
	"github.com/bluesky-social/jetstream/internal/metastore/pebblestore"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) metastore.Store {
	t.Helper()
	return memstore.New()
}

func TestReadPhase_Empty(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	got, err := ReadPhase(t.Context(), st)
	require.NoError(t, err)
	require.Equal(t, Phase(""), got)
}

func TestPhase_RoundTrip(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)

	now := time.Date(2026, 5, 25, 0, 0, 0, 0, time.UTC)
	for _, p := range []Phase{PhaseBootstrap, PhaseMerging, PhaseSteadyState} {
		require.NoError(t, WritePhase(t.Context(), st, p, now))
		got, err := ReadPhase(t.Context(), st)
		require.NoError(t, err)
		require.Equal(t, p, got)
	}
}

func TestReadPhase_UnknownValueRejected(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	require.NoError(t, st.Set(t.Context(), []byte("phase"), []byte("banana")))

	_, err := ReadPhase(t.Context(), st)
	require.Error(t, err)
	require.Contains(t, err.Error(), "banana")
}

func TestWritePhase_RejectsUnknown(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)

	err := WritePhase(t.Context(), st, Phase("banana"), time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "banana")
}

func TestPhaseEnteredAt_RoundTrip(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)

	want := time.Date(2026, 5, 25, 12, 0, 0, 123456000, time.UTC)
	require.NoError(t, WritePhase(t.Context(), st, PhaseBootstrap, want))

	got, err := ReadPhaseEnteredAt(t.Context(), st)
	require.NoError(t, err)
	require.True(t, got.Equal(want), "got %s, want %s", got, want)
}

func TestBackfillTiming_RoundTrip(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)

	startedAt := time.Date(2026, 5, 25, 1, 2, 3, 0, time.UTC)
	completedAt := startedAt.Add(3*24*time.Hour + 7*time.Hour + 5*time.Minute)
	require.NoError(t, WriteBackfillTiming(t.Context(), st, startedAt, completedAt))

	got, err := ReadBackfillTiming(t.Context(), st)
	require.NoError(t, err)
	require.True(t, got.StartedAt.Equal(startedAt), "got %s, want %s", got.StartedAt, startedAt)
	require.True(t, got.CompletedAt.Equal(completedAt), "got %s, want %s", got.CompletedAt, completedAt)
	require.Equal(t, completedAt.Sub(startedAt), got.Duration())
}

func TestReadBackfillTiming_Empty(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	got, err := ReadBackfillTiming(t.Context(), st)
	require.NoError(t, err)
	require.True(t, got.StartedAt.IsZero())
	require.True(t, got.CompletedAt.IsZero())
	require.Equal(t, time.Duration(0), got.Duration())
}

func TestReadPhaseEnteredAt_Empty(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	got, err := ReadPhaseEnteredAt(t.Context(), st)
	require.NoError(t, err)
	require.True(t, got.IsZero())
}

func TestIsSteadyState_Empty(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	require.False(t, IsSteadyState(t.Context(), st))
}

func TestIsSteadyState_NotSteady(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	for _, p := range []Phase{PhaseBootstrap, PhaseMerging} {
		require.NoError(t, WritePhase(t.Context(), st, p, time.Now().UTC()))
		require.False(t, IsSteadyState(t.Context(), st), "phase=%s", p)
	}
}

func TestIsSteadyState_SteadyState(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	require.NoError(t, WritePhase(t.Context(), st, PhaseSteadyState, time.Now().UTC()))
	require.True(t, IsSteadyState(t.Context(), st))
}

func TestIsSteadyState_CorruptIsFalse(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	require.NoError(t, st.Set(t.Context(), []byte("phase"), []byte("banana")))
	// Corrupt phase reads as not steady-state. The orchestrator's
	// startup path will surface the underlying ReadPhase error
	// separately; IsSteadyState's contract is "fail closed."
	require.False(t, IsSteadyState(t.Context(), st))
}

// TestPhase_SurvivesReopen pins that the phase and its timestamps are durable
// in the local-mode store, since startup routing depends on them.
func TestPhase_SurvivesReopen(t *testing.T) {
	t.Parallel()
	fs := vfs.NewMem()
	dir := t.TempDir()
	enteredAt := time.Date(2026, 5, 25, 1, 2, 3, 4, time.UTC)
	startedAt := enteredAt.Add(-time.Hour)

	st, err := pebblestore.Open(dir, nil, pebblestore.WithFS(fs))
	require.NoError(t, err)
	require.NoError(t, WritePhaseWithBackfillTiming(t.Context(), st, PhaseMerging, enteredAt, startedAt, enteredAt))
	require.NoError(t, st.Close())

	st, err = pebblestore.Open(dir, nil, pebblestore.WithFS(fs))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	p, err := ReadPhase(t.Context(), st)
	require.NoError(t, err)
	require.Equal(t, PhaseMerging, p)
	at, err := ReadPhaseEnteredAt(t.Context(), st)
	require.NoError(t, err)
	require.Equal(t, enteredAt, at)
	timing, err := ReadBackfillTiming(t.Context(), st)
	require.NoError(t, err)
	require.Equal(t, BackfillTiming{StartedAt: startedAt, CompletedAt: enteredAt}, timing)
}
