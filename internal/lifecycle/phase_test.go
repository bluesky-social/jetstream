package lifecycle

import (
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestReadPhase_Empty(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	got, err := ReadPhase(st)
	require.NoError(t, err)
	require.Equal(t, Phase(""), got)
}

func TestPhase_RoundTrip(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)

	now := time.Date(2026, 5, 25, 0, 0, 0, 0, time.UTC)
	for _, p := range []Phase{PhaseBootstrap, PhaseMerging, PhaseSteadyState} {
		require.NoError(t, WritePhase(st, p, now))
		got, err := ReadPhase(st)
		require.NoError(t, err)
		require.Equal(t, p, got)
	}
}

func TestReadPhase_UnknownValueRejected(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	require.NoError(t, st.Set([]byte("phase"), []byte("banana"), store.SyncWrites))

	_, err := ReadPhase(st)
	require.Error(t, err)
	require.Contains(t, err.Error(), "banana")
}

func TestWritePhase_RejectsUnknown(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)

	err := WritePhase(st, Phase("banana"), time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "banana")
}

func TestPhaseEnteredAt_RoundTrip(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)

	want := time.Date(2026, 5, 25, 12, 0, 0, 123456000, time.UTC)
	require.NoError(t, WritePhase(st, PhaseBootstrap, want))

	got, err := ReadPhaseEnteredAt(st)
	require.NoError(t, err)
	require.True(t, got.Equal(want), "got %s, want %s", got, want)
}

func TestReadPhaseEnteredAt_Empty(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	got, err := ReadPhaseEnteredAt(st)
	require.NoError(t, err)
	require.True(t, got.IsZero())
}

func TestWritePhase_AtomicWithEnteredAt(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)

	want := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	require.NoError(t, WritePhase(st, PhaseMerging, want))

	gotPhase, err := ReadPhase(st)
	require.NoError(t, err)
	require.Equal(t, PhaseMerging, gotPhase)

	gotAt, err := ReadPhaseEnteredAt(st)
	require.NoError(t, err)
	require.True(t, gotAt.Equal(want))
}

func TestIsSteadyState_Empty(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	require.False(t, IsSteadyState(st))
}

func TestIsSteadyState_NotSteady(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	for _, p := range []Phase{PhaseBootstrap, PhaseMerging} {
		require.NoError(t, WritePhase(st, p, time.Now().UTC()))
		require.False(t, IsSteadyState(st), "phase=%s", p)
	}
}

func TestIsSteadyState_SteadyState(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	require.NoError(t, WritePhase(st, PhaseSteadyState, time.Now().UTC()))
	require.True(t, IsSteadyState(st))
}

func TestIsSteadyState_CorruptIsFalse(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	require.NoError(t, st.Set([]byte("phase"), []byte("banana"), store.SyncWrites))
	// Corrupt phase reads as not steady-state. The orchestrator's
	// startup path will surface the underlying ReadPhase error
	// separately; IsSteadyState's contract is "fail closed."
	require.False(t, IsSteadyState(st))
}
