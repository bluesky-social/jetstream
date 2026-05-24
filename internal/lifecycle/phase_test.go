package lifecycle

import (
	"testing"

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

	for _, p := range []Phase{PhaseBootstrap, PhaseMerging, PhaseSteadyState} {
		require.NoError(t, WritePhase(st, p))
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

	err := WritePhase(st, Phase("banana"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "banana")
}
