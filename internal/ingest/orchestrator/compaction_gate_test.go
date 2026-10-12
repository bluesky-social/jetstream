package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Pause waits out a running pass and keeps new ones from starting; a nil
// gate never pauses.
func TestCompactionGate(t *testing.T) {
	t.Parallel()
	var nilGate *CompactionGate
	require.True(t, nilGate.enter())
	nilGate.exit()
	require.False(t, nilGate.Paused())

	g := NewCompactionGate()
	require.True(t, g.enter(), "a running pass")
	paused := make(chan error, 1)
	go func() { paused <- g.Pause(t.Context()) }()
	select {
	case err := <-paused:
		t.Fatalf("Pause returned %v while a pass ran", err)
	case <-time.After(20 * time.Millisecond):
	}
	require.True(t, g.Paused())
	require.False(t, g.enter(), "no new pass while paused")
	g.exit()
	require.NoError(t, <-paused)

	// A pause that times out stays paused.
	require.True(t, g.Paused())
	g.Resume()
	require.True(t, g.enter())
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, g.Pause(ctx), context.DeadlineExceeded)
	require.True(t, g.Paused())
	g.exit()
	require.NoError(t, g.Pause(t.Context()))
	g.Resume()
	require.False(t, g.Paused())
}
