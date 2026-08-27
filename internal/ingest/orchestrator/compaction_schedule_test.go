package orchestrator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCompactionScheduleStateTransitions(t *testing.T) {
	t.Parallel()

	state := NewCompactionScheduleState()
	_, ok := state.NextCompactionAt()
	require.False(t, ok, "startup schedule must be unknown")

	next := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
	state.completePass(next)
	got, ok := state.NextCompactionAt()
	require.True(t, ok)
	require.Equal(t, next, got)

	passStart := next.Add(-time.Minute)
	state.beginPass(passStart)
	got, ok = state.NextCompactionAt()
	require.True(t, ok)
	require.Equal(t, passStart, got, "active pass must be anchored to its actual start")

	state.failPass()
	_, ok = state.NextCompactionAt()
	require.False(t, ok, "failed pass must not advertise a freshness deadline")

	state.completePass(next.Add(time.Hour))
	state.disable()
	_, ok = state.NextCompactionAt()
	require.False(t, ok, "disabled compaction must publish no schedule")
}
