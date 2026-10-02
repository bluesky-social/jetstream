package jetstreamd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A session that starts after an interval has passed since the last GC run
// collects at once. A fresh ticker per session never fired for a leader
// whose sessions restarted more often than the interval
// (specs/notes/2026-09-28-disaggregated-testbed-findings.md finding 9c).
func TestGCDelay(t *testing.T) {
	t.Parallel()
	last := time.Unix(1000, 0)
	const interval = 10 * time.Minute
	require.Equal(t, interval, gcDelay(last, last, interval))
	require.Equal(t, 4*time.Minute, gcDelay(last, last.Add(6*time.Minute), interval))
	require.Zero(t, gcDelay(last, last.Add(interval), interval))
	require.Zero(t, gcDelay(last, last.Add(time.Hour), interval))
}
