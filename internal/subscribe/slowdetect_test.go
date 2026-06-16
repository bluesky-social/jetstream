package subscribe

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSlowDetector_DropsStalledFarBehindClient(t *testing.T) {
	t.Parallel()
	now := time.Unix(0, 0)
	d := newSlowDetector(slowConfig{
		window: 60 * time.Second, lagThreshold: 10_000, minRate: 5,
		now: func() time.Time { return now },
	})
	// Far behind (lag 1e6) and scanning ~1 seq/sec for 90s: adversarial.
	pos := uint64(0)
	dropped := false
	for range 90 {
		now = now.Add(time.Second)
		pos++ // 1 seq/sec
		tip := uint64(1_000_000)
		if d.observe(pos, tip-pos /* lag */) {
			dropped = true
			break
		}
	}
	require.True(t, dropped, "stalled far-behind client must be dropped after the window")
}

func TestSlowDetector_KeepsSlowButProgressingClient(t *testing.T) {
	t.Parallel()
	now := time.Unix(0, 0)
	d := newSlowDetector(slowConfig{
		window: 60 * time.Second, lagThreshold: 10_000, minRate: 5,
		now: func() time.Time { return now },
	})
	// Far behind but scanning at 100 seq/sec (above floor): never dropped.
	pos := uint64(0)
	for range 120 {
		now = now.Add(time.Second)
		pos += 100
		require.False(t, d.observe(pos, 1_000_000), "progressing client must not be dropped")
	}
}

func TestSlowDetector_KeepsIdleCaughtUpClient(t *testing.T) {
	t.Parallel()
	now := time.Unix(0, 0)
	d := newSlowDetector(slowConfig{
		window: 60 * time.Second, lagThreshold: 10_000, minRate: 5,
		now: func() time.Time { return now },
	})
	// Caught up (lag 0), cursor static because the stream is idle.
	for range 300 {
		now = now.Add(time.Second)
		require.False(t, d.observe(0, 0), "idle caught-up client must not be dropped")
	}
}

func TestSlowDetector_RecoveryResetsWindow(t *testing.T) {
	t.Parallel()
	now := time.Unix(0, 0)
	d := newSlowDetector(slowConfig{
		window: 60 * time.Second, lagThreshold: 10_000, minRate: 5,
		now: func() time.Time { return now },
	})
	pos := uint64(0)
	// 30s slow + far behind (not yet a full window).
	for range 30 {
		now = now.Add(time.Second)
		pos++
		require.False(t, d.observe(pos, 1_000_000))
	}
	// Then it catches up within lagThreshold: the bad streak resets.
	now = now.Add(time.Second)
	pos += 1_000_000
	require.False(t, d.observe(pos, 100))
	// Another 30s slow: still under a full window from the reset.
	for range 30 {
		now = now.Add(time.Second)
		pos++
		require.False(t, d.observe(pos, 1_000_000))
	}
}
