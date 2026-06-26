package backfill

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSelectedBackoffDelay_Deterministic shows an injected jitter makes the
// delay fully reproducible: same seed -> identical sequence.
func TestSelectedBackoffDelay_Deterministic(t *testing.T) {
	t.Parallel()

	base, max := time.Second, 30*time.Second
	seq := func() []time.Duration {
		j := rand.New(rand.NewPCG(1, 2)).Int64N
		out := make([]time.Duration, 6)
		for attempt := range out {
			out[attempt] = selectedBackoffDelay(base, max, attempt, j)
		}
		return out
	}
	assert.Equal(t, seq(), seq(), "same seed must produce identical backoff sequence")
}

// TestSelectedBackoffDelay_Bounds covers the no-jitter shape: exponential
// growth off base, clamped at maxDelay.
func TestSelectedBackoffDelay_Bounds(t *testing.T) {
	t.Parallel()

	base, max := time.Second, 8*time.Second
	zero := func(int64) int64 { return 0 } // disable jitter

	assert.Equal(t, 1*time.Second, selectedBackoffDelay(base, max, 0, zero))
	assert.Equal(t, 2*time.Second, selectedBackoffDelay(base, max, 1, zero))
	assert.Equal(t, 4*time.Second, selectedBackoffDelay(base, max, 2, zero))
	// 1s<<3 = 8s == max, and every higher attempt stays clamped.
	assert.Equal(t, max, selectedBackoffDelay(base, max, 3, zero))
	assert.Equal(t, max, selectedBackoffDelay(base, max, 99, zero))
}

// TestSelectedBackoffDelay_JitterWithinHalf asserts jitter only ever adds up
// to half the base delay and never pushes the result past maxDelay.
func TestSelectedBackoffDelay_JitterWithinHalf(t *testing.T) {
	t.Parallel()

	base, max := time.Second, 30*time.Second
	full := func(n int64) int64 { return n - 1 } // maximal in-range jitter

	got := selectedBackoffDelay(base, max, 0, full)
	require.GreaterOrEqual(t, got, 1*time.Second)
	require.Less(t, got, 1*time.Second+time.Second/2)
	require.LessOrEqual(t, selectedBackoffDelay(base, max, 99, full), max)
}
