package world

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestRand() *rand.Rand {
	return rand.New(rand.NewPCG(1, 2))
}

func TestZipfian_AllInRange(t *testing.T) {
	t.Parallel()
	r := newTestRand()
	for range 10000 {
		idx := zipfian(r, 1.07, 100)
		require.GreaterOrEqual(t, idx, 0)
		require.Less(t, idx, 100)
	}
}

func TestZipfian_FavorsLowIndices(t *testing.T) {
	t.Parallel()
	r := newTestRand()
	hits := make(map[int]int, 100)
	for range 100000 {
		hits[zipfian(r, 1.07, 100)]++
	}
	require.Greater(t, hits[0], hits[50])
	require.Greater(t, hits[50], hits[99])
}

func TestExponentialDelay_MeanInTolerance(t *testing.T) {
	t.Parallel()
	r := newTestRand()
	const want = 0.1 // mean
	var sum float64
	const n = 100000
	for range n {
		sum += exponentialDelay(r, want)
	}
	got := sum / n
	require.InDelta(t, want, got, 0.01)
}

func TestWeightedChoice(t *testing.T) {
	t.Parallel()
	r := newTestRand()
	const a, b, c = "a", "b", "c"
	weights := []weighted[string]{
		{value: a, weight: 7},
		{value: b, weight: 2},
		{value: c, weight: 1},
	}
	hits := map[string]int{}
	for range 100000 {
		hits[weightedChoice(r, weights)]++
	}
	require.Greater(t, hits[a], hits[b])
	require.Greater(t, hits[b], hits[c])
}

func TestLogNormalClamped(t *testing.T) {
	t.Parallel()
	r := newTestRand()
	for range 5000 {
		v := logNormalClamped(r, 4.0, 1.0, 1, 3000)
		require.GreaterOrEqual(t, v, 1)
		require.LessOrEqual(t, v, 3000)
	}
}

func TestGeometricAtLeastOne(t *testing.T) {
	t.Parallel()
	r := newTestRand()
	for range 1000 {
		require.GreaterOrEqual(t, geometricAtLeastOne(r, 0.7), 1)
	}
}
