package world

import (
	"math"
	"math/rand/v2"
)

// zipfian draws an index in [0, n) under a Zipfian (power-law)
// distribution with exponent s. Index 0 is most likely; the tail
// thins out exponentially. Implementation: inverse-CDF sampling
// against an unnormalized Zipf weight; correctness > speed at
// n <= 10k.
func zipfian(r *rand.Rand, s float64, n int) int {
	if n <= 1 {
		return 0
	}
	// Build cumulative weights lazily per call. n is small (<= 10k);
	// the alloc dominates only when called millions of times in tight
	// loops. Acceptable for our event rate.
	weights := make([]float64, n)
	var total float64
	for i := range n {
		w := 1.0 / math.Pow(float64(i+1), s)
		weights[i] = w
		total += w
	}
	target := r.Float64() * total
	var acc float64
	for i, w := range weights {
		acc += w
		if target <= acc {
			return i
		}
	}
	return n - 1
}

// exponentialDelay returns a sample from Exp(λ) with the given mean.
// Used for inter-arrival times between commits — Poisson process.
func exponentialDelay(r *rand.Rand, mean float64) float64 {
	if mean <= 0 {
		return 0
	}
	// Avoid log(0).
	u := r.Float64()
	for u == 0 {
		u = r.Float64()
	}
	return -math.Log(u) * mean
}

// weighted is one option in a weighted-choice draw. Weights need not
// be normalized.
type weighted[T any] struct {
	value  T
	weight float64
}

// weightedChoice draws one value from opts proportional to weight.
func weightedChoice[T any](r *rand.Rand, opts []weighted[T]) T {
	var total float64
	for _, o := range opts {
		total += o.weight
	}
	target := r.Float64() * total
	var acc float64
	for _, o := range opts {
		acc += o.weight
		if target <= acc {
			return o.value
		}
	}
	return opts[len(opts)-1].value
}

// logNormalClamped draws a log-normal sample, rounded to int and
// clamped to [lo, hi]. mu and sigma are the parameters of the
// underlying normal (so the median is exp(mu)).
func logNormalClamped(r *rand.Rand, mu, sigma float64, lo, hi int) int {
	v := math.Exp(mu + sigma*r.NormFloat64())
	n := int(math.Round(v))
	if n < lo {
		return lo
	}
	if n > hi {
		return hi
	}
	return n
}

// geometricAtLeastOne returns 1, 2, 3, … with probability decaying
// at rate p. Models "ops per commit" — almost always 1, occasionally
// more. Math: returns 1 + Geometric(p).
func geometricAtLeastOne(r *rand.Rand, p float64) int {
	n := 1
	for n < 100 && r.Float64() > p {
		n++
	}
	return n
}
