package subscribe

// WithSeamRetryObserver returns a copy of input with an observer that fires
// once per rotation-seam convergence retry, carrying the seq at which the hole
// was observed. Test-only: it exposes the unexported onSeamRetry hook to
// external (subscribe_test) tests so they can assert the retry path is taken.
func WithSeamRetryObserver(input WalkInput, fn func(holeSeq uint64)) WalkInput {
	input.onSeamRetry = fn
	return input
}
