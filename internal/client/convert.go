package client

import "github.com/jcalabro/gt"

// optInt64 wraps a uint64 bound as the gt.Option[int64] the generated XRPC
// input expects. Callers must ensure v <= math.MaxInt64; Planner.Plan rejects
// out-of-range cursors before this conversion so it never wraps negative.
func optInt64(v uint64) gt.Option[int64] {
	return gt.Some(int64(v))
}

// nonNegU64 clamps a signed wire count to uint64, flooring negatives at 0.
// The server reports counts as non-negative integers; this is defensive
// against a malformed response rather than an expected path.
func nonNegU64(v int64) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}
