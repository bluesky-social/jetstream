package jetstream

import "github.com/jcalabro/gt"

// optInt64 wraps a uint64 bound as the gt.Option[int64] the generated XRPC
// input expects. Callers must ensure v <= math.MaxInt64; planner.archivePlan rejects
// out-of-range cursors before this conversion so it never wraps negative.
func optInt64(v uint64) gt.Option[int64] {
	return gt.Some(int64(v))
}
