package segment

import (
	"testing"
	"testing/synctest"
)

// TestWarmEncoderAllowsCrossBubbleEncode locks in the fix for the
// "receive on synctest channel from outside bubble" fatal.
//
// The package-global blockEncoder builds its worker-pool channel lazily on the
// first EncodeAll. If that first encode happens inside a testing/synctest
// bubble, the channel is bound to the bubble and a later EncodeAll from outside
// aborts the whole process. WarmEncoder, called before the bubble, must
// relocate the channel to the no-bubble context so both encodes are safe.
//
// Without the WarmEncoder() call below this test crashes the binary (a runtime
// fatal, not a recoverable failure), so it genuinely fails when the fix
// regresses. It must NOT call t.Parallel (synctest.Test forbids it) and is the
// only bubble in this package, so it is safe under the one-bubble-per-process
// rule.
//
// nolint:paralleltest // synctest.Test forbids t.Parallel inside the bubble.
func TestWarmEncoderAllowsCrossBubbleEncode(t *testing.T) {
	// Relocate the global encoder channel to this no-bubble goroutine first.
	WarmEncoder()

	// Encode inside a bubble: with the warmup this is merely
	// non-durably-blocking (fine); without it, it would bind the global
	// channel to the bubble.
	synctest.Test(t, func(t *testing.T) {
		if got := encodeEmptyBlockCompressed(); len(got) == 0 {
			t.Fatal("in-bubble encode returned empty frame")
		}
	})

	// Encode again from outside the bubble. Pre-fix this is the line that
	// fatals the process ("receive on synctest channel from outside bubble").
	if got := encodeEmptyBlockCompressed(); len(got) == 0 {
		t.Fatal("post-bubble encode returned empty frame")
	}
}
