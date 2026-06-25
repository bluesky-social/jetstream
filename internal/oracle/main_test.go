package oracle

import (
	"testing"

	"github.com/bluesky-social/jetstream/internal/overlay"
	"github.com/bluesky-social/jetstream/internal/subscribe"
	"github.com/bluesky-social/jetstream/segment"
)

// TestMain warms the process-global zstd encoders BEFORE any test runs, so
// their lazily-created worker-pool channels belong to the no-bubble process
// context rather than to whichever test first encodes.
//
// TestOracle_DefaultLifecycle runs the whole runtime inside a testing/synctest
// bubble and writes segments/overlay/subscribe frames there. klauspost/compress
// builds an Encoder's worker channel lazily on the first EncodeAll (the
// encoders were constructed with NewWriter(nil, …), which skips the eager
// init). Without this warmup the bubble's first encode would bind those package
// globals to the bubble; a later out-of-bubble EncodeAll (e.g. a plain
// segment-flush unit test in this same package) then aborts the entire test
// binary with the runtime fatal "receive on synctest channel from outside
// bubble" — taking every other test in the package down with it. Warming here,
// at process start, is the package-wide fix; see segment.WarmEncoder.
//
// Only the encoders need warming: the zstd decoders create their channels
// eagerly in NewReader, at package init, before any bubble exists.
func TestMain(m *testing.M) {
	segment.WarmEncoder()
	overlay.WarmEncoder()
	subscribe.WarmEncoder()
	m.Run()
}
