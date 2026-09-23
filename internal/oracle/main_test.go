package oracle

import (
	"testing"

	"github.com/bluesky-social/jetstream/internal/subscribe"
	"github.com/bluesky-social/jetstream/segment"
)

// TestMain initializes global zstd encoder channels outside the lifecycle's
// synctest bubble, preventing later cross-bubble channel failures.
// NewWriter(nil) defers channel creation until EncodeAll; decoders create
// theirs at package init and need no warmup.
func TestMain(m *testing.M) {
	segment.WarmEncoder()
	subscribe.WarmEncoder()
	m.Run()
}
