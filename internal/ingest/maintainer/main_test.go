package maintainer_test

import (
	"os"
	"testing"

	"github.com/bluesky-social/jetstream/segment"
)

// TestMain warms the shared zstd encoder before the direct writer tests
// enter synctest bubbles (see segment.WarmEncoder).
func TestMain(m *testing.M) {
	segment.WarmEncoder()
	os.Exit(m.Run())
}
