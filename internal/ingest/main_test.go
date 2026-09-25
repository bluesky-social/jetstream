package ingest

import (
	"os"
	"testing"

	"github.com/bluesky-social/jetstream/segment"
)

// TestMain warms the shared zstd encoder before the hot mode tests enter
// synctest bubbles (see segment.WarmEncoder).
func TestMain(m *testing.M) {
	segment.WarmEncoder()
	os.Exit(m.Run())
}
