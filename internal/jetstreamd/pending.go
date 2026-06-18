package jetstreamd

import (
	"sync/atomic"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/segment"
)

// pendingEventsForDID returns a function the status handler uses to fetch the
// live writer's in-memory pending (not-yet-flushed) events for a single DID.
//
// The steady-state live writer buffers appended events in an in-memory block
// and only flushes them to a segment file when the block fills or the periodic
// compaction pass force-rotates it. repoexport reconstruction reads only
// on-disk segments, so without these pending events a record created moments
// ago (e.g. a like) would be invisible to /status MST verification until the
// next flush — reported as a spurious root mismatch with a stale record count.
//
// ref is the same atomic.Pointer the orchestrator publishes its steady-state
// writer into. Before steady-state it holds nil and we return no pending
// events; the verification path tolerates that (it reflects on-disk state).
func pendingEventsForDID(ref *atomic.Pointer[ingest.Writer]) func(did string) []segment.Event {
	return func(did string) []segment.Event {
		w := ref.Load()
		if w == nil {
			return nil
		}
		pending := w.SnapshotPending()
		if len(pending) == 0 {
			return nil
		}
		out := make([]segment.Event, 0, len(pending))
		for i := range pending {
			if pending[i].DID == did {
				out = append(out, pending[i])
			}
		}
		return out
	}
}
