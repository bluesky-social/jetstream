package jetstreamd

import (
	"sync/atomic"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/segment"
)

// pendingEventsForDID returns a function the status handler uses to fetch the
// live writer's in-memory not-yet-durable events for a single DID.
//
// The steady-state live writer's readable log retains every event from seq
// allocation until it is durable, including detached prepared blocks. repoexport
// reconstruction reads only on-disk segments, so without these events a record
// created moments ago (e.g. a like) would be invisible to /status MST
// verification until the next flush — reported as a spurious root mismatch with
// a stale record count.
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
		return w.ReadLog().PendingForDID(did)
	}
}
