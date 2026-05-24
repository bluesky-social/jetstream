package orchestrator

import (
	"context"

	"github.com/bluesky-social/jetstream-v2/internal/obs"
)

// runMerge is the cutover state machine's State 5: compact the
// throwaway segment files in data/backfill/live_segments/ into
// data/segments/.
//
// TODO(merge): implement compaction per DESIGN.md §4.2.
//
// Required behavior of the future implementation:
//
//  1. Read every sealed segment file under data/backfill/live_segments/
//     in seq-ascending file order.
//  2. For each event, look up repo/<did>.BackfillRev. Drop the event
//     if its rev is <= BackfillRev (its data was already written
//     authoritatively by the backfill engine). Keep otherwise.
//  3. Write surviving events to data/segments/ via a fresh
//     ingest.Writer that allocates new seq numbers from
//     live.SteadySeqKey so they continue monotonically from the
//     backfill writer's last allocation.
//  4. Be IDEMPOTENT under partial completion: a crash mid-merge
//     restarts in PhaseMerging and re-runs runMerge. The
//     implementation must not double-write events on retry. A
//     "last-completed-source-segment" pebble key is the natural
//     cursor.
//  5. Once all source files are consumed and survivors are durably
//     flushed in data/segments/, runMerge returns nil. The caller
//     (Run) then writes PhaseSteadyState. Cleanup of
//     data/backfill/live_segments/ sits there harmlessly until a
//     later cleanup pass.
//
// The current implementation is a deliberate no-op until the merge
// logic lands. State 5 is trivially idempotent because it does
// nothing.
//
// The ctx check below is the future merge's cancellation contract
// in miniature: a long-running compaction must observe ctx.Done()
// promptly, and pinning that into the stub keeps callers from
// growing a dependency on "merge always returns immediately".
func (o *Orchestrator) runMerge(ctx context.Context) (retErr error) {
	ctx, _, done := obs.Observe(ctx)
	defer func() { done(retErr) }()

	if err := ctx.Err(); err != nil {
		retErr = err
		return err
	}
	return nil
}
