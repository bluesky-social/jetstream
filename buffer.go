package jetstream

import (
	"context"
	"iter"
)

// LiveBuffer stores live-tail event frames received during the
// backfill-to-live cutover, until the backfill drains and they can be
// emitted in order. The default is an in-memory buffer; a durable,
// file-backed implementation is available via NewFileLiveBuffer for
// long-running backfills whose live backlog cannot fit in memory.
//
// Implementations must be safe for concurrent Append while a Replay is in
// progress is NOT required: the client serializes buffer access (append
// during cutover, then replay once). Append batches are delivered in
// non-decreasing seq order.
type LiveBuffer interface {
	// Append stores a batch of raw event frames. The implementation chooses
	// its own durability/fsync cadence.
	Append(frames [][]byte) error

	// Replay yields buffered frames with seq strictly greater than from, in
	// ascending seq order. Iteration stops on the first yielded error.
	Replay(ctx context.Context, from uint64) iter.Seq2[[]byte, error]

	// Truncate drops buffered frames with seq <= throughSeq once they have
	// been emitted, reclaiming space.
	Truncate(throughSeq uint64) error

	// Close releases buffer resources.
	Close() error
}
