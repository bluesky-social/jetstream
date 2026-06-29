package jetstream

import (
	"context"
	"iter"

	"github.com/jcalabro/gt"
)

// LiveFrame is one buffered live-tail event: its Jetstream sequence number and
// the raw JSON frame bytes exactly as received on /subscribe-v2. The client
// buffers these during the backfill-to-live cutover and replays them in seq
// order once the backfill drains.
type LiveFrame struct {
	// Seq is the Jetstream cursor of this frame. Used to replay from a point
	// and to truncate emitted frames; it is also present inside Data.
	Seq uint64
	// Data is the verbatim single-line JSON frame as received from the server.
	Data []byte
}

// LiveBuffer stores live-tail frames received during the backfill-to-live
// cutover until the backfill drains and they can be replayed in order. The
// default is an in-memory buffer (NewMemLiveBuffer); a durable, file-backed
// JSONL implementation is available via NewFileLiveBuffer for long-running
// backfills whose live backlog cannot fit in memory.
//
// The client serializes buffer access: it appends during cutover, then
// replays once. Implementations need not support concurrent Append and Replay.
// Appended frames arrive in non-decreasing seq order.
type LiveBuffer interface {
	// Append stores a batch of frames. The implementation chooses its own
	// durability/fsync cadence.
	Append(frames []LiveFrame) error

	// Replay yields buffered frames after the given exclusive lower bound, in
	// ascending seq order. None replays from the very beginning (seqs start at 1,
	// so this includes the first-ever event); Some(n) yields only frames with
	// Seq > n. Iteration stops on the first yielded error.
	Replay(ctx context.Context, after gt.Option[uint64]) iter.Seq2[LiveFrame, error]

	// Truncate drops buffered frames with Seq <= throughSeq once they have
	// been replayed and emitted, reclaiming space.
	Truncate(throughSeq uint64) error

	// Close releases buffer resources. For file-backed buffers it flushes and
	// fsyncs any pending data first.
	Close() error
}
