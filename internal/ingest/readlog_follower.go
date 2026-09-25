package ingest

import (
	"fmt"

	"github.com/bluesky-social/jetstream/segment"
)

// FollowerLog is a ReadableLog fed by the disaggregated catalog follower
// instead of a writer (design §11.1). Everything the follower appends is
// already durable in the catalog, so it appends a tick's events and then
// advances the durable watermark past them.
//
// Unlike the writer's internal calls, Append and AdvanceDurable return an
// error for an out-of-order seq instead of panicking: the follower's input
// comes from shared storage, and a hole there is storage corruption the
// follower reports and exits on, not a bug in this process.
//
// A FollowerLog has a single feeder; readers use Log.
type FollowerLog struct {
	log *ReadableLog
}

// NewFollowerLog returns an empty log whose first append must be nextSeq.
// metrics may be nil.
func NewFollowerLog(nextSeq uint64, maxBytes int64, metrics *Metrics) *FollowerLog {
	return &FollowerLog{log: newReadableLog(nextSeq, maxBytes, metrics)}
}

// Log returns the readable log subscribers read.
func (f *FollowerLog) Log() *ReadableLog { return f.log }

// Append appends ev, which must carry the log's next seq. The log copies ev.
func (f *FollowerLog) Append(ev *segment.Event) error {
	if tip := f.log.TipSeq(); ev.Seq != tip {
		return fmt.Errorf("ingest: follower log append seq %d, want %d", ev.Seq, tip)
	}
	f.log.append(ev)
	return nil
}

// AdvanceDurable marks every appended seq below next durable, which lets the
// log evict them under its byte budget.
func (f *FollowerLog) AdvanceDurable(next uint64) error {
	if d, tip := f.log.DurableSeq(), f.log.TipSeq(); next < d || next > tip {
		return fmt.Errorf("ingest: follower log durable %d outside [%d,%d]", next, d, tip)
	}
	f.log.advanceDurable(next)
	return nil
}
