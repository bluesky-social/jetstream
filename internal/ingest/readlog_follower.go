package ingest

import (
	"fmt"

	"github.com/bluesky-social/jetstream/internal/catalog"
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

// AppendRun appends evs, which must carry ascending seqs at or above the
// log's tip, and then advances the tip to next. Seqs between them stay
// vacant, as Skip leaves them. Readers are woken once for the whole run.
// The log copies each event.
func (f *FollowerLog) AppendRun(evs []segment.Event, next uint64) error {
	want := f.log.TipSeq()
	entries := make([]*ReadLogEntry, len(evs))
	for i := range evs {
		if evs[i].Seq < want {
			return fmt.Errorf("ingest: follower log append seq %d, want at least %d", evs[i].Seq, want)
		}
		want = evs[i].Seq + 1
		entries[i] = catalog.NewLogEntry(&evs[i])
	}
	if next < want {
		return fmt.Errorf("ingest: follower log run ends at %d, before seq %d", next, want-1)
	}
	f.log.appendRun(entries, next)
	return nil
}

// Skip advances the log's tip to next without appending, leaving the seqs
// in between vacant: compaction removed them from a sealed block before the
// follower read it. Readers skip vacant seqs.
func (f *FollowerLog) Skip(next uint64) error {
	if tip := f.log.TipSeq(); next < tip {
		return fmt.Errorf("ingest: follower log skip to %d, below tip %d", next, tip)
	}
	f.log.skip(next)
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
