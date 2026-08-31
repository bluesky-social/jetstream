package orchestrator

import (
	"sync/atomic"
	"time"
)

// CompactionScheduleState publishes the next compaction timestamp to serving
// code without exposing the orchestrator or its scheduler. A zero timestamp
// means the schedule is unknown or compaction is disabled, so callers must not
// advertise a cache lifetime.
type CompactionScheduleState struct {
	nextUnixNano atomic.Int64
}

// NewCompactionScheduleState returns an initially unknown compaction schedule.
func NewCompactionScheduleState() *CompactionScheduleState {
	return &CompactionScheduleState{}
}

// NextCompactionAt implements the read-only schedule surface consumed by
// xrpcapi. The timestamp is published atomically and is safe to read while the
// compactor updates it.
func (s *CompactionScheduleState) NextCompactionAt() (time.Time, bool) {
	if s == nil {
		return time.Time{}, false
	}
	n := s.nextUnixNano.Load()
	if n <= 0 {
		return time.Time{}, false
	}
	return time.Unix(0, n).UTC(), true
}

func (s *CompactionScheduleState) setNextCompactionAt(at time.Time) {
	if s == nil || at.IsZero() {
		if s != nil {
			s.nextUnixNano.Store(0)
		}
		return
	}
	s.nextUnixNano.Store(at.UnixNano())
}

func (s *CompactionScheduleState) beginPass(start time.Time) {
	s.setNextCompactionAt(start)
}

func (s *CompactionScheduleState) completePass(next time.Time) {
	s.setNextCompactionAt(next)
}

func (s *CompactionScheduleState) failPass() {
	s.setNextCompactionAt(time.Time{})
}

func (s *CompactionScheduleState) disable() {
	s.setNextCompactionAt(time.Time{})
}
