package xrpcapi

import "time"

// CompactionSchedule is the read-only schedule surface used to decide how long
// an archive response may be cached. The producer owns the schedule and may
// update it concurrently with requests.
type CompactionSchedule interface {
	NextCompactionAt() (time.Time, bool)
}

// dynamicCacheMaxAge returns the cache lifetime that is safe for a response
// issued at now. A response may be cached only until the next compaction
// deadline, and never longer than configuredMaxAge. The schedule is a hint
// about a scheduled pass, not a guarantee that this particular segment will
// be rewritten.
func dynamicCacheMaxAge(now time.Time, configuredMaxAge, grace time.Duration, schedule CompactionSchedule) time.Duration {
	if configuredMaxAge <= 0 || grace < 0 || schedule == nil {
		return 0
	}
	next, ok := schedule.NextCompactionAt()
	if !ok || next.IsZero() {
		return 0
	}
	remaining := next.Add(grace).Sub(now)
	if remaining <= 0 {
		return 0
	}
	if remaining > configuredMaxAge {
		remaining = configuredMaxAge
	}
	// Cache-Control uses whole seconds. Truncate rather than letting the
	// shared formatter round up past the compaction deadline.
	return remaining.Truncate(time.Second)
}
