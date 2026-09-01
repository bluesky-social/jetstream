package xrpcapi

import "time"

// CompactionDeadline is the read-only deadline surface used to decide how
// long an archive response may be cached. The producer owns the state and may
// update it concurrently with requests.
type CompactionDeadline interface {
	NextCompactionAt() (time.Time, bool)
}

// cacheLifetime returns the cache lifetime that is safe for a response issued
// at now. A response may be cached only until the next compaction deadline plus
// the configured grace period. The deadline is a hint about a scheduled pass,
// not a guarantee that this particular segment will be rewritten.
func cacheLifetime(now time.Time, grace time.Duration, deadline CompactionDeadline) time.Duration {
	if grace < 0 || deadline == nil {
		return 0
	}
	next, ok := deadline.NextCompactionAt()
	if !ok || next.IsZero() {
		return 0
	}
	remaining := next.Add(grace).Sub(now)
	if remaining <= 0 {
		return 0
	}
	// Cache-Control uses whole seconds. Truncate rather than letting the
	// shared formatter round up past the compaction deadline.
	return remaining.Truncate(time.Second)
}
