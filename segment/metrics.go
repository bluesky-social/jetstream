package segment

import "time"

// SealObserver records successful seal durations without importing metrics
// libraries into this public package. A nil observer disables recording;
// implementations should also accept nil receivers.
type SealObserver interface {
	// ObserveSeal records a successful seal that started at start. Callers
	// pass the seal error; implementations must ignore non-nil err (failed
	// seals are chased through logs and trace status, not this histogram).
	ObserveSeal(start time.Time, err error)
}
