package segment

import "time"

// SealObserver receives a timing sample each time Writer.Seal completes
// successfully. It is the segment package's only metrics seam: the concrete
// Prometheus implementation lives outside this package (internal/obs) so the
// decode/seal core carries no metrics, OTEL, or Prometheus dependency and
// stays cheap for the public client to import.
//
// A nil SealObserver is valid and disables observation; Writer guards the
// call. Implementations should also tolerate a nil receiver, matching the
// codebase nil-safe-metrics convention.
type SealObserver interface {
	// ObserveSeal records a successful seal that started at start. Callers
	// pass the seal error; implementations must ignore non-nil err (failed
	// seals are chased through logs and trace status, not this histogram).
	ObserveSeal(start time.Time, err error)
}
