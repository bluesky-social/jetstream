package overlay

import "time"

// CacheObserver receives overlay-cache telemetry. It is the overlay package's
// only metrics seam: the concrete Prometheus implementation lives outside this
// package (internal/obs) so the pure encode/decode core (Encode/Decode, the
// format the Go client depends on) carries no Prometheus dependency.
//
// A nil CacheObserver is valid and disables observation; Cache guards each
// call. Implementations should also tolerate a nil receiver.
type CacheObserver interface {
	// ObserveBuild records one overlay blob (re)build: its encode+compress
	// latency, serialized size, and tombstone counts.
	ObserveBuild(d time.Duration, blobBytes, records, dids int)
	// ObserveServe records one getTombstones response of n bytes.
	ObserveServe(n int)
}
