package obs

import "github.com/prometheus/client_golang/prometheus"

// LatencyBucketsFast covers ~0.1 ms → ~1.6 s in 15 exponential
// buckets. Use for hot-ish operations: pebble Get/Set, identity
// cache, anything that should normally complete in microseconds.
var LatencyBucketsFast = prometheus.ExponentialBuckets(0.0001, 2, 15)

// LatencyBucketsSlow covers ~10 ms → ~164 s in 15 exponential
// buckets. Use for heavyweight operations: repo download, segment
// seal, phase transitions. Matches the existing
// jetstream_orchestrator_state_duration_seconds bucket layout so
// dashboards can reuse the same axes.
var LatencyBucketsSlow = prometheus.ExponentialBuckets(0.01, 2, 15)
