package manifest

import "github.com/prometheus/client_golang/prometheus"

const (
	metricsNamespace = "jetstream"
	metricsSubsystem = "manifest"
)

// Metrics owns the prometheus series for the manifest package. A nil
// *Metrics is a valid zero-value: every reference is guarded by a nil
// check at the call site, so tests can skip metric registration.
//
// Subsequent tasks add cache hit/miss counters and a load-seconds
// histogram for the block-index LRU.
type Metrics struct {
	SegmentsLoaded             prometheus.Gauge
	BlockIndexCacheHitsTotal   prometheus.Counter
	BlockIndexCacheMissesTotal prometheus.Counter
	BlockIndexLoadSeconds      prometheus.Histogram
}

// NewMetrics registers the series against reg. Calls reg.MustRegister,
// which panics if these are already registered. Construct exactly once
// per process.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		SegmentsLoaded: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "segments_loaded",
			Help: "Number of sealed segments tracked by the in-memory manifest.",
		}),
		BlockIndexCacheHitsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "block_index_cache_hits_total",
			Help: "Number of BlockIndex calls served from the LRU cache.",
		}),
		BlockIndexCacheMissesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "block_index_cache_misses_total",
			Help: "Number of BlockIndex calls that opened a segment.Reader to load.",
		}),
		BlockIndexLoadSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name:    "block_index_load_seconds",
			Help:    "Wall-clock duration to load one segment's block index from disk on a cache miss.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 4, 8),
		}),
	}
	reg.MustRegister(
		m.SegmentsLoaded,
		m.BlockIndexCacheHitsTotal,
		m.BlockIndexCacheMissesTotal,
		m.BlockIndexLoadSeconds,
	)
	return m
}
