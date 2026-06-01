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
// Block index cache metric names predate the all-resident manifest:
// a "hit" now means the requested segment metadata was resident, and
// a "miss" means the segment idx was unknown.
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
			Help: "Number of BlockIndex calls served from resident manifest metadata.",
		}),
		BlockIndexCacheMissesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "block_index_cache_misses_total",
			Help: "Number of BlockIndex calls for unknown segment indexes.",
		}),
		BlockIndexLoadSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name:    "block_index_load_seconds",
			Help:    "Wall-clock duration to load one sealed segment's resident metadata.",
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
