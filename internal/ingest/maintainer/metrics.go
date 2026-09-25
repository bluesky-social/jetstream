package maintainer

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics are the maintainer's counters and gauges. A nil *Metrics is valid:
// every method is a no-op.
type Metrics struct {
	Folds              *prometheus.CounterVec
	FoldDuration       prometheus.Histogram
	Seals              prometheus.Counter
	SealDuration       prometheus.Histogram
	QueuedBlocks       prometheus.Gauge
	ActiveSegmentBytes prometheus.Gauge
}

// NewMetrics registers the maintainer metrics against reg. Construct it once
// per process; every session's Maintainer shares it.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	const ns, sub = "jetstream", "maintainer"
	m := &Metrics{
		Folds: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "folds_total",
			Help: "Closed blocks folded into active blocks, by whether the block was uploaded or deduplicated against an existing object.",
		}, []string{"result"}),
		FoldDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns, Subsystem: sub, Name: "fold_duration_seconds",
			Help:    "Time to encode, upload, and commit one fold.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
		}),
		Seals: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "seals_total",
			Help: "Active segments sealed into a generation.",
		}),
		SealDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns, Subsystem: sub, Name: "seal_duration_seconds",
			Help:    "Time to read the active blocks, build and upload the footer, and commit one seal.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 14),
		}),
		QueuedBlocks: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: ns, Subsystem: sub, Name: "queued_blocks",
			Help: "Closed blocks waiting to be folded.",
		}),
		ActiveSegmentBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: ns, Subsystem: sub, Name: "active_segment_bytes",
			Help: "Framed bytes of the main active segment's folded blocks, the rotation rule's input.",
		}),
	}
	reg.MustRegister(m.Folds, m.FoldDuration, m.Seals, m.SealDuration, m.QueuedBlocks, m.ActiveSegmentBytes)
	return m
}

func (m *Metrics) observeFold(dedup bool, d time.Duration) {
	if m == nil {
		return
	}
	result := "uploaded"
	if dedup {
		result = "dedup"
	}
	m.Folds.WithLabelValues(result).Inc()
	m.FoldDuration.Observe(d.Seconds())
}

func (m *Metrics) observeSeal(d time.Duration) {
	if m == nil {
		return
	}
	m.Seals.Inc()
	m.SealDuration.Observe(d.Seconds())
}

func (m *Metrics) setQueued(n int) {
	if m != nil {
		m.QueuedBlocks.Set(float64(n))
	}
}

func (m *Metrics) setActiveBytes(n int64) {
	if m != nil {
		m.ActiveSegmentBytes.Set(float64(n))
	}
}
