package obs

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// SegmentMetrics is the Prometheus implementation of segment.SealObserver.
// It lives here, not in package segment, so the segment decode/seal core
// carries no Prometheus or OTEL dependency and stays cheap for the public
// client to import. A nil *SegmentMetrics is a valid no-op.
type SegmentMetrics struct {
	sealDuration prometheus.Histogram
}

// NewSegmentMetrics registers segment metrics against reg.
func NewSegmentMetrics(reg prometheus.Registerer) *SegmentMetrics {
	m := &SegmentMetrics{
		sealDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "segment",
			Name:      "seal_duration_seconds",
			Help:      "End-to-end duration of segment.Writer.Seal: flush + footer + header pwrite + fsyncs.",
			Buckets:   LatencyBucketsSlow,
		}),
	}
	reg.MustRegister(m.sealDuration)
	return m
}

// ObserveSeal records a successful seal duration. Failed seals are not
// recorded — operators chase failures through error logs and trace status,
// not the success-time histogram.
func (m *SegmentMetrics) ObserveSeal(start time.Time, err error) {
	if m == nil || err != nil {
		return
	}
	m.sealDuration.Observe(time.Since(start).Seconds())
}
