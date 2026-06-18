package segment

import (
	"time"

	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics owns prometheus state for the segment package. nil is a
// valid zero-value: every method is a no-op.
type Metrics struct {
	SealDuration prometheus.Histogram
}

// NewMetrics registers segment metrics against reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		SealDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "jetstream",
			Subsystem: "segment",
			Name:      "seal_duration_seconds",
			Help:      "End-to-end duration of segment.Writer.Seal: flush + footer + header pwrite + fsyncs.",
			Buckets:   obs.LatencyBucketsSlow,
		}),
	}
	reg.MustRegister(m.SealDuration)
	return m
}

// ObserveSeal records a successful seal duration. Failed seals are
// not recorded — operators chase failures through error logs and
// trace status, not the success-time histogram.
func (m *Metrics) ObserveSeal(start time.Time, err error) {
	if m == nil || err != nil {
		return
	}
	m.SealDuration.Observe(time.Since(start).Seconds())
}
