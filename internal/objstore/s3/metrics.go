package s3

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/bluesky-social/jetstream/internal/obs"
)

// Operation labels for the jetstream_s3_* series.
const (
	opPut      = "put"
	opGet      = "get"
	opGetRange = "get_range"
	opDelete   = "delete"
)

// Result labels for jetstream_s3_requests_total.
const (
	resultOK           = "ok"
	resultNotFound     = "not_found"
	resultInvalidRange = "invalid_range"
	resultCanceled     = "canceled"
	resultTimeout      = "timeout"
	resultError        = "error"
)

// Metrics owns the jetstream_s3_* request series (design §23). They count
// every HTTP attempt, so retries show up as extra requests. A nil *Metrics
// is valid and records nothing.
type Metrics struct {
	Requests *prometheus.CounterVec
	Duration *prometheus.HistogramVec
	Bytes    *prometheus.CounterVec
}

// NewMetrics registers the series against reg. Construct exactly once per
// process.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "jetstream",
			Subsystem: "s3",
			Name:      "requests_total",
			Help:      "S3 request attempts, by operation and result. Retries count as separate attempts.",
		}, []string{"op", "result"}),
		Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "jetstream",
			Subsystem: "s3",
			Name:      "request_duration_seconds",
			Help:      "S3 request attempt latency, including reading the response body, by operation.",
			Buckets:   obs.LatencyBucketsSlow,
		}, []string{"op"}),
		Bytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "jetstream",
			Subsystem: "s3",
			Name:      "bytes_total",
			Help:      "Object bytes moved by successful S3 requests, by operation.",
		}, []string{"op"}),
	}
	reg.MustRegister(m.Requests, m.Duration, m.Bytes)
	return m
}

func (m *Metrics) observe(op, result string, d time.Duration, n int) {
	if m == nil {
		return
	}
	m.Requests.WithLabelValues(op, result).Inc()
	m.Duration.WithLabelValues(op).Observe(d.Seconds())
	if result == resultOK && n > 0 {
		m.Bytes.WithLabelValues(op).Add(float64(n))
	}
}
