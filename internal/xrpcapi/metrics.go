package xrpcapi

import "github.com/prometheus/client_golang/prometheus"

const (
	metricsNamespace = "jetstream"
	metricsSubsystem = "getblock"

	resultOK         = "ok"
	resultNotFound   = "not_found"
	resultBadRequest = "bad_request"
	resultError      = "error"
)

// Metrics owns the prometheus state for getBlock. A nil *Metrics is valid:
// every method is a no-op, so tests and the zero-config server can skip
// registration.
type Metrics struct {
	requests   *prometheus.CounterVec
	servedByte prometheus.Counter
	duration   prometheus.Histogram
}

// NewMetrics registers and returns the getBlock metrics on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "requests_total", Help: "getBlock requests served, by outcome.",
		}, []string{"result"}),
		servedByte: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "served_bytes_total", Help: "Block frame bytes written to clients.",
		}),
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "duration_seconds", Help: "getBlock handler wall-clock latency.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 12),
		}),
	}
	reg.MustRegister(m.requests, m.servedByte, m.duration)
	return m
}

// observeServe records one request outcome. Nil-safe.
func (m *Metrics) observeServe(result string, bytes int, seconds float64) {
	if m == nil {
		return
	}
	m.requests.WithLabelValues(result).Inc()
	if bytes > 0 {
		m.servedByte.Add(float64(bytes))
	}
	m.duration.Observe(seconds)
}
