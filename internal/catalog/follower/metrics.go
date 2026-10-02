package follower

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricsNamespace = "jetstream"
	metricsSubsystem = "catalog"
)

// Metrics owns the follower's prometheus series (design §23). A nil
// *Metrics is valid and records nothing.
type Metrics struct {
	Revision          prometheus.Gauge
	Lag               prometheus.GaugeFunc
	RefreshDuration   prometheus.Histogram
	RefreshErrors     prometheus.Counter
	NotifyReceived    prometheus.Counter
	ListenErrors      prometheus.Counter
	VisibilityLatency prometheus.Histogram

	// lag is the follower's lag function, read at scrape time so the gauge
	// keeps growing while a tick hangs.
	lag atomic.Pointer[func() float64]
}

// NewMetrics registers the series against reg. Construct exactly once per
// process.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Revision: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "revision",
			Help: "catalog_revision of this pod's mirror.",
		}),
		RefreshDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name:    "refresh_duration_seconds",
			Help:    "Duration of follower ticks that changed the mirror, including footer and pointer-batch fetches.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 16),
		}),
		RefreshErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "refresh_errors_total",
			Help: "Follower ticks that failed and kept the previous mirror.",
		}),
		NotifyReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "notify_received_total",
			Help: "NOTIFY jetstream_catalog messages received by the follower.",
		}),
		ListenErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "listen_errors_total",
			Help: "Failed or dropped LISTEN connections; the follower polls until it reconnects.",
		}),
		VisibilityLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "event_visibility_latency_seconds",
			Help:      "Follower readable-log append time minus the event's witnessed_at, per pod.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 16),
		}),
	}
	m.Lag = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: metricsNamespace, Subsystem: metricsSubsystem,
		Name: "lag_seconds",
		Help: "Now minus the start of the follower's last successful refresh.",
	}, func() float64 {
		if f := m.lag.Load(); f != nil {
			return (*f)()
		}
		return 0
	})
	reg.MustRegister(m.Revision, m.Lag, m.RefreshDuration, m.RefreshErrors,
		m.NotifyReceived, m.ListenErrors, m.VisibilityLatency)
	return m
}

func (m *Metrics) setLagSource(f func() float64) {
	if m != nil {
		m.lag.Store(&f)
	}
}

func (m *Metrics) setRevision(rev uint64) {
	if m != nil {
		m.Revision.Set(float64(rev))
	}
}

func (m *Metrics) observeRefresh(seconds float64) {
	if m != nil {
		m.RefreshDuration.Observe(seconds)
	}
}

func (m *Metrics) refreshFailed() {
	if m != nil {
		m.RefreshErrors.Inc()
	}
}

func (m *Metrics) notifyReceived() {
	if m != nil {
		m.NotifyReceived.Inc()
	}
}

func (m *Metrics) listenFailed() {
	if m != nil {
		m.ListenErrors.Inc()
	}
}

func (m *Metrics) observeVisibility(seconds float64) {
	if m != nil {
		m.VisibilityLatency.Observe(seconds)
	}
}
