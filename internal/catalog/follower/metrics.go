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
	Revision prometheus.Gauge
	Lag      prometheus.GaugeFunc
	// RefreshRunning is how long the in-flight refresh has run: a hung
	// refresh shows as a growing value rather than as a missing sample.
	RefreshRunning    prometheus.GaugeFunc
	RefreshDuration   prometheus.Histogram
	RefreshPhase      *prometheus.HistogramVec
	RefreshErrors     prometheus.Counter
	NotifyReceived    prometheus.Counter
	ListenErrors      prometheus.Counter
	VisibilityLatency prometheus.Histogram

	// FeedLag is how many seqs the readable log trails the mirror by.
	FeedLag    prometheus.GaugeFunc
	FeedPhase  *prometheus.HistogramVec
	FeedEvents prometheus.Counter
	FeedErrors prometheus.Counter

	// The follower's sources for the gauge funcs, read at scrape time so a
	// gauge keeps growing while a tick or a feed hangs.
	lag, refreshRunning, feedLag atomic.Pointer[func() float64]
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
			Help:    "Duration of follower ticks that changed the mirror, including footer fetches. The readable log is fed separately (feed_phase_duration_seconds).",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 19),
		}),
		RefreshPhase: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name:    "refresh_phase_duration_seconds",
			Help:    "Duration of each catalog refresh phase: read (every tick), build and manifest_publish (ticks that changed the mirror), manifest_prepare (the first steady-state tick).",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 19),
		}, []string{"phase"}),
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
		FeedPhase: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name:    "feed_phase_duration_seconds",
			Help:    "Duration of each readable-log feed window's phases: fetch_decode (concurrent block fetch, verify, and decode) and append.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 19),
		}, []string{"phase"}),
		FeedEvents: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "feed_events_total",
			Help: "Events the follower appended to the readable log.",
		}),
		FeedErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "feed_errors_total",
			Help: "Readable-log feed passes that failed; the next tick retries.",
		}),
	}
	m.Lag = gaugeFunc(&m.lag, "lag_seconds", "Now minus the start of the follower's last successful refresh.")
	m.RefreshRunning = gaugeFunc(&m.refreshRunning, "refresh_running_seconds", "How long the follower's in-flight refresh has been running; 0 between refreshes.")
	m.FeedLag = gaugeFunc(&m.feedLag, "feed_lag_events", "The mirror's Main tip minus the readable log's tip.")
	reg.MustRegister(m.Revision, m.Lag, m.RefreshRunning, m.RefreshDuration,
		m.RefreshPhase, m.RefreshErrors, m.NotifyReceived, m.ListenErrors,
		m.VisibilityLatency, m.FeedLag, m.FeedPhase, m.FeedEvents, m.FeedErrors)
	return m
}

func gaugeFunc(src *atomic.Pointer[func() float64], name, help string) prometheus.GaugeFunc {
	return prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: metricsNamespace, Subsystem: metricsSubsystem,
		Name: name, Help: help,
	}, func() float64 {
		if f := src.Load(); f != nil {
			return (*f)()
		}
		return 0
	})
}

func (m *Metrics) setSources(lag, refreshRunning, feedLag func() float64) {
	if m != nil {
		m.lag.Store(&lag)
		m.refreshRunning.Store(&refreshRunning)
		m.feedLag.Store(&feedLag)
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

func (m *Metrics) observeRefreshPhase(phase string, seconds float64) {
	if m != nil {
		m.RefreshPhase.WithLabelValues(phase).Observe(seconds)
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

func (m *Metrics) observeFeedPhase(phase string, seconds float64) {
	if m != nil {
		m.FeedPhase.WithLabelValues(phase).Observe(seconds)
	}
}

func (m *Metrics) fedEvents(n int) {
	if m != nil {
		m.FeedEvents.Add(float64(n))
	}
}

func (m *Metrics) feedFailed() {
	if m != nil {
		m.FeedErrors.Inc()
	}
}
