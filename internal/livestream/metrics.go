package livestream

import "github.com/prometheus/client_golang/prometheus"

const (
	metricsNamespace = "jetstream"
	metricsSubsystem = "livestream"
)

// Metrics owns the prometheus counters and gauges for the livestream
// consumer. A nil *Metrics is a valid zero-value: every method is a
// no-op, so tests can skip metric registration entirely.
type Metrics struct {
	EventsReceived  prometheus.Counter
	EventsConverted prometheus.Counter
	Reconnects      prometheus.Counter
	DecodeErrors    prometheus.Counter
	UnknownEvents   prometheus.Counter
	UpstreamCursor  prometheus.Gauge
}

// NewMetrics registers the livestream counters/gauges against reg.
// Calls reg.MustRegister, which panics if these are already
// registered. Construct exactly once per process.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		EventsReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "events_received_total",
			Help: "Number of upstream firehose events the consumer decoded successfully.",
		}),
		EventsConverted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "events_converted_total",
			Help: "Number of segment.Events emitted by the converter (one per record op for commits, one per non-commit event).",
		}),
		Reconnects: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "reconnects_total",
			Help: "Number of websocket reconnect attempts the atmos client has made.",
		}),
		DecodeErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "decode_errors_total",
			Help: "Number of upstream frames that failed to decode.",
		}),
		UnknownEvents: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "unknown_events_total",
			Help: "Number of upstream events whose kind ConvertEvent did not recognize. " +
				"These do NOT advance the upstream cursor so a future build can replay them.",
		}),
		UpstreamCursor: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "upstream_cursor",
			Help: "Last persisted upstream relay cursor.",
		}),
	}
	reg.MustRegister(
		m.EventsReceived, m.EventsConverted, m.Reconnects,
		m.DecodeErrors, m.UnknownEvents, m.UpstreamCursor,
	)
	return m
}

func (m *Metrics) incEventsReceived() {
	if m != nil {
		m.EventsReceived.Inc()
	}
}

func (m *Metrics) incEventsConverted() {
	if m != nil {
		m.EventsConverted.Inc()
	}
}

func (m *Metrics) incReconnects() {
	if m != nil {
		m.Reconnects.Inc()
	}
}

func (m *Metrics) incDecodeErrors() {
	if m != nil {
		m.DecodeErrors.Inc()
	}
}

func (m *Metrics) incUnknownEvents() {
	if m != nil {
		m.UnknownEvents.Inc()
	}
}

func (m *Metrics) setUpstreamCursor(v int64) {
	if m != nil {
		m.UpstreamCursor.Set(float64(v))
	}
}
