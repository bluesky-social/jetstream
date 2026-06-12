package live

import "github.com/prometheus/client_golang/prometheus"

const (
	metricsNamespace = "jetstream"
	metricsSubsystem = "livestream"
)

// Metrics owns the prometheus counters and gauges for the livestream
// consumer. A nil *Metrics is a valid zero-value: every method is a
// no-op, so tests can skip metric registration entirely.
type Metrics struct {
	EventsReceived         prometheus.Counter
	Reconnects             prometheus.Counter
	DecodeErrors           prometheus.Counter
	UnknownEvents          prometheus.Counter
	DroppedOpsMissingBlock prometheus.Counter
	DroppedEvents          prometheus.Counter
	StaleResyncsDropped    prometheus.Counter
	UpstreamCursor         prometheus.Gauge
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
		DroppedOpsMissingBlock: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "dropped_ops_missing_block_total",
			Help: "Number of create/update ops dropped because the upstream commit's " +
				"CAR diff omitted the referenced record block. The rest of the commit " +
				"is still archived; a non-zero rate signals upstream PDSes shipping " +
				"incomplete CARs (partial CARs are spec-permitted but unarchivable).",
		}),
		DroppedEvents: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "dropped_events_total",
			Help: "Number of upstream events skipped because their fields cannot be represented in the segment format.",
		}),
		StaleResyncsDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "stale_resyncs_dropped_total",
			Help: "Number of async resync events dropped because the verifier chain rev had " +
				"already advanced past the resync's rev (delivery-order guard; the affected " +
				"DID's tombstone coverage waits for its next divergence).",
		}),
		UpstreamCursor: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "upstream_cursor",
			Help: "Last persisted upstream relay cursor.",
		}),
	}
	reg.MustRegister(
		m.EventsReceived, m.Reconnects,
		m.DecodeErrors, m.UnknownEvents, m.DroppedOpsMissingBlock,
		m.DroppedEvents, m.StaleResyncsDropped, m.UpstreamCursor,
	)
	return m
}

func (m *Metrics) incEventsReceived() {
	if m != nil {
		m.EventsReceived.Inc()
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

func (m *Metrics) addDroppedOpsMissingBlock(n int) {
	if m != nil && n > 0 {
		m.DroppedOpsMissingBlock.Add(float64(n))
	}
}

func (m *Metrics) incDroppedEvents() {
	if m != nil {
		m.DroppedEvents.Inc()
	}
}

func (m *Metrics) incStaleResyncsDropped() {
	if m != nil {
		m.StaleResyncsDropped.Inc()
	}
}

func (m *Metrics) setUpstreamCursor(v int64) {
	if m != nil {
		m.UpstreamCursor.Set(float64(v))
	}
}
