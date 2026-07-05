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
	EventsReceived        prometheus.Counter
	Reconnects            prometheus.Counter
	DecodeErrors          prometheus.Counter
	SequenceGaps          prometheus.Counter
	SequenceGapMissedSeqs prometheus.Counter
	UnknownEvents         prometheus.Counter
	StaleResyncsDropped   prometheus.Counter
	ReplayedAccountsDrop  prometheus.Counter
	ReplayedIdentityDrop  prometheus.Counter
	UpstreamCursor        prometheus.Gauge
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
			Help: "Number of upstream frames that failed to decode (garbage or " +
				"malformed frames). Relay-side sequence gaps are counted separately " +
				"in sequence_gaps_total.",
		}),
		SequenceGaps: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "sequence_gaps_total",
			Help: "Number of forward gaps observed in the upstream relay's seq stream. " +
				"A non-zero rate means the relay skipped seqs we never received — " +
				"upstream data loss, not local decode trouble.",
		}),
		SequenceGapMissedSeqs: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "sequence_gap_missed_seqs_total",
			Help: "Total count of upstream seqs skipped across all observed sequence " +
				"gaps (sum of gap widths). Sizes the loss that sequence_gaps_total counts.",
		}),
		UnknownEvents: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "unknown_events_total",
			Help: "Number of upstream events whose kind ConvertEvent did not recognize. " +
				"These do NOT advance the upstream cursor so a future build can replay them.",
		}),
		ReplayedAccountsDrop: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "replayed_account_events_dropped_total",
			Help: "Number of #account events dropped because their upstream seq was at or " +
				"below the DID's applied hosting-state seq — relay seq replays " +
				"(duplicate or regressed streams) whose row is already archived. " +
				"Re-archiving them would let a stale account-delete land above newer rows.",
		}),
		ReplayedIdentityDrop: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "replayed_identity_events_dropped_total",
			Help: "Number of #identity events dropped because their upstream seq was at or " +
				"below the DID's applied identity seq — relay seq replays whose row is " +
				"already archived. Re-archiving them would bloat the immutable archive " +
				"with duplicate rows.",
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
		m.DecodeErrors, m.SequenceGaps, m.SequenceGapMissedSeqs,
		m.UnknownEvents, m.StaleResyncsDropped,
		m.ReplayedAccountsDrop, m.ReplayedIdentityDrop, m.UpstreamCursor,
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

func (m *Metrics) noteSequenceGap(missed int64) {
	if m != nil {
		m.SequenceGaps.Inc()
		if missed > 0 {
			m.SequenceGapMissedSeqs.Add(float64(missed))
		}
	}
}

func (m *Metrics) incUnknownEvents() {
	if m != nil {
		m.UnknownEvents.Inc()
	}
}

func (m *Metrics) incReplayedAccountEventsDropped() {
	if m != nil {
		m.ReplayedAccountsDrop.Inc()
	}
}

func (m *Metrics) incReplayedIdentityEventsDropped() {
	if m != nil {
		m.ReplayedIdentityDrop.Inc()
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
