package subscribe

import "github.com/prometheus/client_golang/prometheus"

const (
	metricsNamespace = "jetstream"
	metricsSubsystem = "subscribe"
)

// Metrics owns the prometheus series for the subscribe package. A nil
// *Metrics is a valid zero-value: every method is a no-op, so tests can
// skip metric registration entirely. Mirrors the convention in
// internal/ingest/live/metrics.go.
type Metrics struct {
	Subscribers         prometheus.Gauge
	CleanDisconnects    prometheus.Counter
	EventsSent          prometheus.Counter
	EventsSkippedSync   prometheus.Counter
	EventsSkippedResync prometheus.Counter
	EncodeErrors        prometheus.Counter

	// Added in 2026-05-27 v1-filtering port:
	EventsFiltered      prometheus.Counter
	EventsOversize      prometheus.Counter
	OptionsUpdates      prometheus.Counter
	OptionsUpdateErrors *prometheus.CounterVec

	// Added in 2026-05-28 v1-cursor port:
	CursorRequests       *prometheus.CounterVec
	CursorResolveSeconds prometheus.Histogram

	// Pull-fanout series (2026-05-31):
	HotReads         prometheus.Counter
	ColdReads        prometheus.Counter
	AdversarialDrops prometheus.Counter
}

// NewMetrics registers the subscribe series against reg. Calls
// reg.MustRegister, which panics if these are already registered.
// Construct exactly once per process.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Subscribers: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "subscribers",
			Help: "Current number of connected /subscribe websocket clients.",
		}),
		CleanDisconnects: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "clean_disconnects_total",
			Help: "Number of /subscribe connections closed by the client or normal shutdown.",
		}),
		EventsSent: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "events_sent_total",
			Help: "Number of JSON frames the handler has written to its websocket clients.",
		}),
		EventsSkippedSync: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name:        "events_skipped_total",
			Help:        "Events deliberately not emitted on the v1-compat wire format.",
			ConstLabels: prometheus.Labels{"reason": "sync"},
		}),
		EventsSkippedResync: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name:        "events_skipped_total",
			Help:        "Events deliberately not emitted on the v1-compat wire format.",
			ConstLabels: prometheus.Labels{"reason": "resync_replacement"},
		}),
		EncodeErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "encode_errors_total",
			Help: "Number of segment.Events the encoder failed to render to JSON.",
		}),
		EventsFiltered: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "events_filtered_total",
			Help: "Events the per-subscriber Filter dropped before encoding (Wants returned false).",
		}),
		EventsOversize: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "events_oversize_total",
			Help: "Encoded frames dropped because their size exceeded the subscriber's maxMessageSizeBytes.",
		}),
		OptionsUpdates: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "options_updates_total",
			Help: "Number of successful options_update messages applied to a connected subscriber.",
		}),
		OptionsUpdateErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: metricsNamespace, Subsystem: metricsSubsystem,
				Name: "options_update_errors_total",
				Help: "Subscriber-sourced messages rejected. Reason label is one of: oversize, bad_envelope_json, bad_payload_json, invalid_options.",
			},
			[]string{"reason"},
		),
		CursorRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "cursor_requests_total",
			Help: "Number of /subscribe connections by cursor resolution mode. Mode is one of: live, seq, time_us, clamped, disabled, unavailable, too_old (v2 seq cursor below the lookback floor, rejected with HTTP 400), resolve_failed (server-side segment read/decode fault during timestamp translation, returned as HTTP 503).",
		}, []string{"mode"}),
		CursorResolveSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name:    "cursor_resolve_seconds",
			Help:    "Wall-clock duration of ResolveCursor (parse + manifest lookup + optional block scan).",
			Buckets: prometheus.ExponentialBuckets(0.0001, 4, 8),
		}),
		HotReads: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "hot_reads_total",
			Help: "Number of ReadFrom calls served from the writer readable log.",
		}),
		ColdReads: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "cold_reads_total",
			Help: "Number of ReadFrom calls that fell through to the cold (disk) reader.",
		}),
		AdversarialDrops: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "adversarial_drops_total",
			Help: "Number of /subscribe connections dropped by the adversarially-slow detector.",
		}),
	}
	reg.MustRegister(
		m.Subscribers, m.CleanDisconnects,
		m.EventsSent, m.EventsSkippedSync, m.EventsSkippedResync, m.EncodeErrors,
		m.EventsFiltered, m.EventsOversize,
		m.OptionsUpdates, m.OptionsUpdateErrors,
		m.CursorRequests, m.CursorResolveSeconds,
		m.HotReads, m.ColdReads, m.AdversarialDrops,
	)
	return m
}

func (m *Metrics) incSubscribers() {
	if m != nil {
		m.Subscribers.Inc()
	}
}
func (m *Metrics) decSubscribers() {
	if m != nil {
		m.Subscribers.Dec()
	}
}
func (m *Metrics) incCleanDisconnects() {
	if m != nil {
		m.CleanDisconnects.Inc()
	}
}
func (m *Metrics) incEventsSent() {
	if m != nil {
		m.EventsSent.Inc()
	}
}
func (m *Metrics) incEventsSkippedSync() {
	if m != nil {
		m.EventsSkippedSync.Inc()
	}
}
func (m *Metrics) incEventsSkippedResync() {
	if m != nil {
		m.EventsSkippedResync.Inc()
	}
}
func (m *Metrics) incEncodeErrors() {
	if m != nil {
		m.EncodeErrors.Inc()
	}
}
func (m *Metrics) incEventsFiltered() {
	if m != nil {
		m.EventsFiltered.Inc()
	}
}
func (m *Metrics) incEventsOversize() {
	if m != nil {
		m.EventsOversize.Inc()
	}
}
func (m *Metrics) incOptionsUpdates() {
	if m != nil {
		m.OptionsUpdates.Inc()
	}
}

// Reasons for incOptionsUpdateError. Defined as constants so callers can't
// drift the label cardinality. Kept unexported — these are pinned to the
// handler in this package; widening the surface invites accidental drift.
const (
	optionsUpdateErrorReasonOversize        = "oversize"
	optionsUpdateErrorReasonBadEnvelopeJSON = "bad_envelope_json"
	optionsUpdateErrorReasonBadPayloadJSON  = "bad_payload_json"
	optionsUpdateErrorReasonInvalidOptions  = "invalid_options"
)

func (m *Metrics) incOptionsUpdateError(reason string) {
	if m != nil {
		m.OptionsUpdateErrors.WithLabelValues(reason).Inc()
	}
}

func (m *Metrics) incCursorRequests(mode string) {
	if m != nil {
		m.CursorRequests.WithLabelValues(mode).Inc()
	}
}

func (m *Metrics) observeCursorResolveSeconds(d float64) {
	if m != nil {
		m.CursorResolveSeconds.Observe(d)
	}
}

func (m *Metrics) incHotReads() {
	if m == nil {
		return
	}
	m.HotReads.Inc()
}
func (m *Metrics) incColdReads() {
	if m == nil {
		return
	}
	m.ColdReads.Inc()
}
func (m *Metrics) incAdversarialDrops() {
	if m == nil {
		return
	}
	m.AdversarialDrops.Inc()
}
