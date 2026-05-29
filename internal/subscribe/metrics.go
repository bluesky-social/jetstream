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
	Subscribers       prometheus.Gauge
	CleanDisconnects  prometheus.Counter
	SlowDrops         prometheus.Counter
	EventsPublished   prometheus.Counter
	EventsSent        prometheus.Counter
	EventsSkippedSync prometheus.Counter
	EncodeErrors      prometheus.Counter
	QueueDepth        prometheus.Histogram

	// Added in 2026-05-27 v1-filtering port:
	EventsFiltered      prometheus.Counter
	EventsOversize      prometheus.Counter
	OptionsUpdates      prometheus.Counter
	OptionsUpdateErrors *prometheus.CounterVec

	// Added in 2026-05-28 v1-cursor port:
	CursorRequests       *prometheus.CounterVec
	CursorResolveSeconds prometheus.Histogram
	LookbackSubscribers  prometheus.Gauge
	LookbackIterations   prometheus.Counter
	RingOverflows        prometheus.Counter
	LookbackSeconds      prometheus.Histogram
	LookbackEvents       prometheus.Counter
	LookbackTerminated   *prometheus.CounterVec
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
		SlowDrops: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "slow_drops_total",
			Help: "Number of /subscribe connections dropped because the per-subscriber buffer overflowed.",
		}),
		EventsPublished: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "events_published_total",
			Help: "Number of segment.Events the broadcaster has fanned out to its subscribers.",
		}),
		EventsSent: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "events_sent_total",
			Help: "Number of JSON frames the handler has written to its websocket clients.",
		}),
		EventsSkippedSync: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name:        "events_skipped_total",
			Help:        "Events deliberately not emitted on the v1-compat wire format (e.g. #sync).",
			ConstLabels: prometheus.Labels{"reason": "sync"},
		}),
		EncodeErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "encode_errors_total",
			Help: "Number of segment.Events the encoder failed to render to JSON.",
		}),
		QueueDepth: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name:    "subscriber_queue_depth",
			Help:    "Distribution of per-subscriber channel depth observed at Publish time.",
			Buckets: prometheus.ExponentialBuckets(1, 4, 9), // 1..65536
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
			Help: "Number of /subscribe connections by cursor resolution mode. Mode is one of: live, seq, time_us, clamped, disabled, unavailable.",
		}, []string{"mode"}),
		CursorResolveSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name:    "cursor_resolve_seconds",
			Help:    "Wall-clock duration of ResolveCursor (parse + manifest lookup + optional block scan).",
			Buckets: prometheus.ExponentialBuckets(0.0001, 4, 8),
		}),
		LookbackSubscribers: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "lookback_subscribers",
			Help: "Current number of subscribers in lookback (cursor-replay) mode.",
		}),
		LookbackIterations: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "lookback_iterations_total",
			Help: "Total ring-overflow restart iterations across all lookback subscribers.",
		}),
		RingOverflows: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "ring_overflows_total",
			Help: "Number of times a lookback subscriber's bounded ring filled.",
		}),
		LookbackSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name:    "lookback_seconds",
			Help:    "End-to-end wall-clock duration of one Replayer.Run.",
			Buckets: prometheus.ExponentialBuckets(0.001, 4, 10),
		}),
		LookbackEvents: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "lookback_events_total",
			Help: "Total events emitted by lookback replay (across the disk walk and ring drain).",
		}),
		LookbackTerminated: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "lookback_terminated_total",
			Help: "Lookback subscribers terminated for reasons other than clean disconnect.",
		}, []string{"reason"}),
	}
	reg.MustRegister(
		m.Subscribers, m.CleanDisconnects, m.SlowDrops,
		m.EventsPublished, m.EventsSent, m.EventsSkippedSync,
		m.EncodeErrors, m.QueueDepth,
		m.EventsFiltered, m.EventsOversize,
		m.OptionsUpdates, m.OptionsUpdateErrors,
		m.CursorRequests, m.CursorResolveSeconds, m.LookbackSubscribers,
		m.LookbackIterations, m.RingOverflows, m.LookbackSeconds,
		m.LookbackEvents, m.LookbackTerminated,
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
func (m *Metrics) incSlowDrops() {
	if m != nil {
		m.SlowDrops.Inc()
	}
}
func (m *Metrics) incEventsPublished() {
	if m != nil {
		m.EventsPublished.Inc()
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
func (m *Metrics) incEncodeErrors() {
	if m != nil {
		m.EncodeErrors.Inc()
	}
}
func (m *Metrics) observeQueueDepth(n int) {
	if m != nil {
		m.QueueDepth.Observe(float64(n))
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

func (m *Metrics) incLookbackSubscribers() {
	if m != nil {
		m.LookbackSubscribers.Inc()
	}
}

func (m *Metrics) decLookbackSubscribers() {
	if m != nil {
		m.LookbackSubscribers.Dec()
	}
}

func (m *Metrics) incLookbackIterations() {
	if m != nil {
		m.LookbackIterations.Inc()
	}
}

func (m *Metrics) incRingOverflows() {
	if m != nil {
		m.RingOverflows.Inc()
	}
}

func (m *Metrics) observeLookbackSeconds(d float64) {
	if m != nil {
		m.LookbackSeconds.Observe(d)
	}
}

func (m *Metrics) incLookbackEvents() {
	if m != nil {
		m.LookbackEvents.Inc()
	}
}

func (m *Metrics) incLookbackTerminated(reason string) {
	if m != nil {
		m.LookbackTerminated.WithLabelValues(reason).Inc()
	}
}
