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
	Subscribers         *prometheus.GaugeVec
	CleanDisconnects    prometheus.Counter
	EventsSent          *prometheus.CounterVec
	EventsSkippedSync   prometheus.Counter
	EventsSkippedResync prometheus.Counter
	EncodeErrors        prometheus.Counter

	// Compression observability (2026-07-09, #294): payload bytes written
	// and pre-compression JSON bytes, labeled by negotiated scheme, so the
	// scheme population mix, per-scheme egress, and the zstd compression
	// ratio are all visible in production.
	BytesSent    *prometheus.CounterVec
	BytesEncoded *prometheus.CounterVec

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
		Subscribers: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "subscribers",
			Help: "Current number of connected /subscribe websocket clients, by negotiated compression scheme (none, deflate, zstd).",
		}, []string{"compression"}),
		CleanDisconnects: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "clean_disconnects_total",
			Help: "Number of /subscribe connections closed by the client or normal shutdown.",
		}),
		EventsSent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "events_sent_total",
			Help: "Number of JSON frames the handler has written to its websocket clients, by negotiated compression scheme.",
		}, []string{"compression"}),
		BytesSent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "bytes_sent_total",
			Help: "Websocket payload bytes written to /subscribe clients, by negotiated compression scheme. For scheme=zstd this is the post-compression frame size; deflate compresses inside the websocket library, so scheme=deflate counts pre-compression bytes (kernel-level egress is the authoritative wire measure there).",
		}, []string{"compression"}),
		BytesEncoded: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "bytes_encoded_total",
			Help: "Pre-compression JSON bytes of frames written to /subscribe clients, by negotiated compression scheme. bytes_sent_total/bytes_encoded_total is the delivered compression ratio for scheme=zstd.",
		}, []string{"compression"}),
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
		m.EventsSent, m.BytesSent, m.BytesEncoded,
		m.EventsSkippedSync, m.EventsSkippedResync, m.EncodeErrors,
		m.EventsFiltered, m.EventsOversize,
		m.OptionsUpdates, m.OptionsUpdateErrors,
		m.CursorRequests, m.CursorResolveSeconds,
		m.HotReads, m.ColdReads, m.AdversarialDrops,
	)
	return m
}

// Compression scheme labels. Pinned as constants so callers can't drift
// the label cardinality (mirrors the optionsUpdateErrorReason convention).
const (
	compressionSchemeNone    = "none"
	compressionSchemeDeflate = "deflate"
	compressionSchemeZstd    = "zstd"
)

func (m *Metrics) incSubscribers(scheme string) {
	if m != nil {
		m.Subscribers.WithLabelValues(scheme).Inc()
	}
}
func (m *Metrics) decSubscribers(scheme string) {
	if m != nil {
		m.Subscribers.WithLabelValues(scheme).Dec()
	}
}
func (m *Metrics) incCleanDisconnects() {
	if m != nil {
		m.CleanDisconnects.Inc()
	}
}

// incEventsSent records one delivered frame: the frame count, the payload
// bytes actually handed to the websocket write (post-zstd for scheme=zstd),
// and the pre-compression JSON size, all under the connection's scheme label.
func (m *Metrics) incEventsSent(scheme string, payloadBytes, encodedBytes int) {
	if m != nil {
		m.EventsSent.WithLabelValues(scheme).Inc()
		m.BytesSent.WithLabelValues(scheme).Add(float64(payloadBytes))
		m.BytesEncoded.WithLabelValues(scheme).Add(float64(encodedBytes))
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
