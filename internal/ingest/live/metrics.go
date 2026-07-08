package live

import (
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricsNamespace = "jetstream"
	metricsSubsystem = "livestream"
)

// Metrics owns the prometheus counters and gauges for the livestream
// consumer. A nil *Metrics is a valid zero-value: every method is a
// no-op, so tests can skip metric registration entirely.
type Metrics struct {
	EventsReceived                 prometheus.Counter
	Reconnects                     prometheus.Counter
	DecodeErrors                   prometheus.Counter
	SequenceGaps                   prometheus.Counter
	SequenceGapMissedSeqs          prometheus.Counter
	UnknownEvents                  prometheus.Counter
	VerifyQueueDrops               prometheus.Counter
	StreamErrorFrames              *prometheus.CounterVec
	StaleResyncsDropped            prometheus.Counter
	ReplayedAccountsDrop           prometheus.Counter
	ReplayedIdentityDrop           prometheus.Counter
	UpstreamCursor                 prometheus.Gauge
	LastSeenUpstreamEventTimestamp prometheus.Gauge

	lastSeenUpstreamEventUnix atomic.Int64
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
			Help: "Number of upstream frames or events this build does not recognize " +
				"(unknown frame type/op from the relay, or an event kind ConvertEvent " +
				"cannot map). Each one is an archival hole a jetstream upgrade would fix: " +
				"the seq is consumed upstream but nothing is stored. The watermark cursor " +
				"advances past them once later events flush, so they are NOT re-delivered " +
				"to a future build unless it re-backfills the range.",
		}),
		VerifyQueueDrops: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "verify_queue_dropped_events_total",
			Help: "Number of upstream events atmos's parallel verifier discarded because " +
				"a single DID overflowed its per-DID verify queue (drop-oldest, cap = " +
				"Parallelism*2). Each one is PERMANENT archival loss for that DID: the " +
				"event was received but never verified or stored, and the watermark " +
				"cursor advances past it, so no reconnect re-delivers it. Includes " +
				"coalesced drops (DropError.AdditionalDropsSuppressed). A sustained " +
				"rate means one hot account is outrunning verification — distinct from " +
				"decode_errors_total (malformed frames) and sequence_gaps_total " +
				"(relay-side loss).",
		}),
		StreamErrorFrames: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "stream_error_frames_total",
			Help: "Number of op=-1 error frames received from the relay, labeled by " +
				"machine-readable code. Codes the subscribeRepos lexicon defines " +
				"(FutureCursor, ConsumerTooSlow) keep their name; anything else " +
				"collapses to \"other\" (or \"missing\" when empty) because the code " +
				"is relay-controlled input — the raw value is in the log line. The " +
				"relay usually closes the connection after sending one; atmos " +
				"reconnects with backoff. A steadily climbing FutureCursor count " +
				"means our persisted cursor is ahead of the relay (cursor corruption " +
				"or a relay restored from backup) — that loop never self-resolves " +
				"and needs an operator.",
		}, []string{"code"}),
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
		LastSeenUpstreamEventTimestamp: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "last_seen_upstream_event_timestamp_seconds",
			Help: "Unix timestamp in seconds when steady-state live ingest last observed an upstream subscribeRepos event.",
		}),
	}
	reg.MustRegister(
		m.EventsReceived, m.Reconnects,
		m.DecodeErrors, m.SequenceGaps, m.SequenceGapMissedSeqs,
		m.UnknownEvents, m.VerifyQueueDrops, m.StreamErrorFrames, m.StaleResyncsDropped,
		m.ReplayedAccountsDrop, m.ReplayedIdentityDrop, m.UpstreamCursor, m.LastSeenUpstreamEventTimestamp,
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

func (m *Metrics) noteVerifyQueueDrops(n uint64) {
	if m != nil && n > 0 {
		m.VerifyQueueDrops.Add(float64(n))
	}
}

func (m *Metrics) incStreamErrorFrames(code string) {
	if m != nil {
		m.StreamErrorFrames.WithLabelValues(normalizeStreamErrorCode(code)).Inc()
	}
}

// normalizeStreamErrorCode bounds the metric's label space. The code
// arrives verbatim from the relay's op=-1 frame — untrusted wire input —
// so passing it through would let a faulty or hostile relay mint one
// time series per distinct string. Only the codes the
// com.atproto.sync.subscribeRepos lexicon defines keep their name; the
// raw value still reaches the log line in noteStreamError.
func normalizeStreamErrorCode(code string) string {
	switch code {
	case "FutureCursor", "ConsumerTooSlow":
		return code
	case "":
		return "missing"
	default:
		return "other"
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

// NoteLastSeenUpstreamEvent records the wall-clock time at which the
// steady-state firehose consumer observed a real upstream relay event.
func (m *Metrics) NoteLastSeenUpstreamEvent(t time.Time) {
	if m == nil || t.IsZero() {
		return
	}
	sec := t.Unix()
	m.lastSeenUpstreamEventUnix.Store(sec)
	m.LastSeenUpstreamEventTimestamp.Set(float64(sec))
}

// LastSeenUpstreamEvent returns the last timestamp recorded by
// NoteLastSeenUpstreamEvent. A zero time means no event has been observed.
func (m *Metrics) LastSeenUpstreamEvent() time.Time {
	if m == nil {
		return time.Time{}
	}
	sec := m.lastSeenUpstreamEventUnix.Load()
	if sec == 0 {
		return time.Time{}
	}
	return time.Unix(sec, 0).UTC()
}
