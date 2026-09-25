package ingest

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricsNamespace = "jetstream"
	metricsSubsystem = "ingest"
)

// Metrics owns the prometheus counters and gauges for the ingest
// writer. A nil *Metrics is a valid zero-value: every method is a
// no-op, which lets tests skip metric registration entirely.
type Metrics struct {
	EventsAppended            prometheus.Counter
	BlocksFlushed             prometheus.Counter
	SegmentsRotated           prometheus.Counter
	AppendErrors              prometheus.Counter
	ActiveSegBytes            prometheus.Gauge
	NextSeq                   prometheus.Gauge
	ReadLogBytes              prometheus.Gauge
	ReadLogPinnedBytes        prometheus.Gauge
	ReadLogPinnedOverrunBytes prometheus.Gauge
	ReadLogFloorSeq           prometheus.Gauge
	ReadLogDurableSeq         prometheus.Gauge
	SeqReservedEnd            prometheus.Gauge
	SeqReservationHeadroom    prometheus.Gauge
	SeqGapCount               prometheus.Gauge
	SeqGapWidth               prometheus.Gauge
	SeqGapsRegistered         prometheus.Counter
	SeqGapValuesRegistered    prometheus.Counter
	// HotBatches and HotBatchEvents are hot mode's committed batches (design
	// §23), labelled by admission class and inline or pointer storage.
	HotBatches     *prometheus.CounterVec
	HotBatchEvents prometheus.Histogram
}

// NewMetrics registers the ingest counters/gauges against reg.
// Calls reg.MustRegister, which panics if these are already registered.
// Construct exactly once per process.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		EventsAppended: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "events_appended_total",
			Help: "Number of events successfully appended to the active segment.",
		}),
		BlocksFlushed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "blocks_flushed_total",
			Help: "Number of zstd-framed blocks fsynced into the active segment.",
		}),
		SegmentsRotated: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "segments_rotated_total",
			Help: "Number of active segments sealed and rotated to the next index.",
		}),
		AppendErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "append_errors_total",
			Help: "Number of Writer.Append calls that returned a non-nil error.",
		}),
		ActiveSegBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "active_segment_bytes",
			Help: "Compressed-bytes-since-header counter for the active segment file.",
		}),
		NextSeq: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "next_seq",
			Help: "Next seq number the writer will allocate.",
		}),
		ReadLogBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "readable_log_bytes",
			Help: "Approximate bytes retained in the writer readable log.",
		}),
		ReadLogPinnedBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "readable_log_pinned_bytes",
			Help: "Approximate readable-log bytes at or above the durable watermark and therefore not evictable.",
		}),
		ReadLogPinnedOverrunBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "readable_log_pinned_overrun_bytes",
			Help: "Approximate pinned readable-log bytes beyond the configured retention budget.",
		}),
		ReadLogFloorSeq: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "readable_log_floor_seq",
			Help: "Oldest seq currently resident in the writer readable log.",
		}),
		ReadLogDurableSeq: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "readable_log_durable_seq",
			Help: "One-past-newest durable seq known to the writer readable log.",
		}),
		SeqReservedEnd: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "seq_reserved_end",
			Help: "Exclusive end of the durable write-ahead sequence lease; zero for writers that do not expose seqs to clients.",
		}),
		SeqReservationHeadroom: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "seq_reservation_headroom",
			Help: "Number of client-visible sequence values remaining in the current durable lease.",
		}),
		SeqGapCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "seq_gap_count",
			Help: "Number of normalized durable sequence-vacancy intervals registered after unclean exits.",
		}),
		SeqGapWidth: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "seq_gap_values",
			Help: "Total number of sequence values covered by durable registered vacancies.",
		}),
		SeqGapsRegistered: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "seq_gaps_registered_total",
			Help: "Number of abandoned sequence leases registered by this process at startup.",
		}),
		SeqGapValuesRegistered: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "seq_gap_values_registered_total",
			Help: "Number of sequence values newly registered as vacant by this process at startup.",
		}),
		HotBatches: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: "hot",
			Name: "batches_total",
			Help: "Number of hot batches committed, by admission class and storage (inline or pointer).",
		}, []string{"class", "storage"}),
		HotBatchEvents: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Subsystem: "hot",
			Name:    "batch_events",
			Help:    "Events per committed hot batch.",
			Buckets: []float64{1, 4, 16, 64, 128, 256, 512, 1024, 4096},
		}),
	}
	reg.MustRegister(
		m.EventsAppended, m.BlocksFlushed, m.SegmentsRotated,
		m.AppendErrors, m.ActiveSegBytes, m.NextSeq,
		m.ReadLogBytes, m.ReadLogPinnedBytes, m.ReadLogPinnedOverrunBytes,
		m.ReadLogFloorSeq, m.ReadLogDurableSeq,
		m.SeqReservedEnd, m.SeqReservationHeadroom,
		m.SeqGapCount, m.SeqGapWidth,
		m.SeqGapsRegistered, m.SeqGapValuesRegistered,
		m.HotBatches, m.HotBatchEvents,
	)
	return m
}

func (m *Metrics) setSeqLease(reservedEnd, nextSeq uint64, gapCount int, gapWidth uint64) {
	if m == nil {
		return
	}
	m.SeqReservedEnd.Set(float64(reservedEnd))
	headroom := uint64(0)
	if reservedEnd > nextSeq {
		headroom = reservedEnd - nextSeq
	}
	m.SeqReservationHeadroom.Set(float64(headroom))
	m.SeqGapCount.Set(float64(gapCount))
	m.SeqGapWidth.Set(float64(gapWidth))
}

func (m *Metrics) setSeqReservationHeadroom(reservedEnd, nextSeq uint64) {
	if m == nil {
		return
	}
	headroom := uint64(0)
	if reservedEnd > nextSeq {
		headroom = reservedEnd - nextSeq
	}
	m.SeqReservationHeadroom.Set(float64(headroom))
}

func (m *Metrics) incSeqGapRegistered(width uint64) {
	if m == nil {
		return
	}
	m.SeqGapsRegistered.Inc()
	m.SeqGapValuesRegistered.Add(float64(width))
}

// Nil-safe inc/set helpers. callers in writer.go don't have to repeat
// the nil check; tests can pass *Metrics(nil) to skip registration.
func (m *Metrics) incEventsAppended() {
	if m != nil {
		m.EventsAppended.Inc()
	}
}

func (m *Metrics) incBlocksFlushed() {
	if m != nil {
		m.BlocksFlushed.Inc()
	}
}

func (m *Metrics) incSegmentsRotated() {
	if m != nil {
		m.SegmentsRotated.Inc()
	}
}

func (m *Metrics) incAppendErrors() {
	if m != nil {
		m.AppendErrors.Inc()
	}
}

func (m *Metrics) setActiveSegBytes(v int64) {
	if m != nil {
		m.ActiveSegBytes.Set(float64(v))
	}
}

func (m *Metrics) setNextSeq(v uint64) {
	if m != nil {
		m.NextSeq.Set(float64(v))
	}
}

func (m *Metrics) setReadLogBytes(v int64) {
	if m != nil {
		m.ReadLogBytes.Set(float64(v))
	}
}

func (m *Metrics) setReadLogPinnedBytes(v int64) {
	if m != nil {
		m.ReadLogPinnedBytes.Set(float64(v))
	}
}

func (m *Metrics) setReadLogPinnedOverrunBytes(v int64) {
	if m != nil {
		m.ReadLogPinnedOverrunBytes.Set(float64(v))
	}
}

func (m *Metrics) setReadLogFloorSeq(v uint64) {
	if m != nil {
		m.ReadLogFloorSeq.Set(float64(v))
	}
}

func (m *Metrics) setReadLogDurableSeq(v uint64) {
	if m != nil {
		m.ReadLogDurableSeq.Set(float64(v))
	}
}

func (m *Metrics) observeHotBatch(class Class, pointer bool, events int) {
	if m == nil {
		return
	}
	storage := "inline"
	if pointer {
		storage = "pointer"
	}
	m.HotBatches.WithLabelValues(class.String(), storage).Inc()
	m.HotBatchEvents.Observe(float64(events))
}
