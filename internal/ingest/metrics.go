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
	EventsAppended  prometheus.Counter
	BlocksFlushed   prometheus.Counter
	SegmentsRotated prometheus.Counter
	AppendErrors    prometheus.Counter
	ActiveSegBytes  prometheus.Gauge
	NextSeq         prometheus.Gauge
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
	}
	reg.MustRegister(
		m.EventsAppended, m.BlocksFlushed, m.SegmentsRotated,
		m.AppendErrors, m.ActiveSegBytes, m.NextSeq,
	)
	return m
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
