package repair

import "github.com/prometheus/client_golang/prometheus"

const (
	metricsNamespace = "jetstream"
	metricsSubsystem = "repair"
)

// Metrics owns Prometheus counters and gauges for the unified per-DID repair
// pipeline. A nil *Metrics is a safe no-op.
type Metrics struct {
	ActiveGatedDIDs  prometheus.Gauge
	JobsEnqueued     *prometheus.CounterVec
	JobsCompleted    *prometheus.CounterVec
	JobsResumed      prometheus.Counter
	InboxCaptured    prometheus.Counter
	InboxDeduped     prometheus.Counter
	InboxDrained     prometheus.Counter
	InboxSkipped     prometheus.Counter
	CutoffDropped    prometheus.Counter
	BulkChunks       prometheus.Counter
	BulkRowsAppended prometheus.Counter
	StagedBytes      prometheus.Counter
}

// NewMetrics registers repair metrics on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		ActiveGatedDIDs: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "active_gated_dids",
			Help: "Number of DIDs currently behind the per-DID repair gate (routing live events to their durable inbox).",
		}),
		JobsEnqueued: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "jobs_enqueued_total",
			Help: "Number of repair jobs enqueued, labeled by source (async_resync, sync_event, failed_retry).",
		}, []string{"source"}),
		JobsCompleted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "jobs_completed_total",
			Help: "Number of repair jobs finished, labeled by outcome (succeeded, aborted, failed).",
		}, []string{"outcome"}),
		JobsResumed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "jobs_resumed_total",
			Help: "Number of in-progress repair jobs restored across a restart or leader failover.",
		}),
		InboxCaptured: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "inbox_groups_captured_total",
			Help: "Number of live event groups captured into a gated DID's durable inbox.",
		}),
		InboxDeduped: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "inbox_groups_deduped_total",
			Help: "Number of duplicate live event groups skipped by per-DID inbox seq deduplication.",
		}),
		InboxDrained: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "inbox_groups_drained_total",
			Help: "Number of durable inbox groups appended to the archive after replacement.",
		}),
		InboxSkipped: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "inbox_groups_skipped_total",
			Help: "Number of durable inbox groups skipped because their rev was already covered by the repair snapshot.",
		}),
		CutoffDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "cutoff_events_dropped_total",
			Help: "Number of stale live commit groups dropped because their rev was at or below a completed repair cutoff.",
		}),
		BulkChunks: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "bulk_chunks_total",
			Help: "Number of bounded ClassBulk replacement chunks appended by the repair worker.",
		}),
		BulkRowsAppended: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "bulk_rows_appended_total",
			Help: "Number of replacement rows appended via ClassBulk by the repair worker.",
		}),
		StagedBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "staged_bytes_total",
			Help: "Total bytes written to immutable staged replacement artifacts.",
		}),
	}
	if reg != nil {
		reg.MustRegister(
			m.ActiveGatedDIDs,
			m.JobsEnqueued,
			m.JobsCompleted,
			m.JobsResumed,
			m.InboxCaptured,
			m.InboxDeduped,
			m.InboxDrained,
			m.InboxSkipped,
			m.CutoffDropped,
			m.BulkChunks,
			m.BulkRowsAppended,
			m.StagedBytes,
		)
	}
	return m
}

func (m *Metrics) setActiveGated(n int) {
	if m != nil {
		m.ActiveGatedDIDs.Set(float64(n))
	}
}

func (m *Metrics) incEnqueued(src Source) {
	if m != nil {
		m.JobsEnqueued.WithLabelValues(string(src)).Inc()
	}
}

func (m *Metrics) incCompleted(outcome string) {
	if m != nil {
		m.JobsCompleted.WithLabelValues(outcome).Inc()
	}
}

func (m *Metrics) incResumed() {
	if m != nil {
		m.JobsResumed.Inc()
	}
}

func (m *Metrics) incInboxCaptured() {
	if m != nil {
		m.InboxCaptured.Inc()
	}
}

func (m *Metrics) incInboxDeduped() {
	if m != nil {
		m.InboxDeduped.Inc()
	}
}

func (m *Metrics) incInboxDrained() {
	if m != nil {
		m.InboxDrained.Inc()
	}
}

func (m *Metrics) incInboxSkipped() {
	if m != nil {
		m.InboxSkipped.Inc()
	}
}

func (m *Metrics) incCutoffDropped() {
	if m != nil {
		m.CutoffDropped.Inc()
	}
}

func (m *Metrics) incBulkChunk(rows int) {
	if m != nil {
		m.BulkChunks.Inc()
		m.BulkRowsAppended.Add(float64(rows))
	}
}

func (m *Metrics) addStagedBytes(n int64) {
	if m != nil && n > 0 {
		m.StagedBytes.Add(float64(n))
	}
}
