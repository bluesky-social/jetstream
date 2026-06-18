package orchestrator

import (
	"time"

	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/tombstone"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricsNamespace           = "jetstream"
	metricsSubsystem           = "orchestrator"
	compactionMetricsSubsystem = "compaction"
)

// Phase gauge values. These are stable wire values — operators
// alerting on phase transitions rely on the integer mapping.
//
// Zero is reserved for "unset": a Prometheus gauge that has never
// been Set reads as 0 by default, and we don't want a process that
// crashed before reaching the dispatch loop to look indistinguishable
// on dashboards from one in PhaseBootstrap.
const (
	PhaseGaugeBootstrap   = 1
	PhaseGaugeMerging     = 2
	PhaseGaugeSteadyState = 3
)

// Metrics owns the prometheus counters and gauges for the
// orchestrator. A nil *Metrics is a valid zero-value: every method
// is a no-op so tests can skip metric registration entirely.
type Metrics struct {
	Phase            prometheus.Gauge
	PhaseTransitions *prometheus.CounterVec
	StateDuration    *prometheus.HistogramVec

	// Merge-phase counters. All increment-only; the merge runs once
	// per data-dir lifetime so totals are stable observables on
	// dashboards.
	MergeEventsKept                  prometheus.Counter
	MergeEventsDropped               prometheus.Counter
	MergeSegmentsConsumed            prometheus.Counter
	MergeDIDLookups                  prometheus.Counter
	MergeRepoRevsUpdated             prometheus.Counter
	MergeDIDsDiscoveredPostBootstrap prometheus.Counter

	CompactionPasses            *prometheus.CounterVec
	CompactionPassDuration      prometheus.Histogram
	CompactionEarlyPasses       prometheus.Counter
	CompactionTombstones        *prometheus.CounterVec
	CompactionTombstoneEntries  prometheus.Collector
	CompactionTombstoneBytes    prometheus.Collector
	CompactionSegmentsExamined  prometheus.Counter
	CompactionSegmentsRewritten prometheus.Counter
	CompactionSegmentsClean     prometheus.Counter
	CompactionManifestReconcile prometheus.Counter
	CompactionRowsDropped       *prometheus.CounterVec
	CompactionBytesRewritten    prometheus.Counter
	CompactionWatermarkSeq      prometheus.Gauge
	CompactionWatermarkLag      prometheus.Gauge
}

// NewMetrics registers the orchestrator counters/gauges against reg.
func NewMetrics(reg prometheus.Registerer, tombstones ...*tombstone.Set) *Metrics {
	var ts *tombstone.Set
	if len(tombstones) > 0 {
		ts = tombstones[0]
	}

	m := &Metrics{
		Phase: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "phase",
			Help: "Current ingestion phase: 0=unset, 1=bootstrap, 2=merging, 3=steady_state.",
		}),
		PhaseTransitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "phase_transitions_total",
			Help: "Number of phase transitions, by from/to phase.",
		}, []string{"from", "to"}),
		StateDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name:    "state_duration_seconds",
			Help:    "Wall-clock seconds spent in each cutover state.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 14),
		}, []string{"state"}),
	}
	m.MergeEventsKept = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace, Subsystem: metricsSubsystem,
		Name: "merge_events_kept_total",
		Help: "Events from live_segments/ that survived the rev filter and were appended to the steady-state segments.",
	})
	m.MergeEventsDropped = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace, Subsystem: metricsSubsystem,
		Name: "merge_events_dropped_total",
		Help: "Events from live_segments/ dropped because their rev was already covered by initial backfill.",
	})
	m.MergeSegmentsConsumed = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace, Subsystem: metricsSubsystem,
		Name: "merge_segments_consumed_total",
		Help: "live_segments/ source files fully drained and committed by the merge phase.",
	})
	m.MergeDIDLookups = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace, Subsystem: metricsSubsystem,
		Name: "merge_did_lookups_total",
		Help: "First-time per-DID repo/<did> reads issued during merge (cache hits do not count).",
	})
	m.MergeRepoRevsUpdated = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace, Subsystem: metricsSubsystem,
		Name: "merge_repo_revs_updated_total",
		Help: "Per-DID repo/<did>.Rev refreshes committed by the merge phase.",
	})
	m.MergeDIDsDiscoveredPostBootstrap = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace, Subsystem: metricsSubsystem,
		Name: "merge_dids_discovered_post_bootstrap_total",
		Help: "DIDs first observed via the merge-phase listRepos resume and queued for steady-state retry.",
	})
	m.CompactionPasses = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace, Subsystem: compactionMetricsSubsystem,
		Name: "passes_total",
		Help: "Delete/update compaction passes by result.",
	}, []string{"result"})
	m.CompactionPassDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace, Subsystem: compactionMetricsSubsystem,
		Name:    "pass_duration_seconds",
		Help:    "Wall-clock seconds spent in delete/update compaction passes.",
		Buckets: prometheus.ExponentialBuckets(0.1, 2, 16),
	})
	m.CompactionEarlyPasses = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace, Subsystem: compactionMetricsSubsystem,
		Name: "passes_early_total",
		Help: "Compaction passes triggered by the tombstone cap.",
	})
	m.CompactionTombstones = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace, Subsystem: compactionMetricsSubsystem,
		Name: "tombstones_collected_total",
		Help: "Tombstones collected by compaction kind.",
	}, []string{"kind"})
	m.CompactionTombstoneEntries = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: metricsNamespace, Subsystem: compactionMetricsSubsystem,
		Name: "tombstone_set_entries",
		Help: "Current number of entries in the live in-memory tombstone set.",
	}, func() float64 {
		if ts == nil {
			return 0
		}
		return float64(ts.Len())
	})
	m.CompactionTombstoneBytes = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: metricsNamespace, Subsystem: compactionMetricsSubsystem,
		Name: "tombstone_set_bytes",
		Help: "Estimated bytes held by the live in-memory tombstone set.",
	}, func() float64 {
		if ts == nil {
			return 0
		}
		return float64(ts.ApproxBytes())
	})
	m.CompactionSegmentsExamined = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace, Subsystem: compactionMetricsSubsystem,
		Name: "segments_examined_total",
		Help: "Sealed segments examined by delete/update compaction.",
	})
	m.CompactionSegmentsRewritten = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace, Subsystem: compactionMetricsSubsystem,
		Name: "segments_rewritten_total",
		Help: "Sealed segments rewritten by delete/update compaction.",
	})
	m.CompactionSegmentsClean = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace, Subsystem: compactionMetricsSubsystem,
		Name: "segments_skipped_clean_total",
		Help: "Compaction candidate segments skipped because no rows were dropped.",
	})
	m.CompactionManifestReconcile = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace, Subsystem: compactionMetricsSubsystem,
		Name: "manifest_reconciled_total",
		Help: "Manifest entries refreshed by compaction reconcile or rewrite handling.",
	})
	m.CompactionRowsDropped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace, Subsystem: compactionMetricsSubsystem,
		Name: "rows_dropped_total",
		Help: "Rows dropped by delete/update compaction reason.",
	}, []string{"reason"})
	m.CompactionBytesRewritten = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace, Subsystem: compactionMetricsSubsystem,
		Name: "bytes_rewritten_total",
		Help: "Bytes in segment files rewritten by delete/update compaction.",
	})
	m.CompactionWatermarkSeq = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace, Subsystem: compactionMetricsSubsystem,
		Name: "watermark_seq",
		Help: "Current compaction/seq watermark.",
	})
	m.CompactionWatermarkLag = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace, Subsystem: compactionMetricsSubsystem,
		Name: "watermark_lag_seconds",
		Help: "Header-granular indexed_at lag between the sealed segment tip and the compaction watermark.",
	})
	reg.MustRegister(
		m.Phase,
		m.PhaseTransitions,
		m.StateDuration,
		m.MergeEventsKept,
		m.MergeEventsDropped,
		m.MergeSegmentsConsumed,
		m.MergeDIDLookups,
		m.MergeRepoRevsUpdated,
		m.MergeDIDsDiscoveredPostBootstrap,
		m.CompactionPasses,
		m.CompactionPassDuration,
		m.CompactionEarlyPasses,
		m.CompactionTombstones,
		m.CompactionTombstoneEntries,
		m.CompactionTombstoneBytes,
		m.CompactionSegmentsExamined,
		m.CompactionSegmentsRewritten,
		m.CompactionSegmentsClean,
		m.CompactionManifestReconcile,
		m.CompactionRowsDropped,
		m.CompactionBytesRewritten,
		m.CompactionWatermarkSeq,
		m.CompactionWatermarkLag,
	)
	return m
}

func (m *Metrics) setPhase(v float64) {
	if m != nil {
		m.Phase.Set(v)
	}
}

func (m *Metrics) incTransition(from, to lifecycle.Phase) {
	if m != nil {
		m.PhaseTransitions.WithLabelValues(string(from), string(to)).Inc()
	}
}

func (m *Metrics) observeState(state string, seconds float64) {
	if m != nil {
		m.StateDuration.WithLabelValues(state).Observe(seconds)
	}
}

func (m *Metrics) incMergeEventsKept() {
	if m != nil {
		m.MergeEventsKept.Inc()
	}
}

func (m *Metrics) incMergeEventsDropped() {
	if m != nil {
		m.MergeEventsDropped.Inc()
	}
}

func (m *Metrics) incMergeSegmentsConsumed() {
	if m != nil {
		m.MergeSegmentsConsumed.Inc()
	}
}

func (m *Metrics) incMergeDIDLookups() {
	if m != nil {
		m.MergeDIDLookups.Inc()
	}
}

func (m *Metrics) addMergeRepoRevsUpdated(n int) {
	if m != nil && n > 0 {
		m.MergeRepoRevsUpdated.Add(float64(n))
	}
}

func (m *Metrics) incMergeDIDsDiscoveredPostBootstrap() {
	if m != nil {
		m.MergeDIDsDiscoveredPostBootstrap.Inc()
	}
}

func (m *Metrics) observeCompactionPass(start time.Time, err error) {
	if m == nil {
		return
	}
	result := "ok"
	if err != nil {
		result = "error"
	}
	m.CompactionPasses.WithLabelValues(result).Inc()
	m.CompactionPassDuration.Observe(time.Since(start).Seconds())
}

func (m *Metrics) incCompactionEarlyPass() {
	if m != nil {
		m.CompactionEarlyPasses.Inc()
	}
}

func (m *Metrics) addCompactionTombstones(kind string, n int) {
	if m != nil && n > 0 {
		m.CompactionTombstones.WithLabelValues(kind).Add(float64(n))
	}
}

func (m *Metrics) incCompactionSegmentsExamined() {
	if m != nil {
		m.CompactionSegmentsExamined.Inc()
	}
}

func (m *Metrics) incCompactionSegmentsRewritten() {
	if m != nil {
		m.CompactionSegmentsRewritten.Inc()
	}
}

func (m *Metrics) incCompactionSegmentsClean() {
	if m != nil {
		m.CompactionSegmentsClean.Inc()
	}
}

func (m *Metrics) incCompactionManifestReconciled() {
	if m != nil {
		m.CompactionManifestReconcile.Inc()
	}
}

func (m *Metrics) addCompactionRowsDropped(reason string, n uint64) {
	if m != nil && n > 0 {
		m.CompactionRowsDropped.WithLabelValues(reason).Add(float64(n))
	}
}

func (m *Metrics) addCompactionBytesRewritten(n int64) {
	if m != nil && n > 0 {
		m.CompactionBytesRewritten.Add(float64(n))
	}
}

func (m *Metrics) setCompactionWatermark(seq uint64) {
	if m != nil {
		m.CompactionWatermarkSeq.Set(float64(seq))
	}
}

func (m *Metrics) setCompactionWatermarkLag(seconds float64) {
	if m != nil {
		m.CompactionWatermarkLag.Set(seconds)
	}
}
