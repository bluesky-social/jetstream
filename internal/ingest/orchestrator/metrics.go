package orchestrator

import (
	"github.com/bluesky-social/jetstream-v2/internal/lifecycle"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricsNamespace = "jetstream"
	metricsSubsystem = "orchestrator"
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
}

// NewMetrics registers the orchestrator counters/gauges against reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
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
