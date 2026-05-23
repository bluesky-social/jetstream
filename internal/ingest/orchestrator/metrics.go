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
	reg.MustRegister(m.Phase, m.PhaseTransitions, m.StateDuration)
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
