package catalog

import "github.com/prometheus/client_golang/prometheus"

// Metrics owns the catalog scripts' counters: storage corruption, shared
// with the object store protocol and the follower, and commit fallbacks. A
// nil *Metrics is valid and records nothing.
type Metrics struct {
	Corruption *prometheus.CounterVec
	// Fallbacks counts one-round-trip commits (FenceBumpAt) whose
	// precondition failed, so the script ran again the ordinary way.
	Fallbacks *prometheus.CounterVec
}

// NewMetrics registers the series against reg. Construct exactly once per
// process.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Corruption: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "jetstream",
			Name:      "storage_corruption_total",
			Help:      "Storage corruption or broken catalog invariants found, by source (design §6.5).",
		}, []string{"source"}),
		Fallbacks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "jetstream",
			Name:      "catalog_commit_fallbacks_total",
			Help:      "One-round-trip leader commits whose fence precondition failed, so they ran again in two round trips, by kind.",
		}, []string{"kind"}),
	}
	reg.MustRegister(m.Corruption, m.Fallbacks)
	return m
}

// ObserveCorruption counts err if it is a CorruptionError.
func (m *Metrics) ObserveCorruption(err error) {
	if m == nil {
		return
	}
	if src, ok := IsCorruption(err); ok {
		m.Corruption.WithLabelValues(src).Inc()
	}
}

func (m *Metrics) fellBack(kind TxKind) {
	if m != nil {
		m.Fallbacks.WithLabelValues(string(kind)).Inc()
	}
}
