package catalog

import "github.com/prometheus/client_golang/prometheus"

// Metrics owns the storage-corruption counter shared by the catalog scripts,
// the object store protocol, and the follower. A nil *Metrics is valid and
// records nothing.
type Metrics struct {
	Corruption *prometheus.CounterVec
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
	}
	reg.MustRegister(m.Corruption)
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
