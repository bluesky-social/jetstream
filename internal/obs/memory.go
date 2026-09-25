package obs

import "github.com/prometheus/client_golang/prometheus"

// Memory budget labels for jetstream_memory_{budget,used}_bytes (design §17).
const (
	BudgetObjectCache = "object_cache"
)

// MemoryMetrics owns the per-budget memory gauges (design §17, §23). Every
// component with an explicit memory budget reports its limit and its current
// use under its own budget label, so one shared pair of vectors exists per
// process. A nil *MemoryMetrics is valid and records nothing.
type MemoryMetrics struct {
	Budget *prometheus.GaugeVec
	Used   *prometheus.GaugeVec
}

// NewMemoryMetrics registers the gauges against reg. Construct exactly once
// per process and hand it to every budgeted component.
func NewMemoryMetrics(reg prometheus.Registerer) *MemoryMetrics {
	m := &MemoryMetrics{
		Budget: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "memory_budget_bytes",
			Help:      "Configured memory budget, by budget (design §17).",
		}, []string{"budget"}),
		Used: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "memory_used_bytes",
			Help:      "Memory currently held against a budget, by budget (design §17).",
		}, []string{"budget"}),
	}
	reg.MustRegister(m.Budget, m.Used)
	return m
}

// Gauges returns the budget and used gauges for one budget label. Both are
// nil when m is nil.
func (m *MemoryMetrics) Gauges(budget string) (limit, used prometheus.Gauge) {
	if m == nil {
		return nil, nil
	}
	return m.Budget.WithLabelValues(budget), m.Used.WithLabelValues(budget)
}
