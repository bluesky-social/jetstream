package leader

import "github.com/prometheus/client_golang/prometheus"

const (
	metricsNamespace = "jetstream"
	metricsSubsystem = "leader"
)

const (
	reasonFatal     = "fatal"
	reasonLeaseLost = "lease_lost"
	reasonShutdown  = "shutdown"
	reasonRestart   = "restart"
)

// Metrics owns the prometheus series for the election loop. A nil *Metrics
// is valid and records nothing.
type Metrics struct {
	IsLeader       prometheus.Gauge
	Epoch          prometheus.Gauge
	SessionStarts  prometheus.Counter
	SessionsTotal  *prometheus.CounterVec
	LeaseLostTotal prometheus.Counter
	AcquireErrors  prometheus.Counter
	RenewErrors    prometheus.Counter
	ReleaseErrors  prometheus.Counter
	FenceFailures  prometheus.Counter
}

// NewMetrics registers the series against reg. Construct exactly once per
// process.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	counter := func(name, help string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem, Name: name, Help: help,
		})
	}
	m := &Metrics{
		IsLeader: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "is_leader",
			Help: "1 while this process runs a writer session, else 0.",
		}),
		Epoch: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "epoch",
			Help: "Writer epoch of this process's most recent session.",
		}),
		SessionStarts: counter("session_starts_total", "Writer sessions started."),
		SessionsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "sessions_total",
			Help: "Writer sessions ended, by result (fatal, lease_lost, shutdown, restart).",
		}, []string{"result"}),
		LeaseLostTotal: counter("lease_lost_total", "Sessions cancelled because the lease was lost."),
		AcquireErrors:  counter("acquire_errors_total", "Acquire attempts that failed for a reason other than the lock being held."),
		RenewErrors:    counter("renew_errors_total", "Renew attempts that failed for a reason other than losing the lock."),
		ReleaseErrors:  counter("release_errors_total", "Best-effort releases that failed."),
		FenceFailures:  counter("fence_failures_total", "Leader write transactions rejected by the epoch fence."),
	}
	for _, r := range []string{reasonFatal, reasonLeaseLost, reasonShutdown, reasonRestart} {
		m.SessionsTotal.WithLabelValues(r)
	}
	reg.MustRegister(m.IsLeader, m.Epoch, m.SessionStarts, m.SessionsTotal,
		m.LeaseLostTotal, m.AcquireErrors, m.RenewErrors, m.ReleaseErrors, m.FenceFailures)
	return m
}

func (m *Metrics) sessionStarted(epoch uint64) {
	if m == nil {
		return
	}
	m.SessionStarts.Inc()
	m.Epoch.Set(float64(epoch))
	m.IsLeader.Set(1)
}

func (m *Metrics) sessionEnded(reason string) {
	if m == nil {
		return
	}
	m.IsLeader.Set(0)
	m.SessionsTotal.WithLabelValues(reason).Inc()
}

func (m *Metrics) leaseLost() {
	if m != nil {
		m.LeaseLostTotal.Inc()
	}
}

func (m *Metrics) acquireError() {
	if m != nil {
		m.AcquireErrors.Inc()
	}
}

func (m *Metrics) renewError() {
	if m != nil {
		m.RenewErrors.Inc()
	}
}

func (m *Metrics) releaseError() {
	if m != nil {
		m.ReleaseErrors.Inc()
	}
}

// FenceFailure counts a write transaction the epoch fence rejected (design
// §6.4). The catalog transaction scripts call it.
func (m *Metrics) FenceFailure() {
	if m != nil {
		m.FenceFailures.Inc()
	}
}
