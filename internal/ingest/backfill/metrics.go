package backfill

import (
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/obs"
	"github.com/prometheus/client_golang/prometheus"
)

const metricsNamespace = "jetstream"
const metricsSubsystem = "backfill"

// Metrics owns the prometheus counters for the backfill engine.
// A nil *Metrics is a valid zero-value: every method is a no-op,
// which lets tests skip metric registration entirely.
type Metrics struct {
	Discovered         prometheus.Counter
	Completed          prometheus.Counter
	Failed             prometheus.Counter
	ActiveFlips        prometheus.Counter
	OnFailErrors       prometheus.Counter
	HandleRepoDuration prometheus.Histogram
	ProgressCompleted  prometheus.Gauge
}

// NewMetrics registers the backfill counters against reg. Pass the
// shared *prometheus.Registry from internal/obs.Metrics so every
// counter shows up on /metrics.
//
// Calls reg.MustRegister, which panics if these counters are already
// registered against reg. Construct exactly once per process; tests
// that don't need metrics should pass a nil *Metrics into NewStore
// rather than building a separate registry.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Discovered: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "discovered_total",
			Help: "Number of DIDs first observed in listRepos and recorded at not_started.",
		}),
		Completed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "completed_total",
			Help: "Number of DIDs whose initial repo download finished successfully.",
		}),
		Failed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "failed_total",
			Help: "Number of DIDs that exhausted their retry budget within a Run.",
		}),
		ActiveFlips: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "active_flips_total",
			Help: "Number of active->inactive or inactive->active transitions observed via listRepos.",
		}),
		OnFailErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "on_fail_store_errors_total",
			Help: "Number of times Store.OnFail itself failed to persist (data-integrity signal).",
		}),
		HandleRepoDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name:    "handle_repo_duration_seconds",
			Help:    "Duration of SegmentHandler.HandleRepo per repo. Successful repos only — failures are surfaced via error logs and trace status, not the success-time histogram.",
			Buckets: obs.LatencyBucketsSlow,
		}),
		ProgressCompleted: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "progress_completed",
			Help: "Number of repos the engine has reported complete in the current Run.",
		}),
	}
	reg.MustRegister(
		m.Discovered, m.Completed, m.Failed, m.ActiveFlips, m.OnFailErrors,
		m.HandleRepoDuration, m.ProgressCompleted,
	)
	return m
}

// Nil-safe inc* helpers centralize the nil-Metrics check so callers
// in store.go don't have to repeat it (and can't forget it). They're
// unexported because every caller lives in this package.
func (m *Metrics) incDiscovered() {
	if m != nil {
		m.Discovered.Inc()
	}
}

func (m *Metrics) incCompleted() {
	if m != nil {
		m.Completed.Inc()
	}
}

func (m *Metrics) incFailed() {
	if m != nil {
		m.Failed.Inc()
	}
}

func (m *Metrics) incActiveFlips() {
	if m != nil {
		m.ActiveFlips.Inc()
	}
}

func (m *Metrics) incOnFailErrors() {
	if m != nil {
		m.OnFailErrors.Inc()
	}
}

// observeHandleRepo records a successful HandleRepo duration. Failed
// repos are not recorded — operators chase failures through error
// logs and trace status, not the success-time histogram. Matches
// segment.Metrics.ObserveSeal's convention.
func (m *Metrics) observeHandleRepo(start time.Time, err error) {
	if m == nil || err != nil {
		return
	}
	m.HandleRepoDuration.Observe(time.Since(start).Seconds())
}

func (m *Metrics) setProgressCompleted(v int64) {
	if m != nil {
		m.ProgressCompleted.Set(float64(v))
	}
}
