package backfill

import (
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/jetstream/internal/obs"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	"github.com/prometheus/client_golang/prometheus"
)

const metricsNamespace = "jetstream"
const metricsSubsystem = "backfill"

// Metrics owns the prometheus counters for the backfill engine.
// A nil *Metrics is a valid zero-value: every method is a no-op,
// which lets tests skip metric registration entirely.
type Metrics struct {
	mu                       sync.Mutex
	hostStates               map[string]string
	lastEnumerated           int64
	Discovered               prometheus.Counter
	Completed                prometheus.Counter
	Failed                   prometheus.Counter
	ActiveFlips              prometheus.Counter
	OnFailErrors             prometheus.Counter
	HandleRepoDuration       prometheus.Histogram
	ProgressCompleted        prometheus.Gauge
	CompletionQueued         prometheus.Counter
	CompletionQueueDepth     prometheus.Gauge
	CompletionDurableBatches prometheus.Counter
	CompletionDurableRepos   prometheus.Counter
	CompletionStageErrors    prometheus.Counter
	CompletionQueueWait      prometheus.Histogram
	ForcedCheckpointFlushes  prometheus.Counter
	RetryPasses              prometheus.Counter
	RetryCandidates          prometheus.Counter
	RetryAttempts            prometheus.Counter
	RetrySucceeded           prometheus.Counter
	RetryFailed              prometheus.Counter
	RetrySkippedHostParked   prometheus.Counter
	HostsTotal               *prometheus.GaugeVec
	HostEnumeratedRepos      prometheus.Counter
	HostAttempts             prometheus.Counter
	HostnameRejected         prometheus.Counter
	RosterCapHits            prometheus.Counter
	DownloadSlotWait         prometheus.Histogram
	EngineActiveHosts        prometheus.Gauge
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
		hostStates: make(map[string]string),
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
		CompletionQueued: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "completion_queued_total",
			Help: "Number of repo completions queued for async durable metadata commit.",
		}),
		CompletionQueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "completion_queue_depth",
			Help: "Current number of repo completions waiting for a writer durable metadata batch.",
		}),
		CompletionDurableBatches: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "completion_durable_batches_total",
			Help: "Number of writer durable metadata batches that committed at least one queued repo completion.",
		}),
		CompletionDurableRepos: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "completion_durable_repos_total",
			Help: "Number of queued repo completions committed by writer durable metadata batches.",
		}),
		CompletionStageErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "completion_stage_errors_total",
			Help: "Number of errors staging queued repo completions into a writer durable metadata batch.",
		}),
		CompletionQueueWait: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name:    "completion_queue_wait_seconds",
			Help:    "Time a repo completion waited in the in-memory queue between being queued and becoming durable. A rising tail surfaces a durability backlog that completion_queue_depth alone cannot distinguish from steady draining.",
			Buckets: obs.LatencyBucketsSlow,
		}),
		ForcedCheckpointFlushes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "forced_checkpoint_flushes_total",
			Help: "Number of forced writer durability drains at a batch/checkpoint barrier (DrainDurability), as opposed to completions that drained naturally on a block boundary.",
		}),
		RetryPasses: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "failed_repo_retry_passes_total",
			Help: "Number of steady-state failed-repo retry scans started.",
		}),
		RetryCandidates: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "failed_repo_retry_candidates_total",
			Help: "Number of failed repo rows found eligible for steady-state retry.",
		}),
		RetryAttempts: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "failed_repo_retry_attempts_total",
			Help: "Number of getRepo retry attempts issued by the steady-state failed-repo retry loop.",
		}),
		RetrySucceeded: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "failed_repo_retry_succeeded_total",
			Help: "Number of steady-state failed-repo retries that completed and marked the repo backfill complete.",
		}),
		RetryFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "failed_repo_retry_failed_total",
			Help: "Number of steady-state failed-repo retry attempts that failed remotely and were rescheduled.",
		}),
		RetrySkippedHostParked: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "failed_repo_retry_skipped_host_parked_total",
			Help: "Number of eligible failed repo retries skipped because their host was parked by a rate-limit response.",
		}),
		HostsTotal: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "hosts_total", Help: "Current PDS roster hosts by engine lifecycle state and coarse host class.",
		}, []string{"state", "host_class"}),
		HostEnumeratedRepos: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "host_enumerated_repos_total", Help: "Valid repositories enumerated directly from PDS listRepos endpoints.",
		}),
		HostAttempts: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "host_attempts_total", Help: "Direct PDS listRepos producer attempts.",
		}),
		HostnameRejected: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "hostname_rejected_total", Help: "Untrusted listHosts hostnames rejected before client construction.",
		}),
		RosterCapHits: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "roster_cap_hits_total", Help: "listHosts crawls that exceeded the configured roster bound.",
		}),
		DownloadSlotWait: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "download_slot_wait_seconds", Help: "Time direct-PDS workers wait for a fleet-wide download slot.",
			Buckets: obs.LatencyBucketsFast,
		}),
		EngineActiveHosts: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "engine_active_hosts", Help: "PDS host producer loops currently active.",
		}),
	}
	reg.MustRegister(
		m.Discovered, m.Completed, m.Failed, m.ActiveFlips, m.OnFailErrors,
		m.HandleRepoDuration, m.ProgressCompleted,
		m.CompletionQueued, m.CompletionQueueDepth, m.CompletionDurableBatches,
		m.CompletionDurableRepos, m.CompletionStageErrors,
		m.CompletionQueueWait, m.ForcedCheckpointFlushes,
		m.RetryPasses, m.RetryCandidates, m.RetryAttempts,
		m.RetrySucceeded, m.RetryFailed, m.RetrySkippedHostParked,
		m.HostsTotal, m.HostEnumeratedRepos, m.HostAttempts,
		m.HostnameRejected, m.RosterCapHits, m.DownloadSlotWait, m.EngineActiveHosts,
	)
	return m
}

func hostClass(host string) string {
	if strings.HasSuffix(strings.ToLower(host), ".host.bsky.network") {
		return "mushroom"
	}
	return "third_party"
}

func (m *Metrics) observeHostState(host atmosbackfill.HostInfo, state atmosbackfill.HostState, _ int) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	class := hostClass(host.Hostname)
	if previous := m.hostStates[host.Hostname]; previous != "" && previous != string(state) {
		m.HostsTotal.WithLabelValues(previous, class).Dec()
		if previous == string(atmosbackfill.HostStateRunning) {
			m.EngineActiveHosts.Dec()
		}
	}
	if m.hostStates[host.Hostname] != string(state) {
		m.HostsTotal.WithLabelValues(string(state), class).Inc()
		if state == atmosbackfill.HostStateRunning {
			m.EngineActiveHosts.Inc()
		}
		m.hostStates[host.Hostname] = string(state)
	}
	if state == atmosbackfill.HostStatePending {
		m.HostAttempts.Inc()
	}
}

func (m *Metrics) observeProgress(stats atmosbackfill.Stats) {
	if m == nil {
		return
	}
	m.setProgressCompleted(stats.Completed)
	m.mu.Lock()
	if stats.ReposEnumerated < m.lastEnumerated {
		// Each atmos Engine is single-shot and its Stats counters start at zero.
		// Bootstrap and merge discovery reuse this process-wide Metrics value.
		m.lastEnumerated = 0
	}
	if delta := stats.ReposEnumerated - m.lastEnumerated; delta > 0 {
		m.HostEnumeratedRepos.Add(float64(delta))
		m.lastEnumerated = stats.ReposEnumerated
	}
	m.mu.Unlock()
}

func (m *Metrics) incHostnameRejected() {
	if m != nil {
		m.HostnameRejected.Inc()
	}
}
func (m *Metrics) incRosterCapHit() {
	if m != nil {
		m.RosterCapHits.Inc()
	}
}
func (m *Metrics) observeDownloadSlotWait(wait time.Duration) {
	if m != nil && wait >= 0 {
		m.DownloadSlotWait.Observe(wait.Seconds())
	}
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

func (m *Metrics) incCompletionQueued() {
	if m != nil {
		m.CompletionQueued.Inc()
	}
}

func (m *Metrics) setCompletionQueueDepth(v int) {
	if m != nil {
		m.CompletionQueueDepth.Set(float64(v))
	}
}

func (m *Metrics) observeCompletionDurableBatch(repos int) {
	if m != nil && repos > 0 {
		m.CompletionDurableBatches.Inc()
		m.CompletionDurableRepos.Add(float64(repos))
	}
}

func (m *Metrics) incCompletionStageErrors() {
	if m != nil {
		m.CompletionStageErrors.Inc()
	}
}

// observeCompletionQueueWait records how long a completion sat in the queue
// before becoming durable. since is the duration from QueueComplete's captured
// timestamp to the durable commit; negative values (clock skew) are dropped.
func (m *Metrics) observeCompletionQueueWait(since time.Duration) {
	if m != nil && since >= 0 {
		m.CompletionQueueWait.Observe(since.Seconds())
	}
}

func (m *Metrics) incForcedCheckpointFlushes() {
	if m != nil {
		m.ForcedCheckpointFlushes.Inc()
	}
}

func (m *Metrics) incRetryPasses() {
	if m != nil {
		m.RetryPasses.Inc()
	}
}

func (m *Metrics) incRetryCandidates() {
	if m != nil {
		m.RetryCandidates.Inc()
	}
}

func (m *Metrics) incRetryAttempts() {
	if m != nil {
		m.RetryAttempts.Inc()
	}
}

func (m *Metrics) incRetrySucceeded() {
	if m != nil {
		m.RetrySucceeded.Inc()
	}
}

func (m *Metrics) incRetryFailed() {
	if m != nil {
		m.RetryFailed.Inc()
	}
}

func (m *Metrics) incRetrySkippedHostParked() {
	if m != nil {
		m.RetrySkippedHostParked.Inc()
	}
}
