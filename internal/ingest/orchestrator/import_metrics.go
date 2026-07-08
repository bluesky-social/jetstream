package orchestrator

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// ImportMetrics holds the prometheus counters/gauges for timestamp import
// (design §6 J). A nil *ImportMetrics is a valid zero value: every method is a
// no-op, so tests and import-disabled deployments skip registration.
type ImportMetrics struct {
	Jobs        *prometheus.CounterVec // by result: ok|error
	JobDuration prometheus.Histogram
	Phase       prometheus.Gauge

	RowsParsed   prometheus.Counter
	RowsRejected *prometheus.CounterVec // by reason
	RowsMutated  prometheus.Counter
	RowsMatched  *prometheus.CounterVec // by scope: all_versions|specific
	RowsCorrupt  prometheus.Counter
	DIDsMatched  prometheus.Counter

	SegmentsExamined prometheus.Counter
	SegmentsPatched  prometheus.Counter
	BytesRewritten   prometheus.Counter
}

// NewImportMetrics registers the import counters/gauges against reg.
func NewImportMetrics(reg prometheus.Registerer) *ImportMetrics {
	m := &ImportMetrics{
		Jobs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: importMetricsSubsystem,
			Name: "jobs_total",
			Help: "Timestamp-import jobs by terminal result.",
		}, []string{"result"}),
		JobDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Subsystem: importMetricsSubsystem,
			Name:    "job_duration_seconds",
			Help:    "Wall-clock seconds spent in a full import job (all phases).",
			Buckets: prometheus.ExponentialBuckets(1, 2, 16),
		}),
		Phase: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: importMetricsSubsystem,
			Name: "phase",
			Help: "Current import phase: 0=idle, 1=parse_bucket, 2=apply.",
		}),
		RowsParsed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: importMetricsSubsystem,
			Name: "rows_parsed_total",
			Help: "CSV rows read by the parser (valid + rejected).",
		}),
		RowsRejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: importMetricsSubsystem,
			Name: "rows_rejected_total",
			Help: "CSV rows rejected during parse, by reason.",
		}, []string{"reason"}),
		RowsMutated: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: importMetricsSubsystem,
			Name: "rows_mutated_total",
			Help: "Segment rows whose display timestamp changed.",
		}),
		RowsMatched: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: importMetricsSubsystem,
			Name: "rows_matched_total",
			Help: "Segment rows matched by an import target, by scope.",
		}, []string{"scope"}),
		RowsCorrupt: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: importMetricsSubsystem,
			Name: "rows_corrupt_offset_total",
			Help: "Offset entries that failed re-validation in Phase C (CSV/offset desync).",
		}),
		DIDsMatched: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: importMetricsSubsystem,
			Name: "dids_matched_total",
			Help: "Valid rows whose DID routed to at least one candidate segment.",
		}),
		SegmentsExamined: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: importMetricsSubsystem,
			Name: "segments_examined_total",
			Help: "Sealed segments opened in Phase C (had an offset file, not resume-skipped).",
		}),
		SegmentsPatched: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: importMetricsSubsystem,
			Name: "segments_patched_total",
			Help: "Sealed segments actually rewritten by import (non-zero mutations).",
		}),
		BytesRewritten: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: importMetricsSubsystem,
			Name: "bytes_rewritten_total",
			Help: "On-disk bytes of segment files rewritten by import.",
		}),
	}
	reg.MustRegister(
		m.Jobs, m.JobDuration, m.Phase,
		m.RowsParsed, m.RowsRejected, m.RowsMutated, m.RowsMatched, m.RowsCorrupt, m.DIDsMatched,
		m.SegmentsExamined, m.SegmentsPatched, m.BytesRewritten,
	)
	return m
}

func (m *ImportMetrics) setPhase(v float64) {
	if m != nil {
		m.Phase.Set(v)
	}
}

// IsCancellationOnly reports whether every leaf of err's tree is a context
// cancellation (Canceled or DeadlineExceeded). It is the shared pause-vs-fail
// classifier for a timestamp-import run: the import manager (importer.run) uses
// it to decide whether a returned error is a resumable pause (pure
// cancellation) or a terminal failure, and observeJob uses the same predicate
// so a job recorded terminal there is also counted terminal here.
//
// A plain errors.Is(err, context.Canceled) is insufficient: RunImport can
// return errors.Join(context.Canceled, realFailure) — a worker cancelled at
// shutdown joined with, say, a failed manifest refresh — and errors.Is matches
// ANY leaf, which would launder the real failure into a pause. Report
// cancellation only when it is the whole story.
func IsCancellationOnly(err error) bool {
	if err == nil {
		return false
	}
	if err == context.Canceled || err == context.DeadlineExceeded { //nolint:errorlint // leaves compared after unwrapping below
		return true
	}
	switch u := err.(type) { //nolint:errorlint // deliberate tree walk
	case interface{ Unwrap() []error }:
		children := u.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !IsCancellationOnly(child) {
				return false
			}
		}
		return true
	case interface{ Unwrap() error }:
		return IsCancellationOnly(u.Unwrap())
	}
	return false
}

// observeJob folds a finished job's result into the counters and records its
// duration + terminal result. Called once per RunImport return.
//
// A context-cancelled run is a graceful pause (the manager leaves the job
// non-terminal and auto-resumes it next boot), not a terminal outcome: it must
// not count toward jobs_total{result="error"} or the duration histogram, or
// every clean shutdown mid-import would fire failed-job alerts. Its partial
// row/segment counters still fold in — that work happened.
func (m *ImportMetrics) observeJob(start time.Time, result ImportResult, err error) {
	if m == nil {
		return
	}
	m.Phase.Set(ImportPhaseGaugeIdle)
	// Classify with IsCancellationOnly, not a bare errors.Is: a job that
	// FAILED while a shutdown cancellation raced it returns
	// errors.Join(context.Canceled, realFailure), which errors.Is(Canceled)
	// would treat as a pause and drop from jobs_total — hiding the very
	// failure its alert exists to catch. This must match the manager's
	// pause-vs-fail decision (importer.run), or a job recorded terminal here
	// escapes the terminal-result counter there.
	if !IsCancellationOnly(err) {
		res := "ok"
		if err != nil {
			res = "error"
		}
		m.Jobs.WithLabelValues(res).Inc()
		m.JobDuration.Observe(time.Since(start).Seconds())
	}

	parse := result.ParseStats()
	if parse.RowsTotal > 0 {
		m.RowsParsed.Add(float64(parse.RowsTotal))
	}
	for reason, n := range parse.RejectsByReason {
		m.RowsRejected.WithLabelValues(string(reason)).Add(float64(n))
	}
	if result.Bucket.RowsRouted > 0 {
		m.DIDsMatched.Add(float64(result.Bucket.RowsRouted))
	}
	if result.RowsMutated > 0 {
		m.RowsMutated.Add(float64(result.RowsMutated))
	}
	if result.RowsMatchedAllVersions > 0 {
		m.RowsMatched.WithLabelValues("all_versions").Add(float64(result.RowsMatchedAllVersions))
	}
	if result.RowsMatchedSpecific > 0 {
		m.RowsMatched.WithLabelValues("specific").Add(float64(result.RowsMatchedSpecific))
	}
	if result.RowsCorruptOffset > 0 {
		m.RowsCorrupt.Add(float64(result.RowsCorruptOffset))
	}
	if result.SegmentsExamined > 0 {
		m.SegmentsExamined.Add(float64(result.SegmentsExamined))
	}
	if result.SegmentsPatched > 0 {
		m.SegmentsPatched.Add(float64(result.SegmentsPatched))
	}
	if result.BytesRewritten > 0 {
		m.BytesRewritten.Add(float64(result.BytesRewritten))
	}
}
