package store

import (
	"errors"
	"time"

	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/cockroachdb/pebble"
	"github.com/prometheus/client_golang/prometheus"
)

// ErrNotFound is re-exported for callers that want a stable handle
// on the "key absent" outcome without importing pebble. It aliases
// pebble.ErrNotFound so errors.Is keeps working through the wrapper.
var ErrNotFound = pebble.ErrNotFound

const (
	statusOK       = "ok"
	statusNotFound = "notfound"
	statusError    = "error"
)

// Metrics owns prometheus state for the metadata pebble store. A
// nil *Metrics is a valid zero-value: every observe* method is a
// no-op, so tests can skip metric registration.
type Metrics struct {
	OpDuration *prometheus.HistogramVec

	// Pre-computed observers. We pay the WithLabelValues cost once at
	// registration time so per-call observe doesn't allocate.
	getOk       prometheus.Observer
	getNotFound prometheus.Observer
	getError    prometheus.Observer
	setOk       prometheus.Observer
	setError    prometheus.Observer
	deleteOk    prometheus.Observer
	deleteError prometheus.Observer
	commitOk    prometheus.Observer
	commitError prometheus.Observer
}

// NewMetrics registers the store histogram against reg and pre-binds
// the per-{op,status} observers.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	hist := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "jetstream",
		Subsystem: "store",
		Name:      "op_duration_seconds",
		Help:      "Duration of pebble metadata-store operations by op and outcome.",
		Buckets:   obs.LatencyBucketsFast,
	}, []string{"op", "status"})
	reg.MustRegister(hist)

	return &Metrics{
		OpDuration:  hist,
		getOk:       hist.WithLabelValues("get", statusOK),
		getNotFound: hist.WithLabelValues("get", statusNotFound),
		getError:    hist.WithLabelValues("get", statusError),
		setOk:       hist.WithLabelValues("set", statusOK),
		setError:    hist.WithLabelValues("set", statusError),
		deleteOk:    hist.WithLabelValues("delete", statusOK),
		deleteError: hist.WithLabelValues("delete", statusError),
		commitOk:    hist.WithLabelValues("batch_commit", statusOK),
		commitError: hist.WithLabelValues("batch_commit", statusError),
	}
}

// ObserveGet records a Get duration tagged by outcome.
func (m *Metrics) ObserveGet(start time.Time, err error) {
	if m == nil {
		return
	}
	d := time.Since(start).Seconds()
	switch {
	case err == nil:
		m.getOk.Observe(d)
	case errors.Is(err, pebble.ErrNotFound):
		m.getNotFound.Observe(d)
	default:
		m.getError.Observe(d)
	}
}

// ObserveSet records a Set duration tagged by outcome.
func (m *Metrics) ObserveSet(start time.Time, err error) {
	if m == nil {
		return
	}
	d := time.Since(start).Seconds()
	if err != nil {
		m.setError.Observe(d)
		return
	}
	m.setOk.Observe(d)
}

// ObserveDelete records a Delete duration tagged by outcome.
func (m *Metrics) ObserveDelete(start time.Time, err error) {
	if m == nil {
		return
	}
	d := time.Since(start).Seconds()
	if err != nil {
		m.deleteError.Observe(d)
		return
	}
	m.deleteOk.Observe(d)
}

// ObserveBatchCommit records a Batch.Commit duration tagged by outcome.
func (m *Metrics) ObserveBatchCommit(start time.Time, err error) {
	if m == nil {
		return
	}
	d := time.Since(start).Seconds()
	if err != nil {
		m.commitError.Observe(d)
		return
	}
	m.commitOk.Observe(d)
}
