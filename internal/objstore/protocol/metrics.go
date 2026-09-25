// Package protocol is the design §7 object protocol over an objstore.Blob and
// the catalog. Uploader runs §7.3 (dedup, the uploading row, PUT, read-back
// verify) inside a leader's catalog.Session. Reader runs §7.5 (lookup by
// object_id, verify, refresh and retry once, corruption) and implements
// objstore.Store.
//
// It never imports a storage driver: the catalog arrives as catalog.DB or a
// RowSource, and the object store as an objstore.Blob.
package protocol

import "github.com/prometheus/client_golang/prometheus"

// Path labels for jetstream_s3_verify_failures_total.
const (
	pathUpload = "upload"
	pathRead   = "read"
)

// Metrics owns jetstream_s3_verify_failures_total (design §23). Storage
// corruption is counted separately, by catalog.Metrics. A nil *Metrics is
// valid and records nothing.
type Metrics struct {
	VerifyFailures *prometheus.CounterVec
}

// NewMetrics registers the series against reg. Construct exactly once per
// process.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		VerifyFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "jetstream",
			Subsystem: "s3",
			Name:      "verify_failures_total",
			Help: "Objects whose bytes were missing or failed the length or SHA-256 check, " +
				"by path (upload read-back or read). Most are retried; see storage_corruption_total.",
		}, []string{"path"}),
	}
	reg.MustRegister(m.VerifyFailures)
	return m
}

func (m *Metrics) verifyFailed(path string) {
	if m != nil {
		m.VerifyFailures.WithLabelValues(path).Inc()
	}
}
