package overlay

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricsNamespace = "jetstream"
	metricsSubsystem = "overlay"
)

// Metrics owns the prometheus state for the overlay cache. A nil
// *Metrics is valid: every method is a no-op so tests can skip
// registration.
type Metrics struct {
	BlobBytes      prometheus.Gauge
	BuildRecords   prometheus.Gauge
	BuildDIDs      prometheus.Gauge
	Rebuilds       prometheus.Counter
	RebuildSeconds prometheus.Histogram
	Requests       prometheus.Counter
	ServeBytes     prometheus.Counter
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		BlobBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "blob_bytes", Help: "Size of the current overlay blob in bytes.",
		}),
		BuildRecords: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "build_records", Help: "Record tombstones in the current overlay blob.",
		}),
		BuildDIDs: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "build_dids", Help: "DID tombstones in the current overlay blob.",
		}),
		Rebuilds: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "rebuilds_total", Help: "Total overlay blob rebuilds.",
		}),
		RebuildSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "rebuild_duration_seconds", Help: "Overlay blob build (encode+compress) latency.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 12),
		}),
		Requests: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "requests_total", Help: "Total getTombstones requests served.",
		}),
		ServeBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "serve_bytes_total", Help: "Total overlay bytes written to clients.",
		}),
	}
	reg.MustRegister(m.BlobBytes, m.BuildRecords, m.BuildDIDs, m.Rebuilds, m.RebuildSeconds, m.Requests, m.ServeBytes)
	return m
}

func (m *Metrics) observeBuild(d time.Duration, blobBytes, records, dids int) {
	if m == nil {
		return
	}
	m.Rebuilds.Inc()
	m.RebuildSeconds.Observe(d.Seconds())
	m.BlobBytes.Set(float64(blobBytes))
	m.BuildRecords.Set(float64(records))
	m.BuildDIDs.Set(float64(dids))
}

func (m *Metrics) observeServe(n int) {
	if m == nil {
		return
	}
	m.Requests.Inc()
	m.ServeBytes.Add(float64(n))
}
