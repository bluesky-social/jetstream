package obs

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const overlaySubsystem = "overlay"

// OverlayMetrics is the Prometheus implementation of overlay.CacheObserver.
// It lives here, not in package overlay, so the overlay encode/decode core
// (which the public Go client depends on for tombstone decoding) carries no
// Prometheus dependency. A nil *OverlayMetrics is a valid no-op.
type OverlayMetrics struct {
	blobBytes      prometheus.Gauge
	buildRecords   prometheus.Gauge
	buildDIDs      prometheus.Gauge
	rebuilds       prometheus.Counter
	rebuildSeconds prometheus.Histogram
	requests       prometheus.Counter
	serveBytes     prometheus.Counter
}

// NewOverlayMetrics registers overlay cache metrics against reg.
func NewOverlayMetrics(reg prometheus.Registerer) *OverlayMetrics {
	m := &OverlayMetrics{
		blobBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: overlaySubsystem,
			Name: "blob_bytes", Help: "Size of the current overlay blob in bytes.",
		}),
		buildRecords: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: overlaySubsystem,
			Name: "build_records", Help: "Record tombstones in the current overlay blob.",
		}),
		buildDIDs: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: overlaySubsystem,
			Name: "build_dids", Help: "DID tombstones in the current overlay blob.",
		}),
		rebuilds: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: overlaySubsystem,
			Name: "rebuilds_total", Help: "Total overlay blob rebuilds.",
		}),
		rebuildSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: overlaySubsystem,
			Name: "rebuild_duration_seconds", Help: "Overlay blob build (encode+compress) latency.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 12),
		}),
		requests: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: overlaySubsystem,
			Name: "requests_total", Help: "Total getTombstones requests served.",
		}),
		serveBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: overlaySubsystem,
			Name: "serve_bytes_total", Help: "Total overlay bytes written to clients.",
		}),
	}
	reg.MustRegister(m.blobBytes, m.buildRecords, m.buildDIDs, m.rebuilds, m.rebuildSeconds, m.requests, m.serveBytes)
	return m
}

// ObserveBuild records one overlay blob (re)build.
func (m *OverlayMetrics) ObserveBuild(d time.Duration, blobBytes, records, dids int) {
	if m == nil {
		return
	}
	m.rebuilds.Inc()
	m.rebuildSeconds.Observe(d.Seconds())
	m.blobBytes.Set(float64(blobBytes))
	m.buildRecords.Set(float64(records))
	m.buildDIDs.Set(float64(dids))
}

// ObserveServe records one getTombstones response of n bytes.
func (m *OverlayMetrics) ObserveServe(n int) {
	if m == nil {
		return
	}
	m.requests.Inc()
	m.serveBytes.Add(float64(n))
}
