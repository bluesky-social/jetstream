package obs

import (
	"time"

	"github.com/bluesky-social/jetstream/internal/version"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

const (
	namespace = "jetstream"
)

// Metrics owns the prometheus registry and the small set of process-wide
// metrics we register at startup. We use a dedicated registry rather than
// prometheus.DefaultRegisterer so tests can spin up isolated instances and
// libraries that auto-register against the default never leak in.
type Metrics struct {
	Registry *prometheus.Registry

	HTTPRequestDuration *prometheus.HistogramVec
}

// NewMetrics creates the registry and registers the standard collectors plus
// our own HTTP and build_info metrics.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()

	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	info := version.Get()
	buildInfo := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:      "build_info",
		Namespace: namespace,
		Help:      "Constant 1, labeled with build metadata.",
	}, []string{"version", "commit", "date"})
	buildInfo.WithLabelValues(info.Version, info.Commit, info.Date).Set(1)
	reg.MustRegister(buildInfo)

	currentTimestamp := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name:      "current_timestamp_seconds",
		Namespace: namespace,
		Help:      "Current Unix timestamp in seconds at scrape time.",
	}, func() float64 {
		return float64(time.Now().Unix())
	})
	reg.MustRegister(currentTimestamp)

	// Labels are deliberately minimal. `commit` is intentionally not
	// a label here even though every request runs under one commit:
	// it would be process-static, duplicate the build_info gauge,
	// and force every Grafana panel to `sum without(commit)` after
	// a deploy when histogram series reset under the new label.
	// Consumers join on build_info when they need build context.
	httpDur := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:      "http_request_duration_seconds",
		Namespace: namespace,
		Help:      "HTTP request latency for the public server.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"handler", "method", "code"})
	reg.MustRegister(httpDur)

	return &Metrics{
		Registry:            reg,
		HTTPRequestDuration: httpDur,
	}
}
