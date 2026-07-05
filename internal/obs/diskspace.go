package obs

import (
	"fmt"

	"github.com/bluesky-social/jetstream/internal/diskspace"
	"github.com/prometheus/client_golang/prometheus"
)

// RegisterDataDirFreeBytes registers the operator-only free-space gauge for
// dataDir. Collection calls statfs directly so the value is scrape-time fresh.
func RegisterDataDirFreeBytes(reg prometheus.Registerer, dataDir string) {
	reg.MustRegister(dataDirFreeBytesCollector{
		desc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "data_dir_free_bytes"),
			"Bytes available to the jetstream process on the filesystem containing the data directory.",
			nil,
			nil,
		),
		dataDir: dataDir,
	})
}

type dataDirFreeBytesCollector struct {
	desc    *prometheus.Desc
	dataDir string
}

func (c dataDirFreeBytesCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

func (c dataDirFreeBytesCollector) Collect(ch chan<- prometheus.Metric) {
	free, err := diskspace.FreeBytes(c.dataDir)
	if err != nil {
		ch <- prometheus.NewInvalidMetric(c.desc, fmt.Errorf("collect data-dir free bytes: %w", err))
		return
	}
	ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(free))
}
