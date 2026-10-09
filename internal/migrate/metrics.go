package migrate

import (
	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics is the migrator's Prometheus state: jetstream_migration_*. All
// methods are nil-safe.
type Metrics struct {
	state            *prometheus.GaugeVec
	localNextSeq     prometheus.Gauge
	remoteNextSeq    prometheus.Gauge
	lagSeqs          prometheus.Gauge
	segmentsImported prometheus.Counter
	blocksShipped    prometheus.Counter
	bytesUploaded    prometheus.Counter
	segmentsRemote   prometheus.Gauge
	segmentsLocal    prometheus.Gauge
	metaKeysWritten  prometheus.Counter
	dirtyKeys        prometheus.Gauge
	dirtyOverflows   prometheus.Counter
	resyncs          prometheus.Counter
	resyncDiffs      prometheus.Counter
	lastResync       prometheus.Gauge
	pausedSince      prometheus.Gauge
	failures         *prometheus.CounterVec
	handoffs         *prometheus.CounterVec
}

// NewMetrics registers the migrator's series against reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	o := func(name, help string) prometheus.Opts {
		return prometheus.Opts{Namespace: "jetstream", Subsystem: "migration", Name: name, Help: help}
	}
	m := &Metrics{
		state:            prometheus.NewGaugeVec(prometheus.GaugeOpts(o("state", "1 for the catalog's current migration/state, as the migrator last read it.")), []string{"state"}),
		localNextSeq:     prometheus.NewGauge(prometheus.GaugeOpts(o("local_next_seq", "The source's durable seq/next."))),
		remoteNextSeq:    prometheus.NewGauge(prometheus.GaugeOpts(o("remote_next_seq", "The catalog's seq/next: everything below it is shipped."))),
		lagSeqs:          prometheus.NewGauge(prometheus.GaugeOpts(o("lag_seqs", "Seqs durable locally and not yet in the catalog."))),
		segmentsImported: prometheus.NewCounter(prometheus.CounterOpts(o("segments_imported_total", "Sealed segments imported or sealed in the catalog."))),
		blocksShipped:    prometheus.NewCounter(prometheus.CounterOpts(o("blocks_shipped_total", "Blocks shipped to the catalog, sealed or active."))),
		bytesUploaded:    prometheus.NewCounter(prometheus.CounterOpts(o("bytes_uploaded_total", "Segment bytes handed to the upload protocol."))),
		segmentsRemote:   prometheus.NewGauge(prometheus.GaugeOpts(o("segments_remote", "Main segments in the catalog, the active one included."))),
		segmentsLocal:    prometheus.NewGauge(prometheus.GaugeOpts(o("segments_local", "Main segments in the source, the active one included."))),
		metaKeysWritten:  prometheus.NewCounter(prometheus.CounterOpts(o("meta_keys_written_total", "Metadata keys set or deleted in the catalog."))),
		dirtyKeys:        prometheus.NewGauge(prometheus.GaugeOpts(o("dirty_keys", "Metadata keys changed locally and not yet copied."))),
		dirtyOverflows:   prometheus.NewCounter(prometheus.CounterOpts(o("dirty_overflows_total", "Times the changed-key set outgrew its bound and a full resync replaced it."))),
		resyncs:          prometheus.NewCounter(prometheus.CounterOpts(o("resyncs_total", "Full metadata resyncs completed."))),
		resyncDiffs:      prometheus.NewCounter(prometheus.CounterOpts(o("resync_diffs_total", "Keys full resyncs found different and repaired."))),
		lastResync:       prometheus.NewGauge(prometheus.GaugeOpts(o("last_resync_timestamp_seconds", "When the last full metadata resync finished."))),
		pausedSince:      prometheus.NewGauge(prometheus.GaugeOpts(o("compaction_paused_since_timestamp_seconds", "When the migration paused local compaction; 0 when it is not paused."))),
		failures:         prometheus.NewCounterVec(prometheus.CounterOpts(o("failures_total", "Migrator sessions that ended in error, by step.")), []string{"step"}),
		handoffs:         prometheus.NewCounterVec(prometheus.CounterOpts(o("handoffs_total", "Handoff attempts by result.")), []string{"result"}),
	}
	reg.MustRegister(m.state, m.localNextSeq, m.remoteNextSeq, m.lagSeqs, m.segmentsImported, m.blocksShipped,
		m.bytesUploaded, m.segmentsRemote, m.segmentsLocal, m.metaKeysWritten, m.dirtyKeys, m.dirtyOverflows,
		m.resyncs, m.resyncDiffs, m.lastResync, m.pausedSince, m.failures, m.handoffs)
	return m
}

func (m *Metrics) setState(st catalog.MigrationState) {
	if m == nil {
		return
	}
	m.state.Reset()
	if st == "" {
		st = "none"
	}
	m.state.WithLabelValues(string(st)).Set(1)
}

func (m *Metrics) setSeqs(local, remote uint64) {
	if m == nil {
		return
	}
	m.localNextSeq.Set(float64(local))
	m.remoteNextSeq.Set(float64(remote))
	if local > remote {
		m.lagSeqs.Set(float64(local - remote))
	} else {
		m.lagSeqs.Set(0)
	}
}

func (m *Metrics) setSegments(local, remote int) {
	if m != nil {
		m.segmentsLocal.Set(float64(local))
		m.segmentsRemote.Set(float64(remote))
	}
}

func (m *Metrics) imported(segments, blocks int, bytes int64) {
	if m != nil {
		m.segmentsImported.Add(float64(segments))
		m.blocksShipped.Add(float64(blocks))
		m.bytesUploaded.Add(float64(bytes))
	}
}

func (m *Metrics) metaWritten(n int) {
	if m != nil {
		m.metaKeysWritten.Add(float64(n))
	}
}

func (m *Metrics) setDirty(n int) {
	if m != nil {
		m.dirtyKeys.Set(float64(n))
	}
}

func (m *Metrics) overflow() {
	if m != nil {
		m.dirtyOverflows.Inc()
	}
}

func (m *Metrics) resynced(diffs int, at float64) {
	if m != nil {
		m.resyncs.Inc()
		m.resyncDiffs.Add(float64(diffs))
		m.lastResync.Set(at)
	}
}

func (m *Metrics) setPausedSince(at float64) {
	if m != nil {
		m.pausedSince.Set(at)
	}
}

func (m *Metrics) failed(step string) {
	if m != nil {
		m.failures.WithLabelValues(step).Inc()
	}
}

func (m *Metrics) handoff(result string) {
	if m != nil {
		m.handoffs.WithLabelValues(result).Inc()
	}
}
