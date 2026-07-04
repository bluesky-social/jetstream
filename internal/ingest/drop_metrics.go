package ingest

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

// DropSource labels which ingest path dropped an upstream record:
// the live firehose consumer or the backfill repo handler.
type DropSource string

// DropReason labels why an upstream record or event was dropped at
// the ingest validation gate. The set is a closed enum: every reason
// is pre-registered at construction so operators see zero-valued
// series immediately and a typo'd label fails loud instead of
// minting a new series.
type DropReason string

const (
	DropSourceLive     DropSource = "live"
	DropSourceBackfill DropSource = "backfill"

	// DropReasonInvalidRev: the commit/sync rev is not a valid TID
	// per the atproto repository spec. Drops the whole event (live)
	// or fails the repo (backfill) — rev ordering drives merge and
	// compaction decisions, so an unparseable rev is never archived.
	DropReasonInvalidRev DropReason = "invalid_rev"

	// DropReasonInvalidCollection: the op's collection is not a
	// spec-valid NSID (also rejects `$`-prefixed names that could
	// shadow the $account/$identity/$sync sentinels). Drops the op;
	// well-formed siblings in the same event survive.
	DropReasonInvalidCollection DropReason = "invalid_collection"

	// DropReasonInvalidRkey: the op's record key fails the atproto
	// record-key syntax. Drops the op; siblings survive.
	DropReasonInvalidRkey DropReason = "invalid_rkey"

	// DropReasonFieldTooLong: spec-valid but unrepresentable in the
	// segment column widths (e.g. a 256–512 byte rkey exceeds our
	// 255-byte column). Kept distinct from the spec-invalid reasons
	// so operators can tell "network sent garbage" from "legal but
	// we chose not to represent".
	DropReasonFieldTooLong DropReason = "field_too_long"

	// DropReasonMissingBlock: a create/update op referenced a CID
	// whose record block was absent from the commit's CAR diff
	// (partial CARs are spec-permitted but unarchivable). Drops the
	// op; siblings survive.
	DropReasonMissingBlock DropReason = "missing_block"
)

var dropSources = []DropSource{DropSourceLive, DropSourceBackfill}

var dropReasons = []DropReason{
	DropReasonInvalidRev,
	DropReasonInvalidCollection,
	DropReasonInvalidRkey,
	DropReasonFieldTooLong,
	DropReasonMissingBlock,
}

// DropMetrics owns the shared dropped-events counter family for both
// ingest paths. A nil *DropMetrics is a valid zero-value: every
// method is a no-op, so tests can skip metric registration entirely.
type DropMetrics struct {
	dropped *prometheus.CounterVec

	// bound caches one pre-resolved counter per (source, reason)
	// pair so hot-path increments skip the CounterVec label lookup
	// (a map access + lock inside prometheus).
	bound map[DropSource]map[DropReason]prometheus.Counter
}

// NewDropMetrics registers the shared dropped-events counter family
// against reg. Calls reg.MustRegister, which panics if already
// registered. Construct exactly once per process.
func NewDropMetrics(reg prometheus.Registerer) *DropMetrics {
	m := &DropMetrics{
		dropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "dropped_events_total",
			Help: "Number of upstream records or events dropped at the ingest validation " +
				"gate, labeled by ingest path (source) and drop reason. Spec-invalid input " +
				"(invalid_rev, invalid_collection, invalid_rkey) is distinguished from " +
				"spec-valid-but-unrepresentable (field_too_long) and from upstream " +
				"omissions (missing_block).",
		}, []string{"source", "reason"}),
		bound: make(map[DropSource]map[DropReason]prometheus.Counter, len(dropSources)),
	}
	for _, s := range dropSources {
		m.bound[s] = make(map[DropReason]prometheus.Counter, len(dropReasons))
		for _, r := range dropReasons {
			m.bound[s][r] = m.dropped.WithLabelValues(string(s), string(r))
		}
	}
	reg.MustRegister(m.dropped)
	return m
}

// Counter returns the pre-bound counter for one (source, reason)
// series, for test assertions via testutil.ToFloat64. Panics on a
// pair outside the registered enum and on a nil receiver — tests
// asserting on metrics must construct real ones.
func (m *DropMetrics) Counter(source DropSource, reason DropReason) prometheus.Counter {
	c, ok := m.bound[source][reason]
	if !ok {
		panic(fmt.Sprintf("ingest: unregistered drop metric pair (%q, %q)", source, reason))
	}
	return c
}

// IncDropped increments the (source, reason) series by one. Panics on
// a pair outside the registered enum — that is programmer error, not
// upstream input.
func (m *DropMetrics) IncDropped(source DropSource, reason DropReason) {
	m.AddDropped(source, reason, 1)
}

// AddDropped increments the (source, reason) series by n. n <= 0 is a
// no-op. Panics on a pair outside the registered enum.
func (m *DropMetrics) AddDropped(source DropSource, reason DropReason, n int) {
	if m == nil || n <= 0 {
		return
	}
	c, ok := m.bound[source][reason]
	if !ok {
		panic(fmt.Sprintf("ingest: unregistered drop metric pair (%q, %q)", source, reason))
	}
	c.Add(float64(n))
}
