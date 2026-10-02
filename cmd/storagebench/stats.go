package main

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// samples is a set of durations, safe for concurrent use.
type samples struct {
	mu sync.Mutex
	d  []time.Duration
}

func (s *samples) add(d time.Duration) {
	s.mu.Lock()
	s.d = append(s.d, d)
	s.mu.Unlock()
}

// summary is a sorted copy's order statistics.
type summary struct {
	n                  int
	p50, p90, p99, max time.Duration
	mean               time.Duration
}

func (s *samples) summary() summary {
	s.mu.Lock()
	d := slices.Clone(s.d)
	s.mu.Unlock()
	if len(d) == 0 {
		return summary{}
	}
	slices.Sort(d)
	var total time.Duration
	for _, v := range d {
		total += v
	}
	q := func(p float64) time.Duration { return d[min(len(d)-1, int(p*float64(len(d))))] }
	return summary{n: len(d), p50: q(0.50), p90: q(0.90), p99: q(0.99), max: d[len(d)-1], mean: total / time.Duration(len(d))}
}

func (s summary) String() string {
	if s.n == 0 {
		return "n=0"
	}
	return fmt.Sprintf("n=%d p50=%s p90=%s p99=%s max=%s mean=%s",
		s.n, ms(s.p50), ms(s.p90), ms(s.p99), ms(s.max), ms(s.mean))
}

func ms(d time.Duration) string {
	return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000)
}

// txnStats records leader transactions by kind: Begin to the end of
// Commit, and Commit alone.
type txnStats struct {
	mu     sync.Mutex
	total  map[catalog.TxKind]*samples
	commit map[catalog.TxKind]*samples
}

func newTxnStats() *txnStats {
	return &txnStats{total: map[catalog.TxKind]*samples{}, commit: map[catalog.TxKind]*samples{}}
}

func (t *txnStats) observe(kind catalog.TxKind, total, commit time.Duration) {
	t.mu.Lock()
	if t.total[kind] == nil {
		t.total[kind], t.commit[kind] = &samples{}, &samples{}
	}
	tot, com := t.total[kind], t.commit[kind]
	t.mu.Unlock()
	tot.add(total)
	com.add(commit)
}

func (t *txnStats) kinds() []catalog.TxKind {
	t.mu.Lock()
	defer t.mu.Unlock()
	var ks []catalog.TxKind
	for k := range t.total {
		ks = append(ks, k)
	}
	slices.Sort(ks)
	return ks
}

// timedDB times the leader transactions a catalog.Session runs. Reads
// pass through.
type timedDB struct {
	catalog.DB
	stats func() *txnStats
}

func (d *timedDB) Begin(ctx context.Context, kind catalog.TxKind) (catalog.Tx, error) {
	start := time.Now()
	tx, err := d.DB.Begin(ctx, kind)
	if err != nil {
		return nil, err
	}
	return &timedTx{Tx: tx, kind: kind, start: start, stats: d.stats()}, nil
}

type timedTx struct {
	catalog.Tx
	kind  catalog.TxKind
	start time.Time
	stats *txnStats
}

func (t *timedTx) Commit(ctx context.Context) error {
	c := time.Now()
	err := t.Tx.Commit(ctx)
	if err == nil {
		end := time.Now()
		t.stats.observe(t.kind, end.Sub(t.start), end.Sub(c))
	}
	return err
}

// observedHistogram forwards each observation to fn as well.
type observedHistogram struct {
	prometheus.Histogram
	fn func(time.Duration)
}

func (h observedHistogram) Observe(v float64) {
	h.Histogram.Observe(v)
	h.fn(time.Duration(v * float64(time.Second)))
}

func gaugeValue(g prometheus.Gauge) float64 {
	var m dto.Metric
	if err := g.Write(&m); err != nil {
		return 0
	}
	return m.GetGauge().GetValue()
}

func counterValue(c prometheus.Counter) float64 {
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		return 0
	}
	return m.GetCounter().GetValue()
}

func mib(b float64) string { return fmt.Sprintf("%.1fMiB", b/(1<<20)) }

func printKV(w io.Writer, rows [][2]string) {
	width := 0
	for _, r := range rows {
		width = max(width, len(r[0]))
	}
	for _, r := range rows {
		_, _ = fmt.Fprintf(w, "  %-*s  %s\n", width, r[0], r[1])
	}
}

func sortedKeys[K cmp.Ordered, V any](m map[K]V) []K {
	ks := make([]K, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	slices.Sort(ks)
	return ks
}
