package protocol

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"golang.org/x/sync/errgroup"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/objstore"
)

// Defaults for the §13 page sizes.
const (
	DefaultGCMarkPage   = 10_000
	DefaultGCClaimLimit = 1_000
)

// GCConfig configures a Collector.
type GCConfig struct {
	Blob      objstore.Blob
	ArchiveID [16]byte
	// Delay and OrphanAge are JETSTREAM_GC_DELAY and JETSTREAM_GC_ORPHAN_AGE.
	Delay     time.Duration
	OrphanAge time.Duration
	// MarkPage and ClaimLimit bound each mark and claim transaction. Zero
	// means DefaultGCMarkPage and DefaultGCClaimLimit.
	MarkPage   int
	ClaimLimit int
	// Concurrency bounds the deletes in flight. Zero means
	// DefaultUploadConcurrency.
	Concurrency int
	Metrics     *GCMetrics
	// Crash may be nil.
	Crash crashpoint.Injector
}

// Collector runs design §13 garbage collection inside a leader's
// catalog.Session. It is safe for concurrent use, but a leader runs one at
// a time.
type Collector struct {
	cfg GCConfig
}

// NewCollector validates cfg and returns a Collector.
func NewCollector(cfg GCConfig) (*Collector, error) {
	if cfg.Blob == nil {
		return nil, errors.New("protocol: collector needs a Blob")
	}
	if cfg.Delay <= 0 || cfg.OrphanAge <= 0 {
		return nil, fmt.Errorf("protocol: gc delay %v and orphan age %v must be positive", cfg.Delay, cfg.OrphanAge)
	}
	if cfg.MarkPage <= 0 {
		cfg.MarkPage = DefaultGCMarkPage
	}
	if cfg.ClaimLimit <= 0 {
		cfg.ClaimLimit = DefaultGCClaimLimit
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = DefaultUploadConcurrency
	}
	return &Collector{cfg: cfg}, nil
}

// GCResult is what one Run did.
type GCResult struct {
	Marked  int
	Claimed int
	Deleted int
}

// Run is one §13 run: mark every unreferenced object, then claim, delete,
// and forget in batches until nothing is left to claim. A claim hands back
// deleting rows a failed run left before any fresh ones, so an interrupted
// run resumes where it stopped.
//
// A failed catalog transaction ends s, and Run returns its error, which
// wraps catalog.ErrSessionEnded or is a CorruptionError. A failed delete
// leaves its batch's rows deleting and fails only the run: the session is
// intact and the next run retries the batch. The objects gauge is refreshed
// from db after every run that reached the catalog; db may be nil.
func (c *Collector) Run(ctx context.Context, s *catalog.Session, db catalog.DB) (res GCResult, err error) {
	ctx, span := tracer.Start(ctx, "objstore.GC")
	start := time.Now()
	defer func() {
		span.SetAttributes(
			attribute.Int("gc.marked", res.Marked),
			attribute.Int("gc.claimed", res.Claimed),
			attribute.Int("gc.deleted", res.Deleted),
		)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
		c.cfg.Metrics.observeRun(time.Since(start), err)
		if db != nil {
			c.cfg.Metrics.refreshObjects(ctx, db)
		}
	}()

	var after uint64
	for {
		page, err := s.GCMark(ctx, after, c.cfg.MarkPage)
		if err != nil {
			return res, fmt.Errorf("protocol: gc mark: %w", err)
		}
		res.Marked += page.Marked
		if page.Scanned < c.cfg.MarkPage {
			break
		}
		after = page.Last
	}
	if err := c.crash(ctx, crashpoint.AfterGCMarkBeforeClaim); err != nil {
		return res, err
	}

	for {
		rows, err := s.GCClaim(ctx, c.cfg.Delay, c.cfg.OrphanAge, c.cfg.ClaimLimit)
		if err != nil {
			return res, fmt.Errorf("protocol: gc claim: %w", err)
		}
		if len(rows) == 0 {
			return res, nil
		}
		res.Claimed += len(rows)
		if err := c.crash(ctx, crashpoint.AfterGCClaimBeforeDelete); err != nil {
			return res, err
		}
		if err := c.delete(ctx, rows); err != nil {
			return res, err
		}
		if err := c.crash(ctx, crashpoint.AfterGCDeleteBeforeForget); err != nil {
			return res, err
		}
		ids := make([]uint64, len(rows))
		for i, r := range rows {
			ids[i] = r.ID
		}
		n, err := s.GCForget(ctx, ids)
		if err != nil {
			return res, fmt.Errorf("protocol: gc forget: %w", err)
		}
		res.Deleted += n
		c.cfg.Metrics.deleted(n)
		// Only this session changes objects rows between its claim and
		// its forget, and a claimed row stays deleting until forgotten.
		if n != len(ids) {
			return res, catalog.Corruptf(catalog.SourceGC, "forget removed %d of %d claimed objects", n, len(ids))
		}
	}
}

// delete removes every row's key. Missing keys succeed (objstore.Blob).
func (c *Collector) delete(ctx context.Context, rows []catalog.ObjectRow) error {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(c.cfg.Concurrency)
	var failed atomic.Int64
	for _, r := range rows {
		g.Go(func() error {
			key := objstore.Key(c.cfg.ArchiveID, r.Key)
			if err := c.cfg.Blob.DeleteKey(gctx, key); err != nil {
				failed.Add(1)
				return fmt.Errorf("protocol: gc delete object %d (%s): %w", r.ID, key, err)
			}
			return nil
		})
	}
	err := g.Wait()
	c.cfg.Metrics.deleteFailed(int(failed.Load()))
	return err
}

func (c *Collector) crash(ctx context.Context, p crashpoint.Point) error {
	if c.cfg.Crash == nil {
		return nil
	}
	return c.cfg.Crash.SimulateCrash(ctx, p)
}

// Outcome labels for jetstream_gc_runs_total.
const (
	gcOutcomeOK    = "ok"
	gcOutcomeError = "error"
)

// GCMetrics owns the design §23 GC series. A nil *GCMetrics is valid and
// records nothing.
type GCMetrics struct {
	Objects        *prometheus.GaugeVec
	Deleted        prometheus.Counter
	DeleteFailures prometheus.Counter
	Runs           *prometheus.CounterVec
	RunDuration    prometheus.Histogram
}

// NewGCMetrics registers the series against reg. Construct exactly once
// per process.
func NewGCMetrics(reg prometheus.Registerer) *GCMetrics {
	m := &GCMetrics{
		Objects: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "jetstream",
			Name:      "objects",
			Help:      "Catalog objects rows by state, as of the leader's last GC run.",
		}, []string{"state"}),
		Deleted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "jetstream",
			Subsystem: "gc",
			Name:      "deleted_total",
			Help:      "Objects GC deleted from the object store and forgot in the catalog.",
		}),
		DeleteFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "jetstream",
			Subsystem: "gc",
			Name:      "delete_failures_total",
			Help:      "Object store deletes that failed. Their rows stay deleting and the next GC run retries them.",
		}),
		Runs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "jetstream",
			Subsystem: "gc",
			Name:      "runs_total",
			Help:      "GC runs by outcome.",
		}, []string{"outcome"}),
		RunDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "jetstream",
			Subsystem: "gc",
			Name:      "run_duration_seconds",
			Help:      "Wall time of one GC run: mark, then claim, delete, and forget until nothing is left.",
			Buckets:   prometheus.ExponentialBuckets(0.01, 4, 10),
		}),
	}
	reg.MustRegister(m.Objects, m.Deleted, m.DeleteFailures, m.Runs, m.RunDuration)
	return m
}

func (m *GCMetrics) observeRun(d time.Duration, err error) {
	if m == nil {
		return
	}
	m.RunDuration.Observe(d.Seconds())
	outcome := gcOutcomeOK
	if err != nil {
		outcome = gcOutcomeError
	}
	m.Runs.WithLabelValues(outcome).Inc()
}

func (m *GCMetrics) deleted(n int) {
	if m != nil {
		m.Deleted.Add(float64(n))
	}
}

func (m *GCMetrics) deleteFailed(n int) {
	if m != nil && n > 0 {
		m.DeleteFailures.Add(float64(n))
	}
}

// refreshObjects sets the objects gauge for every state, zero for states
// with no rows. A failed read, including one a cancelled run cannot start,
// leaves the gauge as it was: a lagging gauge is not worth failing GC for.
func (m *GCMetrics) refreshObjects(ctx context.Context, db catalog.DB) {
	if m == nil {
		return
	}
	r, err := db.BeginRead(ctx)
	if err != nil {
		return
	}
	defer func() { _ = r.Close(context.WithoutCancel(ctx)) }()
	states, err := r.ObjectStates(ctx)
	if err != nil {
		return
	}
	for _, st := range []catalog.ObjectState{catalog.ObjectUploading, catalog.ObjectAvailable, catalog.ObjectDeleting} {
		m.Objects.WithLabelValues(string(st)).Set(float64(states[st]))
	}
}
