package follower

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/internal/objstore"
	"github.com/bluesky-social/jetstream/internal/objstore/objcache"
	"github.com/bluesky-social/jetstream/internal/objstore/protocol"
	"github.com/bluesky-social/jetstream/segment"
)

const (
	DefaultPollInterval    = 250 * time.Millisecond
	DefaultMaxViewAge      = 30 * time.Second
	DefaultReadConcurrency = 32

	listenBackoffMin = 250 * time.Millisecond
	listenBackoffMax = 5 * time.Second
)

// Config configures a Follower.
type Config struct {
	DB catalog.DB
	// Listener delivers NOTIFY revisions. Nil means the follower only polls.
	Listener catalog.Listener
	Blob     objstore.Blob
	// ArchiveID must match the catalog's archive row; a mismatch is fatal.
	ArchiveID [16]byte
	// Cache is the process's verified-object cache. May be nil.
	Cache *objcache.Cache
	// Manifest is a manifest.NewRemote manifest the follower feeds with
	// Main's sealed segments. May be nil in tests.
	Manifest *manifest.Manifest

	PollInterval time.Duration
	MaxViewAge   time.Duration
	// ReadConcurrency bounds concurrent footer fetches
	// (JETSTREAM_S3_READ_CONCURRENCY).
	ReadConcurrency int
	// ReadLogBytes is the readable log's retention budget.
	ReadLogBytes int64

	Logger          *slog.Logger
	Metrics         *Metrics
	CatalogMetrics  *catalog.Metrics
	ProtocolMetrics *protocol.Metrics
	IngestMetrics   *ingest.Metrics
	// Now defaults to time.Now.
	Now func() time.Time
}

// Follower keeps this pod's catalog mirror current (design §11.1) and feeds
// the pod's readable log from it. Readers take the current mirror with one
// atomic load; the follower's single tick loop is the only writer.
//
// It implements catalog.Catalog, catalog.Fetcher (through Fetcher),
// protocol.RowSource, and lifecycle.Readiness.
type Follower struct {
	cfg Config
	log *slog.Logger
	rd  *protocol.Reader

	cur  atomic.Pointer[mirror]
	flog atomic.Pointer[ingest.FollowerLog]
	// footersLoaded is set once the manifest holds every sealed segment of
	// the first steady-state mirror.
	footersLoaded atomic.Bool

	// tickMu serializes ticks. It is a channel so Refresh can give up on
	// its context while waiting.
	tickMu   chan struct{}
	started  atomic.Uint64
	last     atomic.Pointer[tickResult]
	doorbell chan struct{}

	failOnce sync.Once
	failed   chan struct{}
	failErr  error
}

type tickResult struct {
	n   uint64
	err error
}

// fatalError marks a tick failure the follower must not retry.
type fatalError struct{ err error }

func (e *fatalError) Error() string { return e.err.Error() }
func (e *fatalError) Unwrap() error { return e.err }

func isFatal(err error) bool {
	var fe *fatalError
	if errors.As(err, &fe) {
		return true
	}
	_, corrupt := catalog.IsCorruption(err)
	return corrupt
}

// New validates cfg and returns a Follower. Call Run to start it.
func New(cfg Config) (*Follower, error) {
	if cfg.DB == nil || cfg.Blob == nil {
		return nil, errors.New("follower: DB and Blob are required")
	}
	if cfg.ArchiveID == ([16]byte{}) {
		return nil, errors.New("follower: ArchiveID is required")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = DefaultPollInterval
	}
	if cfg.MaxViewAge <= 0 {
		cfg.MaxViewAge = DefaultMaxViewAge
	}
	if cfg.ReadConcurrency <= 0 {
		cfg.ReadConcurrency = DefaultReadConcurrency
	}
	if cfg.ReadLogBytes <= 0 {
		cfg.ReadLogBytes = ingest.DefaultReadLogRetentionBytes
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	f := &Follower{
		cfg:      cfg,
		log:      cfg.Logger.With(slog.String("component", "catalog_follower")),
		tickMu:   make(chan struct{}, 1),
		doorbell: make(chan struct{}, 1),
		failed:   make(chan struct{}),
	}
	rd, err := f.reader(f)
	if err != nil {
		return nil, err
	}
	f.rd = rd
	cfg.Metrics.setLagSource(func() float64 {
		m := f.cur.Load()
		if m == nil {
			return 0
		}
		return f.cfg.Now().Sub(m.refreshed).Seconds()
	})
	return f, nil
}

func (f *Follower) reader(rows protocol.RowSource) (*protocol.Reader, error) {
	return protocol.NewReader(protocol.ReaderConfig{
		Rows:           rows,
		Blob:           f.cfg.Blob,
		ArchiveID:      f.cfg.ArchiveID,
		Cache:          f.cfg.Cache,
		Metrics:        f.cfg.ProtocolMetrics,
		CatalogMetrics: f.cfg.CatalogMetrics,
	})
}

// Run ticks until ctx ends or a tick finds corruption or a foreign archive,
// which it returns. Transient failures keep the previous mirror and retry
// on the next wakeup; readiness reports a mirror that stays stale.
func (f *Follower) Run(ctx context.Context) error {
	poll := time.NewTicker(f.cfg.PollInterval)
	defer poll.Stop()

	var notify <-chan uint64
	var relisten <-chan time.Time
	backoff := listenBackoffMin
	listen := func() {
		if f.cfg.Listener == nil {
			return
		}
		ch, err := f.cfg.Listener.Listen(ctx)
		if err != nil {
			if ctx.Err() == nil {
				f.cfg.Metrics.listenFailed()
				f.log.Warn("catalog listen failed; polling until it reconnects", slog.Any("error", err), slog.Duration("retry_in", backoff))
			}
			relisten = time.After(backoff)
			backoff = min(2*backoff, listenBackoffMax)
			return
		}
		notify, backoff = ch, listenBackoffMin
	}
	listen()

	_ = f.tickLocked(ctx)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-f.failed:
			return f.failErr
		case rev, ok := <-notify:
			if !ok {
				notify = nil
				if ctx.Err() != nil {
					continue
				}
				f.cfg.Metrics.listenFailed()
				relisten = time.After(backoff)
				backoff = min(2*backoff, listenBackoffMax)
				continue
			}
			f.cfg.Metrics.notifyReceived()
			if m := f.cur.Load(); m != nil && rev <= m.rev {
				continue
			}
		case <-relisten:
			relisten = nil
			listen()
			// Anything committed while LISTEN was down was never notified.
		case <-poll.C:
		case <-f.doorbell:
		}
		_ = f.tickLocked(ctx)
	}
}

// Doorbell asks for a tick without waiting for it. The leader pod rings it
// after each commit so its own readers need not wait for NOTIFY.
func (f *Follower) Doorbell() {
	select {
	case f.doorbell <- struct{}{}:
	default:
	}
}

// Refresh runs, or waits for, a tick that started after the call, and
// returns its error (design §11.6). It implements catalog.Catalog.
func (f *Follower) Refresh(ctx context.Context) error {
	want := f.started.Load() + 1
	select {
	case f.tickMu <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-f.failed:
		return f.failErr
	}
	defer func() { <-f.tickMu }()
	if r := f.last.Load(); r != nil && r.n >= want {
		return r.err
	}
	return f.tick(ctx)
}

func (f *Follower) tickLocked(ctx context.Context) error {
	select {
	case f.tickMu <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-f.tickMu }()
	return f.tick(ctx)
}

// tick runs one tick; the caller holds tickMu.
func (f *Follower) tick(ctx context.Context) error {
	n := f.started.Add(1)
	err := f.runTick(ctx)
	f.last.Store(&tickResult{n: n, err: err})
	switch {
	case err == nil || ctx.Err() != nil:
	case isFatal(err):
		f.cfg.CatalogMetrics.ObserveCorruption(err)
		f.log.Error("catalog follower stopping", slog.Any("error", err))
		f.failOnce.Do(func() {
			f.failErr = err
			close(f.failed)
		})
	default:
		f.cfg.Metrics.refreshFailed()
		f.log.Warn("catalog refresh failed; keeping the previous mirror", slog.Any("error", err))
	}
	return err
}

func (f *Follower) runTick(ctx context.Context) error {
	start := f.cfg.Now()
	old := f.cur.Load()
	c, err := f.read(ctx, old)
	if err != nil {
		return err
	}
	if c == nil {
		cp := *old
		cp.refreshed = start
		f.cur.Store(&cp)
		return nil
	}

	var objects objIndex
	if old != nil {
		objects = old.objects
	}
	objects = objects.with(c.objects)
	rd, err := f.reader(tickRows{objects: objects, db: protocol.DBRows{DB: f.cfg.DB}})
	if err != nil {
		return err
	}
	next, loaded, err := f.build(ctx, old, c, rd, objects)
	if err != nil {
		return err
	}
	next.refreshed = start

	steady := next.phase == lifecycle.PhaseSteadyState
	flog := f.flog.Load()
	var events []segment.Event
	if flog != nil && steady {
		// Reading (and verifying) every block the log still lacks,
		// pointer batches included, happens before anything is published,
		// so a failed fetch leaves the whole tick to retry.
		if events, err = f.feed(ctx, next, flog.Log().TipSeq(), rd); err != nil {
			return err
		}
	}
	var initial []manifest.RemoteSegment
	if steady && !f.footersLoaded.Load() && f.cfg.Manifest != nil {
		if initial, err = f.remoteSegments(ctx, next, loaded, rd); err != nil {
			return err
		}
	}

	// The log gets the events before the swap, so a subscriber never sees
	// a mirror tip above the log's; durable advances only after the swap,
	// because advancing lets the log evict, and a reader that saw the new
	// floor must find the evicted seqs in the mirror it loads next.
	for i := range events {
		if err := flog.Append(&events[i]); err != nil {
			return catalog.Corruptf(catalog.SourceInvariant, "%v", err)
		}
	}
	f.cur.Store(next)
	f.cfg.Metrics.setRevision(next.rev)
	if flog != nil && steady {
		if err := flog.AdvanceDurable(flog.Log().TipSeq()); err != nil {
			return catalog.Corruptf(catalog.SourceInvariant, "%v", err)
		}
	}
	if flog == nil && steady {
		// At pod start the log begins at the tip: older seqs are the cold
		// reader's. Seqs start at 1, so an empty archive's log starts there.
		f.flog.Store(ingest.NewFollowerLog(max(next.view.TipSeq(catalog.Main), 1), f.cfg.ReadLogBytes, f.cfg.IngestMetrics))
	}
	now := f.cfg.Now()
	for i := range events {
		f.cfg.Metrics.observeVisibility(now.Sub(time.UnixMicro(events[i].WitnessedAt)).Seconds())
	}

	if err := f.publish(ctx, next, loaded, initial); err != nil {
		return err
	}
	f.cfg.Metrics.observeRefresh(f.cfg.Now().Sub(start).Seconds())
	return nil
}

// publish feeds the manifest: the whole sealed Main archive on the first
// steady-state tick, then each Main generation this tick loaded. Footers
// were fetched before the swap, so only corruption can fail it.
func (f *Follower) publish(ctx context.Context, next *mirror, loaded []loadedGen, initial []manifest.RemoteSegment) error {
	if f.cfg.Manifest == nil || next.phase != lifecycle.PhaseSteadyState {
		return nil
	}
	if !f.footersLoaded.Load() {
		if err := f.cfg.Manifest.LoadRemote(ctx, initial, f.cfg.ReadConcurrency); err != nil {
			return catalog.Corruptf(catalog.SourceGeneration, "initial manifest load: %v", err)
		}
		f.footersLoaded.Store(true)
		return nil
	}
	slices.SortFunc(loaded, func(a, b loadedGen) int { return cmp.Compare(a.gen.idx, b.gen.idx) })
	for _, l := range loaded {
		g := l.gen
		if g.ns != catalog.Main {
			continue
		}
		if err := f.cfg.Manifest.ApplySegment(g.idx, g.header.Checksum, g.headerRaw, l.footer, g.createdAt, g.size); err != nil {
			return catalog.Corruptf(catalog.SourceGeneration, "%v", err)
		}
	}
	return nil
}
