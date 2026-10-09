package migrate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/leader"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/objstore/protocol"
	"golang.org/x/sync/errgroup"
)

// Status is the migrator's progress, served at /debug/migration and
// published to the control table for `jetstream migrate status`.
type Status struct {
	State               string    `json:"state"`
	Step                string    `json:"step"`
	Error               string    `json:"error,omitempty"`
	UpdatedAt           time.Time `json:"updated_at"`
	LocalNextSeq        uint64    `json:"local_next_seq"`
	RemoteNextSeq       uint64    `json:"remote_next_seq"`
	LagSeqs             uint64    `json:"lag_seqs"`
	LocalSegments       int       `json:"local_segments"`
	RemoteActiveSegment uint64    `json:"remote_active_segment"`
	ShippedActiveBlocks int       `json:"shipped_active_blocks"`
	DirtyKeys           int       `json:"dirty_keys"`
	LastResync          time.Time `json:"last_resync,omitzero"`
	LastResyncDiffs     int       `json:"last_resync_diffs"`
	CompactionPausedAt  time.Time `json:"compaction_paused_at,omitzero"`
	LastRequest         string    `json:"last_request,omitempty"`
}

// Migrator migrates the local archive. Construct with New, then Run.
type Migrator struct {
	cfg      Config
	dirty    *dirtySet
	meta     *metaSync
	uploader *protocol.Uploader
	thr      *throttle
	log      *slog.Logger

	drainOnce sync.Once
	finished  atomic.Pointer[catalog.MigrationState]

	mu     sync.Mutex
	status Status
}

// errFinished ends the migrator's election loop: the catalog's state says
// there is nothing left for it to do.
var errFinished = fmt.Errorf("migrate: finished: %w", leader.ErrDone)

// New validates cfg and installs the commit observer on cfg.Meta, so
// every local metadata write from here on is tracked.
func New(cfg Config) (*Migrator, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	up, err := protocol.NewUploader(protocol.UploaderConfig{
		Blob: cfg.Blob, ArchiveID: cfg.ArchiveID, GCDelay: cfg.GCDelay, OrphanAge: cfg.OrphanAge,
		Concurrency: cfg.UploadConcurrency,
	})
	if err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	m := &Migrator{
		cfg:      cfg,
		dirty:    newDirtySet(cfg.DirtyMaxKeys, cfg.Metrics),
		uploader: up,
		thr:      newThrottle(cfg.ReadBytesPerSec),
		log:      cfg.Logger.With(slog.String("component", "migrate")),
	}
	m.meta = &metaSync{local: cfg.Meta, remote: cfg.RemoteMeta, dirty: m.dirty, batch: cfg.MetaBatchKeys, metrics: cfg.Metrics}
	cfg.Meta.SetCommitObserver(m.dirty.observe)
	m.status.Step = "starting"
	return m, nil
}

// Close removes the commit observer.
func (m *Migrator) Close() { m.cfg.Meta.SetCommitObserver(nil) }

// Status returns the migrator's progress.
func (m *Migrator) Status() Status {
	m.mu.Lock()
	s := m.status
	m.mu.Unlock()
	s.DirtyKeys = m.dirty.len()
	s.LastResync, s.LastResyncDiffs = m.meta.lastResyncAt()
	return s
}

func (m *Migrator) update(fn func(*Status)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	fn(&m.status)
	m.status.UpdatedAt = time.Now()
}

func (m *Migrator) setStep(step string) {
	m.update(func(s *Status) { s.Step, s.Error = step, "" })
}

func (m *Migrator) setState(st catalog.MigrationState) {
	m.cfg.Metrics.setState(st)
	m.update(func(s *Status) { s.State = string(st) })
}

func (m *Migrator) setProgress(local uint64, tr *tracker, localSegs int) {
	m.update(func(s *Status) {
		s.LocalNextSeq, s.RemoteNextSeq, s.LocalSegments = local, tr.next, localSegs
		s.RemoteActiveSegment, s.ShippedActiveBlocks = tr.seg, len(tr.active)
		s.LagSeqs = 0
		if local > tr.next {
			s.LagSeqs = local - tr.next
		}
	})
}

// fail records why the migrator stopped. It never stops the process.
func (m *Migrator) fail(step string, err error) {
	m.cfg.Metrics.failed(step)
	m.log.Error("migration stopped; local ingest and reads are unaffected", "step", step, "err", err)
	m.update(func(s *Status) { s.Error = fmt.Sprintf("%s: %v", step, err) })
}

func (m *Migrator) crash(ctx context.Context, p crashpoint.Point) error {
	if m.cfg.CrashInjector == nil {
		return nil
	}
	return m.cfg.CrashInjector.SimulateCrash(ctx, p)
}

// Run migrates until the handoff, an abort, or ctx ends. It returns nil:
// a failure stops the migrator, logs, and counts, and the process carries
// on as a local archive.
func (m *Migrator) Run(ctx context.Context) error {
	defer m.Close()
	stopStatus := m.publishStatus(ctx)
	defer stopStatus()
	if err := m.cfg.CatalogReady(ctx); err != nil {
		if ctx.Err() == nil {
			m.fail("load the local catalog", err)
		}
		return nil
	}
	done, err := m.resolve(ctx)
	if err != nil {
		if ctx.Err() == nil {
			m.fail("resolve the handoff guard", err)
			<-ctx.Done()
		}
		return nil
	}
	if !done {
		err = leader.Run(ctx, leader.Config{
			Locker:          m.cfg.NewLease(),
			Lease:           m.cfg.Lease,
			RenewInterval:   m.cfg.RenewInterval,
			AcquireInterval: m.cfg.AcquireInterval,
			StandbyBackoff:  m.cfg.AcquireInterval * 20,
			Logger:          m.log,
		}, m.session)
		if err != nil {
			m.fail("session", err)
			<-ctx.Done()
			return nil
		}
	}
	if st := m.finished.Load(); st != nil && *st == catalog.MigrationDone {
		m.drain(ctx)
	}
	return nil
}

// drain moves the process to drained mode once.
func (m *Migrator) drain(ctx context.Context) {
	m.drainOnce.Do(func() {
		m.setStep("handed off: serving archive reads only")
		if m.cfg.Drain != nil {
			m.cfg.Drain(ctx)
		}
	})
}

func (m *Migrator) finish(st catalog.MigrationState) error {
	m.finished.Store(&st)
	m.setState(st)
	return errFinished
}

// remoteState reads migration/state outside any transaction. Only done is
// final: a handoff transaction in flight from a dead process can still
// commit after this read, so any other answer must be confirmed under the
// lease.
func (m *Migrator) remoteState(ctx context.Context) (catalog.MigrationState, error) {
	v, err := m.cfg.RemoteMeta.Get(ctx, []byte(catalog.MigrationStateKey))
	if errors.Is(err, metastore.ErrNotFound) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("migrate: read migration/state: %w", err)
	}
	return catalog.ParseMigrationState(v)
}

// resolve settles a handoff that may have committed, before the migrator
// competes for the lease: once done has committed, the lease belongs to the
// pods. It reports whether the migration is over.
func (m *Migrator) resolve(ctx context.Context) (bool, error) {
	m.setStep("reading the catalog")
	for {
		guard, err := ReadGuard(ctx, m.cfg.Meta)
		if err != nil {
			return false, err
		}
		st, err := m.remoteState(ctx)
		if err != nil {
			m.log.Warn("cannot read the catalog; retrying", "err", err)
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			case <-time.After(m.cfg.AcquireInterval * 4):
			}
			continue
		}
		m.setState(st)
		switch {
		case st == catalog.MigrationDone:
			if guard != GuardDone {
				if err := writeGuard(ctx, m.cfg.Meta, GuardDone); err != nil {
					return false, err
				}
			}
			_ = m.finish(st)
			return true, nil
		case guard == GuardDone:
			return false, fatalf("the local guard says the handoff committed, but migration/state is %q", st)
		}
		return false, nil
	}
}

// session is the migrator's leader.SessionFunc. A failure that a new
// session may get past ends this one as a standby, so the next waits out
// a backoff; fatal ones and errFinished end the election loop.
func (m *Migrator) session(ctx context.Context, epoch uint64) error {
	sess := catalog.NewSession(catalog.SessionConfig{DB: m.cfg.DB, Epoch: epoch})
	err := m.runSession(ctx, sess)
	var f interface{ SessionFatal() bool }
	switch {
	case err == nil || errors.Is(err, errFinished) || ctx.Err() != nil:
		return err
	case errors.As(err, &f) && f.SessionFatal():
		return err
	}
	m.fail(m.Status().Step, err)
	return fmt.Errorf("%w: %w", leader.ErrStandby, err)
}

func (m *Migrator) runSession(ctx context.Context, sess *catalog.Session) error {
	m.setStep("starting a session")
	st, err := sess.ReadMigrationState(ctx)
	if err != nil {
		return err
	}
	m.setState(st)
	guard, err := ReadGuard(ctx, m.cfg.Meta)
	if err != nil {
		return err
	}
	switch st {
	case "":
		m.setStep("waiting for `jetstream storage init --migrate-from-local`")
		return errors.New("migrate: the catalog was not initialized for a migration")
	case catalog.MigrationDone:
		if guard != GuardDone {
			if err := writeGuard(ctx, m.cfg.Meta, GuardDone); err != nil {
				return err
			}
		}
		return m.finish(st)
	case catalog.MigrationAborted:
		if err := m.resumeCompaction(ctx); err != nil {
			return err
		}
		m.cfg.Ingest.Start()
		return m.finish(st)
	case catalog.MigrationReverted:
		return m.finish(st)
	}
	if guard == GuardDone {
		return fatalf("the local guard says the handoff committed, but migration/state is %q", st)
	}
	phase, err := lifecycle.ReadPhase(ctx, m.cfg.Meta)
	if err != nil {
		return err
	}
	if phase != lifecycle.PhaseSteadyState {
		m.setStep("waiting for the source to reach steady_state")
		return fmt.Errorf("migrate: the source is in phase %q; only a steady_state archive migrates", phase)
	}

	// Under this lease no handoff from an earlier session can still
	// commit, so a handing_off state or a pending guard means that
	// handoff failed: local ingest resumes.
	if st == catalog.MigrationHandingOff {
		if _, err := sess.SetMigrationState(ctx, st, catalog.MigrationTailing, nil); err != nil {
			return err
		}
		m.cfg.Metrics.handoff("reverted")
		st = catalog.MigrationTailing
		m.setState(st)
	}
	if guard == GuardPending {
		if err := writeGuard(ctx, m.cfg.Meta, GuardNone); err != nil {
			return err
		}
	}
	m.cfg.Ingest.Start()
	m.abandonRequests(ctx)

	m.setStep("pausing local compaction")
	pausedAt, err := m.pauseCompaction(ctx)
	if err != nil {
		return err
	}
	m.setStep("reconciling the catalog with the source")
	tr, snap, err := loadTracker(ctx, m.cfg.DB)
	if err != nil {
		return err
	}
	local, err := localNext(ctx, m.cfg.Meta)
	if err != nil {
		return err
	}
	if err := m.reconcile(ctx, tr, snap, local); err != nil {
		return err
	}

	var tailing atomic.Bool
	tailing.Store(st == catalog.MigrationTailing)
	resynced := make(chan struct{})
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return m.metaLoop(gctx, sess, &tailing, resynced) })
	g.Go(func() error { return m.mainLoop(gctx, sess, tr, st, &tailing, resynced, pausedAt) })
	return g.Wait()
}

// abandonRequests answers every request a previous session left, claimed
// or not: a request is acted on only by the session that claims it, and a
// handoff must be asked for by an operator watching this session, never
// replayed after a restart.
func (m *Migrator) abandonRequests(ctx context.Context) {
	if err := m.cfg.Control.AbandonAll(ctx, "abandoned: the migrator restarted; check `jetstream migrate status` and ask again"); err != nil {
		m.log.Warn("cannot answer requests a previous session left", "err", err)
	}
}

func (m *Migrator) pauseCompaction(ctx context.Context) (time.Time, error) {
	at, ok, err := ReadCompactionPause(ctx, m.cfg.Meta)
	if err != nil {
		return time.Time{}, err
	}
	if err := m.cfg.Compaction.Pause(ctx); err != nil {
		return time.Time{}, fmt.Errorf("migrate: pause local compaction: %w", err)
	}
	if !ok {
		at = time.Now()
		if err := writeCompactionPause(ctx, m.cfg.Meta, at); err != nil {
			return time.Time{}, err
		}
	}
	m.cfg.Metrics.setPausedSince(float64(at.Unix()))
	m.update(func(s *Status) { s.CompactionPausedAt = at })
	return at, nil
}

func (m *Migrator) resumeCompaction(ctx context.Context) error {
	if err := m.cfg.Meta.Delete(ctx, []byte(CompactionPausedKey)); err != nil {
		return fmt.Errorf("migrate: clear %s: %w", CompactionPausedKey, err)
	}
	m.cfg.Compaction.Resume()
	m.cfg.Metrics.setPausedSince(0)
	m.update(func(s *Status) { s.CompactionPausedAt = time.Time{} })
	return nil
}

// metaLoop copies the metadata: a full resync first, then the dirty set
// every MetaFlushInterval and another full resync every VerifyInterval.
func (m *Migrator) metaLoop(ctx context.Context, sess *catalog.Session, tailing *atomic.Bool, resynced chan<- struct{}) error {
	if _, err := m.meta.resync(ctx, sess, tailing.Load(), false); err != nil {
		return err
	}
	close(resynced)
	flush := time.NewTicker(m.cfg.MetaFlushInterval)
	defer flush.Stop()
	verify := time.NewTicker(m.cfg.VerifyInterval)
	defer verify.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-flush.C:
			_, err := m.meta.flush(ctx, sess, tailing.Load())
			if errors.Is(err, errResyncNeeded) {
				_, err = m.meta.resync(ctx, sess, tailing.Load(), false)
			}
			if err != nil {
				return err
			}
		case <-verify.C:
			if _, err := m.meta.resync(ctx, sess, tailing.Load(), false); err != nil {
				return err
			}
		}
	}
}

// mainLoop ships segments, moves seeding to tailing once the seed is
// complete, and serves operator requests.
func (m *Migrator) mainLoop(ctx context.Context, sess *catalog.Session, tr *tracker, st catalog.MigrationState, tailing *atomic.Bool, resynced <-chan struct{}, pausedAt time.Time) error {
	poll := time.NewTicker(m.cfg.TailPollInterval)
	defer poll.Stop()
	ctl := time.NewTicker(m.cfg.ControlInterval)
	defer ctl.Stop()
	for {
		if st == catalog.MigrationSeeding {
			m.setStep("seeding")
		} else {
			m.setStep("tailing")
		}
		if _, err := m.ship(ctx, sess, tr, false); err != nil {
			return err
		}
		if st == catalog.MigrationSeeding && m.seeded(tr, resynced) {
			if err := m.startTailing(ctx, sess); err != nil {
				return err
			}
			st = catalog.MigrationTailing
			tailing.Store(true)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-poll.C:
		case <-ctl.C:
			req, ok, err := m.cfg.Control.Claim(ctx)
			if err != nil {
				m.log.Warn("cannot read operator requests", "err", err)
				continue
			}
			if !ok {
				continue
			}
			result, err := m.handle(ctx, sess, tr, st, req, pausedAt)
			m.update(func(s *Status) { s.LastRequest = fmt.Sprintf("%s: %s", req.Action, result) })
			m.log.Info("operator request", "action", req.Action, "id", req.ID, "result", result)
			if aerr := m.cfg.Control.Ack(ctx, req.ID, result); aerr != nil {
				m.log.Warn("cannot answer the operator request", "id", req.ID, "err", aerr)
			}
			if err != nil {
				return err
			}
		}
	}
}

// seeded reports whether the seed is complete: every sealed local segment
// imported, the active one close behind, and the metadata copied once.
func (m *Migrator) seeded(tr *tracker, resynced <-chan struct{}) bool {
	select {
	case <-resynced:
	default:
		return false
	}
	s := m.Status()
	return tr.seg+1 >= uint64(s.LocalSegments) && s.LagSeqs <= m.cfg.HandoffMaxLagSeqs
}

// startTailing moves seeding to tailing and copies the phase with it: the
// replica is complete, so a pod may now serve it (migration plan §6.2).
func (m *Migrator) startTailing(ctx context.Context, sess *catalog.Session) error {
	phase, err := m.cfg.Meta.Get(ctx, []byte(lifecycle.PhaseKey))
	if err != nil {
		return fmt.Errorf("migrate: read the local phase: %w", err)
	}
	if lifecycle.Phase(phase) != lifecycle.PhaseSteadyState {
		return fmt.Errorf("migrate: the source left steady_state for %q", phase)
	}
	if _, err := sess.SetMigrationState(ctx, catalog.MigrationSeeding, catalog.MigrationTailing, []metastore.Op{
		{Kind: metastore.OpSet, Key: []byte(lifecycle.PhaseKey), Value: phase},
	}); err != nil {
		return err
	}
	m.log.Info("seed complete; the catalog is a live replica and pods may serve it")
	m.setState(catalog.MigrationTailing)
	return nil
}

// handle serves one operator request. A refused request returns its reason
// and a nil error; an error ends the session.
func (m *Migrator) handle(ctx context.Context, sess *catalog.Session, tr *tracker, st catalog.MigrationState, req Request, pausedAt time.Time) (string, error) {
	switch req.Action {
	case ActionAbort:
		if _, err := sess.SetMigrationState(ctx, st, catalog.MigrationAborted, nil); err != nil {
			return "failed: " + err.Error(), err
		}
		if err := m.resumeCompaction(ctx); err != nil {
			return "aborted; resuming compaction failed: " + err.Error(), err
		}
		m.log.Warn("migration aborted by the operator; local compaction resumes")
		return "aborted", m.finish(catalog.MigrationAborted)
	case ActionHandoff:
		if st != catalog.MigrationTailing {
			return fmt.Sprintf("refused: the migration is %s, not tailing", st), nil
		}
		return m.handoff(ctx, sess, tr, pausedAt)
	default:
		return fmt.Sprintf("refused: unknown action %q", req.Action), nil
	}
}

// publishStatus publishes the status to the control table every
// ControlInterval until the returned func is called.
func (m *Migrator) publishStatus(ctx context.Context) func() {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(m.cfg.ControlInterval)
		defer t.Stop()
		for {
			doc, err := json.Marshal(m.Status())
			if err == nil {
				err = m.cfg.Control.SetStatus(ctx, doc)
			}
			if err != nil && ctx.Err() == nil {
				m.log.Debug("cannot publish the migration status", "err", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	return func() {
		// The final state is worth one more publish.
		if doc, err := json.Marshal(m.Status()); err == nil {
			pctx, pcancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
			_ = m.cfg.Control.SetStatus(pctx, doc)
			pcancel()
		}
		cancel()
		<-done
	}
}
