package jetstreamd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	localcatalog "github.com/bluesky-social/jetstream/internal/catalog/local"
	"github.com/bluesky-social/jetstream/internal/ingest/orchestrator"
	"github.com/bluesky-social/jetstream/internal/leader"
	"github.com/bluesky-social/jetstream/internal/migrate"
	"github.com/bluesky-social/jetstream/internal/objstore/s3"
	"github.com/bluesky-social/jetstream/internal/pgstore"
	"github.com/prometheus/client_golang/prometheus"
)

// MigrationConfig is JETSTREAM_MIGRATION_*: the live migration of a local
// archive to disaggregated storage (specs/notes/2026-10-09-local-to-disagg-
// migration.md). Only StandbyBackoff applies in disaggregated mode; the rest
// configure the migrator that runs inside a local-mode process, which
// writes to the PostgreSQL and S3 that Storage names.
type MigrationConfig struct {
	// Enabled runs the migrator (JETSTREAM_MIGRATE_TO_DISAGGREGATED).
	Enabled bool
	// StandbyBackoff is how long a disaggregated pod waits after finding a
	// catalog a migration still owns before it tries for the lease again.
	StandbyBackoff time.Duration

	SegmentConcurrency  int
	ReadBytesPerSec     int64
	MetaFlushInterval   time.Duration
	MetaBatchKeys       int
	DirtyMaxKeys        int
	TailMaxBlocksPerTxn int
	TailPollInterval    time.Duration
	MaxCompactionPause  time.Duration
	DrainSpread         time.Duration
	HandoffTimeout      time.Duration
	VerifyInterval      time.Duration
	HandoffMaxLagSeqs   uint64
	HandoffMaxDiffAge   time.Duration
	HandoffFullVerify   bool
	ControlInterval     time.Duration
}

// DefaultMigrationConfig returns every migration knob at its default, with
// the migrator off.
func DefaultMigrationConfig() MigrationConfig {
	return MigrationConfig{
		StandbyBackoff:      leader.DefaultStandbyBackoff,
		SegmentConcurrency:  migrate.DefaultSegmentConcurrency,
		ReadBytesPerSec:     migrate.DefaultReadBytesPerSec,
		MetaFlushInterval:   migrate.DefaultMetaFlushInterval,
		MetaBatchKeys:       migrate.DefaultMetaBatchKeys,
		DirtyMaxKeys:        migrate.DefaultDirtyMaxKeys,
		TailMaxBlocksPerTxn: migrate.DefaultTailMaxBlocksPerTxn,
		TailPollInterval:    migrate.DefaultTailPollInterval,
		MaxCompactionPause:  migrate.DefaultMaxCompactionPause,
		DrainSpread:         migrate.DefaultDrainSpread,
		HandoffTimeout:      migrate.DefaultHandoffTimeout,
		VerifyInterval:      migrate.DefaultVerifyInterval,
		HandoffMaxLagSeqs:   migrate.DefaultHandoffMaxLagSeqs,
		HandoffMaxDiffAge:   migrate.DefaultHandoffMaxDiffAge,
		ControlInterval:     migrate.DefaultControlInterval,
	}
}

// validate checks the migration settings against the rest of opts. Errors
// name variables, never values.
func (c MigrationConfig) validate(opts Options) error {
	if !c.Enabled {
		return nil
	}
	if opts.Storage.Disaggregated() {
		return errors.New("serve: JETSTREAM_MIGRATE_TO_DISAGGREGATED runs in the local-mode process that holds the archive; unset it with JETSTREAM_STORAGE=disaggregated")
	}
	if opts.StorageBackend == nil {
		st := opts.Storage
		for name, missing := range map[string]bool{
			"JETSTREAM_PG_URL": st.PG.URL == "", "JETSTREAM_S3_REGION": st.S3.Region == "", "JETSTREAM_S3_BUCKET": st.S3.Bucket == "",
		} {
			if missing {
				return fmt.Errorf("serve: %s is required when JETSTREAM_MIGRATE_TO_DISAGGREGATED is set", name)
			}
		}
	}
	for name, v := range map[string]int64{
		"JETSTREAM_MIGRATION_SEGMENT_CONCURRENCY":     int64(c.SegmentConcurrency),
		"JETSTREAM_MIGRATION_META_BATCH_KEYS":         int64(c.MetaBatchKeys),
		"JETSTREAM_MIGRATION_DIRTY_MAX_KEYS":          int64(c.DirtyMaxKeys),
		"JETSTREAM_MIGRATION_TAIL_MAX_BLOCKS_PER_TXN": int64(c.TailMaxBlocksPerTxn),
	} {
		if v <= 0 {
			return fmt.Errorf("serve: %s must be > 0, got %d", name, v)
		}
	}
	if c.ReadBytesPerSec < 0 {
		return fmt.Errorf("serve: JETSTREAM_MIGRATION_READ_BYTES_PER_SEC must be >= 0, got %d", c.ReadBytesPerSec)
	}
	for name, d := range map[string]time.Duration{
		"JETSTREAM_MIGRATION_META_FLUSH_INTERVAL":    c.MetaFlushInterval,
		"JETSTREAM_MIGRATION_TAIL_POLL_INTERVAL":     c.TailPollInterval,
		"JETSTREAM_MIGRATION_MAX_COMPACTION_PAUSE":   c.MaxCompactionPause,
		"JETSTREAM_MIGRATION_HANDOFF_TIMEOUT":        c.HandoffTimeout,
		"JETSTREAM_MIGRATION_VERIFY_INTERVAL":        c.VerifyInterval,
		"JETSTREAM_MIGRATION_HANDOFF_MAX_RESYNC_AGE": c.HandoffMaxDiffAge,
		"JETSTREAM_MIGRATION_CONTROL_INTERVAL":       c.ControlInterval,
	} {
		if d <= 0 {
			return fmt.Errorf("serve: %s must be > 0, got %s", name, d)
		}
	}
	if c.DrainSpread < 0 {
		return fmt.Errorf("serve: JETSTREAM_MIGRATION_DRAIN_SPREAD must be >= 0, got %s", c.DrainSpread)
	}
	return nil
}

// ingestGate lets the migrator stop and restart local ingest. Writer
// sessions enter it before they run; Stop cancels the running session and
// waits for it to close cleanly, and later sessions wait until Start.
type ingestGate struct {
	mu      sync.Mutex
	open    bool
	changed chan struct{}
	cancel  context.CancelFunc
	running chan struct{}
}

func newIngestGate(open bool) *ingestGate {
	return &ingestGate{open: open, changed: make(chan struct{})}
}

func (g *ingestGate) setLocked(open bool) {
	if g.open != open {
		g.open = open
		close(g.changed)
		g.changed = make(chan struct{})
	}
}

// Stop implements migrate.Ingest.
func (g *ingestGate) Stop(ctx context.Context) error {
	g.mu.Lock()
	g.setLocked(false)
	cancel, running := g.cancel, g.running
	g.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if running == nil {
		return nil
	}
	select {
	case <-running:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("local writer session still running: %w", ctx.Err())
	}
}

// Start implements migrate.Ingest.
func (g *ingestGate) Start() {
	g.mu.Lock()
	g.setLocked(true)
	g.mu.Unlock()
}

// enter waits until the gate is open and returns the session's context,
// which Stop cancels, and the func the session calls once it has ended.
func (g *ingestGate) enter(ctx context.Context) (context.Context, func(), error) {
	for {
		g.mu.Lock()
		if g.open {
			sctx, cancel := context.WithCancel(ctx)
			running := make(chan struct{})
			g.cancel, g.running = cancel, running
			g.mu.Unlock()
			return sctx, func() {
				g.mu.Lock()
				if g.running == running {
					g.cancel, g.running = nil, nil
				}
				g.mu.Unlock()
				cancel()
				close(running)
			}, nil
		}
		changed := g.changed
		g.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}
}

// pgMigrationControl is migrate.Control over the PostgreSQL control
// tables.
type pgMigrationControl struct{ pg *pgstore.Store }

func (c pgMigrationControl) Claim(ctx context.Context) (migrate.Request, bool, error) {
	r, ok, err := c.pg.ClaimMigrationRequest(ctx)
	return migrate.Request{ID: r.ID, Action: r.Action}, ok, err
}

func (c pgMigrationControl) AbandonAll(ctx context.Context, result string) error {
	return c.pg.AbandonMigrationRequests(ctx, result)
}

func (c pgMigrationControl) Ack(ctx context.Context, id int64, result string) error {
	return c.pg.AckMigrationRequest(ctx, id, result)
}

func (c pgMigrationControl) SetStatus(ctx context.Context, status []byte) error {
	return c.pg.SetMigrationStatus(ctx, status)
}

// errHandedOff is the subscribe readiness error of a process that handed
// its archive to disaggregated pods.
var errHandedOff = errors.New("this archive moved to disaggregated storage; reconnect to reach it")

// openMigrationGates sets up the ingest and compaction gates from the
// migration's local state, before any writer session can start
// (migration plan §6.5, §6.8).
func (r *Runtime) openMigrationGates(ctx context.Context) error {
	r.compactionGate = orchestrator.NewCompactionGate()
	guard, err := migrate.ReadGuard(ctx, r.metaStore)
	if err != nil {
		return fmt.Errorf("serve: %w", err)
	}
	r.ingest = newIngestGate(guard == migrate.GuardNone)
	switch {
	case guard == migrate.GuardDone:
		r.drained.Store(true)
		r.logger.Warn("this archive was handed off to disaggregated storage; local ingest stays stopped and only archive reads are served")
	case guard == migrate.GuardPending && !r.opts.Migration.Enabled:
		r.logger.Error("a migration handoff may have committed; local ingest stays stopped until the migrator settles it (set JETSTREAM_MIGRATE_TO_DISAGGREGATED) or `jetstream migrate reclaim` clears it")
	}
	pausedAt, paused, err := migrate.ReadCompactionPause(ctx, r.metaStore)
	if err != nil {
		return fmt.Errorf("serve: %w", err)
	}
	switch {
	case paused && r.opts.Migration.Enabled:
		// No session has started, so no pass is running and this cannot
		// wait.
		if err := r.compactionGate.Pause(ctx); err != nil {
			return err
		}
		r.logger.Info("local compaction stays paused for the migration", "paused_at", pausedAt)
	case paused:
		r.logger.Warn("a migration paused local compaction, but JETSTREAM_MIGRATE_TO_DISAGGREGATED is unset; compaction runs", "paused_at", pausedAt)
	}
	return nil
}

// buildMigrator builds the migrator when the migration is enabled and
// registers /debug/migration.
func (r *Runtime) buildMigrator(ctx context.Context, reg prometheus.Registerer, cat *localcatalog.Catalog, ready func(context.Context) error) error {
	if !r.opts.Migration.Enabled {
		return nil
	}
	mc := r.opts.Migration
	st := r.opts.Storage
	backend := r.opts.StorageBackend
	if backend == nil {
		var err error
		if backend, err = openBackend(ctx, st, pgstore.NewMetrics(reg), s3.NewMetrics(reg)); err != nil {
			return fmt.Errorf("serve: migration: %w", err)
		}
		r.migrationBackend = backend
	}
	archive, err := backend.Archive(ctx)
	if err != nil {
		return fmt.Errorf("serve: migration: the target catalog (run `jetstream storage init --migrate-from-local` first): %w", err)
	}
	if backend.MigrationControl == nil {
		return errors.New("serve: migration: the storage backend has no control channel")
	}
	control, err := backend.MigrationControl(ctx)
	if err != nil {
		return fmt.Errorf("serve: migration: %w", err)
	}
	r.migrator, err = migrate.New(migrate.Config{
		Meta:                r.metaStore,
		Catalog:             cat,
		CatalogReady:        ready,
		FS:                  r.opts.StorageFS,
		Ingest:              r.ingest,
		Compaction:          r.compactionGate,
		Drain:               r.drain,
		DB:                  backend.DB,
		Blob:                backend.Blob,
		ArchiveID:           archive.ArchiveID,
		NewLease:            backend.NewLease,
		RemoteMeta:          backend.MetaStore(nil),
		Control:             control,
		Lease:               st.Leader.Lease,
		RenewInterval:       st.Leader.RenewInterval,
		AcquireInterval:     st.Leader.AcquireInterval,
		GCDelay:             st.GC.Delay,
		OrphanAge:           st.GC.OrphanAge,
		UploadConcurrency:   st.S3.UploadConcurrency,
		SegmentConcurrency:  mc.SegmentConcurrency,
		ReadBytesPerSec:     mc.ReadBytesPerSec,
		MetaFlushInterval:   mc.MetaFlushInterval,
		MetaBatchKeys:       mc.MetaBatchKeys,
		DirtyMaxKeys:        mc.DirtyMaxKeys,
		TailMaxBlocksPerTxn: mc.TailMaxBlocksPerTxn,
		TailPollInterval:    mc.TailPollInterval,
		MaxCompactionPause:  mc.MaxCompactionPause,
		HandoffTimeout:      mc.HandoffTimeout,
		VerifyInterval:      mc.VerifyInterval,
		HandoffMaxLagSeqs:   mc.HandoffMaxLagSeqs,
		HandoffMaxDiffAge:   mc.HandoffMaxDiffAge,
		HandoffFullVerify:   mc.HandoffFullVerify,
		ControlInterval:     mc.ControlInterval,
		Logger:              r.processLogger,
		Metrics:             migrate.NewMetrics(reg),
		CrashInjector:       r.opts.CrashInjector,
	})
	if err != nil {
		return fmt.Errorf("serve: migration: %w", err)
	}
	r.server.RegisterDebugRoute("GET /debug/migration", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(r.migrator.Status())
	}))
	r.logger.Info("migration to disaggregated storage enabled", "storage", st)
	return nil
}

// drain is the migrator's Drain: from now on the process only serves
// archive reads, and its subscribers move to the disaggregated pods.
func (r *Runtime) drain(ctx context.Context) {
	r.drained.Store(true)
	if err := r.tail.DrainOver(ctx, r.opts.Migration.DrainSpread); err != nil && ctx.Err() == nil {
		r.logger.Warn("draining subscribers", "err", err)
	}
}
