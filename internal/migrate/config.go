package migrate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/leader"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/metastore/pebblestore"
	"github.com/bluesky-social/jetstream/internal/objstore"
	"github.com/cockroachdb/pebble/vfs"
)

// Defaults for the JETSTREAM_MIGRATION_* knobs.
const (
	DefaultSegmentConcurrency  = 4
	DefaultReadBytesPerSec     = 64 << 20
	DefaultMetaFlushInterval   = 2 * time.Second
	DefaultMetaBatchKeys       = 20_000
	DefaultDirtyMaxKeys        = 5_000_000
	DefaultTailMaxBlocksPerTxn = 16
	DefaultTailPollInterval    = 250 * time.Millisecond
	DefaultMaxCompactionPause  = 5 * 24 * time.Hour
	DefaultDrainSpread         = time.Minute
	DefaultHandoffTimeout      = 2 * time.Minute
	DefaultVerifyInterval      = 6 * time.Hour
	DefaultHandoffMaxLagSeqs   = 2 * 4096
	DefaultHandoffMaxDiffAge   = 24 * time.Hour
	DefaultControlInterval     = time.Second
	// uploadChunkBytes bounds the frames of one segment held in memory
	// between the read and the upload.
	uploadChunkBytes = 32 << 20
)

// LocalCatalog is the source's segment catalog.
type LocalCatalog interface {
	Snapshot() catalog.CatalogView
	// Path is segment idx's file in ns.
	Path(ns catalog.Namespace, idx uint64) string
}

// Ingest controls the source's writer sessions.
type Ingest interface {
	// Stop ends local ingest and returns once the writer session has
	// closed cleanly; ingest stays stopped until Start. An error means the
	// session may still be running.
	Stop(ctx context.Context) error
	// Start lets local ingest run.
	Start()
}

// Compaction pauses the source's compaction.
type Compaction interface {
	Pause(ctx context.Context) error
	Resume()
}

// Request is an operator's request to the migrator.
type Request struct {
	ID     int64
	Action string
}

// The actions a Request may carry.
const (
	ActionHandoff = "handoff"
	ActionAbort   = "abort"
)

// Control carries operator requests to the migrator and its status back.
type Control interface {
	// Claim takes the oldest request neither claimed nor answered. Once
	// claimed, the operator can no longer withdraw it.
	Claim(ctx context.Context) (Request, bool, error)
	Ack(ctx context.Context, id int64, result string) error
	// AbandonAll answers every unanswered request, claimed or not.
	AbandonAll(ctx context.Context, result string) error
	SetStatus(ctx context.Context, status []byte) error
}

// Config configures a Migrator.
type Config struct {
	// Meta is the source's metadata store. New installs its commit
	// observer.
	Meta *pebblestore.Store
	// Catalog is the source's segment catalog; CatalogReady waits for its
	// first load.
	Catalog      LocalCatalog
	CatalogReady func(context.Context) error
	// FS reads the source's segment files. Nil is the OS filesystem.
	FS         vfs.FS
	Ingest     Ingest
	Compaction Compaction
	// Drain moves the process to drained mode once the handoff commits:
	// no new subscribers, existing ones closed over DrainSpread, archive
	// reads still served. It runs once and may block until done.
	Drain func(ctx context.Context)

	// DB, Blob, NewLease, and RemoteMeta are the target archive. RemoteMeta
	// only reads.
	DB         catalog.DB
	Blob       objstore.Blob
	ArchiveID  [16]byte
	NewLease   func() leader.Locker
	RemoteMeta metastore.Store
	Control    Control

	Lease, RenewInterval, AcquireInterval time.Duration
	// GCDelay and OrphanAge are the target's, which the upload protocol
	// keeps to (design §7.3).
	GCDelay, OrphanAge time.Duration
	UploadConcurrency  int

	// SegmentConcurrency sealed segments are read and uploaded at once
	// while seeding; they import in index order.
	SegmentConcurrency int
	// ReadBytesPerSec throttles reads of local segment files. Zero is
	// unthrottled.
	ReadBytesPerSec int64
	// MetaFlushInterval is how often changed metadata keys are copied.
	MetaFlushInterval time.Duration
	// MetaBatchKeys bounds the keys one metadata transaction writes.
	MetaBatchKeys int
	// DirtyMaxKeys bounds the changed-key set; past it, the set is dropped
	// and a full resync replaces it.
	DirtyMaxKeys int
	// TailMaxBlocksPerTxn bounds the active blocks one transaction ships,
	// which bounds what a shadow pod decodes in one tick.
	TailMaxBlocksPerTxn int
	// TailPollInterval is how often local progress is sampled.
	TailPollInterval time.Duration
	// MaxCompactionPause is how long local compaction may stay paused
	// before the migrator refuses to hand off.
	MaxCompactionPause time.Duration
	// HandoffTimeout bounds stopping local ingest at handoff.
	HandoffTimeout time.Duration
	// VerifyInterval is how often a full metadata resync runs while
	// tailing.
	VerifyInterval time.Duration
	// HandoffMaxLagSeqs refuses a handoff while the replica trails local
	// ingest by more seqs than this, so the ingest pause stays short.
	HandoffMaxLagSeqs uint64
	// HandoffMaxDiffAge refuses a handoff unless a full metadata resync
	// finished this recently.
	HandoffMaxDiffAge time.Duration
	// HandoffFullVerify runs a full metadata comparison while ingest is
	// stopped at handoff and refuses on any difference. It lengthens the
	// ingest pause by a full read of both stores.
	HandoffFullVerify bool
	// ControlInterval is how often operator requests are polled and the
	// status published.
	ControlInterval time.Duration

	Logger        *slog.Logger
	Metrics       *Metrics
	CrashInjector crashpoint.Injector
}

func (c *Config) applyDefaults() {
	set := func(d *time.Duration, v time.Duration) {
		if *d <= 0 {
			*d = v
		}
	}
	setInt := func(n *int, v int) {
		if *n <= 0 {
			*n = v
		}
	}
	setInt(&c.SegmentConcurrency, DefaultSegmentConcurrency)
	setInt(&c.MetaBatchKeys, DefaultMetaBatchKeys)
	setInt(&c.DirtyMaxKeys, DefaultDirtyMaxKeys)
	setInt(&c.TailMaxBlocksPerTxn, DefaultTailMaxBlocksPerTxn)
	set(&c.MetaFlushInterval, DefaultMetaFlushInterval)
	set(&c.TailPollInterval, DefaultTailPollInterval)
	set(&c.MaxCompactionPause, DefaultMaxCompactionPause)
	set(&c.HandoffTimeout, DefaultHandoffTimeout)
	set(&c.VerifyInterval, DefaultVerifyInterval)
	set(&c.HandoffMaxDiffAge, DefaultHandoffMaxDiffAge)
	set(&c.ControlInterval, DefaultControlInterval)
	set(&c.Lease, leader.DefaultLease)
	set(&c.RenewInterval, leader.DefaultRenewInterval)
	set(&c.AcquireInterval, leader.DefaultAcquireInterval)
	if c.HandoffMaxLagSeqs == 0 {
		c.HandoffMaxLagSeqs = DefaultHandoffMaxLagSeqs
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
}

func (c *Config) validate() error {
	var missing []string
	for name, ok := range map[string]bool{
		"Meta": c.Meta != nil, "Catalog": c.Catalog != nil, "CatalogReady": c.CatalogReady != nil,
		"Ingest": c.Ingest != nil, "Compaction": c.Compaction != nil, "DB": c.DB != nil,
		"Blob": c.Blob != nil, "NewLease": c.NewLease != nil, "RemoteMeta": c.RemoteMeta != nil,
		"Control": c.Control != nil,
	} {
		if !ok {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("migrate: config is missing %v", missing)
	}
	if c.GCDelay <= 0 || c.OrphanAge <= 0 {
		return errors.New("migrate: GCDelay and OrphanAge must be > 0")
	}
	if c.RenewInterval >= c.Lease {
		return fmt.Errorf("migrate: renew interval %s must be shorter than the lease %s", c.RenewInterval, c.Lease)
	}
	return nil
}
