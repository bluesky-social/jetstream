package jetstreamd

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/bluesky-social/jetstream/internal/leader"
	"github.com/bluesky-social/jetstream/internal/objstore/objcache"
	"github.com/bluesky-social/jetstream/internal/objstore/s3"
	"github.com/bluesky-social/jetstream/internal/pgstore"
)

// StorageMode selects where Jetstream keeps its archive (design §18).
type StorageMode string

const (
	// StorageLocal keeps segments and metadata under DataDir. The empty
	// StorageMode means local too, so existing Options stay valid.
	StorageLocal StorageMode = "local"
	// StorageDisaggregated keeps the catalog and metadata in PostgreSQL and
	// the segment bytes in S3, with no local disk.
	StorageDisaggregated StorageMode = "disaggregated"
)

// Defaults for the disaggregated knobs that have no owning package yet
// (design §17, §18). The rest reuse the owning package's constant.
const (
	DefaultHotBatchMaxAge             = 15 * time.Millisecond
	DefaultBlockMaxAge                = 30 * time.Second
	DefaultHotInlineBytesPerSec       = 4 << 20
	DefaultHotBulkPendingBytes        = 64 << 20
	DefaultHotPendingBytes            = 256 << 20
	DefaultHotMaxUnfoldedEvents       = 65536
	DefaultCatalogPollInterval        = 250 * time.Millisecond
	DefaultMaxViewAge                 = 30 * time.Second
	DefaultMaxArchiveResponseDuration = time.Hour
	DefaultGCInterval                 = 10 * time.Minute
	DefaultGCDelay                    = 6 * time.Hour
	DefaultGCOrphanAge                = time.Hour
	DefaultObjectCacheBytes           = objcache.DefaultMaxBytes
	DefaultCompactionMemoryBytes      = 2 << 30
)

// StorageConfig is the resolved disaggregated-storage configuration (design
// §18). Every field except Mode is ignored in local mode.
//
// PG.URL carries a password. String, GoString, and LogValue redact it, so
// the config is safe to log whole; never log PG.URL on its own.
type StorageConfig struct {
	Mode StorageMode

	PG     PGConfig
	S3     S3Config
	Leader LeaderConfig
	Hot    HotConfig

	// BlockMaxAge is the open-block age cut in hot mode.
	BlockMaxAge time.Duration
	// CatalogPollInterval is the follower's poll period; it covers a lost
	// NOTIFY.
	CatalogPollInterval time.Duration
	// MaxViewAge marks a pod not ready once its catalog mirror is older.
	MaxViewAge time.Duration
	// MaxArchiveResponseDuration cuts off one archive response.
	MaxArchiveResponseDuration time.Duration

	GC GCConfig

	// ObjectCacheBytes bounds the compressed object cache (§17).
	ObjectCacheBytes int64
	// CompactionMemoryBytes bounds the compaction working set (§17). It is
	// declared now so the variable is not rejected; compaction is refused in
	// disaggregated mode until stage 4.
	CompactionMemoryBytes int64
}

// PGConfig is JETSTREAM_PG_*.
type PGConfig struct {
	// URL is a libpq connection string or URL. It holds a password.
	URL      string
	MaxConns int
}

// S3Config is JETSTREAM_S3_*. Credentials are not configured here: they come
// from the AWS SDK default chain (design §24), so no Jetstream variable ever
// holds them.
type S3Config struct {
	Endpoint          string
	Region            string
	Bucket            string
	Prefix            string
	PathStyle         bool
	UploadConcurrency int
	ReadConcurrency   int
	RetryTimeout      time.Duration
}

// LeaderConfig is JETSTREAM_LEADER_*.
type LeaderConfig struct {
	Lease           time.Duration
	RenewInterval   time.Duration
	AcquireInterval time.Duration
}

// HotConfig is JETSTREAM_HOT_*: the hot-mode writer's batch cut and
// backpressure budgets (design §10.3, §10.5).
type HotConfig struct {
	BatchMaxAge       time.Duration
	InlineBytesPerSec int64
	BulkPendingBytes  int64
	PendingBytes      int64
	MaxUnfoldedEvents int
}

// GCConfig is JETSTREAM_GC_*.
type GCConfig struct {
	Interval  time.Duration
	Delay     time.Duration
	OrphanAge time.Duration
}

// DefaultStorageConfig returns local mode with every disaggregated knob at
// its design §18 default.
func DefaultStorageConfig() StorageConfig {
	return StorageConfig{
		Mode: StorageLocal,
		PG:   PGConfig{MaxConns: pgstore.DefaultMaxConns},
		S3: S3Config{
			UploadConcurrency: s3.DefaultUploadConcurrency,
			ReadConcurrency:   s3.DefaultReadConcurrency,
			RetryTimeout:      s3.DefaultRetryTimeout,
		},
		Leader: LeaderConfig{
			Lease:           leader.DefaultLease,
			RenewInterval:   leader.DefaultRenewInterval,
			AcquireInterval: leader.DefaultAcquireInterval,
		},
		Hot: HotConfig{
			BatchMaxAge:       DefaultHotBatchMaxAge,
			InlineBytesPerSec: DefaultHotInlineBytesPerSec,
			BulkPendingBytes:  DefaultHotBulkPendingBytes,
			PendingBytes:      DefaultHotPendingBytes,
			MaxUnfoldedEvents: DefaultHotMaxUnfoldedEvents,
		},
		BlockMaxAge:                DefaultBlockMaxAge,
		CatalogPollInterval:        DefaultCatalogPollInterval,
		MaxViewAge:                 DefaultMaxViewAge,
		MaxArchiveResponseDuration: DefaultMaxArchiveResponseDuration,
		GC: GCConfig{
			Interval:  DefaultGCInterval,
			Delay:     DefaultGCDelay,
			OrphanAge: DefaultGCOrphanAge,
		},
		ObjectCacheBytes:      DefaultObjectCacheBytes,
		CompactionMemoryBytes: DefaultCompactionMemoryBytes,
	}
}

// EffectiveMode is Mode with the empty value resolved to StorageLocal.
func (c StorageConfig) EffectiveMode() StorageMode {
	if c.Mode == "" {
		return StorageLocal
	}
	return c.Mode
}

// Disaggregated reports whether c selects disaggregated mode.
func (c StorageConfig) Disaggregated() bool { return c.Mode == StorageDisaggregated }

// errDisaggregatedUnavailable is returned by Build until the disaggregated
// runtime is wired (plan S2.16), so a pod configured for it fails loudly
// instead of quietly running local mode.
var errDisaggregatedUnavailable = errors.New("serve: JETSTREAM_STORAGE=disaggregated is not available in this build yet")

// Validate checks c on its own and against the rest of opts. Errors name
// variables, never values, so none can carry the PG password.
func (c StorageConfig) Validate(opts Options) error {
	switch c.Mode {
	case "", StorageLocal:
		return nil
	case StorageDisaggregated:
	default:
		return fmt.Errorf("serve: JETSTREAM_STORAGE must be %q or %q, got %q", StorageLocal, StorageDisaggregated, c.Mode)
	}
	// Design §18: no code path may write to local disk by accident.
	if opts.DataDir != "" {
		return errors.New("serve: JETSTREAM_DATA_DIR must be unset when JETSTREAM_STORAGE=disaggregated")
	}
	// Plan D5: compaction is off in disaggregated mode until stage 4.
	if opts.CompactionInterval > 0 {
		return errors.New("serve: JETSTREAM_STORAGE=disaggregated requires JETSTREAM_COMPACTION_INTERVAL=0 (compaction is not supported in disaggregated mode yet)")
	}
	if c.PG.URL == "" {
		return errors.New("serve: JETSTREAM_PG_URL is required when JETSTREAM_STORAGE=disaggregated")
	}
	if c.S3.Region == "" {
		return errors.New("serve: JETSTREAM_S3_REGION is required when JETSTREAM_STORAGE=disaggregated")
	}
	if c.S3.Bucket == "" {
		return errors.New("serve: JETSTREAM_S3_BUCKET is required when JETSTREAM_STORAGE=disaggregated")
	}
	for name, v := range map[string]int64{
		"JETSTREAM_PG_MAX_CONNS":             int64(c.PG.MaxConns),
		"JETSTREAM_S3_UPLOAD_CONCURRENCY":    int64(c.S3.UploadConcurrency),
		"JETSTREAM_S3_READ_CONCURRENCY":      int64(c.S3.ReadConcurrency),
		"JETSTREAM_HOT_INLINE_BYTES_PER_SEC": c.Hot.InlineBytesPerSec,
		"JETSTREAM_HOT_BULK_PENDING_BYTES":   c.Hot.BulkPendingBytes,
		"JETSTREAM_HOT_PENDING_BYTES":        c.Hot.PendingBytes,
		"JETSTREAM_HOT_MAX_UNFOLDED_EVENTS":  int64(c.Hot.MaxUnfoldedEvents),
		"JETSTREAM_OBJECT_CACHE_BYTES":       c.ObjectCacheBytes,
		"JETSTREAM_COMPACTION_MEMORY_BYTES":  c.CompactionMemoryBytes,
	} {
		if v <= 0 {
			return fmt.Errorf("serve: %s must be > 0, got %d", name, v)
		}
	}
	for name, d := range map[string]time.Duration{
		"JETSTREAM_S3_RETRY_TIMEOUT":              c.S3.RetryTimeout,
		"JETSTREAM_LEADER_LEASE":                  c.Leader.Lease,
		"JETSTREAM_LEADER_RENEW_INTERVAL":         c.Leader.RenewInterval,
		"JETSTREAM_LEADER_ACQUIRE_INTERVAL":       c.Leader.AcquireInterval,
		"JETSTREAM_HOT_BATCH_MAX_AGE":             c.Hot.BatchMaxAge,
		"JETSTREAM_BLOCK_MAX_AGE":                 c.BlockMaxAge,
		"JETSTREAM_CATALOG_POLL_INTERVAL":         c.CatalogPollInterval,
		"JETSTREAM_MAX_VIEW_AGE":                  c.MaxViewAge,
		"JETSTREAM_MAX_ARCHIVE_RESPONSE_DURATION": c.MaxArchiveResponseDuration,
		"JETSTREAM_GC_INTERVAL":                   c.GC.Interval,
		"JETSTREAM_GC_DELAY":                      c.GC.Delay,
		"JETSTREAM_GC_ORPHAN_AGE":                 c.GC.OrphanAge,
	} {
		if d <= 0 {
			return fmt.Errorf("serve: %s must be > 0, got %s", name, d)
		}
	}
	// A renew period at or past the lease lets the lease lapse between
	// renewals, so the leader would lose it while healthy.
	if c.Leader.RenewInterval >= c.Leader.Lease {
		return fmt.Errorf("serve: JETSTREAM_LEADER_RENEW_INTERVAL (%s) must be shorter than JETSTREAM_LEADER_LEASE (%s)", c.Leader.RenewInterval, c.Leader.Lease)
	}
	// A follower that polls less often than the readiness bound goes
	// not-ready between polls whenever NOTIFY is lost.
	if c.CatalogPollInterval >= c.MaxViewAge {
		return fmt.Errorf("serve: JETSTREAM_CATALOG_POLL_INTERVAL (%s) must be shorter than JETSTREAM_MAX_VIEW_AGE (%s)", c.CatalogPollInterval, c.MaxViewAge)
	}
	return nil
}

// String describes c with the PG password redacted.
func (c StorageConfig) String() string {
	s3cfg := c.S3
	s3cfg.Endpoint = pgstore.RedactURL(s3cfg.Endpoint)
	return fmt.Sprintf("jetstreamd.StorageConfig{Mode:%q PG:%s S3:%+v Leader:%+v Hot:%+v BlockMaxAge:%s CatalogPollInterval:%s MaxViewAge:%s MaxArchiveResponseDuration:%s GC:%+v ObjectCacheBytes:%d CompactionMemoryBytes:%d}",
		c.Mode, c.PG, s3cfg, c.Leader, c.Hot, c.BlockMaxAge, c.CatalogPollInterval, c.MaxViewAge,
		c.MaxArchiveResponseDuration, c.GC, c.ObjectCacheBytes, c.CompactionMemoryBytes)
}

// GoString redacts %#v the same way.
func (c StorageConfig) GoString() string { return c.String() }

// LogValue implements slog.LogValuer with the PG password redacted.
func (c StorageConfig) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("mode", string(c.Mode)),
		slog.Any("pg", c.PG),
		slog.String("s3_endpoint", pgstore.RedactURL(c.S3.Endpoint)),
		slog.String("s3_region", c.S3.Region),
		slog.String("s3_bucket", c.S3.Bucket),
		slog.String("s3_prefix", c.S3.Prefix),
		slog.Bool("s3_path_style", c.S3.PathStyle),
		slog.Int("s3_upload_concurrency", c.S3.UploadConcurrency),
		slog.Int("s3_read_concurrency", c.S3.ReadConcurrency),
		slog.Duration("s3_retry_timeout", c.S3.RetryTimeout),
		slog.Duration("leader_lease", c.Leader.Lease),
		slog.Duration("leader_renew_interval", c.Leader.RenewInterval),
		slog.Duration("leader_acquire_interval", c.Leader.AcquireInterval),
		slog.Duration("hot_batch_max_age", c.Hot.BatchMaxAge),
		slog.Int64("hot_inline_bytes_per_sec", c.Hot.InlineBytesPerSec),
		slog.Int64("hot_bulk_pending_bytes", c.Hot.BulkPendingBytes),
		slog.Int64("hot_pending_bytes", c.Hot.PendingBytes),
		slog.Int("hot_max_unfolded_events", c.Hot.MaxUnfoldedEvents),
		slog.Duration("block_max_age", c.BlockMaxAge),
		slog.Duration("catalog_poll_interval", c.CatalogPollInterval),
		slog.Duration("max_view_age", c.MaxViewAge),
		slog.Duration("max_archive_response_duration", c.MaxArchiveResponseDuration),
		slog.Duration("gc_interval", c.GC.Interval),
		slog.Duration("gc_delay", c.GC.Delay),
		slog.Duration("gc_orphan_age", c.GC.OrphanAge),
		slog.Int64("object_cache_bytes", c.ObjectCacheBytes),
		slog.Int64("compaction_memory_bytes", c.CompactionMemoryBytes),
	)
}

// String describes the config with the password redacted.
func (c PGConfig) String() string {
	return fmt.Sprintf("{URL:%q MaxConns:%d}", pgstore.RedactURL(c.URL), c.MaxConns)
}

// GoString redacts %#v the same way.
func (c PGConfig) GoString() string { return c.String() }

// LogValue implements slog.LogValuer with the password redacted.
func (c PGConfig) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("url", pgstore.RedactURL(c.URL)),
		slog.Int("max_conns", c.MaxConns),
	)
}

// storageSettingsSet reports whether c carries disaggregated-only settings
// that local mode would silently ignore.
func (c StorageConfig) storageSettingsSet() bool {
	return c.PG.URL != "" || c.S3.Bucket != "" || c.S3.Endpoint != ""
}
