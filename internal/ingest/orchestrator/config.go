package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream/internal/ingest/live"
	"github.com/bluesky-social/jetstream/internal/ingest/syncstate"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/internal/timestamp"
	"github.com/bluesky-social/jetstream/internal/tombstone"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/streaming"
	atmossync "github.com/jcalabro/atmos/sync"
)

// PhaseBarrier is a nil-able hook that can pause between orchestrator
// lifecycle phases.
type PhaseBarrier func(context.Context) error

type CompactionPassResult struct {
	Watermark uint64
	Err       error
}

// Config controls Orchestrator behavior. cmd/jetstream constructs
// exactly one of these per process and hands it to New.
//
// Per-subsystem metrics (ingest, live, backfill) are passed through
// because both the bootstrap and steady-state phases reuse the same
// prometheus registry. The orchestrator-level Metrics covers
// transitions and per-state durations.
type Config struct {
	// DataDir is the root data directory. The orchestrator writes to
	// <DataDir>/segments and <DataDir>/backfill/live_segments.
	DataDir string

	// Store is the shared metadata pebble db. Required.
	Store *store.Store

	// RelayURL is the upstream relay base URL (https or wss).
	RelayURL string

	// HTTPClient is the bulk-download-tuned client used by the backfill
	// engine for getRepo and by xrpc for listRepos. Required.
	HTTPClient *http.Client

	// Directory is the shared identity directory for both backfill
	// (sync.Client) and the live consumer (verifier).
	Directory *identity.Directory

	// Verifier is the Sync 1.1 verifier used by both bootstrap-time
	// and steady-state live consumers.
	Verifier *atmossync.Verifier

	// SyncStateStore is the verifier state store when it supports staged
	// durability. It is forwarded to live consumers so verifier state commits
	// atomically with the relay cursor after block fsync.
	SyncStateStore *syncstate.PebbleStateStore

	// Tombstones is the steady-state live tombstone set. Bootstrap leaves
	// live.Config.Tombstones nil because live_segments are re-sequenced at
	// merge.
	Tombstones *tombstone.Set

	// Logger is required.
	Logger *slog.Logger

	// Metrics is the orchestrator-level metrics handle. Optional;
	// nil means no /metrics counters incrementing.
	Metrics *Metrics

	// IngestMetrics is the canonical durable-archive writer metric
	// handle. It is used by the bootstrap backfill writer, merge
	// destination writer, and steady-state live writer. Bootstrap's
	// temporary live_segments writer deliberately leaves it nil because
	// that tree has a throwaway seq space. Optional.
	IngestMetrics *ingest.Metrics

	// LiveMetrics is shared between the bootstrap-time and
	// steady-state live consumers. Optional.
	LiveMetrics *live.Metrics

	// BackfillMetrics is consumed by the backfill engine in the
	// bootstrap phase only. Optional.
	BackfillMetrics *backfill.Metrics

	// SegmentMetrics is shared by every *ingest.Writer the orchestrator
	// constructs (the bootstrap-time backfill writer, the bootstrap-time
	// live consumer's internal writer, and the bootstrap-seal reopen).
	// Optional. The same instance flows through to live.Config and
	// ingest.Config so all segment.Writer instances under the
	// orchestrator share the seal_duration histogram series.
	SegmentMetrics segment.SealObserver

	// OnEvent, if non-nil, is forwarded to the steady-state live.Consumer
	// so live events can be fanned out to /subscribe websocket clients.
	// Bootstrap-time consumers do NOT receive this hook because their
	// events go to backfill/live_segments and are not user-visible.
	OnEvent func(*segment.Event)

	// OnBootstrapLiveEvent, if non-nil, is forwarded to the bootstrap-time
	// live.Consumer after durable append. This is a validation hook for
	// oracle/restart tests that need deterministic cutover acknowledgements;
	// production leaves it nil so bootstrap events are not user-visible.
	OnBootstrapLiveEvent func(*segment.Event)

	// MaxBackfillRepos, when > 0, caps the bootstrap-phase backfill
	// engine at this many fully-downloaded repos and then advances to
	// the merge phase. Debug-only knob for fast local-dev iteration
	// against a relay with millions of users; leave 0 in production.
	// See backfill.Config.MaxRepos for the precise semantics.
	MaxBackfillRepos int

	// BackfillWorkers controls concurrent repo downloads during normal
	// whole-network bootstrap backfill. Zero leaves the backfill package on
	// its default; cmd/jetstream normally resolves zero to its production
	// default before constructing the orchestrator.
	BackfillWorkers int

	// BackfillBatchSize controls how many listRepos entries are accumulated
	// before atmos shuffles and dispatches a bootstrap backfill batch. Zero
	// leaves the backfill package on its default.
	BackfillBatchSize int

	// BackfillAsyncFlushWorkers enables async segment flush compression for
	// the bootstrap backfill writer only. Zero keeps synchronous flushing.
	BackfillAsyncFlushWorkers int

	// BackfillRepos, when non-empty, replaces bootstrap listRepos
	// discovery with this explicit DID list. Debug-only knob for
	// targeted production smoke tests; leave empty in production.
	BackfillRepos []atmos.DID

	// SkipMergeDiscovery, a debug-only flag that skips the discovery
	// portion of the merge phase for fast feedback loops in local
	// development while running against production.
	SkipMergeDiscovery bool

	// BackfillRetryBaseDelay, when > 0, overrides the bootstrap backfill
	// engine's initial retry backoff (atmos default 1s). The oracle
	// fault-injection harness sets this to a sub-millisecond value so
	// injected transient getRepo 503s recover without paying the
	// production backoff per fault. Production leaves it 0.
	BackfillRetryBaseDelay time.Duration

	// FailedRepoRetryInterval controls steady-state retry scans for repos that
	// exhausted bootstrap retry and remain StatusFailed. Zero disables the
	// background retry loop.
	FailedRepoRetryInterval    time.Duration
	FailedRepoRetryWorkers     int
	FailedRepoRetryHostWorkers int
	FailedRepoRetryMaxDelay    time.Duration

	// LiveReconnectBackoff, when non-nil, overrides atmos's subscribeRepos
	// reconnect backoff for both bootstrap-time and steady-state live
	// consumers. Production leaves it nil.
	LiveReconnectBackoff *streaming.BackoffPolicy

	// LiveDial, when non-nil, overrides atmos's websocket dial for both
	// bootstrap-time and steady-state live consumers. Production leaves it
	// nil; deterministic harnesses feed the firehose in-memory.
	LiveDial streaming.DialFunc

	// IngestOnAfterSeal is forwarded to every writer that appends to
	// <DataDir>/segments. Used by cmd/jetstream to wire the manifest's
	// OnSegmentSealed callback. Optional.
	IngestOnAfterSeal func(idx uint64, path string) error

	// OnSegmentCompacted refreshes serving metadata after a sealed segment is
	// rewritten by compaction. cmd/jetstream wires this to the manifest refresh
	// path (which verifies the rewritten file's checksum as an integrity gate).
	// Optional before steady state; required for live serving freshness.
	OnSegmentCompacted func(idx uint64, path string) error

	// SegmentManifestChecksums returns the manifest's resident header
	// checksums keyed by segment index, as one snapshot. The compactor's
	// reconcile step compares these against on-disk headers and re-fires
	// OnSegmentCompacted only on mismatch, keeping no-op passes cheap.
	// Optional; nil makes reconcile refresh every sealed segment.
	SegmentManifestChecksums func() map[uint64]uint64

	// ImportSelector resolves a DID to the sealed segments that may contain it,
	// from the manifest's resident blooms (no disk I/O). Wired by cmd/jetstream
	// to the manifest; required only to run a timestamp-import job (M5+). nil
	// disables import (RunImport returns ErrImportUnavailable).
	ImportSelector timestamp.Selector

	// CompactionBloomNarrowMaxDIDs bounds the candidate-DID set handed to the
	// segment-level bloom prefilter; larger tombstone sets skip narrowing
	// (probing would cost more than it saves — spec §5). Zero selects the
	// default of 100k.
	CompactionBloomNarrowMaxDIDs int

	// CompactionInterval controls steady-state periodic delete/update
	// compaction. Zero disables compaction scheduling and the merge-tail pass.
	CompactionInterval time.Duration

	// CompactionTombstoneCap is the operator cap for tombstone entries. The
	// first implementation exposes the knob and uses it for trigger accounting;
	// chunking lands with the live tombstone set integration.
	CompactionTombstoneCap int

	// CompactionRewriteWorkers bounds per-segment rewrite parallelism during a
	// compaction chunk. Zero selects the default min(runtime.NumCPU(), 8).
	CompactionRewriteWorkers int

	// OnCompactionPass, if non-nil, fires after each enabled compaction pass
	// attempt, including no-op and failed passes. Test/oracle hook only;
	// production leaves it nil.
	OnCompactionPass func(CompactionPassResult)

	// OnBeforeCompactionPass, if non-nil, fires once per pass that will advance
	// the committed watermark (target watermark strictly above the committed
	// one — note a watermark-advancing pass over a tombstone-free window does no
	// physical rewrite, and the hook still fires, which is what the over-drop
	// oracle wants: it proves a zero-drop pass dropped nothing), after the
	// active segment is force-rotated and sealed but before any rewrite, with
	// the target watermark the pass will compact up to. The
	// oracle uses it to snapshot the pre-compaction on-disk stream so it can
	// metamorphically prove the pass did not over-drop a surviving row.
	// Fires synchronously on the compactor goroutine, so the callback must
	// not block long. Test/oracle hook only; production leaves it nil.
	OnBeforeCompactionPass func(targetWatermark uint64)

	// OnSteadyStateWriter, if non-nil, fires once the steady-state
	// live consumer's ingest.Writer is constructed and registered.
	// Used by cmd/jetstream to publish the writer pointer for the
	// cursor-replay handler. Called exactly once per orchestrator
	// run; nil-safe.
	OnSteadyStateWriter func(*ingest.Writer)

	// BarrierBeforeCutover, if non-nil, runs inside runBootstrap after the
	// backfill engine drains but BEFORE the bootstrap-live consumer's context is
	// cancelled — i.e. while the bootstrap-live consumer is still delivering.
	// Intended for deterministic validation harnesses that inject live traffic
	// during bootstrap and need it fully archived before the cutover tears the
	// consumer down (production re-fetches any in-flight live events from the
	// persisted cursor in steady-state, so production leaves this nil and the
	// cutover proceeds immediately).
	BarrierBeforeCutover PhaseBarrier

	// BarrierAfterBootstrap, if non-nil, runs after bootstrap has durably
	// written PhaseMerging and before merge begins. Intended for deterministic
	// validation harnesses; production leaves it nil.
	BarrierAfterBootstrap PhaseBarrier

	// BarrierAfterMerge, if non-nil, runs after merge has durably written
	// PhaseSteadyState and before the steady-state live consumer starts.
	// Intended for deterministic validation harnesses; production leaves it nil.
	BarrierAfterMerge PhaseBarrier

	// AfterRepoComplete is forwarded to the backfill store after a repo
	// completion row is durable; test-only restart hook; production leaves nil.
	AfterRepoComplete func(context.Context, atmos.DID) error

	// CrashInjector is a test-only deterministic crash simulator. Production
	// leaves it nil, making every simulateCrash checkpoint a no-op.
	CrashInjector crashpoint.Injector
}

func (c *Config) validate() error {
	if c.DataDir == "" {
		return fmt.Errorf("%w: DataDir is required", ErrInvalidConfig)
	}
	if c.Store == nil {
		return fmt.Errorf("%w: Store is required", ErrInvalidConfig)
	}
	if c.RelayURL == "" {
		return fmt.Errorf("%w: RelayURL is required", ErrInvalidConfig)
	}
	if c.BackfillWorkers < 0 {
		return fmt.Errorf("%w: BackfillWorkers must be >= 0", ErrInvalidConfig)
	}
	if c.BackfillBatchSize < 0 {
		return fmt.Errorf("%w: BackfillBatchSize must be >= 0", ErrInvalidConfig)
	}
	if c.BackfillAsyncFlushWorkers < 0 {
		return fmt.Errorf("%w: BackfillAsyncFlushWorkers must be >= 0", ErrInvalidConfig)
	}
	if c.FailedRepoRetryInterval < 0 {
		return fmt.Errorf("%w: FailedRepoRetryInterval must be >= 0", ErrInvalidConfig)
	}
	if c.FailedRepoRetryWorkers < 0 {
		return fmt.Errorf("%w: FailedRepoRetryWorkers must be >= 0", ErrInvalidConfig)
	}
	if c.FailedRepoRetryHostWorkers < 0 {
		return fmt.Errorf("%w: FailedRepoRetryHostWorkers must be >= 0", ErrInvalidConfig)
	}
	if c.FailedRepoRetryMaxDelay < 0 {
		return fmt.Errorf("%w: FailedRepoRetryMaxDelay must be >= 0", ErrInvalidConfig)
	}
	if c.HTTPClient == nil {
		return fmt.Errorf("%w: HTTPClient is required", ErrInvalidConfig)
	}
	if c.Directory == nil {
		return fmt.Errorf("%w: Directory is required", ErrInvalidConfig)
	}
	if c.Verifier == nil {
		return fmt.Errorf("%w: Verifier is required", ErrInvalidConfig)
	}
	if c.Logger == nil {
		return fmt.Errorf("%w: Logger is required", ErrInvalidConfig)
	}
	return nil
}
