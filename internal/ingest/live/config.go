package live

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/segment"
	atmossync "github.com/jcalabro/atmos/sync"
)

// Pebble keys used by the live consumer. Exported so the orchestrator
// can wire bootstrap-time and steady-state consumers to the right
// counters without duplicating the literals.
const (
	// BootstrapSeqKey holds the throwaway seq counter for the
	// live_segments/ tree written during bootstrap. Disjoint from
	// SteadySeqKey so the bootstrap-time consumer cannot collide
	// with the backfill writer's seq allocator on the segments/ tree.
	BootstrapSeqKey = "live_segments/seq/next"

	// SteadySeqKey is the seq counter for segments/. Shared with the
	// backfill writer; the steady-state consumer resumes from where
	// backfill left off.
	SteadySeqKey = "seq/next"

	// CursorKey is the persisted upstream relay firehose cursor.
	// Shared across phases: bootstrap writes into it, steady-state
	// resumes from it, and the merge step will not rewrite it.
	CursorKey = "relay/cursor"
)

// Config controls Consumer behavior.
type Config struct {
	// SegmentsDir is where the consumer writes seg_*.jss files.
	// For bootstrap phase this is "<data-dir>/backfill/live_segments".
	// Once the merge step lands, steady-state callers point this at
	// "<data-dir>/segments".
	SegmentsDir string

	// Store is the shared metadata pebble db.
	Store *store.Store

	// SeqKey is the pebble key used by the underlying ingest.Writer
	// for its seq counter. Bootstrap uses "live_segments/seq/next";
	// steady state uses "seq/next".
	SeqKey string

	// CursorKey is the pebble key for the upstream relay cursor.
	// Both phases use "relay/cursor" (the merge step will hand
	// cursor ownership over without renaming the key).
	CursorKey string

	// RelayURL is the upstream relay HTTP base URL — the same value
	// the operator passes via --relay-url. The consumer derives the
	// WebSocket URL from this internally.
	RelayURL string

	// Logger is required.
	Logger *slog.Logger

	// Metrics is optional; nil means no /metrics counters incrementing.
	Metrics *Metrics

	// Verifier runs Sync 1.1 verification on every #commit and #sync
	// before the consumer's Operations() iterator yields ops to the
	// converter. Required.
	//
	// Construct via sync.NewVerifier in the cmd boundary; livestream
	// does not own verifier lifecycle (the verifier's resync worker
	// pool is a process-wide resource and is reusable across a
	// future steady-state consumer).
	Verifier *atmossync.Verifier

	// MaxSegmentBytes / MaxEventsPerBlock forward to ingest.Config.
	// Zero means use ingest defaults.
	MaxSegmentBytes   int64
	MaxEventsPerBlock int

	// SegmentMetrics flows through the consumer's internal *ingest.Writer
	// to every segment.New it makes. Optional.
	SegmentMetrics *segment.Metrics

	// OnEvent is called once per segment.Event after it has been
	// durably appended to the writer. The event passed in carries
	// its assigned Seq.
	//
	// Optional. If nil, Consumer is a pure archive-only sink. If set,
	// OnEvent must be non-blocking and goroutine-safe; the caller is
	// expected to be a fan-out broadcaster that protects itself with
	// bounded buffers.
	//
	// OnEvent runs synchronously in the per-batch loop, so a slow
	// OnEvent will throttle the upstream firehose. Don't block here.
	//
	// Aliasing: the *segment.Event passed in points into processBatch's
	// local slice. The hook MUST NOT retain the pointer past return —
	// it must dereference and forward a fresh value (or copy) to any
	// longer-lived consumer.
	OnEvent func(*segment.Event)

	// now is overridable for tests; production uses time.Now.
	now func() time.Time
}

func (c *Config) validate() error {
	if c.SegmentsDir == "" {
		return fmt.Errorf("%w: SegmentsDir is required", ErrInvalidConfig)
	}
	if c.Store == nil {
		return fmt.Errorf("%w: Store is required", ErrInvalidConfig)
	}
	if c.SeqKey == "" {
		return fmt.Errorf("%w: SeqKey is required", ErrInvalidConfig)
	}
	if c.CursorKey == "" {
		return fmt.Errorf("%w: CursorKey is required", ErrInvalidConfig)
	}
	if c.RelayURL == "" {
		return fmt.Errorf("%w: RelayURL is required", ErrInvalidConfig)
	}
	if c.Logger == nil {
		return fmt.Errorf("%w: Logger is required", ErrInvalidConfig)
	}
	if c.Verifier == nil {
		return fmt.Errorf("%w: Verifier is required", ErrInvalidConfig)
	}
	return nil
}

func (c *Config) applyDefaults() {
	if c.now == nil {
		c.now = time.Now
	}
}
