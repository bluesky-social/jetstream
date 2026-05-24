package orchestrator

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream-v2/internal/ingest/live"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/jcalabro/atmos/identity"
	atmossync "github.com/jcalabro/atmos/sync"
)

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

	// Logger is required.
	Logger *slog.Logger

	// Metrics is the orchestrator-level metrics handle. Optional;
	// nil means no /metrics counters incrementing.
	Metrics *Metrics

	// IngestMetrics is consumed by the bootstrap-phase backfill
	// writer only. The steady-state live consumer's internal
	// *ingest.Writer hardcodes Metrics: nil (see
	// internal/ingest/live/consumer.go) to avoid prometheus
	// duplicate-series registration with the backfill writer's
	// series, so this field does not flow into the steady-state
	// path. Optional.
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
	SegmentMetrics *segment.Metrics
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
