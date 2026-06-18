// Package jetstreamd builds and runs the jetstream service graph used by
// cmd/jetstream and integration tests.
package jetstreamd

import (
	"context"
	"io"
	"time"

	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/streaming"
)

const (
	DefaultBackfillWorkers           = 100
	DefaultBackfillBatchSize         = 100_000
	DefaultBackfillAsyncFlushWorkers = 4
)

// PhaseBarrier is a test hook that can pause execution after a major
// lifecycle phase before the daemon advances to the next phase.
type PhaseBarrier func(context.Context) error

type CompactionPassResult struct {
	Watermark uint64
	Err       error
}

// Options is the typed runtime configuration for one jetstream daemon
// instance, after CLI and environment inputs have been resolved.
type Options struct {
	PublicAddr         string
	DebugAddr          string
	DataDir            string
	RelayURL           string
	PLCURL             string
	OTelServiceName    string
	LogLevel           string
	LogFormat          string
	LogOutput          io.Writer
	ShutdownTimeout    time.Duration
	ClientDrainTimeout time.Duration

	MaxBackfillRepos          int
	BackfillWorkers           int
	BackfillBatchSize         int
	BackfillAsyncFlushWorkers int
	BackfillRepos             []atmos.DID
	SkipMergeDiscovery        bool

	// DisableRepoActionRateLimits disables the per-source-IP limiter for
	// expensive operator-triggered repo actions on the status UI.
	DisableRepoActionRateLimits bool

	// BackfillRetryBaseDelay, when > 0, overrides the bootstrap backfill
	// engine's initial retry backoff (atmos default 1s). Used by the
	// oracle fault-injection harness to keep injected transient getRepo
	// faults fast; production leaves it 0.
	BackfillRetryBaseDelay time.Duration

	// LiveReconnectBackoff, when non-nil, overrides atmos's subscribeRepos
	// reconnect backoff for internal integration harnesses. Production
	// leaves it nil.
	LiveReconnectBackoff *streaming.BackoffPolicy

	CursorLookback            time.Duration
	SegmentCacheMaxAge        time.Duration
	PlanMaxDIDs               int
	PlanMaxCollections        int
	PlanMaxEntries            int
	PlanWholeSegmentThreshold float64
	SubscribeHotTailBytes     int
	SubscribeBlockCacheBytes  int
	SubscribeReadBatch        int
	SubscribeSlowWindow       time.Duration
	SubscribeSlowMinRate      float64
	CursorBlockIndexCacheSize int
	CompactionInterval        time.Duration
	CompactionTombstoneCap    int
	CompactionRewriteWorkers  int
	OverlayRebuildInterval    time.Duration
	BarrierAfterBootstrap     PhaseBarrier
	BarrierAfterMerge         PhaseBarrier
	OnCompactionPass          func(CompactionPassResult)
	AfterRepoComplete         func(context.Context, atmos.DID) error
	CrashInjector             crashpoint.Injector
	OnBootstrapLiveEvent      func(*segment.Event)
	OnSteadyStateEvent        func(*segment.Event)
}

func (o Options) effectiveBackfillWorkers() int {
	if o.BackfillWorkers > 0 {
		return o.BackfillWorkers
	}
	return DefaultBackfillWorkers
}

func (o Options) effectiveBackfillBatchSize() int {
	if o.BackfillBatchSize > 0 {
		return o.BackfillBatchSize
	}
	return DefaultBackfillBatchSize
}
