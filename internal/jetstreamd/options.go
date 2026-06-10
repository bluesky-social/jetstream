// Package jetstreamd builds and runs the jetstream service graph used by
// cmd/jetstream and integration tests.
package jetstreamd

import (
	"context"
	"io"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/crashpoint"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/streaming"
)

// PhaseBarrier is a test hook that can pause execution after a major
// lifecycle phase before the daemon advances to the next phase.
type PhaseBarrier func(context.Context) error

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

	MaxBackfillRepos   int
	BackfillRepos      []atmos.DID
	SkipMergeDiscovery bool

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
	SubscribeHotTailBytes     int
	SubscribeBlockCacheBytes  int
	SubscribeReadBatch        int
	SubscribeSlowWindow       time.Duration
	SubscribeSlowMinRate      float64
	CursorBlockIndexCacheSize int
	CompactionInterval        time.Duration
	CompactionTombstoneCap    int
	BarrierAfterBootstrap     PhaseBarrier
	BarrierAfterMerge         PhaseBarrier
	AfterRepoComplete         func(context.Context, atmos.DID) error
	CrashInjector             crashpoint.Injector
	OnBootstrapLiveEvent      func(*segment.Event)
	OnSteadyStateEvent        func(*segment.Event)
}
