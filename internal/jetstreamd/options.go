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
	SkipMergeDiscovery bool

	CursorLookback            time.Duration
	SegmentCacheMaxAge        time.Duration
	SubscribeHotTailBytes     int
	SubscribeBlockCacheBytes  int
	SubscribeReadBatch        int
	SubscribeSlowWindow       time.Duration
	SubscribeSlowMinRate      float64
	CursorBlockIndexCacheSize int
	BarrierAfterBootstrap     PhaseBarrier
	BarrierAfterMerge         PhaseBarrier
	AfterRepoComplete         func(context.Context, atmos.DID) error
	CrashInjector             crashpoint.Injector
	OnBootstrapLiveEvent      func(*segment.Event)
	OnSteadyStateEvent        func(*segment.Event)
}
