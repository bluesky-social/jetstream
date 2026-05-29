package subscribe

import (
	"fmt"
	"log/slog"
)

// DefaultSubscriberBufferSize is the per-subscriber channel depth.
//
// At ~500 events/sec steady state with planning headroom up to ~2k/sec,
// 16k slots gives ~8 seconds of headroom at peak — enough to ride out
// GC pauses and brief network stalls, bounded enough that a misbehaving
// subscriber doesn't sit on much memory before being dropped.
const DefaultSubscriberBufferSize = 16384

// DefaultLookbackRingSize is the per-subscriber buffer for live
// events captured during cursor-replay. 16384 events at peak rate
// (~2k/sec) gives ~8 seconds of headroom — enough to absorb a slow
// disk replay without forcing a restart, bounded enough that 1000
// concurrent lookback subscribers fit in ~128MB of pointers.
const DefaultLookbackRingSize = 16384

// DefaultMaxLookbackIterations bounds the ring-overflow restart loop.
// 16 successive overflows means the subscriber's outbox is wedged
// for ~minutes at full traffic; we'd rather disconnect than livelock.
const DefaultMaxLookbackIterations = 16

// ErrInvalidConfig is returned by validate when required fields are missing.
var ErrInvalidConfig = fmt.Errorf("subscribe: invalid config")

// Config controls Broadcaster + Handler behavior.
type Config struct {
	// Logger is required.
	Logger *slog.Logger

	// Metrics is optional; nil means no /metrics counters incrementing.
	Metrics *Metrics

	// SubscriberBufferSize is the per-subscriber channel depth. 0 means
	// DefaultSubscriberBufferSize.
	SubscriberBufferSize int

	// LookbackRingSize is the per-subscriber bounded ring size used
	// during the lookback-to-live handoff. Defaults to
	// DefaultLookbackRingSize when zero.
	LookbackRingSize int

	// MaxLookbackIterations caps the number of ring-overflow restarts
	// before a subscriber is terminated with ErrLookbackTooSlow.
	// Defaults to DefaultMaxLookbackIterations when zero.
	MaxLookbackIterations int
}

func (c *Config) validate() error {
	if c.Logger == nil {
		return fmt.Errorf("%w: Logger is required", ErrInvalidConfig)
	}
	return nil
}

func (c *Config) applyDefaults() {
	if c.SubscriberBufferSize <= 0 {
		c.SubscriberBufferSize = DefaultSubscriberBufferSize
	}
	if c.LookbackRingSize <= 0 {
		c.LookbackRingSize = DefaultLookbackRingSize
	}
	if c.MaxLookbackIterations <= 0 {
		c.MaxLookbackIterations = DefaultMaxLookbackIterations
	}
}
