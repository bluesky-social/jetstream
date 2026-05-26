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
}
