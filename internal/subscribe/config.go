package subscribe

import (
	"fmt"
	"log/slog"
	"time"
)

const (
	// DefaultReadBatch is the max events ReadFrom returns per call.
	DefaultReadBatch = 1024

	// DefaultSlowWindow is the sustained window over which an adversarially
	// slow client is judged.
	DefaultSlowWindow = 60 * time.Second

	// DefaultSlowMinRate is the events/sec floor below which a far-behind
	// client is adversarial.
	DefaultSlowMinRate = 5.0

	// DefaultSlowLagThreshold is how many events behind the tip a client must
	// be before the slow detector considers it "far behind".
	DefaultSlowLagThreshold = 100_000
)

var ErrInvalidConfig = fmt.Errorf("subscribe: invalid config")

// Config controls Tail + Handler behavior. The cold-path block cache is sized
// independently via ColdReaderConfig.BlockCacheBytes (owned by the injected
// cold reader), so it is not a field here.
type Config struct {
	Logger  *slog.Logger // required
	Metrics *Metrics     // optional; nil = no-op

	ReadBatch        int           // 0 -> DefaultReadBatch
	SlowWindow       time.Duration // 0 -> DefaultSlowWindow
	SlowMinRate      float64       // 0 -> DefaultSlowMinRate
	SlowLagThreshold uint64        // 0 -> DefaultSlowLagThreshold
}

func (c *Config) validate() error {
	if c.Logger == nil {
		return fmt.Errorf("%w: Logger is required", ErrInvalidConfig)
	}
	return nil
}

func (c *Config) applyDefaults() {
	if c.ReadBatch <= 0 {
		c.ReadBatch = DefaultReadBatch
	}
	if c.SlowWindow <= 0 {
		c.SlowWindow = DefaultSlowWindow
	}
	if c.SlowMinRate <= 0 {
		c.SlowMinRate = DefaultSlowMinRate
	}
	if c.SlowLagThreshold == 0 {
		c.SlowLagThreshold = DefaultSlowLagThreshold
	}
}
