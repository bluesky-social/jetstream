package subscribe

import "time"

type slowConfig struct {
	window       time.Duration
	lagThreshold uint64  // events behind tip to be considered "far behind"
	minRate      float64 // events/sec floor below which (when far behind) is adversarial
	now          func() time.Time
}

// slowDetector disconnects a subscriber only when it remains far behind and
// advances below minRate for a full window. It measures cursor progress, not
// delivered frames, so selective filters do not count as slow readers. Each
// subscriber's single goroutine owns its detector; it is not
// concurrency-safe.
type slowDetector struct {
	cfg slowConfig

	streakStart    time.Time // start of the current continuous bad streak
	streakStartPos uint64    // cursor position at streak start
	inStreak       bool
}

func newSlowDetector(cfg slowConfig) *slowDetector {
	if cfg.now == nil {
		cfg.now = time.Now
	}
	return &slowDetector{cfg: cfg}
}

// observe records the subscriber's current log position (its cursor: the next
// seq it will read, a monotonic measure of scan progress) and its current lag
// (tip - cursor). Returns true when the client should be dropped.
func (d *slowDetector) observe(pos, lag uint64) bool {
	farBehind := lag > d.cfg.lagThreshold
	now := d.cfg.now()

	if !farBehind {
		// Caught up enough: not a candidate. Reset the streak.
		d.inStreak = false
		return false
	}

	if !d.inStreak {
		// First far-behind observation: anchor the streak.
		d.inStreak = true
		d.streakStart = now
		d.streakStartPos = pos
		return false
	}

	elapsed := now.Sub(d.streakStart).Seconds()
	if elapsed < d.cfg.window.Seconds() {
		return false // streak not yet a full window
	}

	// Full window of continuous far-behind: judge the average scan rate over it.
	rate := float64(pos-d.streakStartPos) / elapsed
	if rate >= d.cfg.minRate {
		// It was far behind but kept scanning at an acceptable rate: re-anchor
		// and keep watching rather than dropping a client that's working.
		d.streakStart = now
		d.streakStartPos = pos
		return false
	}
	return true
}
