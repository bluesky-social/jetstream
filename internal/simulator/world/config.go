package world

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
)

// ErrDataDirReserved is returned by New when DataDir resolves to the
// jetstream data directory. The simulator owns its own pebble db and
// must never share a directory with the production binary.
var ErrDataDirReserved = errors.New("world: --data-dir cannot be ./data; use ./data/simulator")

// Config drives *World construction.
type Config struct {
	DataDir           string
	Reset             bool
	Seed              uint64
	Accounts          int
	InitialRecords    int
	InitialRecordsMin int
	InitialRecordsMax int
	CommitsPerSec     float64
	RateMultiplier    float64
	FirehoseHistory   int
	TrafficMix        TrafficMix
}

// TrafficMix is the weighted event-kind distribution the live traffic
// pump draws from. Weights are relative, not percentages. It is a
// Config field (rather than a package constant) so future swarm-style
// tiers can draw a different mix per seed (#233).
//
// The commit-action weights are deliberately NOT production-shaped: a
// 180s production sample (2026-07-04) measured create 95.5 / delete
// 3.9 / update 0.6 and identity at 0.061% of all events. The mix
// over-weights tombstone-forming ops (update/delete) because that is
// where compaction bugs live, and holds identity well above its
// production rate so a default-scale oracle run (~200 live events)
// still exercises the path several times instead of 0.12 times.
// Production-shaped regression coverage is the corpus tier's job.
type TrafficMix struct {
	Create   float64
	Update   float64
	Delete   float64
	Identity float64
}

// DefaultTrafficMix returns the design-doc action distribution plus
// the identity weight discussed on #202.
func DefaultTrafficMix() TrafficMix {
	return TrafficMix{Create: 75, Update: 15, Delete: 10, Identity: 3}
}

// DefaultConfig returns simulator defaults matching the design doc.
func DefaultConfig() Config {
	return Config{
		DataDir:           "./data/simulator",
		Reset:             false,
		Seed:              42,
		Accounts:          10000,
		InitialRecords:    0,
		InitialRecordsMin: 0,
		InitialRecordsMax: 1000,
		CommitsPerSec:     10,
		RateMultiplier:    1.0,
		FirehoseHistory:   10000,
		TrafficMix:        DefaultTrafficMix(),
	}
}

func (c Config) validate() error {
	if c.DataDir == "" {
		return errors.New("world: DataDir is required")
	}
	// We compare against the production "./data" directory after
	// resolving symlinks on both sides. filepath.Abs alone is not
	// enough: on macOS /var is a symlink to /private/var, so the same
	// physical directory can be spelled two different ways and a
	// naive string comparison would let a caller bypass this check.
	abs, err := canonicalPath(c.DataDir)
	if err != nil {
		return fmt.Errorf("world: resolve DataDir %q: %w", c.DataDir, err)
	}
	prodAbs, err := canonicalPath("./data")
	if err != nil {
		return fmt.Errorf("world: resolve production data dir: %w", err)
	}
	if abs == prodAbs {
		return ErrDataDirReserved
	}
	if c.Accounts <= 0 {
		return fmt.Errorf("world: Accounts must be > 0 (got %d)", c.Accounts)
	}
	if c.CommitsPerSec <= 0 {
		return fmt.Errorf("world: CommitsPerSec must be > 0 (got %v)", c.CommitsPerSec)
	}
	if c.RateMultiplier <= 0 {
		return fmt.Errorf("world: RateMultiplier must be > 0 (got %v)", c.RateMultiplier)
	}
	if c.InitialRecords < 0 {
		return fmt.Errorf("world: InitialRecords must be >= 0 (got %d)", c.InitialRecords)
	}
	if c.InitialRecordsMin < 0 {
		return fmt.Errorf("world: InitialRecordsMin must be >= 0 (got %d)", c.InitialRecordsMin)
	}
	if c.InitialRecordsMax < 0 {
		return fmt.Errorf("world: InitialRecordsMax must be >= 0 (got %d)", c.InitialRecordsMax)
	}
	if c.InitialRecordsMax < c.InitialRecordsMin {
		return fmt.Errorf("world: InitialRecordsMax must be >= InitialRecordsMin (got %d < %d)", c.InitialRecordsMax, c.InitialRecordsMin)
	}
	if c.FirehoseHistory < 0 {
		return fmt.Errorf("world: FirehoseHistory must be >= 0 (got %d)", c.FirehoseHistory)
	}
	if err := c.TrafficMix.validate(); err != nil {
		return err
	}
	return nil
}

func (m TrafficMix) validate() error {
	for _, w := range []struct {
		name string
		val  float64
	}{
		{"Create", m.Create}, {"Update", m.Update},
		{"Delete", m.Delete}, {"Identity", m.Identity},
	} {
		// NaN fails every comparison and +Inf is not < 0, so a plain
		// range check would let non-finite weights through to the
		// weighted tables (empty-table panic or an Inf-dominated draw).
		if math.IsNaN(w.val) || math.IsInf(w.val, 0) {
			return fmt.Errorf("world: TrafficMix.%s must be finite (got %v)", w.name, w.val)
		}
		if w.val < 0 {
			return fmt.Errorf("world: TrafficMix.%s must be >= 0 (got %v)", w.name, w.val)
		}
	}
	return nil
}

// isZero reports whether the mix is entirely unset. New treats a
// zero-value mix as "use DefaultTrafficMix" (matching the zero-value
// defaulting convention elsewhere, e.g. backfill.LiveEnqueuerConfig)
// so literal Config construction keeps working. An explicit all-zero
// mix is indistinguishable from unset by design: a mix that generates
// nothing is never a meaningful configuration, and weightedChoice
// would silently return the last option. A partial mix (e.g. only
// Identity set) is honored as-is — a future swarm tier (#233) may
// deliberately omit kinds.
func (m TrafficMix) isZero() bool {
	return m == TrafficMix{}
}

// canonicalPath returns an absolute, symlink-resolved version of p.
//
// We can't simply call filepath.EvalSymlinks: the leaf path (and any
// number of parents) may not exist yet — that's the whole point of
// validating before we mkdir. So we walk upward from p until we find
// an existing ancestor, EvalSymlinks that, and re-attach the
// non-existent suffix. The result is a canonical name for whatever
// physical directory p will eventually live in.
func canonicalPath(p string) (string, error) {
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", err
	}
	suffix := ""
	cur := abs
	for {
		if _, err := os.Lstat(cur); err == nil {
			break
		} else if !os.IsNotExist(err) {
			return "", err
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			// Reached the root without finding an existing entry —
			// nothing to resolve symlinks against; return abs as-is.
			return abs, nil
		}
		suffix = filepath.Join(filepath.Base(cur), suffix)
		cur = parent
	}
	resolved, err := filepath.EvalSymlinks(cur)
	if err != nil {
		return "", err
	}
	if suffix == "" {
		return resolved, nil
	}
	return filepath.Join(resolved, suffix), nil
}
