package world

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ErrDataDirReserved is returned by New when DataDir resolves to the
// jetstream data directory. The simulator owns its own pebble db and
// must never share a directory with the production binary.
var ErrDataDirReserved = errors.New("world: --data-dir cannot be ./data; use ./data/simulator")

// Config drives *World construction.
type Config struct {
	DataDir         string
	Reset           bool
	Seed            uint64
	Accounts        int
	InitialRecords  int
	CommitsPerSec   float64
	RateMultiplier  float64
	FirehoseHistory int
	RepoCacheSize   int
}

// DefaultConfig returns simulator defaults matching the design doc.
func DefaultConfig() Config {
	return Config{
		DataDir:         "./data/simulator",
		Reset:           false,
		Seed:            42,
		Accounts:        10000,
		InitialRecords:  5,
		CommitsPerSec:   10,
		RateMultiplier:  1.0,
		FirehoseHistory: 10000,
		RepoCacheSize:   512,
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
	if c.FirehoseHistory < 0 {
		return fmt.Errorf("world: FirehoseHistory must be >= 0 (got %d)", c.FirehoseHistory)
	}
	if c.RepoCacheSize <= 0 {
		return fmt.Errorf("world: RepoCacheSize must be > 0 (got %d)", c.RepoCacheSize)
	}
	return nil
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
