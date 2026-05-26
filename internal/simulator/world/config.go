package world

import (
	"errors"
	"fmt"
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
	abs, err := filepath.Abs(c.DataDir)
	if err != nil {
		return fmt.Errorf("world: resolve DataDir %q: %w", c.DataDir, err)
	}
	prodAbs, err := filepath.Abs("./data")
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
