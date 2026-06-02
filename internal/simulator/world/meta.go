package world

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
)

// ErrSeedMismatch is returned by EnsureSeed when the persisted seed
// does not match cfg.Seed. Operators must --reset (or change cfg.Seed
// back) before continuing.
var ErrSeedMismatch = errors.New("world: seed mismatch; pass --reset or restore previous --seed")

// loadSeed reads sim/meta/seed. The bool is false on first-run (no
// row).
func (w *World) loadSeed() (uint64, bool, error) {
	val, closer, err := w.db.Get(keyMetaSeed)
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("world: load seed: %w", err)
	}
	defer func() { _ = closer.Close() }()
	if len(val) != 8 {
		return 0, false, fmt.Errorf("world: load seed: got %d bytes, want 8", len(val))
	}
	return binary.BigEndian.Uint64(val), true, nil
}

// saveSeed persists sim/meta/seed with pebble.Sync — this is the
// commit point of the bootstrap handshake.
func (w *World) saveSeed(seed uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], seed)
	if err := w.db.Set(keyMetaSeed, buf[:], pebble.Sync); err != nil {
		return fmt.Errorf("world: save seed: %w", err)
	}
	return nil
}

// EnsureSeed implements the seed handshake:
//   - first run (no row): persists cfg.Seed, returns (true, nil) →
//     "caller should run bootstrap"
//   - matching row: returns (false, nil) → "resume"
//   - mismatched row: returns (_, ErrSeedMismatch)
func (w *World) EnsureSeed() (wantBootstrap bool, err error) {
	persisted, ok, err := w.loadSeed()
	if err != nil {
		return false, err
	}
	if !ok {
		if err := w.saveSeed(w.cfg.Seed); err != nil {
			return false, err
		}
		return true, nil
	}
	if persisted != w.cfg.Seed {
		return false, fmt.Errorf("%w: persisted=%d cfg=%d", ErrSeedMismatch, persisted, w.cfg.Seed)
	}
	return false, nil
}

// loadSeq reads sim/meta/seq, returning 0 if absent (first run).
func (w *World) loadSeq() (int64, error) {
	val, closer, err := w.db.Get(keyMetaSeq)
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("world: load seq: %w", err)
	}
	defer func() { _ = closer.Close() }()
	if len(val) != 8 {
		return 0, fmt.Errorf("world: load seq: got %d bytes, want 8", len(val))
	}
	return int64(binary.BigEndian.Uint64(val)), nil
}

// saveSeq writes sim/meta/seq with pebble.NoSync. Live firehose
// generation uses persistFirehoseFrame so seq and frame share one
// commit point; saveSeq exists for tests and direct metadata setup.
func (w *World) saveSeq(seq int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(seq))
	if err := w.db.Set(keyMetaSeq, buf[:], pebble.NoSync); err != nil {
		return fmt.Errorf("world: save seq: %w", err)
	}
	return nil
}
