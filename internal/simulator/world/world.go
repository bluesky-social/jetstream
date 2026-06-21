package world

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"

	"github.com/bluesky-social/jetstream/internal/simulator/fanout"
	"github.com/cockroachdb/pebble"
)

// World is the simulator's runtime handle: pebble db + the in-memory
// state that derives from it. Goroutine-safety: pebble itself is safe;
// mutationMu serializes post-bootstrap event generation, including the
// shared RNG and logical-clock state. Sequence allocation is via
// atomic.Int64.
type World struct {
	cfg Config
	db  *pebble.DB

	mutationMu sync.Mutex
	rng        *rand.Rand
	fanout     *fanout.Registry
	seq        atomic.Int64
}

// New opens (creating if needed) the simulator pebble db at
// cfg.DataDir. With cfg.Reset = true, removes the directory first.
// Refuses to operate when cfg.DataDir resolves to "./data".
func New(_ context.Context, cfg Config) (*World, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if cfg.Reset {
		if err := os.RemoveAll(cfg.DataDir); err != nil {
			return nil, fmt.Errorf("world: reset %s: %w", cfg.DataDir, err)
		}
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("world: mkdir %s: %w", cfg.DataDir, err)
	}
	db, err := pebble.Open(cfg.DataDir, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("world: open pebble at %s: %w", cfg.DataDir, err)
	}
	return &World{cfg: cfg, db: db}, nil
}

// Close releases the pebble db. Idempotent.
func (w *World) Close() error {
	if w.db == nil {
		return nil
	}
	err := w.db.Close()
	w.db = nil
	if err != nil && !errors.Is(err, pebble.ErrClosed) {
		return fmt.Errorf("world: close pebble: %w", err)
	}
	return nil
}

// AttachRuntime wires in the live RNG and fanout. Called once after
// New + EnsureSeed + Bootstrap by cmd/simulator's serve action.
func (w *World) AttachRuntime(r *rand.Rand, fan *fanout.Registry) error {
	w.rng = r
	w.fanout = fan
	cur, err := w.loadSeq()
	if err != nil {
		return err
	}
	w.seq.Store(cur)
	return nil
}

// CurrentSeq returns the latest persisted firehose seq.
func (w *World) CurrentSeq() int64 { return w.seq.Load() }

// FirehoseRange exposes the read-side of the ring buffer for relay
// subscribers (Task 15).
func (w *World) FirehoseRange(cursor int64, limit int) ([][]byte, error) {
	return w.firehoseRange(cursor, limit)
}
