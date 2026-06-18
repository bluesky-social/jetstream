package client

import (
	"context"
	"fmt"
	"sync"

	"github.com/bluesky-social/jetstream/api/jetstream"
	"github.com/bluesky-social/jetstream/internal/overlay"
	"github.com/bluesky-social/jetstream/internal/tombstone"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/xrpc"
)

// Suppressor holds the combined tombstone set the client applies to every
// materialization (create/update) row before emitting it. It unifies the
// compaction overlay covering (W, M] with live-tail tombstones covering
// (M, inf): a create/update row is suppressed iff a record-key or DID-level
// tombstone exists at a strictly higher seq (tombstone.Snapshot.ShouldDrop).
//
// Per the design note's resolved O1, suppression is eager + at-least-once:
// the client drops rows against the tombstones it ALREADY holds, but never
// holds a row back waiting for a tombstone it has not yet seen. Deletes/updates
// that arrive later on the live tail flow through as their own rows.
//
// A Suppressor is safe for concurrent ShouldDrop and Merge.
type Suppressor struct {
	mu   sync.RWMutex
	snap tombstone.Snapshot

	// watermark (W) and maxSeq (M) from the overlay the suppressor was seeded
	// with. M is the live-tail cutover floor for tombstone coverage.
	watermark uint64
	maxSeq    uint64
}

// NewSuppressor returns an empty Suppressor (no rows suppressed). Seed it with
// SeedFromOverlay and grow it with Merge as live tombstones arrive.
func NewSuppressor() *Suppressor {
	return &Suppressor{snap: emptySnapshot()}
}

func emptySnapshot() tombstone.Snapshot {
	return tombstone.Snapshot{
		Records: make(map[tombstone.RecordKey]uint64),
		DIDs:    make(map[string]tombstone.DIDTombstone),
	}
}

// OverlayCoverage reports the (W, M] window the seeded overlay covers. M is
// the highest seq folded into the tombstone set (including the active,
// unsealed segment), which the engine uses to reason about the live-tail
// tombstone handoff. Valid only after SeedFromOverlay.
func (s *Suppressor) OverlayCoverage() (w, m uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.watermark, s.maxSeq
}

// SeedFromOverlay fetches the current getTombstones blob, decodes it, and
// installs its tombstones as the suppressor's base set. Returns the decoded
// (W, M) coverage envelope.
func (s *Suppressor) SeedFromOverlay(ctx context.Context, xc *xrpc.Client) (w, m uint64, err error) {
	blob, err := jetstream.JetstreamGetTombstones(ctx, xc)
	if err != nil {
		return 0, 0, fmt.Errorf("jetstream: getTombstones: %w", err)
	}
	w, m, snap, err := overlay.Decode(blob)
	if err != nil {
		return 0, 0, fmt.Errorf("jetstream: decode tombstone overlay: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snap = ensureSnapshotMaps(snap)
	s.watermark = w
	s.maxSeq = m
	return w, m, nil
}

// Merge folds additional tombstones (e.g. derived from live-tail rows) into
// the suppressor, keeping the max seq per key.
func (s *Suppressor) Merge(other tombstone.Snapshot) {
	if other.Empty() {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snap.Merge(other)
}

// ObserveLive folds a single live event into the suppressor if it is a
// tombstone (delete, account-delete, or sync divergence). Materialization rows
// are ignored. This keeps the combined set current as the live tail advances
// so later-downloaded historical rows are suppressed by tombstones already
// seen live.
func (s *Suppressor) ObserveLive(ev *segment.Event) error {
	part, err := tombstone.Fold([]segment.Event{*ev}, 0)
	if err != nil {
		return err
	}
	s.Merge(part)
	return nil
}

// ShouldDrop reports whether a materialization row must be suppressed because
// a higher-seq tombstone supersedes it. Non-materialization rows are never
// dropped. The second return is the suppression reason for diagnostics.
func (s *Suppressor) ShouldDrop(ev *segment.Event) (bool, string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snap.ShouldDrop(ev)
}

func ensureSnapshotMaps(snap tombstone.Snapshot) tombstone.Snapshot {
	if snap.Records == nil {
		snap.Records = make(map[tombstone.RecordKey]uint64)
	}
	if snap.DIDs == nil {
		snap.DIDs = make(map[string]tombstone.DIDTombstone)
	}
	return snap
}
