package client

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

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
// Concurrency: ShouldDrop is on the hot path — every backfill row, decoded by
// many parallel workers (#142). It must be cheap and contention-free. The
// tombstone set is read-mostly: seeded once from the overlay, then occasionally
// grown by live-tail tombstones, against millions of reads. So the snapshot is
// kept immutable behind an atomic.Pointer: ShouldDrop does a lock-free atomic
// load (no per-row mutex), and writers (SeedFromOverlay, Merge) build a fresh
// snapshot under a small write mutex and atomically swap it in (copy-on-write).
// A reader always observes one complete, internally-consistent snapshot — never
// a torn or partially-merged map.
type Suppressor struct {
	// snap is the current immutable tombstone snapshot. Readers load it
	// atomically; writers replace it wholesale under writeMu. Never mutate the
	// maps of a published snapshot in place — that would race lock-free readers.
	snap atomic.Pointer[tombstone.Snapshot]
	// writeMu serializes writers so two concurrent merges/seeds cannot lose an
	// update by both copying the same base and racing their swaps.
	writeMu sync.Mutex
}

// NewSuppressor returns an empty Suppressor (no rows suppressed). Seed it with
// SeedFromOverlay and grow it with Merge as live tombstones arrive.
func NewSuppressor() *Suppressor {
	s := &Suppressor{}
	empty := emptySnapshot()
	s.snap.Store(&empty)
	return s
}

func emptySnapshot() tombstone.Snapshot {
	return tombstone.Snapshot{
		Records: make(map[tombstone.RecordKey]uint64),
		DIDs:    make(map[string]tombstone.DIDTombstone),
	}
}

// SeedFromOverlay fetches the current getTombstones blob, decodes it, and
// installs its tombstones as the suppressor's base set. Returns the decoded
// (W, M) coverage envelope.
//
// The decoded snapshot is published as-is (the overlay decode produces a fresh,
// privately-owned snapshot), replacing whatever was there. Seeding happens once
// at startup before the backfill reads begin.
func (s *Suppressor) SeedFromOverlay(ctx context.Context, xc *xrpc.Client) (w, m uint64, err error) {
	blob, err := jetstream.JetstreamGetTombstones(ctx, xc)
	if err != nil {
		return 0, 0, fmt.Errorf("jetstream: getTombstones: %w", err)
	}
	w, m, snap, err := overlay.Decode(blob)
	if err != nil {
		return 0, 0, fmt.Errorf("jetstream: decode tombstone overlay: %w", err)
	}
	snap = ensureSnapshotMaps(snap)
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.snap.Store(&snap)
	return w, m, nil
}

// Merge folds additional tombstones (e.g. derived from live-tail rows) into the
// suppressor via copy-on-write: it builds a new snapshot from the current one
// plus other, then atomically swaps it in, so concurrent lock-free readers are
// unaffected and never see a partially-merged map. A no-op for an empty delta
// (the common case for a non-tombstone live event), so the hot live path pays
// nothing.
func (s *Suppressor) Merge(other tombstone.Snapshot) {
	if other.Empty() {
		return
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	// Copy the current snapshot, fold other into the copy, then publish. The
	// published snapshot is never mutated again, so readers holding the old
	// pointer keep reading a valid set until they reload.
	cur := s.snap.Load()
	next := tombstone.Snapshot{
		Records: make(map[tombstone.RecordKey]uint64, len(cur.Records)+len(other.Records)),
		DIDs:    make(map[string]tombstone.DIDTombstone, len(cur.DIDs)+len(other.DIDs)),
	}
	for k, v := range cur.Records {
		next.Records[k] = v
	}
	for k, v := range cur.DIDs {
		next.DIDs[k] = v
	}
	next.Merge(other) // keeps the max seq per key
	s.snap.Store(&next)
}

// ObserveLive folds a single live event into the suppressor if it is a
// tombstone (delete, account-delete, or sync divergence). Materialization rows
// are ignored (Fold yields an empty delta, so Merge is a cheap no-op). This
// keeps the combined set current as the live tail advances so later-downloaded
// historical rows are suppressed by tombstones already seen live.
func (s *Suppressor) ObserveLive(ev *segment.Event) error {
	part, err := tombstone.Fold([]segment.Event{*ev}, 0)
	if err != nil {
		return err
	}
	s.Merge(part)
	return nil
}

// ShouldDrop reports whether a materialization row must be suppressed because a
// higher-seq tombstone supersedes it. Non-materialization rows are never
// dropped. The second return is the suppression reason for diagnostics.
//
// Hot path: a lock-free atomic load of the immutable snapshot, then a pure map
// read. No mutex, so parallel decode workers do not contend here.
func (s *Suppressor) ShouldDrop(ev *segment.Event) (bool, string) {
	return s.snap.Load().ShouldDrop(ev)
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
