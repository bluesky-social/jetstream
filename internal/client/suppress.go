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
// Concurrency + cost model. ShouldDrop is the hot path: every backfill row,
// decoded by many parallel workers (#142), so it must be lock-free. The set is
// also asymmetric — a large overlay base (often 100K+ tombstones) seeded ONCE,
// plus a thin stream of live-tail tombstones folded in during the backfill. So
// the set is split into two layers:
//
//   - base: the immutable overlay seed. Stored behind an atomic.Pointer and
//     swapped only by SeedFromOverlay (once, before reads begin). Never copied
//     per write — that is the whole point of separating it from live.
//   - live: tombstones observed on the live tail, also behind an atomic.Pointer.
//     Merge rebuilds only THIS (small) layer copy-on-write and swaps it in.
//
// ShouldDrop loads both pointers lock-free and returns drop if EITHER layer
// suppresses the row. Splitting the layers keeps writes O(live) instead of
// O(base): folding a live tombstone copies only the handful of live entries,
// not the 100K-entry overlay. Published snapshots are never mutated in place,
// so a lock-free reader always sees two complete, self-consistent maps.
type Suppressor struct {
	base atomic.Pointer[tombstone.Snapshot] // immutable overlay seed; swapped once
	live atomic.Pointer[tombstone.Snapshot] // live-tail tombstones; copy-on-write

	// writeMu serializes live-layer writers so two concurrent merges cannot lose
	// an update by both copying the same base and racing their swaps.
	writeMu sync.Mutex
}

// NewSuppressor returns an empty Suppressor (no rows suppressed). Seed it with
// SeedFromOverlay and grow it with Merge as live tombstones arrive.
func NewSuppressor() *Suppressor {
	s := &Suppressor{}
	base := emptySnapshot()
	live := emptySnapshot()
	s.base.Store(&base)
	s.live.Store(&live)
	return s
}

func emptySnapshot() tombstone.Snapshot {
	return tombstone.Snapshot{
		Records: make(map[tombstone.RecordKey]uint64),
		DIDs:    make(map[string]tombstone.DIDTombstone),
	}
}

// SeedFromOverlay fetches the current getTombstones blob, decodes it, and
// installs its tombstones as the suppressor's immutable base layer. Returns the
// decoded (W, M) coverage envelope. Seeding happens once at startup before the
// backfill reads begin; the decoded snapshot is published as-is and never
// copied again.
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
	s.base.Store(&snap)
	return w, m, nil
}

// Merge folds additional tombstones (e.g. derived from live-tail rows) into the
// LIVE layer via copy-on-write: it copies only the current live layer (small —
// the base is untouched), folds other into the copy, then atomically swaps it
// in. Concurrent lock-free readers are unaffected and never see a partial map.
// A no-op for an empty delta (the common case for a non-tombstone live event),
// so the hot live path pays nothing.
func (s *Suppressor) Merge(other tombstone.Snapshot) {
	if other.Empty() {
		return
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	cur := s.live.Load()
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
	s.live.Store(&next)
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
// higher-seq tombstone supersedes it, in EITHER the base or live layer. Non-
// materialization rows are never dropped. The second return is the suppression
// reason for diagnostics.
//
// Hot path: two lock-free atomic loads + pure map reads, no mutex, so parallel
// decode workers never contend. A row is dropped if either layer has a
// superseding tombstone; the layers are disjoint in time (base = (W,M], live =
// (M,inf)) so "either suppresses" is the correct union of the combined set.
func (s *Suppressor) ShouldDrop(ev *segment.Event) (bool, string) {
	if !ev.Kind.IsMaterialization() {
		return false, ""
	}
	if drop, reason := s.base.Load().ShouldDrop(ev); drop {
		return drop, reason
	}
	return s.live.Load().ShouldDrop(ev)
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
