package jetstreamd

import (
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/internal/tombstone"
)

// overlaySource adapts the live tombstone set + metadata store to the
// overlay.Source interface. Watermark reads compaction/seq with the same
// versioned-uint64 encoding the compactor writes it with.
type overlaySource struct {
	set   *tombstone.Set
	store *store.Store
}

func (o overlaySource) SnapshotRange(low, high uint64) tombstone.Snapshot {
	return o.set.SnapshotRange(low, high)
}

func (o overlaySource) Dirty() uint64 { return o.set.Dirty() }

func (o overlaySource) Watermark() uint64 {
	// compaction/seq is owned by the compactor and written via
	// SetVersionedUint64LE(key, 0x01, seq). A missing key (no pass yet)
	// or a read error means "nothing compacted": treat W as 0 so the
	// overlay covers the whole live set. This is read-only and best
	// effort; the compactor remains the single writer.
	v, ok, err := o.store.GetVersionedUint64LE("compaction/seq", 0x01)
	if err != nil || !ok {
		return 0
	}
	return v
}
