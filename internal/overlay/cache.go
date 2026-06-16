package overlay

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/tombstone"
	"github.com/zeebo/xxh3"
)

// Source supplies the data the overlay blob is built from. The concrete
// *tombstone.Set plus a watermark accessor satisfy it via a thin adapter
// constructed in runtime.go.
type Source interface {
	// SnapshotRange returns tombstones in (lowExclusive, highInclusive].
	SnapshotRange(lowExclusive, highInclusive uint64) tombstone.Snapshot
	// Watermark is the current compaction/seq (the blob's W floor).
	Watermark() uint64
	// Dirty returns a value that changes whenever the underlying set
	// mutates. The cache rebuilds only when this differs from the value
	// captured at the last build.
	Dirty() uint64
}

// Blob is one immutable, serialized overlay. Never mutated after publish;
// concurrent readers share Bytes.
type Blob struct {
	Bytes      []byte
	ETag       string
	Watermark  uint64
	MaxSeq     uint64
	NumRecords int
	NumDIDs    int
	dirtyAt    uint64 // Source.Dirty() value this blob was built from
}

// Cache holds the latest published *Blob and rebuilds it from Source.
type Cache struct {
	src     Source
	metrics *Metrics

	mu  sync.RWMutex
	cur *Blob
}

// NewCache builds an initial blob immediately so Current never returns nil.
func NewCache(src Source, m *Metrics) *Cache {
	c := &Cache{src: src, metrics: m}
	c.cur = c.build()
	return c
}

// Current returns the latest published blob. Safe for concurrent use; the
// returned *Blob is immutable.
func (c *Cache) Current() *Blob {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cur
}

// Rebuild rebuilds and publishes a new blob if the source has changed
// since the last build; otherwise it is a cheap no-op. Called from the
// compaction-pass hook and the background ticker.
func (c *Cache) Rebuild() {
	c.mu.RLock()
	prev := c.cur
	c.mu.RUnlock()
	if prev != nil && c.src.Dirty() == prev.dirtyAt {
		return
	}
	next := c.build()
	c.mu.Lock()
	c.cur = next
	c.mu.Unlock()
}

// build snapshots the source and serializes a new blob. The snapshot is
// taken under the set's own lock (inside SnapshotRange) and released
// before encode+compress, so a slow build never blocks ingestion.
func (c *Cache) build() *Blob {
	start := time.Now()
	// Capture Dirty BEFORE the snapshot. With this ordering, a mutation
	// racing the build can only cause one extra (harmless) rebuild on the
	// next tick — never a missed one: if the mutation lands after this
	// read but before/within the snapshot, dirtyAt is stale and the next
	// Rebuild sees a mismatch and rebuilds again. The inverse ordering
	// (capture after snapshot) could record a Dirty that already accounts
	// for a mutation the snapshot missed, dropping it until the *next*
	// mutation — a correctness bug. Do not reorder.
	dirty := c.src.Dirty()
	w := c.src.Watermark()
	snap := c.src.SnapshotRange(w, ^uint64(0))

	m := w
	for _, seq := range snap.Records {
		if seq > m {
			m = seq
		}
	}
	for _, ts := range snap.DIDs {
		if ts.Seq > m {
			m = ts.Seq
		}
	}

	bytes := Encode(snap, w, m)
	blob := &Blob{
		Bytes:      bytes,
		ETag:       fmt.Sprintf("%q", fmt.Sprintf("%016x", xxh3.Hash(bytes))),
		Watermark:  w,
		MaxSeq:     m,
		NumRecords: len(snap.Records),
		NumDIDs:    len(snap.DIDs),
		dirtyAt:    dirty,
	}
	c.metrics.observeBuild(time.Since(start), len(bytes), blob.NumRecords, blob.NumDIDs)
	return blob
}

// RunTicker rebuilds on a coalescing interval until ctx is cancelled. It
// bounds blob staleness to ~interval; Rebuild itself skips when the source
// is unchanged, so an idle firehose costs nothing. Run as a lifecycle
// goroutine.
func (c *Cache) RunTicker(ctx context.Context, interval time.Duration) error {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			c.Rebuild()
		}
	}
}

// ObserveServe records that n bytes of the overlay blob were written to a
// client. Called by the getTombstones handler after a successful write.
func (c *Cache) ObserveServe(n int) { c.metrics.observeServe(n) }
