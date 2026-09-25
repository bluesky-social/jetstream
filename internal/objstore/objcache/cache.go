// Package objcache is the compressed object cache (design §17): an LRU of
// verified object bytes keyed by SHA-256, bounded by a byte budget.
//
// Keying by content rather than object_id means a deduplicated object is
// cached once however many rows reference it, and an entry can never be
// stale: bytes that hash to the key are the right bytes for every reference
// to that hash. It holds raw block and pointer-batch frames, still
// compressed; footers are cached decoded elsewhere.
package objcache

import (
	"container/list"
	"crypto/sha256"
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/bluesky-social/jetstream/internal/obs"
)

// DefaultMaxBytes is JETSTREAM_OBJECT_CACHE_BYTES's default (design §18).
const DefaultMaxBytes = 2 << 30

// Config configures a Cache.
type Config struct {
	// MaxBytes bounds the total size of cached objects. Zero or negative
	// disables the cache: every Get misses and Add keeps nothing.
	MaxBytes int64
	// Memory, when set, receives the budget and current use under
	// jetstream_memory_{budget,used}_bytes{budget="object_cache"}.
	Memory *obs.MemoryMetrics
}

// Cache is a byte-bounded LRU safe for concurrent use. A nil *Cache is a
// valid, always-empty cache.
//
// Get returns the cached slice itself, and Add keeps the caller's slice, to
// avoid copying frames on the read hot path. Callers must treat both as
// immutable.
type Cache struct {
	max  int64
	used prometheus.Gauge

	mu      sync.Mutex
	size    int64
	lru     *list.List // front is most recent; values are *entry
	entries map[[sha256.Size]byte]*list.Element
}

type entry struct {
	sha  [sha256.Size]byte
	data []byte
}

// New returns an empty cache.
func New(cfg Config) *Cache {
	c := &Cache{
		max:     max(cfg.MaxBytes, 0),
		lru:     list.New(),
		entries: make(map[[sha256.Size]byte]*list.Element),
	}
	limit, used := cfg.Memory.Gauges(obs.BudgetObjectCache)
	if limit != nil {
		limit.Set(float64(c.max))
		used.Set(0)
	}
	c.used = used
	return c
}

// Get returns the bytes cached under sha and marks them recently used.
func (c *Cache) Get(sha [sha256.Size]byte) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.entries[sha]
	if !ok {
		return nil, false
	}
	c.lru.MoveToFront(el)
	e, _ := el.Value.(*entry)
	return e.data, true
}

// Add caches data under sha, evicting least recently used entries to stay
// within budget. The caller must already have verified that data hashes to
// sha. An object larger than the whole budget is not cached, since it would
// evict everything else and still not fit.
func (c *Cache) Add(sha [sha256.Size]byte, data []byte) {
	if c == nil || c.max == 0 || int64(len(data)) > c.max {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[sha]; ok {
		c.lru.MoveToFront(el)
		return
	}
	c.entries[sha] = c.lru.PushFront(&entry{sha: sha, data: data})
	c.size += int64(len(data))
	for c.size > c.max {
		old, _ := c.lru.Remove(c.lru.Back()).(*entry)
		delete(c.entries, old.sha)
		c.size -= int64(len(old.data))
	}
	if c.used != nil {
		c.used.Set(float64(c.size))
	}
}

// Size returns the bytes currently cached.
func (c *Cache) Size() int64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.size
}
