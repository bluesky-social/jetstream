package subscribe

import (
	"container/list"
	"sync"

	"github.com/bluesky-social/jetstream/segment"
)

// blockKey identifies one immutable decoded block.
type blockKey struct {
	segIdx     uint64
	checksum   uint64
	blockIdx   uint64
	generation uint64
}

// blockCache is a byte-bounded LRU of decoded blocks shared by all cold-path
// subscriber goroutines. Concurrency-safe. Only immutable blocks (sealed +
// active-flushed) are ever keyed; the pending in-memory block is never cached.
//
// Each cached block carries one *Entry per event, built at decode time, so
// every cold subscriber replaying a resident block shares the same entries —
// and therefore the same memoized JSON encodes and compressed frames — the
// way hot (read-log) subscribers do (#295). Entries charge their
// lazily-memoized bodies back to the cache budget via Entry.grow, so a cold
// zstd replay storm cannot silently inflate the cache past maxBytes.
type blockCache struct {
	mu       sync.Mutex
	maxBytes int
	curBytes int
	ll       *list.List // front = most recently used
	items    map[blockKey]*list.Element

	generationBySegment map[uint64]uint64

	// Single-flight: in-flight decodes keyed by blockKey. Each inflight holds
	// the decode result or error, populated by the first goroutine to hit the
	// key, and a completion channel closed when decode finishes.
	inFlight map[blockKey]*inflight
}

type blockCacheItem struct {
	key blockKey
	// entries wrap the decoded events 1:1; entries[i].Event points into the
	// decoded slice, which keeps the whole block alive while any entry is
	// referenced. bytes starts at the raw decoded size and grows as entries
	// memoize wire bodies (see blockCache.grow).
	entries []*Entry
	bytes   int
}

// inflight tracks one in-progress decode so concurrent getOrDecode calls for
// the same key wait for the first decode rather than redundantly decoding.
type inflight struct {
	done    chan struct{}
	entries []*Entry
	err     error
}

func newBlockCache(maxBytes int) *blockCache {
	if maxBytes <= 0 {
		panic("subscribe: blockCache maxBytes must be > 0")
	}
	return &blockCache{
		maxBytes: maxBytes,
		ll:       list.New(),
		items:    make(map[blockKey]*list.Element),
		inFlight: make(map[blockKey]*inflight),

		generationBySegment: make(map[uint64]uint64),
	}
}

func (c *blockCache) keyForBlock(segIdx, checksum uint64, blockIdx int) blockKey {
	c.mu.Lock()
	defer c.mu.Unlock()
	return blockKey{
		segIdx:     segIdx,
		checksum:   checksum,
		blockIdx:   uint64(blockIdx),
		generation: c.generationBySegment[segIdx],
	}
}

func (c *blockCache) invalidateSegment(segIdx uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.generationBySegment[segIdx]++
	for el := c.ll.Front(); el != nil; {
		next := el.Next()
		item := itemOf(el)
		if item.key.segIdx == segIdx {
			c.ll.Remove(el)
			delete(c.items, item.key)
			c.curBytes -= item.bytes
		}
		el = next
	}
}

// getOrDecode returns the cached block's shared entries for key, or runs
// decode, wraps the events in entries, caches the result, and returns it.
// A decode error is propagated and NOT cached. The returned entries are
// shared across every cold subscriber and read-only (their events alias a
// pinned decompressed buffer).
//
// When multiple goroutines concurrently call getOrDecode for the same key,
// only the first runs decode; the others wait for completion and share the
// result. This single-flight ensures a slow zstd decode doesn't serialize
// all readers while guaranteeing exactly-once decode per key.
func (c *blockCache) getOrDecode(key blockKey, decode func() ([]segment.Event, error)) ([]*Entry, error) {
	c.mu.Lock()
	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el)
		entries := itemOf(el).entries
		c.mu.Unlock()
		return entries, nil
	}

	// Check if another goroutine is already decoding this key.
	if inf, ok := c.inFlight[key]; ok {
		c.mu.Unlock()
		<-inf.done // wait for the first goroutine to finish decode
		return inf.entries, inf.err
	}

	// We are the first goroutine for this key: create inflight, unlock, decode.
	inf := &inflight{done: make(chan struct{})}
	c.inFlight[key] = inf
	c.mu.Unlock()

	// Decode outside the main lock so slow zstd doesn't block concurrent reads.
	evs, err := decode()
	if err == nil {
		inf.entries = c.wrapEntries(key, evs)
	}
	inf.err = err
	close(inf.done)

	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.inFlight, key)

	if err != nil {
		// Decode error is not cached; remove inflight so next call retries.
		return nil, err
	}

	if c.generationBySegment[key.segIdx] != key.generation {
		return inf.entries, nil
	}

	// Cache the result if not already present (a concurrent decode could have
	// raced and inserted; last writer wins and both callers get valid blocks).
	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el)
		return itemOf(el).entries, nil
	}
	item := &blockCacheItem{key: key, entries: inf.entries, bytes: blockBytes(evs)}
	el := c.ll.PushFront(item)
	c.items[key] = el
	c.curBytes += item.bytes
	c.evictLocked()
	return inf.entries, nil
}

// wrapEntries builds the shared per-event entries for a freshly decoded
// block, wiring each entry's grow hook back to this cache so memoized wire
// bodies are charged against the byte budget as they materialize.
func (c *blockCache) wrapEntries(key blockKey, evs []segment.Event) []*Entry {
	entries := make([]*Entry, len(evs))
	for i := range evs {
		e := newEntry(&evs[i])
		e.grow = func(delta int) { c.grow(key, delta) }
		entries[i] = e
	}
	return entries
}

// grow charges delta freshly-memoized bytes to the cached block identified
// by key, then enforces the budget. If the block has been evicted or
// invalidated the charge is a no-op: the entries are no longer reachable
// through the cache, so their memo growth dies with the in-flight batches
// that still hold them.
func (c *blockCache) grow(key blockKey, delta int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key]
	if !ok {
		return
	}
	itemOf(el).bytes += delta
	c.curBytes += delta
	c.evictLocked()
}

func (c *blockCache) evictLocked() {
	for c.curBytes > c.maxBytes && c.ll.Len() > 0 {
		el := c.ll.Back()
		item := itemOf(el)
		c.ll.Remove(el)
		delete(c.items, item.key)
		c.curBytes -= item.bytes
	}
}

// itemOf extracts the typed cache item from a list element. The list only
// ever holds *blockCacheItem, so the assertion cannot fail; the comma-ok
// form keeps errcheck satisfied without an unchecked panic path.
func itemOf(el *list.Element) *blockCacheItem {
	item, _ := el.Value.(*blockCacheItem)
	return item
}

func (c *blockCache) bytes() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.curBytes
}

// blockBytes estimates the decoded footprint of a block. Payload plus the
// string fields dominate; a fixed per-event overhead bounds tiny events.
func blockBytes(evs []segment.Event) int {
	total := 0
	for i := range evs {
		e := &evs[i]
		total += len(e.Payload) + len(e.DID) + len(e.Collection) +
			len(e.Rkey) + len(e.Rev) + entryFixedOverhead
	}
	return total
}
